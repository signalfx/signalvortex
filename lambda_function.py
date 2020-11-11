#   Copyright 2020 Splunk Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import argparse
from aws_lambda_context import LambdaContext
from caches import Cache, FileCache, MemCache, ReliableCache
from defaults import defaults
import elasticache_auto_discovery
import inputs
import json
import os
import pickle
import pprint
from queue import Queue
import signalfx
import textwrap
import threading
import traceback
import sched
import time
import utils


#################################################

class KeepaliveState(object):
    ''' State that is written to cache as the 'keepalive' by a backing job '''
    def __init__(self, params: inputs.Inputs, id: str, resolution_ms: int):
        self.id = id
        self.cache_keys = CacheKeys(params.cache_key_prefix())
        self.last_keepalive_ms = 0
        self.data_timestamps = set()
        self.resolution_ms = resolution_ms
        self.in_streaming_phase = False

    def to_dict(self):
        return {
            "id": self.id,
            "cache_keys": self.cache_keys.to_dict(),
            "last_keepalive_ms": time.ctime(self.last_keepalive_ms / 1000),
            "resolution_ms": self.resolution_ms,
            "data_timestamps": sorted(self.data_timestamps),
            "in_streaming_phase": self.in_streaming_phase,
        }

    def __str__(self):
        return str(self.to_dict())


#################################################

def update_keepalive(params: inputs.Inputs, keepalive_state: KeepaliveState, cache: Cache):
    ''' Update the keepalive state in cache. Also check if the current backing job owns the keepalive. If not, exit '''
    try:
        cache_keys = keepalive_state.cache_keys
        exit_if_necessary(keepalive_state, cache)
        keepalive_state.last_keepalive_ms = utils.millitime()
        cache.set(cache_keys.keepalive, pickle.dumps(keepalive_state))
    except Exception as e:
        print("update_keepalive: exception", e, traceback.format_exc())


def keepalive_fn(scheduler: sched.scheduler, params: inputs.Inputs, context: LambdaContext,
                 keepalive_state: KeepaliveState, cache: Cache):
    ''' Each iteration of keepalive_thread runs this code. Add the next iteration of keepalive before exiting to
    continue the keepalive thread. Otherwise keepalives will stop '''
    try:
        update_keepalive(params, keepalive_state, cache)
        keepalive_fn.num_keepalives += 1
        if keepalive_fn.num_keepalives % defaults.KEEPALIVE_PRINT_EVERY == 0:
            print("keepalive_fn: keepalive #{}: state={}".format(keepalive_fn.num_keepalives, keepalive_state))

        if context.invoked_function_arn and context.get_remaining_time_in_millis() < defaults.RETRIGGER_BEFORE_EXPIRY_MS:
            # if invoked as lambda (not CLI), then retrigger backing job if this instance of it will expire soon
            cache_keys = keepalive_state.cache_keys
            lastaccess_ms = int(cache.get(cache_keys.lastaccess))
            lastaccess_age_ms = utils.millitime() - lastaccess_ms

            if lastaccess_age_ms > (defaults.BACKING_JOB_LIFETIME_MS * 0.9):
                # There were no recent calls to fetch the data produced by this backing job. No need to re-issue
                print("Exiting backing job by ending keepalive thread. lastaccess_age_ms = ", lastaccess_age_ms)
                return False

            if not params.is_streaming():
                ''' Fixed time-range jobs need not be reissued '''
                print("keepalive_fn: backing job won't be restarted because it is not a streaming job", params)
                return False

            # Restart this job again in another lambda invocation.
            # Before doing that, don't keepalive for a while to make it stale. Otherwise the new invocation
            # will assume there is another backing job already running and will auto-exit
            print("keepalive_fn: backing job needs to be restarted. lastaccess_age_ms =", lastaccess_age_ms)
            time.sleep(defaults.KEEPALIVE_INTERVAL_SEC * defaults.KEEPALIVE_EXPIRY_MULTIPLE)
            start_backing_job_if_necessary(params, context, keepalive_state, cache)
            print("keepalive_fn: exiting current backing job after re-issuing a new one")
            return False
    except Exception as e:
        print("keepalive_fn: exception", e, traceback.format_exc())

    # schedule the next iteration of keepalive thread
    scheduler.enter(defaults.KEEPALIVE_INTERVAL_SEC, 1, keepalive_fn,
                    argument=(scheduler, params, context, keepalive_state, cache))
keepalive_fn.num_keepalives = 0


def keepalive_thread_fn(params: inputs.Inputs, context: LambdaContext,
                        data_queue: Queue, metadata_queue: Queue,
                        keepalive_state: KeepaliveState, cache: Cache):
    ''' This lambda periodically updates a keepalive key in cache which other
    invocations can query to detect if a backing job is running. The cache payload for the key also
    also lists the other cache keys (for metadata and data) related to this job'''
    try:
        print("keepalive_thread_fn: started", keepalive_state)
        update_keepalive(params, keepalive_state, cache)

        # start threads to consume data and metadata messages from job
        threading.Thread(target=data_consumer_thread_fn,
                         args=(params, context, data_queue, keepalive_state, cache)).start()
        threading.Thread(target=metadata_consumer_thread_fn, args=(metadata_queue, keepalive_state, cache)).start()

        scheduler = sched.scheduler(time.time, time.sleep)
        scheduler.enter(defaults.KEEPALIVE_INTERVAL_SEC, 1, keepalive_fn,
                        argument=(scheduler, params, context, keepalive_state, cache))
        scheduler.run(blocking=True)  # keepalive_fn will enqueue future runs automatically until we need to exit
    except Exception as e:
        print("keepalive_thread_fn: exception", e, traceback.format_exc())
    finally:
        print("keepalive_thread_fn: thread ended")
        os._exit(1)


#################################################

def healthcheck_thread_fn(params: inputs.Inputs, context: LambdaContext, keepalive_state: KeepaliveState,
                          cache: Cache):
    ''' In backing job mode, this lambda function stores data in memcache which may drop/expire data any time.
    If that happens, it will cause gaps in charts when users view the data, and those won't go away until job
    times out and is restarted (usually every 15 mins or so).
    This thread queries its own data (like a read API would) and if it finds gaps, then exits the job so that a new
    instance is started which will fill in the gaps by republishing the whole dataset '''

    def healthcheck_fn(scheduler: sched.scheduler, params: inputs.Inputs, context: LambdaContext,
                       keepalive_state: KeepaliveState, cache: Cache):
        ''' Code that is executed each time healthcheck is performed. Schdules the next run before returning, otherwise
         healthchecks will be stopped '''
        try:
            cached_result = get_cached_result(params, context, cache)
            cache_misses = set(cached_result["missing_timestamps_ms"])
            if len(cache_misses):
                print("healthcheck_fn: {} cache misses {}".format(len(cache_misses), sorted(cache_misses)))
            consecutive_misses = cache_misses.difference(healthcheck_fn.previous_cache_misses)
            if len(consecutive_misses):
                # The same data keys could not be fetched twice in a row in consecutive healthcheck runs
                print("healthcheck_fn: exiting backing job to trigger restart due to {} consecutive cache misses".
                      format(len(consecutive_misses), sorted(consecutive_misses)))
                return False
            healthcheck_fn.previous_cache_misses = cache_misses
            healthcheck_fn.consecutive_errors = 0
        except Exception as e:
            print("healthcheck_fn: exception", e, traceback.format_exc())
            healthcheck_fn.consecutive_errors += 1
            if healthcheck_fn.consecutive_errors >= defaults.HEALTHCHECK_EXIT_ERRORMULTIPLE:
                print("healthcheck_fn: exiting due to too many consecutive errors", healthcheck_fn.consecutive_errors)
                return False
        # schedule the next iteration of healthcheck thread
        scheduler.enter(defaults.HEALTHCHECK_INTERVAL_SEC, 1, healthcheck_fn,
                        argument=(scheduler, params, context, keepalive_state, cache))
    healthcheck_fn.previous_cache_misses = set()
    healthcheck_fn.consecutive_errors = 0

    try:
        print("healthcheck_thread_fn: started", keepalive_state)

        scheduler = sched.scheduler(time.time, time.sleep)
        scheduler.enter(defaults.KEEPALIVE_INTERVAL_SEC, 1, healthcheck_fn,
                        argument=(scheduler, params, context, keepalive_state, cache))
        scheduler.run(blocking=True)  # healthcheck_fn will enqueue future runs automatically until we need to exit
    except Exception as e:
        print("healthcheck_fn: exception", e, traceback.format_exc())
    finally:
        print("healthcheck_fn: thread ended")
        os._exit(1)


#################################################

def data_consumer_thread_fn(params: inputs.Inputs, context: LambdaContext, data_queue: Queue,
                            keepalive_state: KeepaliveState, cache: Cache):
    ''' Thread that consumes data messages from analytics job, and sticks each one individually into cache.
     Also detects when job moves from batch to stream phase. Unfortunately that requires 'auto-detection' where
     data does not arrive for close to a second :-( '''
    print("data_consumer_thread_fn: started")
    try:
        cache_keys = keepalive_state.cache_keys
        data_to_encache = {}
        last_datamsg_walltime_ms = 0
        while True:
            now_ms = utils.millitime()
            try:
                if params.is_streaming():
                    # remove trailing data keys that are beyond the scope of the current 'window' of a streaming job
                    valid_timestamps = [ts for ts in keepalive_state.data_timestamps if ts >= (now_ms - params.job_duration_ms() - keepalive_state.resolution_ms)]
                    keepalive_state.data_timestamps = set(valid_timestamps)

                msg = data_queue.get(False)
                last_datamsg_walltime_ms = utils.millitime()
                data_to_encache.setdefault(msg.logical_timestamp_ms, {})
                data_to_encache[msg.logical_timestamp_ms].update(msg.data)
            except Exception as e:
                # No data found in queue. However there may be pending data from previous messages that need caching
                timestamps_encached = set()
                for timestamp, values in data_to_encache.items():
                    try:
                        cache.set(cache_keys.data_prefix + str(timestamp), values)
                        timestamps_encached.add(timestamp)
                    except Exception as e:
                        # Failed to set data in cache
                        None
                for timestamp_encached in timestamps_encached:
                    data_to_encache.pop(timestamp_encached)
                    keepalive_state.data_timestamps.add(timestamp_encached)
                if data_to_encache:
                    print("data_consumer_thread_fn: will retry writing {} data keys to cache {}".format(
                        len(data_to_encache), list(data_to_encache)))
                elif not keepalive_state.in_streaming_phase:
                    # Now that all data is successfully published, 'Auto-detect' whether we have completed batch phase
                    # and entered stream phase. If so, update keepalive_state
                    if last_datamsg_walltime_ms > 0 and (
                            now_ms - last_datamsg_walltime_ms) > defaults.STREAM_PHASE_DETECTION_INTERVAL_MS:
                        keepalive_state.in_streaming_phase = True
                        print("data_consumer_thread_fn: backing job entered stream phase after {} datapoints. now={}, last={}".format(
                            len(keepalive_state.data_timestamps), now_ms, last_datamsg_walltime_ms))
                        # start healthcheck thread now that data is flowing in
                        threading.Thread(target=healthcheck_thread_fn,
                                         args=(params, context, keepalive_state, cache)).start()

                time.sleep(defaults.STREAM_PHASE_DETECTION_INTERVAL_MS / 1000 / 5)
    except Exception as e:
        print("data_consumer_thread_fn exception", e, traceback.format_exc())
    finally:
        print("data_consumer_thread_fn: ended")


#################################################

def metadata_consumer_thread_fn(metadata_queue: Queue, keepalive_state: KeepaliveState, cache: Cache):
    ''' Thread that consumes metadata messages, updates full metadata in cache '''
    print("metadata_consumer_thread_fn: started")
    try:
        cache_keys = keepalive_state.cache_keys
        metadata = {}
        while (True):
            try:
                publish = False
                while (not metadata_queue.empty()):
                    msg = metadata_queue.get()
                    metadata[msg.tsid] = msg.properties
                    publish = True
                if publish:
                    cache.set(cache_keys.metadata, metadata)
            except Exception as e:
                print("metadata_consumer_thread_fn: exception", e, traceback.format_exc())
            time.sleep(1.0)
    finally:
        print("metadata_consumer_thread_fn: ended")


#################################################


def exit_if_necessary(keepalive_state: KeepaliveState, cache: Cache):
    ''' if backing job ever discovers that another instance of the same thing is currently running and owns the
    keepalive key in cache, then it exits '''
    cache_keys = keepalive_state.cache_keys
    try:
        cached_state: KeepaliveState = pickle.loads(cache.get(cache_keys.keepalive))
        if cached_state.id != keepalive_state.id:
            expiry_ms = defaults.KEEPALIVE_EXPIRY_MULTIPLE * defaults.KEEPALIVE_INTERVAL_SEC * 1000
            if utils.millitime() - cached_state.last_keepalive_ms < expiry_ms:
                # Another backing job is running, and it has published a keepalive recently
                print("exit_if_necessary: exiting because another instance already running",
                      cached_state.id, time.ctime(cached_state.last_keepalive_ms / 1000))
                os._exit(1)
    except Exception as e:
        print("exit_if_necessary: failed to read keepalive from cache", e)


#################################################

def start_backing_job_if_necessary(params: inputs.Inputs, context: LambdaContext, cache: Cache):
    ''' If no backing job is running for a given signalflow program and duration, start one
    Returns keepalive_state from cache if active backing job is found (to prevent a duplicate cache read by callers '''

    def start_backing_job_as_lambda(params: inputs.Inputs, tstart, tend, context: LambdaContext):
        # Start new backing job that runs as a lambda function
        print("start_backing_job_as_lambda: started")
        import boto3
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName=context.invoked_function_arn,
            InvocationType='Event',
            Payload=json.dumps({
                "program": params.program,
                "start_time_ms": tstart,
                "end_time_ms": tend,
                "resolution_hint_ms": params.resolution_hint_ms,
                "api_token": params.api_token,
                "api_endpoint": params.api_endpoint,
                "daemon": True
            })
        )

    def start_backing_job_as_process(params: inputs.Inputs, tstart, tend):
        # Start new backing job that runs as a python process
        print("start_backing_job_as_process: started")
        cmd: str = "nohup python3 {script} --program=\"{program}\" --token={token} \
                    --start_time_ms={tstart} --end_time_ms={tend} --resolution_hint_ms={res} --endpoint={endpoint}".format(
            script=__file__, program=params.program, tstart=tstart, tend=tend,
            res=params.resolution_hint_ms, token=params.api_token, endpoint=params.api_endpoint)
        cmd += " --daemon > /tmp/{}.log 2>&1 &".format(params.cache_key_prefix())
        print("start_backing_job_as_process:", cmd)
        os.system(cmd)

    # begin code for start_backing_job_if_necessary()
    try:
        cache_keys = CacheKeys(params.cache_key_prefix())
        print("start_backing_job_if_necessary: started", cache_keys)
        now_ms = utils.millitime()
        cached_state: KeepaliveState = pickle.loads(cache.get(cache_keys.keepalive))
        keepalive_age_ms = now_ms - cached_state.last_keepalive_ms
        expiry_ms = defaults.KEEPALIVE_EXPIRY_MULTIPLE * defaults.KEEPALIVE_INTERVAL_SEC * 1000

        if keepalive_age_ms < expiry_ms:
            print("start_backing_job_if_necessary: found active backing job already running. keepalive_age_ms =",
                  keepalive_age_ms)
            return cached_state

        print("start_backing_job_if_necessary: found expired keepalive_age_ms =", keepalive_age_ms)
        cache.set(cache_keys.keepalive, None)
    except Exception as e:
        print("start_backing_job_if_necessary: no keeplive found in cache", e)

    tstart = params.start_time_ms
    tend = params.end_time_ms
    if not params.is_streaming():
        tstart = params.absolute_ms(tstart)
        tend = params.absolute_ms(tend)

    if context.invoked_function_arn:
        # This backing job was invoked as a lambda. So invoke a new lambda
        start_backing_job_as_lambda(params, tstart, tend, context)
    else:
        start_backing_job_as_process(params, tstart, tend)

    return None


#################################################

class CacheKeys(object):
    def __init__(self, prefix):
        self.keepalive = "{}-keepalive".format(prefix)
        self.data_prefix = "{}-data-".format(prefix)
        self.metadata = "{}-metadata".format(prefix)
        self.lastaccess = "{}-lastaccess".format(prefix)

    def to_dict(self):
        return {
            "keepalive": self.keepalive,
            "data_prefix": self.data_prefix,
            "metadata": self.metadata,
            "lastaccess": self.lastaccess
        }

    def __str__(self):
        return str(self.to_dict())


#################################################

def run_as_backing_job(params: inputs.Inputs, context: LambdaContext, cache: Cache):
    ''' Run in backing job mode, which starts the signalflow program, and puts the received data/metadata into cache '''
    try:
        sfx = signalfx.SignalFx(api_endpoint=params.api_endpoint,
                                ingest_endpoint=params.api_endpoint,
                                stream_endpoint=params.api_endpoint)

        data_queue = Queue(maxsize=100000)
        metadata_queue = Queue(maxsize=100000)

        with sfx.signalflow(params.api_token) as flow:
            print('run_as_backing_job: executing backing job. duration={}-{} program={}'.format(
                time.ctime(params.job_start_ms() / 1000), time.ctime(params.job_end_ms() / 1000), params.program))
            computation = flow.execute(params.program, start=params.job_start_ms(), stop=params.job_end_ms(),
                                       resolution=params.resolution_hint_ms, max_delay=None, persistent=False,
                                       immediate=False, disable_all_metric_publishes=None)

            print("run_as_backing_job: waiting for messages ...")
            job_resolution = 0
            for msg in computation.stream():
                if isinstance(msg, signalfx.signalflow.messages.DataMessage):
                    data_queue.put(msg)
                elif isinstance(msg, signalfx.signalflow.messages.ControlMessage):
                    None
                elif isinstance(msg, signalfx.signalflow.messages.MetadataMessage):
                    if job_resolution < 1:
                        job_resolution = msg.properties["sf_resolutionMs"]
                        print("run_as_backing_job: job resolution_ms = ", job_resolution)
                        keepalive_state = KeepaliveState(params, context.aws_request_id, job_resolution)
                        # start keepalive thread now that we know job resolution
                        keepalive_thread = threading.Thread(
                            target=keepalive_thread_fn,
                            args=(params, context, data_queue, metadata_queue, keepalive_state, cache))
                        keepalive_thread.start()
                    metadata_queue.put(msg)
                else:
                    None
                    # print('run_as_backing_job: dequeued message {0}: {1}'.format(msg, msg.__dict__))
            print("run_as_backing_job: received last message from job. exiting ...")
            # give time for enqueued messages to be processed by other threads and memcache states to be updated before exiting
            time.sleep(defaults.BACKING_JOB_SHUTDOWN_MS / 1000)
    except Exception as e:
        print("run_as_backing_job: exception", e, traceback.format_exc())
    finally:
        print("run_as_backing_job: ended")


#################################################

def get_cached_result(params: inputs.Inputs, context: LambdaContext, cache: Cache):
    ''' Backing job is already running. So just query cached data from and return result '''

    def wait_for_backing_job_to_exit_batch_phase(keepalive_state: KeepaliveState, cache: Cache,
                                                 cache_keys: CacheKeys, wait_until_ms: int):
        print("wait_for_backing_job_to_exit_batch_phase: started", cache_keys.keepalive)
        while not keepalive_state or not keepalive_state.in_streaming_phase:
            # wait for backing job to be running and advance to streaming state
            if utils.millitime() > wait_until_ms:
                raise Exception("wait_for_backing_job_to_exit_batch_phase: timed out")
            print("get_cached_result: waiting for batch phase to end. keepalive_state=", keepalive_state)
            time.sleep(1)
            try:
                keepalive_state: KeepaliveState = pickle.loads(cache.get(cache_keys.keepalive))
            except Exception as e:
                print("wait_for_backing_job_to_exit_batch_phase: failed to read keepalive from cache",
                      cache_keys.keepalive, e)
        print("wait_for_backing_job_to_exit_batch_phase: backing job is ready", keepalive_state)
        return keepalive_state

    print("get_cached_result: started")

    # Update 'lastaccess' timestamp in memcache to indicate the corresponding backing job's data was recently queried
    cache_keys: CacheKeys = CacheKeys(params.cache_key_prefix())
    now_ms = params.invoke_time_ms
    try:
        cache.set(cache_keys.lastaccess, now_ms)
    except Exception as e:
        print("get_cached_result: failed to set lastaccess cache key {}={}, {}".format(
            cache_keys.lastaccess, now_ms, e))

    # start the backing job if one is not running, or if the backing job's keepalive timestamp is stale
    keepalive_state: KeepaliveState = start_backing_job_if_necessary(params, context, cache)

    # now that backing job is surely running, wait for it to become 'ready' - i.e. go from batch to streaming phase
    keepalive_state = wait_for_backing_job_to_exit_batch_phase(keepalive_state, cache, cache_keys,
                                                               now_ms + defaults.API_TIMEOUT_MS)

    # compute which cache keys need to be fetched
    if not params.is_streaming():
        tstart = params.absolute_ms(params.start_time_ms)
        tend = params.absolute_ms(params.end_time_ms)
    else:
        tend = now_ms
        tstart = tend - params.duration_ms()

    timestamps = sorted([ts for ts in keepalive_state.data_timestamps if ts >= tstart and ts <= tend])
    data_keys = [cache_keys.data_prefix + str(ts) for ts in timestamps]

    # retrieve metadata and data from cache. retry if necessary
    metadata = cache.get(cache_keys.metadata)
    if len(timestamps):
        print("get_cached_result: fetching {} timestamps {} - {} @ {}ms".format(len(timestamps),
            time.ctime(timestamps[0] / 1000), time.ctime(timestamps[-1] / 1000), keepalive_state.resolution_ms))
    data = cache.multiget(data_keys)
    missing_keys = set(data_keys) - set(data.keys())
    if (len(missing_keys)):
        print("get_cached_result: retrying fetch of {}/{} keys: {}".format(len(missing_keys), len(data_keys), sorted(missing_keys)))
        data.update(cache.multiget(list(missing_keys)))

    # Fill in results in results struct
    result = {
        "start_time_ms": tstart,
        "end_time_ms": tend,
        "earliest_result_ms": 0,
        "latest_result_ms": 0,
        "resolution_ms": keepalive_state.resolution_ms,
        "metadata": metadata,
        "data": {},
        "missing_timestamps_ms": []
    }

    # First fill in retrieved data
    tsids = set()
    missing_timestamps = []
    for timestamp in timestamps:
        k = cache_keys.data_prefix + str(timestamp)
        if k in data.keys():
            for tsid, value in data[k].items():
                if not result["earliest_result_ms"]:
                    result["earliest_result_ms"] = timestamp
                if timestamp > result["latest_result_ms"]:
                    result["latest_result_ms"] = timestamp;
                tsids.add(tsid)
                result["data"].setdefault(tsid, [])
                result["data"][tsid].append([timestamp, value])
        else:
            missing_timestamps.append(timestamp)

    # Second, fill in metadata of only the relevant mts that have data
    remove_metadata_ids = set(metadata.keys()).difference(tsids)
    for tsid in remove_metadata_ids:
        metadata.pop(tsid)

    result["missing_timestamps_ms"] = missing_timestamps;
    return result


#################################################

def lambda_handler(event: dict, context: LambdaContext):
    ''' Lambda entry point function invoked by AWS '''

    print("lambda_handler: started. event={}, context={}".format(event, vars(context)))
    result = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {},
        "multiValueHeaders": {},
        "body": ""
    }
    try:
        params: inputs.Inputs = inputs.Inputs()
        if "body" in event:
            params.parse_http_body(event)
        else:
            params.parse_event(event)
        print("lambda_handler: params =", params)

        # setup cache
        cache_keys: CacheKeys = CacheKeys(params.cache_key_prefix())
        print("cache keys", cache_keys)
        if params.cache_type() == inputs.Cachetype.FILE:
            print("Using FileCache")
            cache = FileCache("/tmp")
        else:
            key_expiry_secs = int(defaults.BACKING_JOB_LIFETIME_MS / 1000)  # for as long as the backing jobs run
            print("Using MemCache:", params.cache_location, key_expiry_secs)
            memcache = MemCache(params.cache_location, key_expiry_secs)
            cache = ReliableCache(memcache, 3, 100, 1000)

        # run in backing job mode or api mode
        if params.is_daemon:
            print("lambda_handler: running as backing job")
            run_as_backing_job(params, context, cache)
        else:
            print("lambda_handler: returning cached results")
            cached_result = get_cached_result(params, context, cache)
            result["body"] = json.dumps(cached_result)

    except Exception as ex:
        print("lambda_handler: exception", ex, traceback.format_exc())
        result["statusCode"] = 500
        result["body"] = str(ex) + "\n" + get_usage()

    if not context.invoked_function_arn:
        print(result["statusCode"])
        try:
            pprint.pprint(json.loads(result["body"]))
        except:
            print(result["body"])
    return result


#################################################

def get_usage():
    ''' returns description of inputs/outputs '''
    return '''
SignalVortex: a streaming=>batch bridge to make efficient batch calls to SignalFx to fetch metrics data. 

Automatically starts persistent backing jobs in the background, caches results into memcache, serves subsequent calls
from cache without starting new jobs.
 
Usage: 
    curl -H 'X-SF-TOKEN: <sfx-token>' -X POST '<this_url>' -d '<bodyJSON>'
    where <sfx-token> is
        - single token value as string. E.g. "abc123"
        - multiple named tokens specific in JSON. Any unnamed token is used as default
            - E.g. '[{"prod":["abc123", "https://api.signalfx.com"]}, {"mon":["xyz123", "https://mon.signalfx.com"]}, "unnamed123", {"mon2":["XYZ123", "https://mon.signalfx.com"]}]'
    where '<bodyJSON>' is of the format
    {
        "program"       : str, # signalflow program. Prefix with "<token_name>|" to select token/endpoint from above. E.g. "mon2|data(cpu).publish()"
        "start_time_ms" : int, # negative values are relative to 'now'
        "end_time_ms"   : int, # optional. negative values are relative to 'now'
        "resolution_hint_ms": int # optional. minimum resolution of output data
        "api_endpoint"   : string: optional. override default api endpoint to use instead of https://api.signalfx.com) 
    }
        
Result is JSON of the format:
{
    "start_time_ms"      : int, # 'start time' of data that was queried
    "end_time_ms"        : int, # 'end time' of data that was queried
    "earliest_result_ms" : int, # earliest timestamp among all datapoints in the result
    "latest_result_ms"   : int, # latest   timestamp among all datapoints in the result
    "resolution_ms"      : int, # job resolution  
    "metadata"           : {         # properties of all timeseries IDs in the result       
      "tsid1": {"key":"value", "key":"value", ...} # metadata of timeseries
      "tsid2": ...
      ...
    },
    "data": {
        "tsid1": [[timestamp1:value1], [timestamp2, value2], ...],
        "tsid2": ...
        ...
    },
    "missing_timestamps": [timestamp1, timestamp2, ...]   # timestamps of cache misses. should be empty otherwise indicates some errors
}
    '''


''' If invoked from script, use file cache and default parameters '''
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent(
                """\
                Run a SignalFlow query ending at 'now - maxdelay' and returns results immediately.
                If run in 'daemon' mode, starts a persistent SignalFlow computation and keeps streaming results into cache
                If not in daemon mode, it reads results from cache, and returns to user
                """
            ),
            epilog=textwrap.dedent(
                """\
                EXAMPLE:
                - lambda_function.py --query='data(cpu.utilization)' --duration=<duration_in_seconds> [--daemon]
                """
            ),
        )

        # useful for copying into lambda test event in AWS console

        parser.add_argument("--program", required=False, help="signalflow program",
                            default="data('cpu.utilization').count().publish()")
        parser.add_argument("--start_time_ms", required=False, default=None, help="duration in milliseconds")
        parser.add_argument("--end_time_ms", required=False, default=None, help="duration in milliseconds")
        parser.add_argument("--resolution_hint_ms", required=False, default=0,
                            help="resolution hint (minimum) in milliseconds")
        parser.add_argument("--endpoint", required=False, help="api endpoint")
        parser.add_argument("--token", required=False, help="api token")
        parser.add_argument("--cache_url", required=False, default="",
                            help="memcache configuration url is host:port form")
        parser.add_argument("--daemon", action="store_true")
        args = parser.parse_args()

        test_event = {
            "program": args.program,
            "api_token": args.token,
            "cache_url": args.cache_url,
            "daemon": args.daemon
        }
        if args.endpoint:
            test_event["api_endpoint"] = args.endpoint
        if args.start_time_ms:
            test_event["start_time_ms"] = args.start_time_ms
        if args.end_time_ms:
            test_event["end_time_ms"] = args.end_time_ms
        if args.resolution_hint_ms:
            test_event["resolution_hint_ms"] = args.resolution_hint_ms
        context = LambdaContext()
        context.aws_request_id = utils.millitime()
        context.invoked_function_arn = None
        lambda_handler(test_event, context)

    except Exception as e:
        print("Exception", e, traceback.format_exc())
        os._exit(1)
