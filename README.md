   >ℹ️&nbsp;&nbsp;SignalFx was acquired by Splunk in October 2019. See [Splunk SignalFx](https://www.splunk.com/en_us/investor-relations/acquisitions/signalfx.html) for more information.
   
   Copyright 2020 Splunk Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

# SignalVortex

A batch <=> streaming bridge for SignalFx

SignalFx has a streaming API that expects callers to start persistent
computations which fire events when new data is available. However
most tools/users of timeseries data expect a synchronous batch API
which they call to retrieve the full resultset each time.

SignalVortex is an AWS Lambda that bridges the two worlds. When 
configured behind an API gateway, it provides an efficient batch
API for SignalFx data. If you're repeatedly querying the same data
with a sliding window (e.g. latest 15 mins) it can lead to 
order-of-magnitude improvements is concurrency and response time.

---

## Table of Contents (Optional)
If your `README` has a lot of info, section headers might be nice.

- [Usage](#usage)
- [Build](#build)
- [Documentation](#documentation)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)


---

## Usage

```sh
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
```
---

## Build
1. Checkout this repo into a directory - e.g. signalvortex
1. ```cd signalvortex```
1. ```python3 -m venv v-env```
1. ```source ./v-env/bin/activate```
1. ```pip3 install -r requirements.txt```
1. ```sh ./make_zip_archive.sh``` # this will produce signalvortex.zip in current directory

## Documentation
### PURPOSE
An efficient 'batch' API for signalfx (which provides streaming APIs) implemented as a lambda function. Inputs are
- signalflow program
- start and optionally end time (negative values are relative to 'now'. E.g. start=-600000 shows last 10 mins)
- resolution hint
You can also configure the signalfx api token and endpoint to use - making a single deployment of this lambda able to
provide a batch api for any SignalFx deployment.

### EFFICIENCY
Efficiency is achieved by starting a background "backing" job when a user invokes this API.
The backing job continues to run and stores results in cache. Subsequently all to the same API will serve results from
this cache rather than starting another SignalFx analytics job. Each API invocation checks and starts a backing job
if necessary. Empirically this is about 10-20x faster than calling the SignalFx streaming api each time.

### MULTIPLE MODES AND BACKING JOBS
The same lambda can work in both 'daemon mode' (i.e. backing job) or api mode (which returns results to user). This is
done primarily for simplicity - e.g. so a lambda run in api mode can easily recursively invoke another copy of itself
in daemon mode. You can always create two lambdas with this same code in order to independently configure concurrency of
api calls and backing jobs independently.

Backing jobs cannot run for longer than lambda max timeout (which is currently 15 minutes in AWS).
Liveliness of backing job is determined by a 'keepalive' cache key that it updates periodically. If it is absent, or
the last keepalive is old, then a new backing job is issued.

Once a backing job nears its max age, it automatically re-issues itself recursively if the data it is producing
has been queried recently. This is done by having a 'last queried time' cache key that is updated each time a user
calls this API.

### SUPPORTED CACHES
Caches supported are Elasticache(memcached) and local filesystem (for testing purposes). If using memcached (recommended)
you need to configure the discovery host:port in the lambda environment ("cache_url") or in API calls as a parameter.
Cache timeout is configured to be similar to the max lifetime of a backing job (15 mins)

### SYNCHRONIZATION
Synchronization is achieved through cache to prevent multiple copies of the exact same backing job.
For e.g. backing job A checks the keepalive key in cache to determine if another identical instance B
owns it and is still running. If so A will exit. Otherwise, A will claim the key and continue to run. Claiming the key
is done by including the lambda invocation id inside the keepalive payload.

### DATA FRESHNESS AND BACKFILL
Streaming jobs never show results of backfilled data which can be a problem is some situations. This implementation
takes a middle-of-the-road approach to handling backfill which is better than streaming, and worse than pure batch.
Since the backing job cannot run longer than lambda max timeout (15 mins), users are guaranteed to see impact of
backfill data within than timerange because a new backfill job will be issued after the current one dies and that
job will query the full width of the chart/api call's timerange before going into streaming mode. As a result, effects
of backfill will show up in the results within 15 minutes or less.

### RELIABILITY OF RESULTS
Caches are inherently not durable, and may drop cached data any time. To address that, backing jobs run a periodic
health check where they query their own data to determine if there are any gaps (it maintains a list of timestamps
for which data was received inside the keepalive payload). If two consecutive health checks encounter the same gaps,
then the backing job exits in order to trigger a new one next time the API is invoked. The healthcheck is performed by
the backing job instead of during API calls in order to reduce the number of backing
job restarts - otherwise being too "trigger happy" may reissue too many backing
jobs unnecessarily
* There may be transient cache errors which should not trigger a new backing job. The API path needs to be
as fast as possible and cannot wait for transient issues to time out
* Multiple concurrent APIs should not all try to restart the same backing job

---

## Deployment

### Basic
1. Modify any settings in defaults.py if you want
1. Upload and deploy signalvortex.zip as an AWS Lambda - say SignalVortexLambda
1. Setup a memcached cluster (e.g. ElastiCache) and 
1. Make sure Lambda and Memcache cluster can talk to each other
    * Configure SignalVortexLambda's environment variable ```cache_url``` with host:port of memcached cluster
1. Define an API Gateway REST API to trigger the Lambda
    * Parameters to be passed in are documented above
1. In the Lambda settings, 
    * Set the timeout of the Lambda to be the maximum allowed (15 minutes)
        * This is so that the backing jobs run as long as possible before being reissued
    * Set the concurrency to some limit (e.g. 50, 100) to control how many backing jobs + query jobs can be running at the same time

### Better
You can alternatively create two Lambdas from the same code instead of one. This will allow you to independently control concurrency of backing jobs and API calls. Backing jobs are a measure of 'load' on SignalFx, so being able to control that independently is quite desirable.
* One for running backing jobs only (with a as-long-as-possible timeout)
    * Note that you should slightly overprovision concurrency limit. E.g. if you want at most 300 backing jobs (which is equivalent to 30 different dashboards with 10 chars each), configure backing jobs lambda concurrency an extra 10-15% - e.g. set of 330. This is because SignalVortex uses memcache for synchronization which is simple (no extra services to use/deploy) but error prone. Multiple calls to the same API (e.g. simulataneous loads of the same dashboard by different users) might start multiple identical backing jobs. All of them except for one will kill themselves within a few seconds by detecting this race, but it still means that there may be a few extra backing jobs for a short interval. It is good to have a buffer for such occurrences. 
* One for running queries (triggered by API gateway) only with a shorter timeout 

This will require some minor code changes to pass in an optional ARN of the 'backing Lambda' to the 'query Lambda'. This is left as an exercise to the reader.

---

## Contributing

Go ahead!

---

## FAQ

---

## Support

---

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

- **[Apache 2.0 License](https://github.com/arijitmukherji/SignalVortex/blob/master/LICENSE)**
