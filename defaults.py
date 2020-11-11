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

''' Various defaults used in this program '''

class defaults(object):
    API_TIMEOUT_MS = 30000 # API timeout when invoked by callers
    API_URL = "https://api.signalfx.com"

    BACKING_JOB_LIFETIME_MS = 900000    # Lambda max lifetime is 15 minutes
    BACKING_JOB_SHUTDOWN_MS = 10000 # When backing job is shutting down, wait this long to process remaining messages

    KEEPALIVE_INTERVAL_SEC = 1 # wait this many seconds between keepalive iterations
    KEEPALIVE_EXPIRY_MULTIPLE = 3.5 # a keeplive is considered expired if older this this many keepalive intervals
    KEEPALIVE_PRINT_EVERY = 15 # print status of keepalive thread every N iterations

    HEALTHCHECK_INTERVAL_SEC = 10 # wait this many seconds between healthcheck iterations
    HEALTHCHECK_EXIT_ERRORMULTIPLE = 3 # if this many consecutive healthchecks fail, then exit backing job

    STREAM_PHASE_DETECTION_INTERVAL_MS = 700 # if data messages get paused for this long, means job entered stream phase

    RETRIGGER_BEFORE_EXPIRY_MS = 60000  # re-trigger backing job when this much time is left before lambda expires
