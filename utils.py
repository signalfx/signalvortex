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

import time


def millitime():
    return int(time.time() * 1000)


def rounding_factor(n):
    ''' Jobs "durations" are adjusted up to round numbers. E.g. if user selects last 45s, then we'll run the job
    over the last 1-min. This way, memcached data will match more jobs with similar but not exact durations '''
    milestones = [0, 60000, 300000, 3600000, 86400000, 86400000 * 7, 86400000 * 28, 86400000 * 365, 86400000 * 365 * 100, float('inf')]
    for i in range(len(milestones)):
        if n < milestones[i]:
            return milestones[i - 1]
