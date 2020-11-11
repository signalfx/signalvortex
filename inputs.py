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

from defaults import defaults
from enum import Enum
import hashlib
import json
import os
import re
import time
import utils

class Cachetype(Enum):
    FILE = 1
    MEMCACHE = 2

class Inputs(object):
    ''' Data structure containing the various input parameters '''

    def __init__(self):
        self.invoke_time_ms = utils.millitime()
        self.cache_location = os.environ.get("cache_url", None)
        self.api_token = None
        self.api_endpoint = os.environ.get("api_endpoint", "https://api.signalfx.com") # SignalFx API endpoint
        self.program = None # signalflow program
        self.start_time_ms = None
        self.end_time_ms = 0
        self.resolution_hint_ms = 0
        self.is_daemon = False

    def __str__(self):
        return str({
            "invoke_time_ms": time.ctime(self.invoke_time_ms/1000),
            "cache_type": self.cache_type(),
            "cache_location": self.cache_location,
            "cache_key_prefix": self.cache_key_prefix(),
            "api_token": self.api_token,
            "api_endpoint": self.api_endpoint,
            "program": self.program,
            "start_time_ms": self.start_time_ms,
            "end_time_ms": self.end_time_ms,
            "resolution_hint_ms": self.resolution_hint_ms,
            "is_daemon": self.is_daemon,
            "duration_ms": self.duration_ms(),
            "job_start_ms": time.ctime(self.job_start_ms() / 1000),
            "job_end_ms": time.ctime(self.job_end_ms() / 1000),
            "job_duration_ms": self.job_duration_ms(),
            "is_streaming": self.is_streaming()
        })

    def __post_parse(self):
        if self.start_time_ms != None:
            self.start_time_ms = int(self.start_time_ms)
        if self.end_time_ms != None:
            self.end_time_ms = int(self.end_time_ms)
        if self.resolution_hint_ms != None:
            self.resolution_hint_ms = int(self.resolution_hint_ms)
        self.parse_token_and_program()

    def parse_http_body(self, event: dict):
        ''' If this serverless function is invoked as REST HTTP request, then input parameters are in the request body.
        This function parses them out and additionally the api token from the request header '''
        body = json.loads(event["body"])

        # Different tools/libraries randomly change the string case of header keys e.g. CAPITAL, Camel lower etc.
        self.api_token = {k.lower(): v for k, v in event["headers"].items()}["x-sf-token"]

        # Extract other parameters from body of request
        self.cache_location = body.get("cache_url", self.cache_location)
        self.api_endpoint = body.get("api_endpoint", self.api_endpoint)
        self.program = body["program"]
        self.start_time_ms = body.get("start_time_ms", self.start_time_ms)
        self.end_time_ms = body.get("end_time_ms", self.end_time_ms)
        self.resolution_hint_ms = body.get("resolution_hint_ms", self.resolution_hint_ms)
        self.is_daemon = not(str(body.get("daemon", "False")).lower() == "false")
        self.__post_parse()

    def parse_event(self, event: dict):
        ''' If this serverless function is invoked through lambda API or CLI, then input parameters are in the 'event'.
        This function parses them out '''

        self.api_token = event["api_token"]

        # Extract other parameters from event object
        self.api_endpoint = event.get("api_endpoint", self.api_endpoint)
        self.program = event["program"]
        self.start_time_ms = event.get("start_time_ms", self.start_time_ms)
        self.end_time_ms = event.get("end_time_ms", self.end_time_ms)
        self.resolution_hint_ms = event.get("resolution_hint_ms", self.resolution_hint_ms)
        self.is_daemon = not(str(event.get("daemon", "False")).lower() == "false")
        self.__post_parse()

    def parse_token_and_program(self):
        ''' if api_token encodes multiple tokens with names in JSON format, then extract each one
        Format = <token> or "{token_name:<token_def>, ...}" where
            token_def = [token, [<api_endpoint>]] where api_endpoint is optional, defaults to us0 realm
        E.g.
            x-sf-token=blablabla
            x-sf-token=["abc123", {"prod2":"1234"}, {"mon":["nnn111", "https://mon.signalfx.com"]}]

        The "program" parameter can refer to specific tokens by name. To do that, prepend with "<token_name>|"
        E.g. "<sfx_program>" becomes "prod1|<sfx_program>"

        This function parses tokens, detects if one of them is referred to in the program, and
        - 'fixes' the token field to be the specific token (or default) used in the program
        - 'fixes' the program field to remove the token name and make it valid signalflow again
        '''

        tokens = {}
        default_api = defaults.API_URL
        if self.api_endpoint:
            default_api = self.api_endpoint

        try:
            token_defs: list = json.loads(self.api_token)
            for token_def in token_defs:
                try:
                    for token_name, token_details in token_def.items():
                        try:
                            token_val = token_details[0]
                            token_api = default_api
                            if len(token_details) > 1:
                                token_api = token_details[1]
                            tokens[token_name] = [token_val, token_api]
                        except:
                            # token_details is a simple string
                            tokens[token_name] = [token_details, default_api]
                except:
                    # token_def is a plain string
                    tokens["default"] = [token_def, default_api]

            match = re.match('(\w+)\|(.*)$', self.program)
            if match:
                (self.api_token, self.api_endpoint) = tokens[match.group(1).strip()]
                self.program = match.group(2).strip()
            else:
                (self.api_token, self.api_endpoint) = tokens["default"]
        except:
            # User specific a single token
            self.api_endpoint = default_api
            return

    def cache_type(self):
        if self.cache_location:
            return Cachetype.MEMCACHE
        return Cachetype.FILE

    def is_streaming(self):
        if self.end_time_ms and self.end_time_ms <= self.invoke_time_ms:
            return False
        return True

    def absolute_ms(self, time_ms):
        result = int(time_ms or self.invoke_time_ms)
        if result <= 0:
            result = self.invoke_time_ms + result
        return result

    def duration_ms(self):
        return self.absolute_ms(self.end_time_ms or self.invoke_time_ms) - self.absolute_ms(self.start_time_ms)

    def job_duration_ms(self):
        f = utils.rounding_factor(self.duration_ms())
        return int((self.duration_ms() + f - 1) / f) * f

    def job_start_ms(self):
        f = utils.rounding_factor(self.duration_ms())
        if self.is_streaming():
            return self.invoke_time_ms /f * f - self.job_duration_ms()
        else:
            return self.absolute_ms(self.start_time_ms)

    def job_end_ms(self):
        if self.is_streaming():
            return self.invoke_time_ms + defaults.BACKING_JOB_LIFETIME_MS + 60000
        else:
            return self.absolute_ms(self.end_time_ms)

    def cache_key_prefix(self):
        if self.is_streaming():
            return "{token_program_hash}-{job_duration}-{resolution_hint}".format(
                token_program_hash=hashlib.sha3_256((self.api_token + self.program).encode('utf-8')).hexdigest(),
                job_duration=self.job_duration_ms(),
                resolution_hint=self.resolution_hint_ms)
        else:
            return "{token_program_hash}-{tstart}-{tend}-{resolution_hint}".format(
                token_program_hash=hashlib.sha3_256((self.api_token + self.program).encode('utf-8')).hexdigest(),
                tstart=self.job_start_ms(), tend=self.job_end_ms(),
                resolution_hint=self.resolution_hint_ms)
