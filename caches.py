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

from abc import ABC, abstractmethod
import elasticache_auto_discovery
import os
import pickle
from pymemcache.client.hash import HashClient
from pymemcache import serde
import time
import traceback


####################################################

class Cache(ABC):

    @abstractmethod
    def reinit(self):
        pass

    @abstractmethod
    def set(self, cache_key, value):
        pass

    @abstractmethod
    def get(self, cache_key):
        pass

    @abstractmethod
    def multiget(self, cache_keys):
        pass

    @abstractmethod
    def delete(self, cache_key):
        pass

####################################################

class ReliableCache(Cache):
    ''' A wrapper around cache that does the following
        - retries an operation if it fails for any reason
        - if failures still occur, also re-initializes the cache connection at most once per configured interval'''

    def __init__(self, cache: Cache, num_retries:int = 1, retry_internval_ms:int = 10, reinit_interval_ms:int = 1000):
        self._cache = cache
        self._num_retries = num_retries
        self._retry_interval_ms = retry_internval_ms
        self._retry_interval_ms = reinit_interval_ms
        self._need_to_reinit = False
        self._last_reinit_ms = int(time.time() * 1000)

    def reinit(self):
        if self._need_to_reinit:
            now_ms = int(time.time() * 1000)
            if now_ms > self._last_reinit_ms + self._retry_interval_ms:
                try:
                    self._cache = self._cache.reinit()
                    self._need_to_reinit = False
                    self._last_reinit_ms = now_ms
                except Exception as e:
                    print("ReliableCache: failed to reinit.", e)

    def __do_reliably(self, f, *args):
        for n in range(self._num_retries):
            try:
                self.reinit()
                return f(*args)
            except Exception as e:
                print("ReliableCache: encountered exception during {}: {}".format(f, e))
                self._need_to_reinit = True
                if n >= self._num_retries - 1:
                    raise e
                time.sleep(self._retry_interval_ms / 1000)
        raise Exception("ReliableCache: shouldn't really get here")

    def set(self, cache_key, value):
        return self.__do_reliably(self._cache.set, cache_key, value)

    def get(self, cache_key):
        return self.__do_reliably(self._cache.get, cache_key)

    def multiget(self, cache_keys):
        return self.__do_reliably(self._cache.multiget, cache_keys)

    def delete(self, cache_key):
        return self.__do_reliably(self._cache.delete, cache_key)

####################################################

class FileCache(Cache):
    def __init__(self, directory="/tmp"):
        self._directory = directory

    def reinit(self):
        return FileCache(self._directory)

    def set(self, cache_key, value):
        with open(os.path.join(self._directory, cache_key), "wb") as f:
            pickle.dump(value, f)

    def get(self, cache_key):
        try:
            with open(os.path.join(self._directory, cache_key), "rb") as f:
                return pickle.load(f)
        except:
            return None

    def multiget(self, cache_keys):
        result = {}
        for cache_key in cache_keys:
            try:
                result[cache_key] = self.get(cache_key)
            except:
                None
        return result

    def delete(self, cache_key):
        path = os.path.join(self._directory, cache_key)
        if os.path.exists(path):
            os.remove(path)

####################################################

class MemCache(Cache):

    def __init__(self, memcache_conf_url, key_expiry_secs):
        try:
            self._conf_url = memcache_conf_url
            self._expire_secs = key_expiry_secs
            print("config url", self._conf_url)
            nodes = elasticache_auto_discovery.discover(self._conf_url)
            print("nodes 1:", nodes)
            nodes = map(lambda x: (x[1], int(x[2])), nodes)
            print("nodes 2:", nodes)
            self._client = HashClient(
                nodes,
                serde=serde.PickleSerde(pickle_version=3),
                default_noreply=False,
                ignore_exc=False,
                connect_timeout=3,
                timeout=5,
                retry_attempts=3,
                retry_timeout=1
            )
            print("memcache client", self._client)
        except Exception as e:
            print("MemCache exception", e)
            print(traceback.format_exc())
        finally:
            print("~MemCache constructor")

    def reinit(self):
        return MemCache(self._conf_url, self._expire_secs)

    def set(self, cache_key, value):
        try:
            self._client.set(cache_key, value, expire=self._expire_secs)
            #print("wrote to memcache {} = {}".format(cache_key, value))
        except Exception as e:
            print("ERROR writing '{}' to memcache: ".format(cache_key, e))
            raise e

    def get(self, cache_key):
        return self._client.get(cache_key)

    def multiget(self, cache_keys):
        return self._client.get_multi(cache_keys)

    def delete(self, cache_key):
        self._client.delete(cache_key)
