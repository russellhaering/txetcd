"""
Copyright 2013 Russell Haering.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import random

from treq import json_content
from treq.client import HTTPClient


class EtcdClient(object):
    API_VERSION = 'v1'

    def __init__(self, seeds=[('localhost', 4001)]):
        self.nodes = set(seeds)
        self.http_client = HTTPClient.with_config()

    def _get_node(self, leader):
        # TODO: discover the rest of the cluster
        # TODO: cache the leader
        return random.sample(self.nodes, 1)[0]

    def _decode_response(self, response):
        return json_content(response)

    def _request(self, method, path, params=None, leader=False):
        node = self._get_node(leader)
        url = 'http://{host}:{port}/{version}{path}'.format(
            host=node[0],
            port=node[1],
            version=self.API_VERSION,
            path=path,
        )
        d = self.http_client.request(method, url, params=params)
        d.addCallback(self._decode_response)
        return d

    def set(self, key, value, prev_value=None, ttl=None):
        path = '/keys/{key}'.format(key=key)
        params = {'value': value}

        if ttl is not None:
            params['ttl'] = ttl

        if prev_value is not None:
            params['prevValue'] = prev_value

        return self._request('POST', path, params=params, leader=True)

    def watch(self, key, index=None):
        path = '/watch/{key}'.format(key=key)
        params = None

        if index is not None:
            params['index'] = index

        return self._request('GET', path)

    def delete(self, key):
        path = '/keys/{key}'.format(key=key)
        return self._request('DELETE', path)
