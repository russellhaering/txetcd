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
from urllib import urlencode

from dateutil.parser import parse as parse_datetime
from twisted.python import log

from treq import json_content
from treq.client import HTTPClient


class EtcdError(Exception):
    pass


class EtcdServerError(EtcdError):
    def __init__(self, **kwargs):
        super(EtcdServerError, self).__init__(kwargs)
        self.code = kwargs.get('errorCode')
        self.message = kwargs.get('message')
        self.cause = kwargs.get('cause')


class EtcdNode(object):
    directory = False

    def __init__(self, **kwargs):
        self.key = kwargs.get('key')

        if 'expiration' in kwargs:
            self.expiration = parse_datetime(kwargs['expiration'])
        else:
            self.expiration = None

    @staticmethod
    def from_response(**kwargs):
        if kwargs.get('dir', False):
            return EtcdDirectory(**kwargs)
        else:
            return EtcdFile(**kwargs)


class EtcdFile(EtcdNode):
    def __init__(self, **kwargs):
        super(EtcdFile, self).__init__(**kwargs)
        self.value = kwargs.get('value')


class EtcdDirectory(EtcdNode):
    directory = True

    def __init__(self, **kwargs):
        super(EtcdDirectory, self).__init__(**kwargs)
        if 'kvs' in kwargs:
            self.children = [EtcdNode.from_response(**obj) for obj in kwargs['kvs']]
        else:
            self.children = None


class EtcdClient(object):
    API_VERSION = 'v2'

    def __init__(self, seeds=[('localhost', 4001)]):
        self.nodes = set(seeds)
        self.http_client = HTTPClient.with_config()

    def _get_node(self, prefer_leader=False):
        # TODO: discover the rest of the cluster
        # TODO: cache the leader
        return random.sample(self.nodes, 1)[0]

    def _decode_response(self, response):
        return json_content(response).addCallback(self._construct_response_object)

    def _construct_response_object(self, obj):
        if 'errorCode' in obj:
            raise EtcdError(**obj)
        return EtcdNode.from_response(**obj)

    def _log_failure(self, failure):
        log.err(failure)
        return failure

    def _request(self, method, path, params=None, data=None, prefer_leader=False):
        node = self._get_node(prefer_leader=prefer_leader)
        url = 'http://{host}:{port}/{version}{path}'.format(
            host=node[0],
            port=node[1],
            version=self.API_VERSION,
            path=path,
        )
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        kwargs = {}

        if data:
            kwargs['data'] = urlencode(data)

        if params:
            kwargs['params'] = params

        d = self.http_client.request(method, url, headers=headers, **kwargs)
        d.addErrback(self._log_failure)
        return d

    def _validate_key(self, key):
        if not key.startswith('/'):
            raise RuntimeError('keys must start with /')

    def _build_params(self, params, method_kwargs, param_map):
        for key in param_map.iterkeys():
            if key in method_kwargs:
                params[param_map[key]] = method_kwargs[key]

        return params

    def create(self, key, value, **kwargs):
        self._validate_key(key)
        path = '/keys{key}'.format(key=key)
        params = self._build_params({'value': value}, kwargs, {
            'ttl': 'ttl',
        })
        d = self._request('POST', path, params=params, prefer_leader=True)
        d.addCallback(self._decode_response)
        return d

    def set(self, key, **kwargs):
        self._validate_key(key)
        path = '/keys{key}'.format(key=key)
        data = self._build_params({}, kwargs, {
            'ttl': 'ttl',
            'value': 'value',
            'prev_index': 'prevIndex',
            'prev_value': 'prevValue',
            'prev_exists': 'prevExists',
        })

        d = self._request('PUT', path, data=data, prefer_leader=True)
        d.addCallback(self._decode_response)
        return d

    def delete(self, key):
        self._validate_key(key)
        path = '/keys{key}'.format(key=key)
        d = self._request('DELETE', path, prefer_leader=True)
        d.addCallback(self._decode_response)
        return d

    def get(self, key, **kwargs):
        self._validate_key(key)
        path = '/keys{key}'.format(key=key)
        params = self._build_params({}, kwargs, {
            'sorted': 'sorted',
            'recursive': 'recursive',
            'consistent': 'consistent',
            'wait': 'wait',
            'wait_index': 'waitIndex',
        })

        d = self._request('GET', path, params=params)
        d.addCallback(self._decode_response)
        return d
