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

import uuid

from twisted.internet import defer
from twisted.python import log

from txetcd.client import EtcdError


class EtcdLock(object):
    def __init__(self, base_path, id, ttl):
        self.base_path = base_path
        self.id = id
        self.full_path = base_path + '/' + str(id)
        self.ttl = ttl
        self.lock_achieved = False


class EtcdLockManager(object):
    """
    An etcd-backed lock manager.
    """

    def __init__(self, client, base_path):
        self.id = str(uuid.uuid4())
        self.client = client
        self.base_path = base_path.rstrip('/')

        if self.base_path is not '' and not self.base_path.startswith('/'):
            raise RuntimeError('EtcdLockManager requires an absolute base path')

    def _abs_path(self, path, id=None):
        if id is None:
            return self.base_path + path
        else:
            return self.base_path + path + '/' + str(id)

    def _extract_id(self, path):
        return int(path.rsplit('/', 1)[1])

    def _test_ownership(self, lock):
        def _on_response(result):
            ids = [self._extract_id(node.key) for node in result.node.children]
            return (ids[0] == lock.id, result.index)

        d = self.client.get(lock.base_path, sorted=True)
        d.addCallback(_on_response)
        return d

    def _create_node(self, path, ttl):
        abs_path = self._abs_path(path)
        d = self.client.create(abs_path, self.id, ttl=ttl)
        d.addCallback(lambda result: EtcdLock(abs_path, self._extract_id(result.node.key), ttl))
        return d

    def _refresh_node(self, path, id, ttl):
        node_path = self._abs_path(path, id=id)
        return self.client.set(node_path, prev_nil=False, value=self.id, ttl=ttl).addCallback(lambda result: id)

    def _wait_for_lock(self, lock):
        print "Waiting for lock with id {id}".format(id=lock.id)
        d = defer.Deferred()

        def _on_response((owned, index)):
            if owned:
                d.callback(lock)
            else:
                print "Not locked, watching for changes since {index}".format(index=index)
                d1 = self.client.get(lock.base_path, wait=True, wait_index=index, recursive=True)
                d1.addCallback(lambda result: self._test_ownership(lock))
                d1.addCallbacks(_on_response, d.errback)

        self._test_ownership(lock).addCallbacks(_on_response, d.errback)

        return d

    def lock(self, path, ttl=60):
        path = path.rstrip('/')
        if not path.startswith('/'):
            raise RuntimeError('EtcdLockManager lock paths must be absolute')

        d = self._create_node(path, ttl)
        d.addCallback(self._wait_for_lock)
        return d
