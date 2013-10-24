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
    def __init__(self, client, full_path, manager_id, ttl):
        paths = full_path.rsplit('/', 1)
        self.client = client
        self.base_path = paths[0]
        self.id = int(paths[1])
        self.full_path = full_path
        self.manager_id = manager_id
        self.ttl = ttl
        self.lock_achieved = False

    def _refresh_node(self):
        return self.client.set(self.full_path, prev_nil=False, value=self.manager_id, ttl=ttl)

    def _test_ownership(self):
        def _on_response(result):
            ids = [int(node.key.rsplit('/', 1)[1]) for node in result.node.children]
            return (ids[0] == self.id, result.index)

        d = self.client.get(self.base_path, sorted=True)
        d.addCallback(_on_response)
        return d

    def _wait(self):
        print "Waiting for lock with id {id}".format(id=self.id)
        d = defer.Deferred()

        def _on_response((owned, index)):
            if owned:
                d.callback(self)
            else:
                print "Not locked, watching for changes since {index}".format(index=index)
                d1 = self.client.get(self.base_path, wait=True, wait_index=index, recursive=True)
                d1.addCallback(lambda result: self._test_ownership())
                d1.addCallbacks(_on_response, d.errback)

        self._test_ownership().addCallbacks(_on_response, d.errback)
        return d


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

    def _create_lock(self, path, ttl):
        path = path.rstrip('/')
        if not path.startswith('/'):
            return defer.fail(RuntimeError('EtcdLockManager lock paths must be absolute'))

        d = self.client.create(self.base_path + path, self.id, ttl=ttl)
        d.addCallback(lambda result: EtcdLock(self.client, result.node.key, self.id, ttl))
        return d

    def lock(self, path, ttl=60):
        d = self._create_lock(path, ttl)
        d.addCallback(lambda lock: lock._wait())
        return d
