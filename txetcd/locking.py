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

from twisted.internet import defer, task, reactor
from twisted.python import log

from txetcd.client import EtcdServerError


class EtcdLock(object):
    def __init__(self, client, full_path, manager_id, ttl):
        paths = full_path.rsplit('/', 1)
        self.client = client
        self.base_path = paths[0]
        self.id = int(paths[1])
        self.full_path = full_path
        self.manager_id = manager_id
        self.ttl = ttl
        self.update_loop = task.LoopingCall(self._refresh_node)
        self.locked = False
        self.last_successful_update = reactor.seconds()
        self._pending_update_deferred = defer.succeed(self)

    def _refresh_node(self):
        self._pending_update_deferred = defer.Deferred()

        def _on_success(result):
            self.last_successful_update = reactor.seconds()
            self._pending_update_deferred.callback(None)

        def _on_failure(failure):
            abort = False

            if failure.check(EtcdServerError) and failure.value.code == 100:
                # The node we tried to update didn't exist, the lock (if we had it) has been lost
                abort = True
            self._pending_update_deferred.callback(None)

            if abort:
                return failure
            else:
                return None

        d = self.client.set(self.full_path, prev_exist='true', value=self.manager_id, ttl=self.ttl)
        d.addCallbacks(_on_success, _on_failure)
        return d

    def _remove_node(self):
        return self.client.delete(self.full_path)

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
                self.locked = True
                d.callback(self)
            else:
                print "Not locked, watching for changes since {index}".format(index=index)
                d1 = self.client.get(self.base_path, wait=True, wait_index=index, recursive=True)
                d1.addCallback(lambda result: self._test_ownership())
                d1.addCallbacks(_on_response, d.errback)

        self._test_ownership().addCallbacks(_on_response, d.errback)
        return d

    def _start_refresh(self):
        self.update_loop.start(self.ttl / 3, now=False)

    def _stop_refresh(self):
        if self.update_loop.running:
            self.update_loop.stop()
        return self._pending_update_deferred

    def _set_unlocked(self):
        self.locked = False
        return self

    def release(self):
        d = self._stop_refresh()
        d.addCallback(lambda result: self._remove_node())
        d.addCallback(lambda result: self._set_unlocked())
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
        def _wait(lock):
            lock._start_refresh()
            return lock._wait()

        d = self._create_lock(path, ttl)
        d.addCallback(_wait)
        return d
