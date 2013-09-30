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

from twisted.python import log

from txetcd import EtcdError, EtcdResponse


class EtcdTask(object):
    def __init__(self, id, value, queue):
        self.id = id
        self.value = value
        self.queue = queue

    def __repr__(self):
        return '{cls}(id={id})'.format(cls=self.__class__.__name__, id=self.id)

    def complete(self):
        return self.queue._delete_task(self.id)

    def abort(self):
        return self.queue._free_task(self.id)


class NoAvailableTasksError(Exception):
    def __init__(self, at_index):
        self.at_index = at_index


class EtcdTaskQueue(object):
    """
    An etcd-backed task queue with support for push and take operations. NOTE:
    this approach does not currently incorporate any sort of locking across
    processes (or instances of EtcdTaskQueue). This will be resolved when
    the v2 API of etcd is available.
    """

    def __init__(self, client, base_path):
        self.processor_id = uuid.uuid4()
        self.client = client
        self.base_path = base_path
        self.latest_index = None
        self.taken_tasks = set()

    def _list_queue_dir(self):
        return self.client.get(self.base_path)

    def _get_task_id(self, task_node):
        return task_node.key.lstrip('/{base_path}/'.format(base_path=self.base_path))

    def _find_untaken_task(self, task_nodes):
        for task_node in task_nodes:
            task_id = self._get_task_id(task_node)
            if task_id not in self.taken_tasks:
                self.taken_tasks.add(task_id)
                return EtcdTask(task_id, task_node.value, self)

        # TODO: there is a race condition here, if there are no nodes we have
        # no idea what index to watch since. Instead we just watch with no
        # index. We could miss a task added in the meantime. This will be
        # improved upon in the v2 etcd API.
        raise NoAvailableTasksError(task_nodes[0].index if len(task_nodes) > 0 else None)

    def _free_task(self, task_id):
        self.taken_tasks.remove(task_id)

    def _delete_task(self, task_id):
        key = '{base_path}/{task_id}'.format(base_path=self.base_path, task_id=task_id)
        return self.client.delete(key).addCallback(lambda result: self._free_task(task_id))

    def push(self, task):
        task_id = uuid.uuid1()
        key = '{base_path}/{task_id}'.format(base_path=self.base_path, task_id=task_id)
        return self.client.set(key, task).addCallback(lambda result: task_id)

    def take(self):
        def _on_list_error(failure):
            # Key not found - watch with index 1 will find the next change
            if failure.check(EtcdError) and failure.value.code == 100:
                raise NoAvailableTasksError(1)
            return failure

        def _wait_if_none(failure):
            if failure.check(NoAvailableTasksError):
                d = self.client.watch(self.base_path, index=failure.value.at_index)
                return d.addCallback(lambda event: self.take())
            return failure

        d = self._list_queue_dir().addCallbacks(self._find_untaken_task, _on_list_error)
        return d.addErrback(_wait_if_none)
