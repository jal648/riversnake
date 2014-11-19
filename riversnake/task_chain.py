import base64
from operator import itemgetter
import uuid
import work_queue
from zoodict import ZooDict

class TaskChain(object):
    """
    Manages the relationships between tasks
    """

    def _gen_uuid(self):
        return base64.b32encode(uuid.uuid4().bytes).strip('=')


    def __init__(self, flow_name, _zoodict_impl_for_testing=None):
        self.flow_name = flow_name

        zd = ZooDict(path_root='/riversnake/' + flow_name) if not _zoodict_impl_for_testing else _zoodict_impl_for_testing(self.flow_name)
        zd.start()
        self.zoo_dict = zd


    def register_task(self, task_name):
        """ Register a given task, basically creating a uid for it so that it can be referred to in joins.
        """
        task = dict(task=task_name, outputs={}, id=self._gen_uuid())
        self.zoo_dict.set(task['id'], task)
        return task['id']

    def clear_all_tasks(self):

        all_keys = list(map(itemgetter(0), self.zoo_dict.items()))
        for key in all_keys:
            self.zoo_dict.delete(key)

    def _fetch_and_ensure_task_data(self, sender_task_id):
        task_data = self.get_task_data(sender_task_id)
        if not task_data:
            raise RuntimeError("No task found for task_id: {}".format(sender_task_id))
        return task_data

    def join_tasks(self, sender_task_id, receiver_task_id, status_code='success'):
        task_data = self._fetch_and_ensure_task_data(sender_task_id)
        self._fetch_and_ensure_task_data(receiver_task_id)

        task_data['outputs'][status_code] = receiver_task_id
        self.zoo_dict.set(sender_task_id, task_data)

    def get_task_data(self, task_id):
        return self.zoo_dict.get(task_id)

    def find_output_task(self, sender_task_id, status_code='success'):
        return ((self.get_task_data(sender_task_id) or {}).get('outputs') or {}).get(status_code)

    def __repr__(self):
        return self.flow_name

class DeployedTaskChain(object):

    """Takes a task chain, and a string identifier representing the 'deployment' creates and finds the queues for a given task.
    Then is a lookup for that director for finding queues.
    """

    def __init__(self, task_chain, deployment_id, _work_queue_impl_for_testing=None):
        self.task_chain = task_chain
        self.deployment_id = deployment_id
        self.work_queue_impl = _work_queue_impl_for_testing or work_queue.WorkQueueOnRabbitMQ

    def get_input_queue(self, task_id):
        queue_name = self.deployment_id + '_' + task_id
        return self.work_queue_impl(queue_name)

    def get_output_queue(self, task_name, status_code):
        receiver_task_id = self.task_chain.find_output_task(task_name, status_code)
        if not receiver_task_id:
            return None
        return self.get_input_queue(receiver_task_id)

    def get_task_data(self, task_id):
        return self.task_chain.get_task_data(task_id)

    def list_all_tasks(self):
        return map(itemgetter(1), self.task_chain.zoo_dict.items())

    def __repr__(self):
        return self.task_chain.__repr__() + '/' + self.deployment_id




