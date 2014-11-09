import work_queue
from zoodict import ZooDict


class TaskChain(object):
    """
    Manages the relationships between tasks
    """

    def __init__(self, collection_name, _zoodict_impl_for_testing=None):
        self.collection_name = collection_name
        zd = ZooDict(path_root=collection_name) if not _zoodict_impl_for_testing else _zoodict_impl_for_testing(self.collection_name)
        zd.start()
        self.zoo_dict = zd

    def join_tasks(self, sender_task_name, receiver_task_name, status_code='success'):
        task_data = self.zoo_dict.get(sender_task_name)
        if not task_data:
            task_data = { 'outputs':{} }
        if not task_data.get('outputs'):
            task_data['outputs'] = {}

        task_data['outputs'][status_code] = receiver_task_name
        self.zoo_dict.set(sender_task_name, task_data)

    def find_output_task(self, sender_task_name, status_code='success'):
        return ((self.zoo_dict.get(sender_task_name) or {}).get('outputs') or {}).get(status_code)

    def __repr__(self):
        return self.collection_name

class DeployedTaskChain(object):

    """Takes a task chain, and a string identifier representing the 'deployment' creates and finds the queues for a given task.
    Then is a lookup for that director for finding queues.
    """

    def __init__(self, task_chain, deployment_id, _work_queue_impl_for_testing=None):
        self.task_chain = task_chain
        self.deployment_id = deployment_id
        self.work_queue_impl = _work_queue_impl_for_testing or work_queue.WorkQueueOnAmazonSQS
    def _get_queue_safe_name(self, task_name):
        return task_name.lower().replace('.', '_')

    def get_input_queue(self, task_name):
        queue_name = self.deployment_id + '_' + self._get_queue_safe_name(task_name)
        return self.work_queue_impl(queue_name)

    def get_output_queue(self, task_name, status_code):
        receiver_task = self.task_chain.find_output_task(task_name, status_code)
        return self.get_input_queue(receiver_task)

    def __repr__(self):
        return self.task_chain.__repr__() + '/' + self.deployment_id


