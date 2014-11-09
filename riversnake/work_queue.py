__author__ = 'jleslie'

import boto.sqs
import json


class WorkQueue(object):

    def __init__(self, queue_name):
        self.queue_name = queue_name

    def read(self):
        raise NotImplementedError

    def write(self, data_dict):
        raise NotImplementedError

    def write_data(self, **values):
        self.write(values)



test_work_queues = {}
def lookup_test_work_queue(queue_name):
    return test_work_queues.setdefault(queue_name, MockWorkQueueForTesting(queue_name))

class MockWorkQueueForTesting(WorkQueue):



    def __init__(self, queue_name):
        WorkQueue.__init__(self, queue_name)
        self.q = []

    def read(self):
        if len(self.q) > 0:
            d = self.q[0]
            self.q = self.q[1:]
            return d

    def write(self, data_dict):
        self.q.append(data_dict)

    def __repr__(self):
        return 'testqueue: {}, data:\n{}'.format(self.queue_name, self.q)


class WorkQueueOnAmazonSQS(WorkQueue):

    def __init__(self, queue_name):
        WorkQueue.__init__(self, queue_name)

    def _declare_queue(self):
        conn = boto.sqs.connect_to_region('eu-west-1')
        q = conn.get_queue(self.queue_name)
        if not q:
            q = conn.create_queue(self.queue_name)
        return q

    def read(self, wait_time_seconds=20):
        q = self._declare_queue()

        message = q.read(wait_time_seconds=wait_time_seconds)
        if message:
            result = json.loads(message.get_body())
            q.delete_message(message)
            return result

    def write(self, data_dict):
        q = self._declare_queue()

        message = boto.sqs.message.Message()
        message.set_body(json.dumps(data_dict))
        return q.write(message)



