import zoodict

__author__ = 'jleslie'

import boto.sqs
import json


class WorkQueue(object):

    def __init__(self, queue_name):
        self.queue_name = queue_name

    def read(self, wait_time_seconds=20):
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


import pika

class RabbitMQChannelWrapper(object):

    def __init__(self, queue_name):
        self.queue_name = queue_name


    def __enter__(self):
        with zoodict.ZooDict('/.riversnake_conf') as zd:
            connection_details = (zd.get('rabbitmq') or {}).get('connection') or {}
            parameters = pika.ConnectionParameters(
                                connection_details.get('host') or '10.141.141.10',
                                credentials=pika.credentials.PlainCredentials(
                                                connection_details.get('user') or 'riversnake',
                                                connection_details.get('password') or 'riversnake'))

            self.connection = pika.BlockingConnection(parameters)
            channel = self.connection.channel()
            channel.queue_declare(queue=self.queue_name)
            return channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

class WorkQueueOnRabbitMQ(WorkQueue):

    def __init__(self, queue_name):
        WorkQueue.__init__(self, queue_name)

    def _declare_queue(self):
        return RabbitMQChannelWrapper(self.queue_name)

    def read(self, wait_time_seconds=20):
        # http://stackoverflow.com/questions/9876227/rabbitmq-consume-one-message-if-exists-and-quit
        with self._declare_queue() as channel:
            method_frame, header_frame, body = channel.basic_get(queue=self.queue_name)
            if method_frame and method_frame.NAME != 'Basic.GetEmpty':
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                return json.loads(body)

    def write(self, data_dict):
        with self._declare_queue() as channel:
            channel.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(data_dict))