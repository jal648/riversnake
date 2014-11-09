from boto.sqs.message import Message
import time

__author__ = 'jleslie'

import boto.sqs
import json
import rslogger as log
from dfdb import mongo_bsdev, mong_upsert

from zoodict import ZooDict


def _import_class(class_string):
    """Returns class object specified by a string.

    Args:
        class_string: The string representing a class.

    Raises:
        ValueError if module part of the class is not specified.
    """
    module_name, _, class_name = class_string.rpartition('.')
    if module_name == '':
        raise ValueError('Class name must contain module part.')
    return getattr(
        __import__(module_name, globals(), locals(), [class_name], -1),
        class_name)

class Task(object):
    """The interface one must implement in order to get some work done!
    """

    def run(self, **kwargs):
        raise NotImplementedError



class TaskInvoker(object):
    """
    Wraps a Task, and runs it.
    - lookup the inq
    - read from it
    - if a message in the queue, run the task on it
    - if there's one or more results
    - if mSupplying the data form the in_queue and directing to the appropriate out_queue
    Read message, give message to task, if no exceptions, delete message.
    """

    def __init__(self, task_name, deployed_task_chain):
        self.task_name = task_name
        self.task = _import_class(task_name)()
        self.deployed_task_chain = deployed_task_chain


    def _convert_file_handles_to_hdfs(self, data_dict):
        # TODO: store files in hdfs, and return a reference. for now, not supported.
        return data_dict

    def poll(self, wait_time_seconds=20):
        log.debug('[task:%s] polling', self.task_name)

        m = self.deployed_task_chain.get_input_queue(self.task_name).read(wait_time_seconds=wait_time_seconds)

        if m:
            try:
                log.debug('[task:%s] found message', self.task_name)

                result = self.task.run(**m)

            except Exception as e:
                # TODO treat as 'unhandled_error' and put message back onto queue (retry)
                # and implement a 'dead letter' after x retries.

                print e

            else:
                # want this in the root form: [ ( {}, str ), ... ]

                if result:
                    if isinstance(result, dict):
                        result = [ (result, 'success') ]

                    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict) and isinstance(result[1], basestring):
                        result = [ result ]


                    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], list) and isinstance(result[1], basestring):
                        # convert [d1, d2, d3,...] 'status' into [(d1, 'status'), (d2, 'status'), ..]
                        result = [ (x, result[1]) for x in result[0] ]

                    if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
                        # convert [d1, d2, d3] into [(d1, 'success'), (d2, 'success'),..]
                        result = [ (x, 'success') for x in result ]

                    if not isinstance(result, list):
                        print "Could not handle task return value", result

                    else:
                        for data_dict, status in result:
                            data_dict = self._convert_file_handles_to_hdfs(data_dict)
                            out_queue = self.deployed_task_chain.get_output_queue(self.task_name, status)
                            if out_queue:
                                out_queue.write(data_dict)

                            else:
                                print "no next queue found for ", self.task_name, status

        return True

    def run(self):
        while self.poll():
            pass



    def __repr__(self):
        return self.task.__repr__() + '/' + self.deployed_task_chain.__repr__()




