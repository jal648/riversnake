import time

__author__ = 'jleslie'


from marathon import MarathonClient
from marathon.models import MarathonApp

class MarathonDeployer(object):


    def __init__(self):
        self.url = 'http://10.114.120.56:8080/'
        self.client = MarathonClient(self.url)

    def deploy(self, task_chain):
        for task in task_chain.list_all_tasks():
            name = task.get('_id')
            safe = name.replace('.', '').lower()
            try:
                if self.client.get_app(safe):
                    self.client.delete_app(safe)
                    time.sleep(2)
            except Exception:
                pass

            self.client.create_app(safe,
                    MarathonApp(cmd='/home/jclouds/invoke.py {0} {1}'.format(task_chain.collection_name, name),
                    mem=16, cpus=1))
