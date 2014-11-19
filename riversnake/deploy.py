import time
from riversnake import TaskInvoker
from riversnake.task_chain import DeployedTaskChain, TaskChain

__author__ = 'jleslie'


from marathon import MarathonClient
from marathon.models import MarathonApp

class MarathonDeployer(object):


    def __init__(self, marathon_url):
        self.url = marathon_url
        self.client = MarathonClient(self.url)

    def deploy(self, task_chain, environment_name):
        deployed_chain = DeployedTaskChain(task_chain, environment_name)
        for task in deployed_chain.list_all_tasks():
            task_id = task['id']
            safe_name = task_id.lower()
            # safe_name = task['name'].replace('.', '').lower()
            try:
                if self.client.get_app(safe_name):
                    self.client.delete_app(safe_name)
                    time.sleep(2)
            except Exception:
                pass

            app = MarathonApp(cmd='/var/riversnake/invoke.py {0} {1} {2}'.format(
                        task_chain.flow_name,
                        environment_name,
                        task_id),
                    mem=16, cpus=1)

            self.client.create_app(safe_name, app)


def build_deployed_chain(task_chain_name, deployed_environment_name):
    return DeployedTaskChain(TaskChain(task_chain_name), deployed_environment_name)


def build_task_invoker(task_chain_name, deployed_environment_name, task_id):
    invoker = TaskInvoker(task_id, build_deployed_chain(task_chain_name, deployed_environment_name ))
    print "Invoker Built ", invoker
    return invoker