from riversnake import Task

class AddThree(Task):
    def run(self, amount=0):
        return dict(amount=amount+3)

class DifferentStatusIfGreatherThanFive(Task):
    def run(self, amount=0):
        return dict(amount=amount), 'low' if amount <= 5 else 'high'

class RepeatTenTimes(Task):
    def run(self, **kwargs):
        return [ kwargs for i in range(10) ]

class SumAmountInternal(Task):
    value=0
    def run(self, **kwargs):
        self.value += kwargs['amount']


import datetime
class WriteToALocalFile(Task):
    def run(self, **kwargs):
        with open('/tmp/WriteToALocalFile', 'a') as f:
            f.write(datetime.datetime.today().isoformat() + ' ' + str(kwargs))

def taskname(task):
    return 'riversnake.tests.example_tasks.' + task.__name__


