#! /usr/bin/python
import sys
from jobs import TaskChain
from jobs.deploy import MarathonDeployer

chain_name = sys.argv[1] if len(sys.argv) > 1 else 'chain_test_1'

print "Deploying: ", chain_name

chain = TaskChain(chain_name)

print "Chain: \n", str(chain)

deployer = MarathonDeployer()
deployer.deploy(chain)