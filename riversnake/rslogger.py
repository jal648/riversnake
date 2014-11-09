"""

"""

import os
os.environ['BRIGHTSUN_LOG_TO_STD_OUT'] = 'y'
from logging import INFO
os.environ['BRIGHTSUN_LOG_LEVEL'] = str(INFO)

from logging import *
import sys

os.environ.get('BRIGHTSUN_LOG_ROOT', '.')

TRACE = DEBUG-1
addLevelName(TRACE, 'TRACE')

def trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)


log_format = '[%(asctime)s] [%(process)05d] [%(levelname)s] [%(funcName)s] %(message)s'
root_logger = getLogger()
root_logger.setLevel(INFO)
boto_logger = getLogger('boto')
boto_logger.setLevel(INFO)

handler = FileHandler('/var/log/riversnake.log', encoding='utf8')
handler.setFormatter(Formatter(log_format))
root_logger.addHandler(handler)
