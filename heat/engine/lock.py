#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import datetime
import eventlet


from heat.common import exception

from heat.db import api as db_api
from heat.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class LockManager(object):
    def __init__(self, context, name, data):
        self.context = context
        self.name = name
        self.data = data
        self.lock = None

    def try_acquire(self, timeout):
        """
        Try to acquire a  lock, but don't raise an
        exception or try to steal lock.
        """
        start_time = datetime.datetime.now().replace(microsecond=0)
        while True:
            try:
                self.acquire()
            except Exception as e:
                current_time = datetime.datetime.now().replace(microsecond=0)
                if start_time - current_time >= timeout:
                    raise e
                eventlet.sleep(0.5)

    def acquire(self):
        """Acquire a general lock """
        self.lock = db_api.lock_create(self.context, self.name, self.data)
        if not self.lock:
            raise exception.AcquireLockFailed()

    def release(self):
        """Release a stack lock."""
        if self.lock:
            self.lock = None
            db_api.lock_release(self.name)
        else:
            raise exception.ReleaseLockFailed(self.name)

    def try_steal(self):
        raise NotImplemented()

    def _steal(self):
        self.lock = db_api.lock_update(self.name, self.data)