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

import contextlib

from oslo.config import cfg
from oslo.utils import excutils

from heat.common import exception
from heat.engine import lock
from heat.openstack.common import log as logging

cfg.CONF.import_opt('engine_life_check_timeout', 'heat.common.config')

LOG = logging.getLogger(__name__)


class StackLock(lock.LockManager):
    """Stack Lock"""
    def __init__(self, context, stack_id, engine_id):
        self.engine_id = engine_id
        super(StackLock, self).__init__(context, stack_id, engine_id)

    def try_steal(self):
        if lock.engine_alive(self.context, self.engine_id):
            raise exception.ActionInProgress("")
        self._steal()

    @contextlib.contextmanager
    def thread_lock(self):
        """
        Acquire a lock and release it only if there is an exception.  The
        release method still needs to be scheduled to be run at the
        end of the thread using the Thread.link method.
        """
        try:
            self.acquire()
            yield
        except:  # noqa
            with excutils.save_and_reraise_exception():
                self.release()

    @contextlib.contextmanager
    def try_thread_lock(self, timeout=60):
        """
        Similar to thread_lock, but acquire the lock using try_acquire
        and only release it upon any exception after a successful
        acquisition.
        """
        result = None
        try:
            result = self.try_acquire(timeout)
            yield result
        except:  # noqa
            if result is None:  # Lock was successfully acquired
                with excutils.save_and_reraise_exception():
                    self.release()
            raise