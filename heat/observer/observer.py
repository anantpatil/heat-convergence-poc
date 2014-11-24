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

"""
Provides the class which observes resources.
"""

import datetime
import eventlet
import six

from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.common import timeutils
from heat.db import api as db_api
from heat.engine import resource as resourcem
from heat.engine import stack as stackm
from heat.openstack.common import log as logging
from heat.rpc import client as rpc_client


LOG = logging.getLogger(__name__)


class Observer(object):
    def __init__(self):
        self.rpc_client = rpc_client.EngineClient()

    def _get_observed_state(self, cnxt, rsrc):
        """
        1. Fetch the observed properties from DB (which are populated by the
        listener of Ceilometer event notifications).
        2. If there is no observed state for the resource, fetch the current
        state of resource using the corresponding resource plug-in.
        :param cnxt: request context
        :param rsrc: resource object
        :return: map/dict of resource's observed state.
        """
        def fetch_observed_state_from_db(rsrc):
            # TODO: Fetch the observed state once event listener is in place.
            return  {}

        observed_state = fetch_observed_state_from_db(rsrc)
        if not observed_state:
            try:
                observed_state = rsrc.get_current_state()
            except Exception as ex:
                LOG.warn(_LW("Failure fetching resource %(id)s current state. "
                             "Error: %(ex)s"), {'id': rsrc.id, 'ex': ex})
                observed_state = {}
        return observed_state

    def observe_resource(self, cnxt, resource_id):
        """
        1. load the resource from DB and create resource object.
        2. fetch the desired and observed state of the resource.
        3. if desired matches observed, set status COMPLETE
           else set status FAILED
        4. notify the engine-worker that resource has been observed.
        :param cnxt: request context
        :param resource_id: resource identifier
        :return: None
        """
        db_rsrc = db_api.resource_get(cnxt, resource_id)
        stack = stackm.Stack.load(cnxt, db_rsrc.stack_id)
        rsrc = resourcem.Resource.load(db_rsrc, stack)

        desired_state = rsrc.get_desired_state()

        # NOTE(UG): wait for a maximum of stack timeout period for the current
        # state to match the desired state.
        timeout = stack.get_timeout_delta()
        start_time = datetime.datetime.now().replace(microsecond=0)
        current_time = datetime.datetime.now().replace(microsecond=0)
        attempt = 1
        while current_time - start_time <= datetime.timedelta(seconds=timeout):
            observed_state = self._get_observed_state(cnxt, rsrc)
            all_is_well = True
            for param, value in six.iteritems(desired_state):
                if param in observed_state:
                    observed_value = observed_state[param]
                    if value != observed_value and observed_value not in value:
                        LOG.info(_LI('Resource %(resource_id)s parameter: '
                                     '%(param)s desired value: %(value)s '
                                     'observed value: %(observed_value)s'),
                                 locals())
                        all_is_well = False
                else:
                    LOG.info(_LI('Observed value for resource %(resource_id)s '
                                 'parameter: %(param)s not available!'),
                             locals())
                    all_is_well = False

            if all_is_well:
                break
            else:
                # the wait time should be reasonable enough to avoid OverLimit
                if attempt > 9:
                    attempt = 1
                delay = timeutils.retry_backoff_delay(attempt, 0.2)
                attempt += 1
                eventlet.sleep(delay)
                current_time = datetime.datetime.now().replace(microsecond=0)

        action = rsrc.action
        if all_is_well:
            rsrc.state_set(action, rsrc.COMPLETE)
        else:
            rsrc.state_set(action, rsrc.FAILED, '%s aborted' % action)
        self.rpc_client.notify_resource_observed(cnxt,
                                                 db_rsrc.stack_id,
                                                 db_rsrc.name,
                                                 db_rsrc.version)
