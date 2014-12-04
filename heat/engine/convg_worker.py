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

from parser import Stack

from heat.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class CONVERGE_RESPONSE:
    OK = 0      # Resource converged
    PANIC = 1   # Woker detects new update to stack, panics
    FAILED = 2  # Attempted, but failed

class ConvergenceWorker:
    """"
    Loads the object from DB and starts converging it.
    """

    @staticmethod
    def do_converge(context, incoming_req_id, stack_id, resource_id, timeout):
        stack = Stack.load(context, stack_id=stack_id)
        if incoming_req_id != stack.current_req_id():
            # panic: a new stack operation was issued
            stack.rpc_client.notify_resource_observed(
                context, incoming_req_id, stack_id, resource_id,
                CONVERGE_RESPONSE.PANIC)
        try:
            stack.resource_action_runner(resource_id, timeout)
        except Exception as e:
            LOG.exception(e)
            stack.rpc_client.notify_resource_observed(context, incoming_req_id,
                                                     stack.id, resource_id,
                                                     CONVERGE_RESPONSE.FAILED)

        stack.rpc_client.notify_resource_observed(
            context, incoming_req_id, stack_id,
            resource_id, CONVERGE_RESPONSE.OK)

