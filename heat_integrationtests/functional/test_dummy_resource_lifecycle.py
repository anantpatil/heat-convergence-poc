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

import logging

from heatclient import exc as heat_exc

from heat_integrationtests.common import test


LOG = logging.getLogger(__name__)


class CreateDeleteStackTest(test.HeatIntegrationTest):
    template = '''
heat_template_version: 2013-05-23
resources:
  dummy1:
    type: OS::Heat::Dummy
    properties:
      name_len: 32
      max_wait_secs: 0.2
'''

    def setUp(self):
        super(CreateDeleteStackTest, self).setUp()
        self.client = self.orchestration_client

    def list_resources(self, stack_identifier):
        resources = self.client.resources.list(stack_identifier)
        return dict((r.resource_name, r.resource_type) for r in resources)

    def test_create_stack(self):
        stack_name = self._stack_rand_name()
        self.client.stacks.create(
            stack_name=stack_name,
            template=self.template,
            files={},
            disable_rollback=True,
            parameters={},
            environment={}
        )
        self.addCleanup(self.client.stacks.delete, stack_name)

        stack = self.client.stacks.get(stack_name)
        stack_identifier = '%s/%s' % (stack_name, stack.id)

        self._wait_for_stack_status(stack_identifier, 'CREATE_COMPLETE')
        # check stack resources
        expected_resources = {'dummy1': 'OS::Heat::Dummy'}
        self.assertEqual(expected_resources,
                         self.list_resources(stack_identifier))

    def test_create_stack_same_name(self):
        stack_name = self._stack_rand_name()
        self.client.stacks.create(
            stack_name=stack_name,
            template=self.template,
            files={},
            disable_rollback=True,
            parameters={},
            environment={}
        )
        self.addCleanup(self.client.stacks.delete, stack_name)

        stack = self.client.stacks.get(stack_name)
        stack_identifier = '%s/%s' % (stack_name, stack.id)

        self._wait_for_stack_status(stack_identifier, 'CREATE_COMPLETE')
        self.assertRaises(heat_exc.HTTPConflict,
                          self.client.stacks.create,
                          stack_name=stack_name,
                          template=self.template)

class UpdateStackTest(test.HeatIntegrationTest):
    template = '''
heat_template_version: 2013-05-23
resources:
  dummy1:
    type: OS::Heat::Dummy
    properties:
      name_len: 32
      max_wait_secs: 0.2
'''

    def setUp(self):
        super(UpdateStackTest, self).setUp()
        self.client = self.orchestration_client

    def update_stack(self, stack_identifier, template):
        stack_name = stack_identifier.split('/')[0]
        self.client.stacks.update(
            stack_id=stack_identifier,
            stack_name=stack_name,
            template=template,
            files={},
            disable_rollback=True,
            parameters={},
            environment={}
        )
        self._wait_for_stack_status(stack_identifier, 'UPDATE_COMPLETE')

    def list_resources(self, stack_identifier):
        resources = self.client.resources.list(stack_identifier)
        return dict((r.resource_name, r.resource_type) for r in resources)

    def _create_and_verify_stack(self, stack_name, template=None,
                                 verify_create=True):
        self.client.stacks.create(
            stack_name=stack_name,
            template=template if template else self.template,
            files={},
            disable_rollback=True,
            parameters={},
            environment={}
        )
        self.addCleanup(self.client.stacks.delete, stack_name)

        stack = self.client.stacks.get(stack_name)
        stack_identifier = '%s/%s' % (stack_name, stack.id)

        if verify_create:
            self._wait_for_stack_status(stack_identifier, 'CREATE_COMPLETE')
            # check stack resources
            expected_resources = {'dummy1': 'OS::Heat::Dummy'}
            self.assertEqual(expected_resources,
                             self.list_resources(stack_identifier))
        return stack_identifier

    def test_update_stack_create_in_progress(self):
        template = '''
heat_template_version: 2013-05-23
resources:
  dummy2:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 6
'''
        stack_name = self._stack_rand_name()
        stack_identifier = self._create_and_verify_stack(stack_name,
                                                         template=template,
                                                         verify_create=False)

        u_template = '''
heat_template_version: 2013-05-23
resources:
  dummy2:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 0.2
'''
        # Update with resource property changes
        self.update_stack(stack_identifier, u_template)
        expected_resources = {'dummy2': 'OS::Heat::Dummy'}
        self.assertEqual(expected_resources,
                         self.list_resources(stack_identifier))

    def test_update_stack(self):
        stack_name = self._stack_rand_name()
        stack_identifier = self._create_and_verify_stack(stack_name)

        u_template = '''
heat_template_version: 2013-05-23
resources:
  dummy1:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 0.2
'''
        # Update with resource property changes
        self.update_stack(stack_identifier, u_template)
        expected_resources = {'dummy1': 'OS::Heat::Dummy'}
        self.assertEqual(expected_resources,
                         self.list_resources(stack_identifier))

    def test_update_stack_nochange(self):
        stack_name = self._stack_rand_name()
        stack_identifier = self._create_and_verify_stack(stack_name)

        # Update with no changes, resources should be unchanged
        self.update_stack(stack_identifier, self.template)
        expected_resources = {'dummy1': 'OS::Heat::Dummy'}
        self.assertEqual(expected_resources,
                         self.list_resources(stack_identifier))

    def test_update_stack_add_resource(self):
        stack_name = self._stack_rand_name()
        stack_identifier = self._create_and_verify_stack(stack_name)

        # add a new resource in update
        template = '''
heat_template_version: 2013-05-23
resources:
  dummy1:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 0.2
  dummy2:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 0.2
'''
        self.update_stack(stack_identifier, template)
        expected_resources = {'dummy1': 'OS::Heat::Dummy',
                              'dummy2': 'OS::Heat::Dummy'}
        self.assertEqual(expected_resources,
                         self.list_resources(stack_identifier))

    def test_update_stack_concurrent(self):
        stack_name = self._stack_rand_name()
        stack_identifier = self._create_and_verify_stack(stack_name)

        # Update with an additional resource
        template1 = '''
heat_template_version: 2013-05-23
resources:
  dummy1:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 0.2
  dummy2:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 10
'''
        stack_name = stack_identifier.split('/')[0]
        self.client.stacks.update(
            stack_id=stack_identifier,
            stack_name=stack_name,
            template=template1,
            files={},
            disable_rollback=True,
            parameters={},
            environment={}
        )

        # Update with a single (new) resource.
        template2 = '''
heat_template_version: 2013-05-23
resources:
  dummy3:
    type: OS::Heat::Dummy
    properties:
      name_len: 64
      max_wait_secs: 0.2
'''
        self.update_stack(stack_identifier, template2)
        expected_resources = {'dummy3': 'OS::Heat::Dummy'}
        self.assertEqual(expected_resources,
                         self.list_resources(stack_identifier))
