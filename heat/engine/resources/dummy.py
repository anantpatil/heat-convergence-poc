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

import eventlet
import random
import string

from six.moves import xrange

from heat.common import exception
from heat.common.i18n import _
from heat.engine import attributes
from heat.engine import constraints
from heat.engine import properties
from heat.engine import resource
from heat.engine import support

from heat.openstack.common import log as logging

LOG = logging.getLogger(__name__)

class Dummy(resource.Resource):
    '''
    A dummy resource with random string as resource ID.
    This is for testing convergence PoC.
    '''

    support_status = support.SupportStatus(version='2014.1')

    PROPERTIES = (
        LENGTH, SEQUENCE, UPDATE_IN_PLACE_PROP, FAIL_PROP
    ) = (
        'name_len', 'sequence', 'update_in_place_prop', 'fail_prop',
    )

    ATTRIBUTES = (
        VALUE,
    ) = (
        'value',
    )

    properties_schema = {
        LENGTH: properties.Schema(
            properties.Schema.INTEGER,
            _('Length of the string to generate.'),
            default=32,
            constraints=[
                constraints.Range(1, 512),
            ]
        ),
        SEQUENCE: properties.Schema(
            properties.Schema.STRING,
            _('Sequence of characters to build the random string from.'),
            constraints=[
                constraints.AllowedValues(['lettersdigits', 'letters',
                                           'lowercase', 'uppercase',
                                           'digits', 'hexdigits',
                                           'octdigits']),
            ],
            default='hexdigits'
        ),
        UPDATE_IN_PLACE_PROP: properties.Schema(
            properties.Schema.STRING,
            _('Value which can be set or changed on stack update to trigger '
              'the resource for update in place.'),
            default='',
            update_allowed=True,
        ),
        FAIL_PROP: properties.Schema(
            properties.Schema.STRING,
            _('Value which can be set for resource to fail to test failure'
              'scenarios.'),
            default='no',
        ),
    }

    _sequences = {
        'lettersdigits': string.ascii_letters + string.digits,
        'letters': string.ascii_letters,
        'lowercase': string.ascii_lowercase,
        'uppercase': string.ascii_uppercase,
        'digits': string.digits,
        'hexdigits': string.digits + 'ABCDEF',
        'octdigits': string.octdigits
    }

    @staticmethod
    def random_string(sequence, length):
        rand = random.SystemRandom()
        return ''.join(rand.choice(sequence) for x in xrange(length))

    def validate(self):
        pass

    def _get_random_sleep_secs(self):
        return random.randint(0, 1)
    
    def handle_create(self):
        length = self.properties.get(self.LENGTH)
        fail_prop = self.properties.get(self.FAIL_PROP)
        sequence = self.properties.get(self.SEQUENCE)

        char_seq = self._sequences[sequence]
        random_string = self.random_string(char_seq, length)

        self.data_set('value', random_string, redact=True)
        self.resource_id_set(random_string)
        
        # sleep for random time 
        sleep_secs = self._get_random_sleep_secs()
        LOG.debug("Resource %s sleeping for %s seconds", self.name, sleep_secs)
        eventlet.sleep(sleep_secs)
       
        # emulate failure 
        if fail_prop.lower() == 'yes' or fail_prop.lower() == 'y':
            raise Exception("Dummy failed %s", self.name)
            
    def handle_update(self, json_snippet=None, tmpl_diff=None, prop_diff=None):
        fail_prop = self.properties.get(self.FAIL_PROP)
        sleep_secs = self._get_random_sleep_secs()
        LOG.debug("Update of Resource %s sleeping for %s seconds", self.name, sleep_secs)
        eventlet.sleep(sleep_secs)
       
        # emulate failure 
        if fail_prop.lower() == 'yes' or fail_prop.lower() == 'y':
            raise Exception("Dummy failed %s", self.name)
        
    def handle_delete(self):
        sleep_secs = self._get_random_sleep_secs()
        LOG.debug("Delete of Resource %s sleeping for %s seconds", self.name, sleep_secs)
        eventlet.sleep(sleep_secs)

    def _resolve_attribute(self, name):
        if name == self.VALUE:
            return self.data().get(self.VALUE)


def resource_mapping():
    return {
        'OS::Heat::Dummy': Dummy,
    }
