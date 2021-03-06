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

from heat.engine.resources import autoscaling as asc
from heat.tests import common


class TestCapacityChanges(common.HeatTestCase):
    # below:
    # n CHANGE_IN_CAPACITY (+up, -down)
    # b bounded
    # r rounded (+up, -down)
    # e EXACT_CAPACITY
    # p PERCENT_CHANGE_IN_CAPACITY
    scenarios = [
        ('+n', dict(current=2, adjustment=3,
                    adjustment_type=asc.CHANGE_IN_CAPACITY,
                    minimum=0, maximum=10, expected=5)),
        ('-n', dict(current=6, adjustment=-2,
                    adjustment_type=asc.CHANGE_IN_CAPACITY,
                    minimum=0, maximum=5, expected=4)),
        ('+nb', dict(current=2, adjustment=8,
                     adjustment_type=asc.CHANGE_IN_CAPACITY,
                     minimum=0, maximum=5, expected=5)),
        ('-nb', dict(current=2, adjustment=-10,
                     adjustment_type=asc.CHANGE_IN_CAPACITY,
                     minimum=1, maximum=5, expected=1)),
        ('e', dict(current=2, adjustment=4,
                   adjustment_type=asc.EXACT_CAPACITY,
                   minimum=0, maximum=5, expected=4)),
        ('+eb', dict(current=2, adjustment=11,
                     adjustment_type=asc.EXACT_CAPACITY,
                     minimum=0, maximum=5, expected=5)),
        ('-eb', dict(current=4, adjustment=1,
                     adjustment_type=asc.EXACT_CAPACITY,
                     minimum=3, maximum=5, expected=3)),
        ('+p', dict(current=4, adjustment=50,
                    adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                    minimum=1, maximum=10, expected=6)),
        ('-p', dict(current=4, adjustment=-25,
                    adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                    minimum=1, maximum=10, expected=3)),
        ('+pb', dict(current=4, adjustment=100,
                     adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                     minimum=1, maximum=6, expected=6)),
        ('-pb', dict(current=6, adjustment=-50,
                     adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                     minimum=4, maximum=10, expected=4)),
        ('-p+r', dict(current=2, adjustment=-33,
                      adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                      minimum=0, maximum=10, expected=1)),
        ('+p+r', dict(current=1, adjustment=33,
                      adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                      minimum=0, maximum=10, expected=2)),
        ('-p-r', dict(current=2, adjustment=-66,
                      adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                      minimum=0, maximum=10, expected=1)),
        ('+p-r', dict(current=1, adjustment=225,
                      adjustment_type=asc.PERCENT_CHANGE_IN_CAPACITY,
                      minimum=0, maximum=10, expected=3)),
    ]

    def test_calc(self):
        self.assertEqual(self.expected,
                         asc._calculate_new_capacity(
                             self.current, self.adjustment,
                             self.adjustment_type,
                             self.minimum, self.maximum))
