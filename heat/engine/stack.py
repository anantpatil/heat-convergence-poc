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

import collections
from oslo.utils import timeutils
import re

from oslo.config import cfg
from oslo.utils import encodeutils
from osprofiler import profiler
import six

from heat.common import context as common_context
from heat.common import exception
from heat.common.exception import StackValidationFailed
from heat.common.i18n import _
from heat.common.i18n import _LE
from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.common import identifier
from heat.db import api as db_api
from heat.engine import dependencies
from heat.engine import function
from heat.engine.notification import stack as notification
from heat.engine.parameter_groups import ParameterGroups
from heat.engine import resource
from heat.engine import resources
from heat.engine import scheduler
from heat.engine.template import Template
from heat.openstack.common import log as logging
from heat.openstack.common import uuidutils
from heat.rpc import client as rpc_client

LOG = logging.getLogger(__name__)

ERROR_WAIT_TIME = 240


class ForcedCancel(BaseException):
    """Exception raised to cancel task execution."""

    def __str__(self):
        return "Operation cancelled"

class TASK_STATUS:
    UN_SCHEDULED = 0 # Task pending to be taken-up
    SCHEDULED = 1    # Task running
    DONE = 2         # Task done

class CONVERGE_RESPONSE:
    OK = 1      # Resource converged
    PANIC = 2   # Woker detects new update to stack, panics
    FAILED = 3  # Attempted, but failed

class Stack(collections.Mapping):

    ACTIONS = (
        CREATE, DELETE, UPDATE, ROLLBACK, SUSPEND, RESUME, ADOPT,
        SNAPSHOT, CHECK, RESTORE
    ) = (
        'CREATE', 'DELETE', 'UPDATE', 'ROLLBACK', 'SUSPEND', 'RESUME', 'ADOPT',
        'SNAPSHOT', 'CHECK', 'RESTORE'
    )

    STATUSES = (IN_PROGRESS, GC_IN_PROGRESS, FAILED, COMPLETE
                ) = ('IN_PROGRESS', 'GC_IN_PROGRESS', 'FAILED', 'COMPLETE')

    _zones = None

    def __init__(self, context, stack_name, tmpl,
                 stack_id=None, action=None, status=None,
                 status_reason='', timeout_mins=None, resolve_data=True,
                 disable_rollback=True, parent_resource=None, owner_id=None,
                 adopt_stack_data=None, stack_user_project_id=None,
                 created_time=None, updated_time=None,
                 user_creds_id=None, tenant_id=None,
                 use_stored_context=False, username=None, request_id=None):
        '''
        Initialise from a context, name, Template object and (optionally)
        Environment object. The database ID may also be initialised, if the
        stack is already in the database.
        '''

        if owner_id is None:
            if re.match("[a-zA-Z][a-zA-Z0-9_.-]*$", stack_name) is None:
                raise ValueError(_('Invalid stack name %s'
                                   ' must contain only alphanumeric or '
                                   '\"_-.\" characters, must start with alpha'
                                   ) % stack_name)

        self.id = stack_id
        self.owner_id = owner_id
        self.context = context
        self.t = tmpl
        self.name = stack_name
        self.action = self.CREATE if action is None else action
        self.status = self.IN_PROGRESS if status is None else status
        self.status_reason = status_reason
        self.timeout_mins = timeout_mins
        self.disable_rollback = disable_rollback
        self.parent_resource = parent_resource
        self._resources = None
        self._rsrc_defns = None
        self._dependencies = None
        self._access_allowed_handlers = {}
        self._db_resources = None
        self.adopt_stack_data = adopt_stack_data
        self.stack_user_project_id = stack_user_project_id
        self.created_time = created_time
        self.updated_time = updated_time
        self.user_creds_id = user_creds_id
        self.rpc_client = rpc_client.EngineClient()
        self._request_id = request_id or uuidutils.generate_uuid()

        if use_stored_context:
            self.context = self.stored_context()

        self.clients = self.context.clients

        # This will use the provided tenant ID when loading the stack
        # from the DB or get it from the context for new stacks.
        self.tenant_id = tenant_id or self.context.tenant_id
        self.username = username or self.context.username

        resources.initialise()

        self.env = self.t.env
        self.parameters = self.t.parameters(self.identifier(),
                                            user_params=self.env.params)
        # store the parameters with template
        # self.t.env = self.parameters
        # LOG.debug("==== Stack parameters %s", str(self.parameters))
        # for k, v in self.parameters.items():
            #LOG.debug("==== %s : %s", str(k), str(v))

        # LOG.debug("==== Stack env %s", self.env.user_env_as_dict())

        self._set_param_stackid()

        if resolve_data:
            self.outputs = self.resolve_static_data(self.t[self.t.OUTPUTS])
        else:
            self.outputs = {}

    def resolve_static_data(self, snippet):
        return self.t.parse(self, snippet)

    def stored_context(self):
        if self.user_creds_id:
            creds = db_api.user_creds_get(self.user_creds_id)
            # Maintain request_id from self.context so we retain traceability
            # in situations where servicing a request requires switching from
            # the request context to the stored context
            creds['request_id'] = self.context.request_id
            # We don't store roles in the user_creds table, so disable the
            # policy check for admin by setting is_admin=False.
            creds['is_admin'] = False
            return common_context.RequestContext.from_dict(creds)
        else:
            msg = _("Attempt to use stored_context with no user_creds")
            raise exception.Error(msg)

    @property
    def resources(self, load_from_db=True):
        if self._resources is None:
            self._resources = dict((name, resource.Resource(name, data, self, load_from_db=load_from_db))
                                   for (name, data) in
                                   self.t.resource_definitions(self).items())
            # There is no need to continue storing the db resources
            # after resource creation
            self._db_resources = None
        return self._resources

    def iter_resources(self, nested_depth=0):
        '''
        Iterates over all the resources in a stack, including nested stacks up
        to `nested_depth` levels below.
        '''
        for res in self.values():
            yield res

            get_nested = getattr(res, 'nested', None)
            if not callable(get_nested) or nested_depth == 0:
                continue

            nested_stack = get_nested()
            if nested_stack is None:
                continue

            for nested_res in nested_stack.iter_resources(nested_depth - 1):
                yield nested_res

    def db_resource_get(self, name):
        if not self.id:
            return None
        if self._db_resources is None:
            try:
                self._db_resources = db_api.resource_get_all_by_stack(
                    self.context, self.id)
            except exception.NotFound:
                return None
        return self._db_resources.get(name)

    @property
    def dependencies(self):
        if self._dependencies is None:
            self._dependencies = self._get_dependencies(
                self.resources.itervalues())
        return self._dependencies

    def get_deps_from_current_template(self):
        return self._get_dependencies(self.resources.itervalues())

    def get_dependencies_from_db(self):
        deps = db_api.graph_get_all_by_stack(self.context, self.id)
        return deps

    def _store_edges(self, resource_name, required_by, template_id):
        value = {'resource_name': resource_name, 'stack_id': self.id,
                 'template_id':template_id}
        if required_by:
            for req in required_by:
                value['needed_by'] = req
                db_api.graph_insert_edge(self.context, value)
        else:
            db_api.graph_insert_edge(self.context, value)

    def _update_edges(self, resource_name, required_by, prev_template_id,
                      new_template_id):
        value = {'resource_name': resource_name, 'stack_id': self.id,
                 'template_id':prev_template_id}
        if required_by:
            for req in required_by:
                value['needed_by'] = req
                db_api.graph_update_edge(self.context, value, new_template_id)
        else:
            db_api.graph_update_edge(self.context, value, new_template_id)

    def update_dependencies(self):
        new_deps = self.get_deps_from_current_template()
        previous_template_id = db_api.raw_template_get(self.context,
                                    self.t.id).predecessor
        # stack.store method is called multiple times in stack create.
        # Remove below two lines when the calls are fixed.
        if not previous_template_id:
            previous_template_id = self.t.id

        with db_api.transaction(self.context):
            for res in new_deps:
                new_required_by = set([req.name for req in
                                   new_deps.required_by(res)])
                old_required_by = set(db_api.get_resource_required_by(self.context,
                                                                  self.id,
                                                                  res.name,
                                                                  previous_template_id))
                resource_exists = db_api.resource_exists_in_graph(self.context,
                                                                  self.id,
                                                                  res.name,
                                                                  previous_template_id)

                # if it is a new resource then insert all the edges for
                # this resource
                if not resource_exists:
                    self._store_edges(res.name, new_required_by, self.t.id)
                elif new_required_by == old_required_by:
                    # Just update the template ID
                    self._update_edges(res.name, new_required_by, previous_template_id, self.t.id)
                else:
                    # edges got updated
                    if new_required_by & old_required_by:
                        # few edges are overlapping, few are new
                        unchanged_edges = old_required_by & new_required_by
                        new_edges_added = new_required_by - old_required_by
                        self._update_edges(res.name, unchanged_edges, previous_template_id, self.t.id)
                        self._store_edges(res.name, new_edges_added, self.t.id)
                    else:
                        # no overlapping edges, all new
                        self._store_edges(res.name, new_required_by, self.t.id)

    def _set_param_stackid(self):
        '''
        Update self.parameters with the current ARN which is then provided
        via the Parameters class as the StackId pseudo parameter
        '''
        if not self.parameters.set_stack_id(self.identifier()):
            LOG.warn(_LW("Unable to set parameters StackId identifier"))

    @staticmethod
    def _get_dependencies(resources):
        '''Return the dependency graph for a list of resources.'''
        deps = dependencies.Dependencies()
        for resource in resources:
            resource.add_dependencies(deps)

        return deps

    @classmethod
    def load(cls, context, stack_id=None, stack=None, parent_resource=None,
             show_deleted=True, use_stored_context=False):
        '''Retrieve a Stack from the database.'''
        if stack is None:
            stack = db_api.stack_get(context, stack_id,
                                     show_deleted=show_deleted,
                                     eager_load=True)
        if stack is None:
            message = _('No stack exists with id "%s"') % str(stack_id)
            raise exception.NotFound(message)

        return cls._from_db(context, stack, parent_resource=parent_resource,
                            use_stored_context=use_stored_context)

    @classmethod
    def load_all(cls, context, limit=None, marker=None, sort_keys=None,
                 sort_dir=None, filters=None, tenant_safe=True,
                 show_deleted=False, resolve_data=True,
                 show_nested=False):
        stacks = db_api.stack_get_all(context, limit, sort_keys, marker,
                                      sort_dir, filters, tenant_safe,
                                      show_deleted, show_nested) or []
        for stack in stacks:
            yield cls._from_db(context, stack, resolve_data=resolve_data)

    def _load_all_children(self, parents):
        """
        Load the dependents of resource with given res_name along with the
        resource. All immediate children are loaded.
        """
        if self._resources:
            return self._resources
        self._resources = dict()
        all_children = []
        for parent in parents:
            all_children += db_api.get_res_children(self.context, self.t.id, parent)
        resource_definitions = self.t.resource_definitions(self)
        for name in all_children:
            data = resource_definitions[name]
            self._resources[name] = resource.Resource(name, data, self,
                                                      load_from_db=True)
        # load the parents, they should not be in DB
        for name in parents:
            data = resource_definitions[name]
            self._resources[name] = resource.Resource(name, data, self)
        # There is no need to continue storing the db resources
        # after resource creation
        self._db_resources = None
        return self._resources

    @classmethod
    def _from_db(cls, context, stack, parent_resource=None, resolve_data=True,
                 use_stored_context=False):
        template = Template.load(
            context, stack.raw_template_id, stack.raw_template)
        return cls(context, stack.name, template,
                   stack.id, stack.action, stack.status, stack.status_reason,
                   stack.timeout, resolve_data, stack.disable_rollback,
                   parent_resource, owner_id=stack.owner_id,
                   stack_user_project_id=stack.stack_user_project_id,
                   created_time=stack.created_at,
                   updated_time=stack.updated_at,
                   user_creds_id=stack.user_creds_id, tenant_id=stack.tenant,
                   use_stored_context=use_stored_context,
                   username=stack.username, request_id=stack.req_id)

    @profiler.trace('Stack.store', hide_args=False)
    def store(self, backup=False):
        '''
        Store the stack in the database and return its ID
        If self.id is set, we update the existing stack
        '''
        s = {
            'name': self.name,
            'raw_template_id': self.t.store(self.context),
            'owner_id': self.owner_id,
            'username': self.username,
            'tenant': self.tenant_id,
            'action': self.action,
            'status': self.status,
            'status_reason': self.status_reason,
            'timeout': self.timeout_mins,
            'disable_rollback': self.disable_rollback,
            'stack_user_project_id': self.stack_user_project_id,
            'updated_at': self.updated_time,
            'user_creds_id': self.user_creds_id,
            'req_id': self.current_req_id(),
            'backup': backup
        }
        if self.id:
            db_api.stack_update(self.context, self.id, s)
        else:
            if not self.user_creds_id:
                # Create a context containing a trust_id and trustor_user_id
                # if trusts are enabled
                if cfg.CONF.deferred_auth_method == 'trusts':
                    keystone = self.clients.client('keystone')
                    trust_ctx = keystone.create_trust_context()
                    new_creds = db_api.user_creds_create(trust_ctx)
                else:
                    new_creds = db_api.user_creds_create(self.context)
                s['user_creds_id'] = new_creds.id
                self.user_creds_id = new_creds.id

            new_s = db_api.stack_create(self.context, s)
            self.id = new_s.id
            self.created_time = new_s.created_at

        self.update_dependencies()
        self._set_param_stackid()

        return self.id

    def identifier(self):
        '''
        Return an identifier for this stack.
        '''
        return identifier.HeatIdentifier(self.tenant_id, self.name, self.id)

    def __iter__(self):
        '''
        Return an iterator over the resource names.
        '''
        return iter(self.resources)

    def __len__(self):
        '''Return the number of resources.'''
        return len(self.resources)

    def __getitem__(self, key):
        '''Get the resource with the specified name.'''
        return self.resources[key]

    def add_resource(self, resource):
        '''Insert the given resource into the stack.'''
        template = resource.stack.t
        resource.stack = self
        definition = resource.t.reparse(self, template)
        resource.t = definition
        resource.reparse()
        self.resources[resource.name] = resource
        self.t.add_resource(definition)
        if self.t.id is not None:
            self.t.store(self.context)

    def remove_resource(self, resource_name):
        '''Remove the resource with the specified name.'''
        del self.resources[resource_name]
        self.t.remove_resource(resource_name)
        if self.t.id is not None:
            self.t.store(self.context)

    def __contains__(self, key):
        '''Determine whether the stack contains the specified resource.'''
        if self._resources is not None:
            return key in self.resources
        else:
            return key in self.t[self.t.RESOURCES]

    def __eq__(self, other):
        '''
        Compare two Stacks for equality.

        Stacks are considered equal only if they are identical.
        '''
        return self is other

    def __str__(self):
        '''Return a human-readable string representation of the stack.'''
        text = 'Stack "%s" [%s]' % (self.name, self.id)
        return encodeutils.safe_encode(text)

    def __unicode__(self):
        '''Return a human-readable string representation of the stack.'''
        text = 'Stack "%s" [%s]' % (self.name, self.id)
        return encodeutils.safe_encode(text)

    @profiler.trace('Stack.validate', hide_args=False)
    def validate(self):
        '''
        Validates the template.
        '''
        # TODO(sdake) Should return line number of invalid reference

        # validate overall template (top-level structure)
        self.t.validate()

        # Validate parameters
        self.parameters.validate(context=self.context)

        # Validate Parameter Groups
        parameter_groups = ParameterGroups(self.t)
        parameter_groups.validate()

        # Check duplicate names between parameters and resources
        dup_names = set(self.parameters.keys()) & set(self.keys())

        if dup_names:
            LOG.debug("Duplicate names %s" % dup_names)
            raise StackValidationFailed(message=_("Duplicate names %s") %
                                                dup_names)

        for res in self.dependencies:
            try:
                result = res.validate()
            except exception.HeatException as ex:
                LOG.info(ex)
                raise ex
            except Exception as ex:
                LOG.exception(ex)
                raise StackValidationFailed(message=encodeutils.safe_decode(
                    six.text_type(ex)))
            if result:
                raise StackValidationFailed(message=result)

            for val in self.outputs.values():
                try:
                    if isinstance(val, six.string_types):
                        message = _('"Outputs" must contain '
                                    'a map of output maps, '
                                    'find a string "%s".') % val
                        raise StackValidationFailed(message=message)
                    if not val or not val.get('Value', ''):
                        msg = _('Every Output object must '
                                'contain a Value member.')
                        raise StackValidationFailed(message=msg)
                    function.validate(val.get('Value', ''))
                except Exception as ex:
                    reason = 'Output validation error: %s' % six.text_type(ex)
                    raise StackValidationFailed(message=reason)

    @profiler.trace('Stack.state_set', hide_args=False)
    def state_set(self, action, status, reason):
        '''Update the stack state in the database.'''
        if action not in self.ACTIONS:
            raise ValueError(_("Invalid action %s") % action)

        if status not in self.STATUSES:
            raise ValueError(_("Invalid status %s") % status)

        self.action = action
        self.status = status
        self.status_reason = reason

        if self.id is None:
            return

        stack = db_api.stack_get(self.context, self.id)
        if stack is not None:
            stack.update_and_save({'action': action,
                                   'status': status,
                                   'status_reason': reason})
            LOG.info(_LI('Stack %(action)s %(status)s (%(name)s): '
                         '%(reason)s'),
                     {'action': action,
                      'status': status,
                      'name': self.name,
                      'reason': reason})
            notification.send(self)

    @property
    def state(self):
        '''Returns state, tuple of action, status.'''
        return (self.action, self.status)

    def timeout_secs(self):
        '''
        Return the stack action timeout in seconds.
        '''
        if self.timeout_mins is None:
            return cfg.CONF.stack_action_timeout

        return self.timeout_mins * 60

    def _schedule_create_job(self, res_names, curr_template_id):
        for res_name in res_names:
            res = self.resources[res_name]
            res.store_update(res.CREATE, res.SCHEDULED, "Scheduled for create")
            self.rpc_client.converge_resource(self.context, self.current_req_id(),
                                              self.id, curr_template_id, res.id,
                                              self._get_remaining_timeout_secs())
            self._mark_task_as_scheduled(res_name, template_id=self.t.id)
        return True

    def _resource_exists_in_current_template(self, res_name):
        return self.resources.has_key(res_name)

    def _schedule_update_job_if_needed(self, res_names, curr_template_id):
        """
        Look back for any matching resource definitions.
        :param res_name:
        :return:
        """
        scheduled_a_job = False
        for res_name in res_names:
            all_versions = resource.Resource.load_all_versions(self.context,
                                                               res_name, self)
            if self._resource_exists_in_current_template(res_name):
                new_res = self.resources[res_name]
                for old_res in all_versions.values():
                    if old_res.matches_definitions(new_res.frozen_definition()):
                        # This is the one, update the template ID of this res
                        old_res.template_id = new_res.template_id
                        old_res.store_update(old_res.action, old_res.status,
                                             'Update not needed')
                        self._mark_task_as_done(res_name, curr_template_id)
                        break
                else:
                    # update or create. Load children resources to satisfy any get_attr
                    # dependency for this resource
                    if all_versions:
                        curr_res = all_versions[self.t.predecessor]
                        new_res.store()
                        new_res.state_set(curr_res.action, curr_res.status,
                                          "Scheduled for update.")
                        curr_res.state_set(curr_res.UPDATE, curr_res.SCHEDULED,
                                          "Updating resource")
                        self.rpc_client.converge_resource(self.context,
                                                          self.current_req_id(),
                                                          self.id, curr_template_id, curr_res.id,
                                                          self._get_remaining_timeout_secs())
                        self._mark_task_as_scheduled(res_name, template_id=self.t.id)
                        scheduled_a_job = True
                    else:
                        # create new version
                        new_res.store_update(new_res.CREATE, new_res.SCHEDULED,
                                             "Creating resource")
                        self.rpc_client.converge_resource(
                            self.context, self.current_req_id(), self.id, curr_template_id,
                            new_res.id, self._get_remaining_timeout_secs())
                        self._mark_task_as_scheduled(res_name, template_id=self.t.id)
                        scheduled_a_job = True
            else:
                # No-op. Will be deleted in resource clean-up phase
                self._mark_task_as_done(res_name, template_id=self.t.id)
        return scheduled_a_job

    def _schedule_delete_job(self, res_names, template_id):
        scheduled_a_job = False
        for res_name in res_names:
            db_res = db_api.resource_get_by_name_and_template(self.context,
                                                              res_name,
                                                              template_id=template_id)
            if db_res:
                res = resource.Resource.load(self, db_res.id, db_resource=db_res)
                res.store_update(res.DELETE, res.SCHEDULED, "Scheduled for deletion")
                self.rpc_client.converge_resource( self.context,
                                                   self.current_req_id(),
                                                   self.id, template_id, res.id,
                                                   self._get_remaining_timeout_secs())
                self._mark_task_as_scheduled(res_name, template_id)
                scheduled_a_job = True
            else:
                self._mark_task_as_done(res_name, template_id)
        return scheduled_a_job

    def _schedule_gc_job(self, res_names, template_id):
        scheduled_a_job = False
        for res_name in res_names:
            if template_id != self.t.id:
                # Resource clean-up for older template
                name_list = [res_name]
                scheduled_a_job = self._schedule_delete_job(name_list, template_id)
            else:
                # search for older versions and delete
                all_versions = resource.Resource.load_all_versions(self.context,
                                                                   res_name, self)
                if len(all_versions) == 1:
                    # no older versions to be deleted
                    self._mark_task_as_done(res_name, template_id)
                    continue
                for res in all_versions.values():
                    # del if resource is not current
                    if res.template_id != self.t.id:
                        res.store_update(res.DELETE, res.SCHEDULED, "Scheduled for deletion")
                        self.rpc_client.converge_resource( self.context,
                                                           self.current_req_id(),
                                                           self.id, template_id, res.id,
                                                           self._get_remaining_timeout_secs())
                        scheduled_a_job = True
                        self._mark_task_as_scheduled(res_name, template_id)
        return scheduled_a_job

    def _mark_task_as_scheduled(self, res_name, template_id):
        db_api.update_graph_traversal(self.context, self.id,
                                      TASK_STATUS.SCHEDULED,
                                      resource_name=res_name, template_id=template_id)

    def _mark_task_as_done(self, res_name, template_id):
        db_api.update_graph_traversal(self.context, self.id,
                                      TASK_STATUS.DONE,
                                      resource_name=res_name, template_id=template_id)

    def _filter_tasks_with_previous_versions_scheduled(self, ready_nodes):
        # to avoid processing a resource having its older version in in progress
        in_progress_filters = {'status': resource.Resource.IN_PROGRESS}
        scheduled_filters = {'status': resource.Resource.SCHEDULED}
        try:
            result = db_api.resource_get_all_by_stack(
                self.context, self.id, filters=in_progress_filters)
            in_progress_resources = result.values()
        except exception.NotFound:
            in_progress_resources = []

        try:
            result = db_api.resource_get_all_by_stack(
                self.context, self.id, filters=scheduled_filters)
            scheduled_resources = result.values()
        except exception.NotFound:
            scheduled_resources = []

        all_scheduled_resources = in_progress_resources + scheduled_resources

        nodes = [(name, status)
                 for name, status in ready_nodes
                 if name not in all_scheduled_resources
        ]
        return nodes

    def _schedule_convg_jobs(self, ready_nodes, curr_template_id):
        scheduled_a_job = False
        res_names = [res_name for res_name,_ in ready_nodes]
        # load the resource with their children to get realized rsrc_defn
        if not self._destructive_action_in_progress():
            parents = [res_name for res_name,_ in ready_nodes]
            self._load_all_children(parents)
        if (self.action, self.status) ==  (Stack.CREATE, Stack.IN_PROGRESS):
            scheduled_a_job = self._schedule_create_job(res_names, curr_template_id)
        elif (self.action, self.status) in ((Stack.UPDATE, Stack.IN_PROGRESS),
                                            (Stack.ROLLBACK, Stack.IN_PROGRESS)):
            scheduled_a_job = self._schedule_update_job_if_needed(res_names,
                                                                  curr_template_id)
        elif (self.action, self.status) == (Stack.DELETE, Stack.IN_PROGRESS):
            scheduled_a_job = self._schedule_delete_job(res_names, curr_template_id)
        elif (self.action, self.status) in ((Stack.UPDATE, Stack.GC_IN_PROGRESS),
                                            (Stack.ROLLBACK, Stack.GC_IN_PROGRESS)):
            scheduled_a_job = self._schedule_gc_job(res_names, curr_template_id)
        else:
            # TODO:
            raise Exception("Stack %s in unknown state: %s, %s", self.name,
                            self.action, self.status)

        return scheduled_a_job

    def _handle_stack_action_complete(self):
        LOG.debug("==== handle stack action complete")
        reason = "%s complete" % self.action
        if self._destructive_action_in_progress():
            if self.action == self.DELETE:
                self.delete_complete()
            else:
                self.state_set(self.action, self.COMPLETE, reason)
                self._purge_edges()
        else:
            self.state_set(self.action, self.COMPLETE, reason)
        self._purge_old_templates()

    def _purge_old_templates(self):
        all_previous_template_ids = self._get_all_previous_template_ids()
        if not all_previous_template_ids:
            return
        # unlink current from previous to avoid Foriegn Key error
        self.t.predecessor = None
        self.t.store()
        # Delete all previous templates
        for id in all_previous_template_ids:
            db_api.raw_template_delete(self.context, id)

    def _get_initial_template_id(self):
        """
        Initial template is the once with which update started. It
        won't have a predecessor.
        :return:
        """
        initial_template = self.t
        while initial_template.predecessor:
            initial_template = db_api.raw_template_get(self.context, initial_template.predecessor)
        return initial_template.id

    def _get_successor(self, template_id):
        """
        Get successor of this template in a sequence of concurrent update
        templates.
        :param template_id:
        :return:
        """
        successor = db_api.raw_template_get_by_predecessor(self.context, template_id)
        return successor.id if successor else None

    def _prepare_tasks(self, curr_template_id):
        """
        If update, current template is the template id.
        If GC_IN_PROGRESS, template ID is previous to current.
        :return:
        """
        if not curr_template_id:
            return [], [], curr_template_id

        reverse = self._is_traversal_order_reverse()
        running_or_unscheduled_tasks = db_api.get_ready_nodes(self.context,
                                                              self.id,
                                                              curr_template_id,
                                                              reverse)
        if not running_or_unscheduled_tasks:
            if (self.action, self.status) in ((self.CREATE, self.IN_PROGRESS),
                                              (self.DELETE, self.IN_PROGRESS)):
                return [], [], curr_template_id
            elif (self.action, self.status) in ((self.ROLLBACK, self.IN_PROGRESS),
                                                (self.UPDATE, self.IN_PROGRESS)):
                self.state_set(self.action, self.GC_IN_PROGRESS, 'Resource clean up running')
                db_api.update_graph_traversal(self.context, self.id,
                                              status=TASK_STATUS.UN_SCHEDULED)
                return self._prepare_tasks(self._get_initial_template_id())
            elif (self.action, self.status) in ((self.ROLLBACK, self.GC_IN_PROGRESS),
                                                (self.UPDATE, self.GC_IN_PROGRESS)):
                return self._prepare_tasks(self._get_successor(curr_template_id))

        unscheduled_tasks= self._get_tasks_matching_status(running_or_unscheduled_tasks,
                                                           TASK_STATUS.UN_SCHEDULED)
        running_tasks = set(running_or_unscheduled_tasks) - set(unscheduled_tasks)
        return unscheduled_tasks, running_tasks, curr_template_id

    def _trigger_convergence(self, curr_template_id=None,
                             concurrent_update_on=False):
        if self._get_remaining_timeout_secs() <= 0:
            self.handle_timeout()
            return

        template_id = curr_template_id or self.t.id
        unscheduled_tasks, running_tasks, template_id = self._prepare_tasks(template_id)
        if unscheduled_tasks:
            if self.action in (self.UPDATE, self.ROLLBACK):
                # If and only if there is a concurrent update going, do the
                # following filtering to avoid scheduling converge jobs on
                # resources already being converged.
                unscheduled_tasks = self._filter_tasks_with_previous_versions_scheduled(unscheduled_tasks)
            if unscheduled_tasks:
                scheduled = self._schedule_convg_jobs(unscheduled_tasks, template_id)
                # make sure to schedule next set of nodes if in the previous run of this
                # did not yield any job.
                if not scheduled:
                    self._reset_resources()
                    self._trigger_convergence(curr_template_id=template_id,
                                              concurrent_update_on=concurrent_update_on)
            else:
                return
        else:
            if not running_tasks:
                self._handle_stack_action_complete()

    @scheduler.wrappertask
    def resource_action(self, r):
        # Find e.g resource.create and call it
        action_l = r.action.lower()
        handle = getattr(r, '%s' % action_l)

        # If a local _$action_kwargs function exists, call it to get the
        # action specific argument list, otherwise an empty arg list
        handle_kwargs = getattr(self,
                                '_%s_kwargs' % action_l, lambda x: {})
        yield handle(**handle_kwargs(r))

    def run_resource_action(self, resource_id, curr_template_id, timeout):
        rsrc = resource.Resource.load(self, resource_id)
        rsrc.update_to_template_id = curr_template_id
        LOG.debug("=====Resource name %s, from template %s", rsrc.name,
                           rsrc.template_id)
        action_task = scheduler.TaskRunner(
                            self.resource_action,
                            rsrc)
        action_task(timeout=timeout)

    @scheduler.wrappertask
    def resource_delete(self, rsrc_name):
        db_resources = db_api.resource_get_all_versions_by_name_and_stack(
            self.context, rsrc_name, self.id)
        for db_resource in db_resources:
            if db_resource.nova_instance:
                LOG.debug("==== Destroy resource %s ", rsrc_name)
                res = resource.Resource.load(self, db_resource.id,
                                             db_resource=db_resource)
                yield res.destroy()
            else:
                db_api.resource_delete(self.context, db_resource.id)

    def _reset_resources(self):
        if self._resources:
            self._resources.clear()

    @staticmethod
    @profiler.trace('Stack.update', hide_args=False)
    def update(curr_stack, new_stack=None, event=None, action=UPDATE):
        '''
        Compare the current stack with new_stack,
        and where necessary create/update/delete the resources until
        this stack aligns with new_stack.

        Note update of existing stack resources depends on update
        being implemented in the underlying resource types

        Update will fail if it exceeds the specified timeout. The default is
        60 minutes, set in the constructor
        '''
        concurrent_update_on = False
        if curr_stack.status in (curr_stack.IN_PROGRESS, curr_stack.GC_IN_PROGRESS):
            concurrent_update_on = True
        curr_stack.state_set(action, curr_stack.IN_PROGRESS,
                       'Stack %s started' % action)
        new_stack.id = curr_stack.id
        new_stack.action = action
        new_stack.status = curr_stack.IN_PROGRESS
        new_stack.status_reason = curr_stack.status_reason
        new_stack.created_time = curr_stack.created_time
        new_stack.updated_time = timeutils.utcnow()
        # update request ID
        new_stack._generate_new_req_id()
        new_stack.t.predecessor = curr_stack.t.id
        new_stack.store()
        # reset the resources
        new_stack._reset_resources()
        
        # Mark all nodes as UN_SCHEDULED
        db_api.update_graph_traversal(curr_stack.context, new_stack.id,
                                         status=TASK_STATUS.UN_SCHEDULED)

        curr_stack = None
        new_stack._trigger_convergence(concurrent_update_on=concurrent_update_on)

    @profiler.trace('Stack.delete', hide_args=False)
    def delete_start(self, action=DELETE, backup=False, abandon=False):
        '''
        Delete all of the resources, and then the stack itself.
        The action parameter is used to differentiate between a user
        initiated delete and an automatic stack rollback after a failed
        create, which amount to the same thing, but the states are recorded
        differently.

        Note abandon is a delete where all resources have been set to a
        RETAIN deletion policy, but we also don't want to delete anything
        required for those resources, e.g the stack_user_project.
        '''
        if action not in (self.DELETE, self.ROLLBACK):
            LOG.error(_LE("Unexpected action %s passed to delete!"), action)
            self.state_set(self.DELETE, self.FAILED,
                           "Invalid action %s" % action)
            return

        self.state_set(action, self.IN_PROGRESS, 'Stack %s started' %
                       action)
        # update request ID
        self._generate_new_req_id()
        self.store()
        db_api.update_graph_traversal(context=self.context,
                                         stack_id=self.id,
                                         status=TASK_STATUS.UN_SCHEDULED)
        self._trigger_convergence()

    def delete_complete(self, abandon=False):
        #(TODO) : Handle failure
        # If the stack delete succeeded, and it's
        # not a nested stack, we should delete the credentials
        if self.status != self.FAILED and not self.owner_id:
            # Cleanup stored user_creds so they aren't accessible via
            # the soft-deleted stack which remains in the DB
            if self.user_creds_id:
                user_creds = db_api.user_creds_get(self.user_creds_id)
                # If we created a trust, delete it
                if user_creds is not None:
                    trust_id = user_creds.get('trust_id')
                    if trust_id:
                        try:
                            # If the trustor doesn't match the context user the
                            # we have to use the stored context to cleanup the
                            # trust, as although the user evidently has
                            # permission to delete the stack, they don't have
                            # rights to delete the trust unless an admin
                            trustor_id = user_creds.get('trustor_user_id')
                            if self.context.user_id != trustor_id:
                                LOG.debug('Context user_id doesn\'t match '
                                          'trustor, using stored context')
                                sc = self.stored_context()
                                sc.clients.client('keystone').delete_trust(
                                    trust_id)
                            else:
                                self.clients.client('keystone').delete_trust(
                                    trust_id)
                        except Exception as ex:
                            LOG.exception(ex)
                            stack_status = self.FAILED
                            reason = ("Error deleting trust: %s" %
                                      six.text_type(ex))

                # Delete the stored credentials
                try:
                    db_api.user_creds_delete(self.context, self.user_creds_id)
                except exception.NotFound:
                    LOG.info(_LI("Tried to delete user_creds that do not "
                                 "exist (stack=%(stack)s user_creds_id="
                                 "%(uc)s)"),
                             {'stack': self.id, 'uc': self.user_creds_id})

                try:
                    self.user_creds_id = None
                    self.store()
                except exception.NotFound:
                    LOG.info(_LI("Tried to store a stack that does not exist "
                                 "%s "), self.id)

            # If the stack has a domain project, delete it
            if self.stack_user_project_id and not abandon:
                try:
                    keystone = self.clients.client('keystone')
                    keystone.delete_stack_domain_project(
                        project_id=self.stack_user_project_id)
                except Exception as ex:
                    LOG.exception(ex)
                    stack_status = self.FAILED
                    reason = "Error deleting project: %s" % six.text_type(ex)

            try:
                reason = 'Stack %s completed successfully' % self.action
                self.state_set(self.action, self.COMPLETE, reason)
            except exception.NotFound:
                LOG.info(_LI("Tried to delete stack that does not exist "
                             "%s "), self.id)

        if self.status != self.FAILED:
            # delete the stack resource graph
            LOG.debug("==== Deleting resource graph for %s", self.name)
            try:
                db_api.graph_delete(self.context, self.id)
            except Exception as e:
                LOG.info(_("Failed to delete stack %s resource graph."),
                         self.id)
            LOG.debug("==== Deleting stack for %s", self.name)
            # delete the stack
            try:
                db_api.stack_delete(self.context, self.id)
            except exception.NotFound:
                LOG.info(_LI("Tried to delete stack that does not exist "
                             "%s "), self.id)
            self.id = None

    @profiler.trace('Stack.output', hide_args=False)
    def output(self, key):
        '''
        Get the value of the specified stack output.
        '''
        value = self.outputs[key].get('Value', '')
        try:
            return function.resolve(value)
        except Exception as ex:
            self.outputs[key]['error_msg'] = six.text_type(ex)
            return None

    def set_stack_user_project_id(self, project_id):
        self.stack_user_project_id = project_id
        self.store()

    @profiler.trace('Stack.create_stack_user_project_id', hide_args=False)
    def create_stack_user_project_id(self):
        project_id = self.clients.client(
            'keystone').create_stack_domain_project(self.id)
        self.set_stack_user_project_id(project_id)

    def reset_resource_attributes(self):
        # nothing is cached if no resources exist
        if not self._resources:
            return
        # a change in some resource may have side-effects in the attributes
        # of other resources, so ensure that attributes are re-calculated
        for res in self.resources.itervalues():
            res.attributes.reset_resolved_values()

    @staticmethod
    def rollback(curr_stack):
        last_complete_template_id = curr_stack._get_initial_template_id()
        last_complete_raw_template = db_api.raw_template_get(curr_stack.context, last_complete_template_id)
        last_complete_template = Template.load(curr_stack.context,
                                               last_complete_raw_template.id,
                                               last_complete_raw_template)
        # set the pred of new template to current
        last_complete_template.predecessor = curr_stack.t.id
        # clear the link from successor template
        successor_tmpl_id = curr_stack._get_successor(last_complete_template_id)
        successor_tmpl = Template.load(curr_stack.context, successor_tmpl_id)
        successor_tmpl.predecessor = None
        successor_tmpl.store(curr_stack.context)
        last_complete_template.store(curr_stack.context)
        curr_stack._delete_all_edges(last_complete_template.id)
        new_stack = Stack(curr_stack.context, curr_stack.name, last_complete_template)
        Stack.update(curr_stack, new_stack, action=curr_stack.ROLLBACK)

    def _delete_all_edges(self, template_id):
        db_api.graph_delete_edges_for_template(self.context, template_id)

    def _purge_edges(self):
        """
        Called in the end to purge the edges from the graph. Edges of the resources 
        no longer in current stack.
        """
        all_previous_template_ids = self._get_all_previous_template_ids()
        db_api.dep_task_graph_delete_all_edges(self.context, self.id,
                                               all_previous_template_ids)

    def _get_all_previous_template_ids(self):
        predecessors = []
        template = self.t
        while template.predecessor:
            predecessors.append(template.predecessor)
            template = db_api.raw_template_get(self.context, template.predecessor)
        return predecessors

    def _get_remaining_timeout_secs(self):
        if self.action == Stack.CREATE:
            start_time = self.created_time
        elif self.action == Stack.DELETE:
            # by default 60 mins
            return 3600
        else:
            start_time = self.updated_time
        current_time = timeutils.utcnow()
        delta = current_time - start_time
        delta_timeout = self.timeout_secs() - delta.seconds
        return delta_timeout

    def _destructive_action_in_progress(self):
        return self.action == Stack.DELETE or \
               (self.action, self.status) in \
               ((Stack.UPDATE, Stack.GC_IN_PROGRESS),
                (Stack.ROLLBACK, Stack.GC_IN_PROGRESS))

    def _is_traversal_order_reverse(self):
        return self._destructive_action_in_progress()

    def _handle_stack_failure(self, curr_template_id):
        # Trigger rollback. Make sure you have consumed all the notifications
        # before triggering rollbacK
        reverse = self._is_traversal_order_reverse()
        ready_nodes = db_api.get_ready_nodes(self.context, self.id,
                                             template_id=curr_template_id,
                                             reverse=reverse)
        processing_nodes = self._get_tasks_matching_status(
                                 ready_nodes, TASK_STATUS.SCHEDULED)
        if not processing_nodes:
            # Rollback = True and action is CREATE/UPDATE
            if (not self.disable_rollback and
                        self.action in (Stack.CREATE, Stack.UPDATE)):
                Stack.rollback(self)
            elif self.action == Stack.DELETE:
                # still do delete_complete
                self.delete_complete()
            else:
                pass
        else:
            # Ignore notification. Wait for others to complete
            pass

    def _handle_success(self, curr_template_id):
        self._trigger_convergence(curr_template_id)

    def handle_timeout(self):
        values = \
            {
            'status': Stack.FAILED,
            'status_reason': 'stack timed-out'
        }
        db_api.stack_update(self.context, self.id, values)

    def _get_tasks_matching_status(self, nodes, status):
        return filter(lambda (res_name, res_status): res_status == status, nodes)

    def process_resource_notif(self, incoming_req_id, template_id,
                               resource_id, convg_status):
        if convg_status == CONVERGE_RESPONSE.PANIC:
            # a new update was issued, worker panicked
            db_api.resource_delete(self.context, resource_id)
            self._trigger_convergence(concurrent_update_on=True)
            return

        if incoming_req_id != self.current_req_id():
            # an older version was converged
            self._trigger_convergence(concurrent_update_on=True)
            return

        res = db_api.resource_get(self.context, resource_id)
        db_api.update_graph_traversal(self.context, self.id,
                                         TASK_STATUS.DONE,
                                         resource_name=res.name,
                                         template_id=template_id)
        if self.status == Stack.FAILED:
            # an earlier event might have marked stack as failure
            self._handle_stack_failure(template_id)
        else:
            if convg_status == CONVERGE_RESPONSE.OK:
                if self._destructive_action_in_progress():
                    db_api.resource_delete(self.context, res.id)
                self._handle_success(template_id)
            else:
                # Failure scenario
                values = {
                    'status': Stack.FAILED,
                    'status_reason': res.status_reason
                }
                db_api.stack_update(self.context, self.id, values)
                self._handle_stack_failure(template_id)

    def _generate_new_req_id(self):
       self._request_id = uuidutils.generate_uuid()

    def current_req_id(self):
        return self._request_id
