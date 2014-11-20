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
import copy
from datetime import datetime
import re
import warnings

from oslo.config import cfg
from oslo.utils import encodeutils
from osprofiler import profiler
import six
import json

from heat.common import context as common_context
from heat.common import exception
from heat.common.exception import StackValidationFailed
from heat.common.i18n import _
from heat.common.i18n import _LE
from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.common import identifier
from heat.common import lifecycle_plugin_utils
from heat.db import api as db_api
from heat.engine import dependencies
from heat.engine import environment
from heat.engine import function
from heat.engine.notification import stack as notification
from heat.engine.parameter_groups import ParameterGroups
from heat.engine import resource
from heat.engine import resources
from heat.engine import rsrc_defn
from heat.engine import scheduler
from heat.engine.template import Template
from heat.engine import update
from heat.openstack.common import log as logging
from heat.rpc import api as rpc_api
from heat.rpc import client as rpc_client

LOG = logging.getLogger(__name__)

ERROR_WAIT_TIME = 240


class ForcedCancel(BaseException):
    """Exception raised to cancel task execution."""

    def __str__(self):
        return "Operation cancelled"


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
                 use_stored_context=False, username=None):
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
        self._dependencies = None
        self._access_allowed_handlers = {}
        self._db_resources = None
        self.adopt_stack_data = adopt_stack_data
        self.stack_user_project_id = stack_user_project_id
        self.created_time = created_time
        self.updated_time = updated_time
        self.user_creds_id = user_creds_id
        self.rpc_client = rpc_client.EngineClient()

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
    def resources(self):
        if self._resources is None:
            self._resources = dict((name, resource.Resource(name, data, self, load_from_db=True))
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

    def get_dependencies_from_template(self):
        return self._get_dependencies(self.resources.itervalues())

    def get_dependencies_from_db(self):
        deps = db_api.graph_get_all_by_stack(self.context, self.id)
        return deps

    def _store_edges(self, resource_name, required_by):
        value = {'resource_name': resource_name, 'stack_id': self.id}
        if required_by:
            for req in required_by:
                value['needed_by'] = req
                db_api.graph_insert_egde(self.context, value)
        else:
            db_api.graph_insert_egde(self.context, value)

    def store_dependencies(self):
        deps = self.get_dependencies_from_template()
        with db_api.transaction(self.context):
            for res in deps:
                required_by = [req.name for req in deps.required_by(res)]
                self._store_edges(res.name, required_by)

    def update_dependencies(self):
        new_deps = self.get_dependencies_from_template()
        with db_api.transaction(self.context):
            for res in new_deps:
                value = {'resource_name': res.name, 'stack_id': self.id}
                new_required_by = [req.name for req in
                                   new_deps.required_by(res)]
                old_required_by = db_api.get_resource_required_by(self.context,
                                                                  self.id,
                                                                  res.name)
                resource_exists = db_api.resource_exists_in_graph(self.context,
                                                                  self.id,
                                                                  res.name)

                # if it is a new resource then insert all the edges for
                # this resource
                if not resource_exists:
                    self._store_edges(res.name, new_required_by)

                # if resource exists in new template but dependencies
                # are changed
                elif set(new_required_by) != set(old_required_by):
                    deleted_edges = set(old_required_by) - set(new_required_by)
                    for edge in deleted_edges:
                        value = {'resource_name': res.name,
                                 'needed_by': edge,
                                 'stack_id': self.id }
                        db_api.graph_delete_egde(self.context, value)

                    added_edges = set(new_required_by) - set(old_required_by)
                    self._store_edges(res.name, added_edges)

    @classmethod
    def process_ready_resources(cls, cnxt, stack_id, ready_nodes=[],
                                reverse=False, timeout=None):
        if not ready_nodes:
            ready_nodes = db_api.get_ready_nodes(context=cnxt,
                                           stack_id=stack_id,
                                           reverse=reverse)
        def converge_resource(rsrc_name):
            db_api.update_resource_traversal(context=cnxt,
                                             stack_id=stack_id,
                                             status='PROCESSING',
                                             resource_name=rsrc_name)
            version = Stack.get_converge_resource_version(cnxt, rsrc_name,
                                                          stack_id)

            rc = rpc_client.EngineClient()
            if version is not None:
                rc.converge_resource(cnxt, stack_id, rsrc_name, version=version,
                                     timeout=timeout)

        def filter_in_progress_nodes():
            # to avoid processing a resource which is IN_PROGRESS,
            # return only those resources which are not in IN_PROGRESS.
            node_dict = dict(ready_nodes)
            filters = {'status': resource.Resource.IN_PROGRESS}
            try:
                resources = db_api.resource_get_all_by_stack(cnxt,
                                                             stack_id,
                                                             filters)
            except exception.NotFound:
                return ready_nodes
            nodes = [(name, status)
                     for name, status in ready_nodes
                     if name not in resources
            ]
            return nodes

        ready_nodes = filter_in_progress_nodes()
        [converge_resource(rsrc_name) for rsrc_name, status in ready_nodes
                                                if status == 'UNPROCESSED']

    def reset_dependencies(self):
        self._dependencies = None

    @property
    def root_stack(self):
        '''
        Return the root stack if this is nested (otherwise return self).
        '''
        if (self.parent_resource and self.parent_resource.stack):
            return self.parent_resource.stack.root_stack
        return self

    def total_resources(self):
        '''
        Return the total number of resources in a stack, including nested
        stacks below.
        '''
        def total_nested(res):
            get_nested = getattr(res, 'nested', None)
            if callable(get_nested):
                nested_stack = get_nested()
                if nested_stack is not None:
                    return nested_stack.total_resources()
            return 0

        return len(self) + sum(total_nested(res) for res in self.itervalues())

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
                   username=stack.username)

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
            'backup': backup
        }
        if self.id:
            db_api.stack_update(self.context, self.id, s)
            self.update_dependencies()
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
            self.store_dependencies()

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

    def resource_by_refid(self, refid):
        '''
        Return the resource in this stack with the specified
        refid, or None if not found
        '''
        for r in self.values():
            if r.state in (
                    (r.INIT, r.COMPLETE),
                    (r.CREATE, r.IN_PROGRESS),
                    (r.CREATE, r.COMPLETE),
                    (r.RESUME, r.IN_PROGRESS),
                    (r.RESUME, r.COMPLETE),
                    (r.UPDATE, r.IN_PROGRESS),
                    (r.UPDATE, r.COMPLETE)) and r.FnGetRefId() == refid:
                return r

    def register_access_allowed_handler(self, credential_id, handler):
        '''
        Register a function which determines whether the credentials with
        a give ID can have access to a named resource.
        '''
        assert callable(handler), 'Handler is not callable'
        self._access_allowed_handlers[credential_id] = handler

    def access_allowed(self, credential_id, resource_name):
        '''
        Returns True if the credential_id is authorised to access the
        resource with the specified resource_name.
        '''
        if not self.resources:
            # this also triggers lazy-loading of resources
            # so is required for register_access_allowed_handler
            # to be called
            return False

        handler = self._access_allowed_handlers.get(credential_id)
        return handler and handler(resource_name)

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

    def requires_deferred_auth(self):
        '''
        Returns whether this stack may need to perform API requests
        during its lifecycle using the configured deferred authentication
        method.
        '''
        return any(res.requires_deferred_auth for res in self.values())

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

    def preview_resources(self):
        '''
        Preview the stack with all of the resources.
        '''
        return [resource.preview()
                for resource in self.resources.itervalues()]

    @profiler.trace('Stack.create', hide_args=False)
    def create(self):
        '''
        Create the stack and all of the resources.
        '''
        def rollback():
            if not self.disable_rollback and self.state == (self.CREATE,
                                                            self.FAILED):
                self.delete(action=self.ROLLBACK)

        creator = scheduler.TaskRunner(self.stack_task,
                                       action=self.CREATE,
                                       reverse=False,
                                       post_func=rollback,
                                       error_wait_time=ERROR_WAIT_TIME)
        creator(timeout=self.timeout_secs())

    def create_start(self):
        for res in self.resources.values():
            res.state_set(res.CREATE, res.INIT)
        Stack.process_ready_resources(self.context, stack_id=self.id,
                                      timeout=self.timeout_secs())

    def _adopt_kwargs(self, resource):
        data = self.adopt_stack_data
        if not data or not data.get('resources'):
            return {'resource_data': None}

        return {'resource_data': data['resources'].get(resource.name)}

    @scheduler.wrappertask
    def stack_task(self, action, reverse=False, post_func=None,
                   error_wait_time=None,
                   aggregate_exceptions=False):
        '''
        A task to perform an action on the stack and all of the resources
        in forward or reverse dependency order as specified by reverse
        '''
        try:
            lifecycle_plugin_utils.do_pre_ops(self.context, self,
                                              None, action)
        except Exception as e:
            self.state_set(action, self.FAILED, e.args[0] if e.args else
                           'Failed stack pre-ops: %s' % six.text_type(e))
            if callable(post_func):
                post_func()
            return
        self.state_set(action, self.IN_PROGRESS,
                       'Stack %s started' % action)

        if self.action != self.CREATE:
            db_api.update_resource_traversal(context=self.context,
                                             stack_id=self.id,
                                             status="UNPROCESSED")

        stack_status = self.COMPLETE
        reason = 'Stack %s completed successfully' % action

        action_task = scheduler.DependencyTaskGroup(
            self.context,
            self.id,
            task=self.converge,
            reverse=reverse,
            error_wait_time=error_wait_time,
            aggregate_exceptions=aggregate_exceptions)

        try:
            yield action_task()
        except (exception.ResourceFailure, scheduler.ExceptionGroup) as ex:
            stack_status = self.FAILED
            reason = 'Resource %s failed: %s' % (action, six.text_type(ex))
        except scheduler.Timeout:
            stack_status = self.FAILED
            reason = '%s timed out' % action.title()

        self.state_set(action, stack_status, reason)

        if callable(post_func):
            post_func()
        lifecycle_plugin_utils.do_post_ops(self.context, self, None, action,
                                           (self.status == self.FAILED))

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

    @classmethod
    def get_converge_resource_version(cls, cnxt, rsrc_name, stack_id):
        # In GC we will be working on multiple versions.
        # Determine the version of resource on which worker needs to converge
        # TODO: Handle concurrent update, improvise logic
        stack = Stack.load(cnxt, stack_id)
        rsrc = db_api.resource_get_by_name_and_stack(cnxt, rsrc_name, stack_id)
        if stack.status != stack.GC_IN_PROGRESS:
            return rsrc.version
        res = resource.Resource
        if (rsrc.action, rsrc.status) == (res.DELETE, res.INIT):
            # Resource deleted in new update.
            # Delete older db versions and converge to delete the phy resource
            res.delete_older_versions_from_db(
                cnxt, rsrc, stack_id, rsrc.version)
            return rsrc.version
        elif (rsrc.action, rsrc.status) == (res.CREATE, res.COMPLETE):
            # Update Replace
            res.delete_older_versions_from_db(
                cnxt, rsrc, stack_id, rsrc.version-1)
            return rsrc.version-1
        else:
            # update inplace or delete complete
            res.delete_older_versions_from_db(
                cnxt, rsrc, stack_id, rsrc.version)
            rc = rpc_client.EngineClient()
            # nothing to converge
            rc.notify_resource_observed(cnxt, stack_id, rsrc.name, rsrc.version)
            return

    def resource_action_runner(self, resource_name, version, timeout):
        db_rsrc = db_api.resource_get_by_name_and_stack(self.context,
                                                        resource_name,
                                                        self.id,
                                                        version)
        LOG.debug("=====Resource name %s, version %s", resource_name, version)
        rsrc = resource.Resource.load(db_rsrc, self)
        LOG.debug("=====Resource %s", rsrc)
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
                res = resource.Resource.load(db_resource, self)
                yield res.destroy()
            else:
                db_api.resource_delete(self.context, db_resource.id)

    def converge(self, rsrc_name):
        # Just look for stack action and take steps
        if self.action in (self.CREATE, self.ADOPT):
            res = self.resources[rsrc_name]
            LOG.debug("==== Converging resource %s ", rsrc_name)
            # Find e.g resource.create and call it
            return self.resource_action(res)
        elif self.action == self.DELETE:
            return self.resource_delete(rsrc_name)

    @profiler.trace('Stack.check', hide_args=False)
    def check(self):
        self.updated_time = datetime.utcnow()
        checker = scheduler.TaskRunner(self.stack_task, self.CHECK,
                                       post_func=self.supports_check_action,
                                       aggregate_exceptions=True)
        checker()

    def supports_check_action(self):
        def is_supported(stack, res):
            if hasattr(res, 'nested'):
                return res.nested().supports_check_action()
            else:
                return hasattr(res, 'handle_%s' % self.CHECK.lower())

        supported = [is_supported(self, res)
                     for res in self.resources.values()]

        if not all(supported):
            msg = ". '%s' not fully supported (see resources)" % self.CHECK
            reason = self.status_reason + msg
            self.state_set(self.CHECK, self.status, reason)

        return all(supported)

    @profiler.trace('Stack.adopt', hide_args=False)
    def adopt(self):
        '''
        Adopt a stack (create stack with all the existing resources).
        '''
        def rollback():
            if not self.disable_rollback and self.state == (self.ADOPT,
                                                            self.FAILED):
                self.delete(action=self.ROLLBACK)

        creator = scheduler.TaskRunner(
            self.stack_task,
            action=self.ADOPT,
            reverse=False,
            post_func=rollback)
        creator(timeout=self.timeout_secs())

    @profiler.trace('Stack.update', hide_args=False)
    def update(self, newstack=None, event=None, action=UPDATE):
        '''
        Compare the current stack with newstack,
        and where necessary create/update/delete the resources until
        this stack aligns with newstack.

        Note update of existing stack resources depends on update
        being implemented in the underlying resource types

        Update will fail if it exceeds the specified timeout. The default is
        60 minutes, set in the constructor
        '''
        self.state_set(action, self.IN_PROGRESS,
                       'Stack %s started' % action)
        newstack.id = self.id
        newstack.action = self.action
        newstack.status = self.status
        newstack.status_reason = self.status_reason
        newstack.updated_time = datetime.utcnow()
        newstack.store()
        
        # Mark all nodes as UNPROCESSED
        db_api.update_resource_traversal(self.context, newstack.id,
                                         status="UNPROCESSED")
        
        def create_new_resource_version(new_res_obj, old_res, action):
            new_res_obj.id = None
            new_res_obj.action = action
            new_res_obj.status = new_res_obj.INIT
            new_res_obj.resource_id = old_res.nova_instance
            new_res_obj.version = old_res.version + 1
            new_res_obj.store()

            old_res_obj = resource.Resource.load(old_res, self)
            old_res_obj.resource_id = None
            old_res_obj.store_update(old_res_obj.action,
                                     old_res_obj.status,
                                     "Remove nova_instance from old resource version")

        for res_name in db_api.get_all_resources_from_graph(self.context, self.id):
            old_res = db_api.resource_get_by_name_and_stack(context=self.context,
                                                            resource_name=res_name,
                                                            stack_id=self.id)

            try:
                new_res = newstack.resources[res_name]
            except KeyError:
                #Resource does not exists in new template therefore it is deleted
                #create_new_resource_version( old_res, res.DELETE)
                res_obj = resource.Resource.load(old_res, self)
                create_new_resource_version(res_obj, old_res, res_obj.DELETE)
            else:
                if old_res:
                    old_rsrc_defn = rsrc_defn.ResourceDefinition.from_dict(
                                        json.loads(old_res.rsrc_defn),
                                        old_res.rsrc_defn_hash)
                    if new_res.needs_update(old_rsrc_defn):
                        create_new_resource_version(new_res, old_res, new_res.UPDATE)
                    else:
                        db_api.update_resource_traversal(self.context, self.id,
                                                         status="PROCESSED",
                                                         resource_name=new_res.name)
                else:
                    new_res.state_set(new_res.CREATE, new_res.INIT)
        Stack.process_ready_resources(self.context, stack_id=self.id, timeout=self.timeout_secs())

    def pre_update_complete(self):
        self.state_set(self.UPDATE, self.GC_IN_PROGRESS, "Stack GC started")
        for rsrc_name in db_api.get_all_resources_from_graph(self.context,
                                                             self.id):
                db_resources = db_api.\
                    resource_get_all_versions_by_name_and_stack(
                    self.context, rsrc_name, self.id)
                if len(db_resources) > 1:
                    db_api.update_resource_traversal(self.context, self.id,
                                                     status="UNPROCESSED",
                                                     resource_name=rsrc_name)

        Stack.process_ready_resources(self.context, self.id, reverse=True)

    @scheduler.wrappertask
    def update_task(self, oldstack, action=UPDATE, event=None):
        if action not in (self.UPDATE, self.ROLLBACK, self.RESTORE):
            LOG.error(_LE("Unexpected action %s passed to update!"), action)
            self.state_set(self.UPDATE, self.FAILED,
                           "Invalid action %s" % action)
            return

        try:
            lifecycle_plugin_utils.do_pre_ops(self.context, oldstack,
                                              self, action)
        except Exception as e:
            self.state_set(action, self.FAILED, e.args[0] if e.args else
                           'Failed stack pre-ops: %s' % six.text_type(e))
            return
        if self.status == self.IN_PROGRESS:
            if action == self.ROLLBACK:
                LOG.debug("Starting update rollback for %s" % self.name)
            else:
                self.state_set(action, self.FAILED,
                               'State invalid for %s' % action)
                return

        self.state_set(action, self.IN_PROGRESS,
                       'Stack %s started' % action)

        old_template_id = self.t.predecessor

        try:
            # 3. traverse the graph and update
            # 4. deletion phase for resources
            # 5. Clean up old raw_template
            # self.timeout_mins = oldstack.timeout_mins
            self._set_param_stackid()

            # mark the graph as not traversed
            db_api.update_resource_traversal(self.context, self.id,
                                             status="UNPROCESSED")

            update_task = update.StackUpdate(self,
                                             rollback=action == self.ROLLBACK,
                                             error_wait_time=ERROR_WAIT_TIME)
            updater = scheduler.TaskRunner(update_task)

            try:
                updater.start(timeout=self.timeout_secs())
                yield
                while not updater.step():
                    if event is None or not event.ready():
                        yield
                    else:
                        message = event.wait()
                        if message == rpc_api.THREAD_CANCEL:
                            raise ForcedCancel()
            finally:
                self.reset_dependencies()

            if action == self.UPDATE:
                reason = 'Stack successfully updated'
            elif action == self.RESTORE:
                reason = 'Stack successfully restored'
            else:
                reason = 'Stack rollback completed'
            stack_status = self.COMPLETE

        except scheduler.Timeout:
            stack_status = self.FAILED
            reason = 'Timed out'
        except ForcedCancel as e:
            reason = six.text_type(e)

            stack_status = self.FAILED
            if action == self.UPDATE:
                update_task.updater.cancel_all()
                yield self.update_task(oldstack, action=self.ROLLBACK)
                return

        except exception.ResourceFailure as e:
            reason = six.text_type(e)

            stack_status = self.FAILED
            if action == self.UPDATE:
                # If rollback is enabled, we do another update, with the
                # existing template, so we roll back to the original state
                if not self.disable_rollback:
                    yield oldstack.update_task(None, action=self.ROLLBACK)
                    return
        else:
            # flip the template to the newstack values
            #self.t = newstack.t
            template_outputs = self.t[self.t.OUTPUTS]
            self.outputs = self.resolve_static_data(template_outputs)
            self.t.predecessor = None
            self.t.store()

            LOG.debug('Deleting old template')
            db_api.raw_template_delete(self.context, old_template_id)

        # Don't use state_set to do only one update query and avoid race
        # condition with the COMPLETE status
        self.action = action
        self.status = stack_status
        self.status_reason = reason

        self.store()
        lifecycle_plugin_utils.do_post_ops(self.context, oldstack,
                                           self, action,
                                           (self.status == self.FAILED))

        notification.send(self)

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
        for res in self.resources.values():
            db_res = db_api.resource_get_by_name_and_stack(self.context,
                                                           res.name,
                                                           self.id)
            res_obj = resource.Resource.load(db_res, self)
            res_obj.state_set(res_obj.DELETE, res_obj.INIT)

        db_api.update_resource_traversal(context=self.context,
                                         stack_id=self.id,
                                         status="UNPROCESSED")

        snapshots = db_api.snapshot_get_all(self.context, self.id)
        for snapshot in snapshots:
            self.delete_snapshot(snapshot)

        Stack.process_ready_resources(self.context, stack_id=self.id,
                                     reverse=True, timeout=self.timeout_secs())

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

    @profiler.trace('Stack.suspend', hide_args=False)
    def suspend(self):
        '''
        Suspend the stack, which invokes handle_suspend for all stack resources
        waits for all resources to become SUSPEND_COMPLETE then declares the
        stack SUSPEND_COMPLETE.
        Note the default implementation for all resources is to do nothing
        other than move to SUSPEND_COMPLETE, so the resources must implement
        handle_suspend for this to have any effect.
        '''
        # No need to suspend if the stack has been suspended
        if self.state == (self.SUSPEND, self.COMPLETE):
            LOG.info(_LI('%s is already suspended'), six.text_type(self))
            return

        sus_task = scheduler.TaskRunner(self.stack_task,
                                        action=self.SUSPEND,
                                        reverse=True)
        sus_task(timeout=self.timeout_secs())

    @profiler.trace('Stack.resume', hide_args=False)
    def resume(self):
        '''
        Resume the stack, which invokes handle_resume for all stack resources
        waits for all resources to become RESUME_COMPLETE then declares the
        stack RESUME_COMPLETE.
        Note the default implementation for all resources is to do nothing
        other than move to RESUME_COMPLETE, so the resources must implement
        handle_resume for this to have any effect.
        '''
        # No need to resume if the stack has been resumed
        if self.state == (self.RESUME, self.COMPLETE):
            LOG.info(_LI('%s is already resumed'), six.text_type(self))
            return

        sus_task = scheduler.TaskRunner(self.stack_task,
                                        action=self.RESUME,
                                        reverse=False)
        sus_task(timeout=self.timeout_secs())

    @profiler.trace('Stack.snapshot', hide_args=False)
    def snapshot(self):
        '''Snapshot the stack, invoking handle_snapshot on all resources.'''
        sus_task = scheduler.TaskRunner(self.stack_task,
                                        action=self.SNAPSHOT,
                                        reverse=False)
        sus_task(timeout=self.timeout_secs())

    @profiler.trace('Stack.delete_snapshot', hide_args=False)
    def delete_snapshot(self, snapshot):
        '''Remove a snapshot from the backends.'''
        for name, rsrc in self.resources.iteritems():
            data = snapshot.data['resources'].get(name)
            scheduler.TaskRunner(rsrc.delete_snapshot, data)()

    @profiler.trace('Stack.restore', hide_args=False)
    def restore(self, snapshot):
        '''
        Restore the given snapshot, invoking handle_restore on all resources.
        '''
        self.updated_time = datetime.utcnow()

        tmpl = Template(snapshot.data['template'])

        for name, defn in tmpl.resource_definitions(self).iteritems():
            rsrc = resource.Resource(name, defn, self)
            data = snapshot.data['resources'].get(name)
            handle_restore = getattr(rsrc, 'handle_restore', None)
            if callable(handle_restore):
                defn = handle_restore(defn, data)
            tmpl.add_resource(defn, name)

        newstack = self.__class__(self.context, self.name, tmpl, self.env,
                                  timeout_mins=self.timeout_mins,
                                  disable_rollback=self.disable_rollback)
        newstack.parameters.set_stack_id(self.identifier())

        updater = scheduler.TaskRunner(self.update_task, newstack,
                                       action=self.RESTORE)
        updater()

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

    def restart_resource(self, resource_name):
        '''
        stop resource_name and all that depend on it
        start resource_name and all that depend on it
        '''
        deps = self.dependencies[self[resource_name]]
        failed = False

        for res in reversed(deps):
            try:
                scheduler.TaskRunner(res.destroy)()
            except exception.ResourceFailure as ex:
                failed = True
                LOG.error(_LE('Resource %(name)s delete failed: %(ex)s'),
                          {'name': res.name, 'ex': ex})

        for res in deps:
            if not failed:
                try:
                    res.state_reset()
                    scheduler.TaskRunner(res.create)()
                except exception.ResourceFailure as ex:
                    LOG.exception(_('Resource %(name)s create failed: %(ex)s')
                                  % {'name': res.name, 'ex': ex})
                    failed = True
            else:
                res.state_set(res.CREATE, res.FAILED,
                              'Resource restart aborted')
        # TODO(asalkeld) if any of this fails we Should
        # restart the whole stack

    def get_availability_zones(self):
        nova = self.clients.client('nova')
        if self._zones is None:
            self._zones = [
                zone.zoneName for zone in
                nova.availability_zones.list(detailed=False)]
        return self._zones

    def set_stack_user_project_id(self, project_id):
        self.stack_user_project_id = project_id
        self.store()

    @profiler.trace('Stack.create_stack_user_project_id', hide_args=False)
    def create_stack_user_project_id(self):
        project_id = self.clients.client(
            'keystone').create_stack_domain_project(self.id)
        self.set_stack_user_project_id(project_id)

    @profiler.trace('Stack.prepare_abandon', hide_args=False)
    def prepare_abandon(self):
        return {
            'name': self.name,
            'id': self.id,
            'action': self.action,
            'environment': self.env.user_env_as_dict(),
            'status': self.status,
            'template': self.t.t,
            'resources': dict((res.name, res.prepare_abandon())
                              for res in self.resources.values()),
            'project_id': self.tenant_id,
            'stack_user_project_id': self.stack_user_project_id
        }

    def resolve_static_data(self, snippet):
        return self.t.parse(self, snippet)

    def resolve_runtime_data(self, snippet):
        """DEPRECATED. Use heat.engine.function.resolve() instead."""
        warnings.warn('Stack.resolve_runtime_data() is deprecated. '
                      'Use heat.engine.function.resolve() instead',
                      DeprecationWarning)
        return function.resolve(snippet)

    def reset_resource_attributes(self):
        # nothing is cached if no resources exist
        if not self._resources:
            return
        # a change in some resource may have side-effects in the attributes
        # of other resources, so ensure that attributes are re-calculated
        for res in self.resources.itervalues():
            res.attributes.reset_resolved_values()

    def rollback(self):
        if self.t.predecessor:
            raw_template = Template.load(self.context,
                                         self.t.predecessor_id)
        else:
            # NOTE: update with an empty template to DELETE the stack.
            empty_template = {'heat_template_version': self.t.version[1]}
            raw_template = Template(empty_template)

        new_stack = Stack(self.context, self.name, raw_template, self.env)
        self.update(new_stack, action=self.ROLLBACK)
