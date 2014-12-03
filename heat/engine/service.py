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
import functools
import json

import datetime
import eventlet
from oslo.config import cfg
from oslo import messaging
from osprofiler import profiler
import six
import warnings
import webob

from heat.common import context
from heat.common import exception
from heat.common.i18n import _
from heat.common.i18n import _LI
from heat.common.i18n import _LW
from heat.common import identifier
from heat.common import messaging as rpc_messaging
from heat.db import api as db_api
from heat.engine import api
from heat.engine import clients
from heat.engine import environment
from heat.engine.event import Event
from heat.engine import parameter_groups
from heat.engine import properties
from heat.engine import resource as resourcem
from heat.engine import resources
from heat.engine import stack as parser
from heat.engine import stack_lock
from heat.engine import template as templatem
from heat.openstack.common import log as logging
from heat.openstack.common import service
from heat.openstack.common import threadgroup
from heat.openstack.common import uuidutils
from heat.rpc import api as rpc_api

cfg.CONF.import_opt('engine_life_check_timeout', 'heat.common.config')
cfg.CONF.import_opt('max_resources_per_stack', 'heat.common.config')
cfg.CONF.import_opt('max_stacks_per_tenant', 'heat.common.config')
cfg.CONF.import_opt('enable_stack_abandon', 'heat.common.config')
cfg.CONF.import_opt('enable_stack_adopt', 'heat.common.config')

LOG = logging.getLogger(__name__)


def request_context(func):
    @functools.wraps(func)
    def wrapped(self, ctx, *args, **kwargs):
        if ctx is not None and not isinstance(ctx, context.RequestContext):
            ctx = context.RequestContext.from_dict(ctx.to_dict())
        try:
            return func(self, ctx, *args, **kwargs)
        except exception.HeatException:
            raise messaging.rpc.dispatcher.ExpectedException()
    return wrapped


class ThreadGroupManager(object):

    def __init__(self):
        super(ThreadGroupManager, self).__init__()
        self.groups = {}
        self.events = collections.defaultdict(list)

        # Create dummy service task, because when there is nothing queued
        # on self.tg the process exits
        self.add_timer(cfg.CONF.periodic_interval, self._service_task)

    def _service_task(self):
        """
        This is a dummy task which gets queued on the service.Service
        threadgroup.  Without this service.Service sees nothing running
        i.e has nothing to wait() on, so the process exits..
        This could also be used to trigger periodic non-stack-specific
        housekeeping tasks
        """
        pass

    def _serialize_profile_info(self):
        prof = profiler.get()
        trace_info = None
        if prof:
            trace_info = {
                "hmac_key": prof.hmac_key,
                "base_id": prof.get_base_id(),
                "parent_id": prof.get_id()
            }
        return trace_info

    def _start_with_trace(self, trace, func, *args, **kwargs):
        if trace:
            profiler.init(**trace)
        return func(*args, **kwargs)

    def start(self, stack_id, func, *args, **kwargs):
        """
        Run the given method in a sub-thread.
        """
        if stack_id not in self.groups:
            self.groups[stack_id] = threadgroup.ThreadGroup()
        return self.groups[stack_id].add_thread(self._start_with_trace,
                                                self._serialize_profile_info(),
                                                func, *args, **kwargs)

    def start_with_lock(self, cnxt, stack, engine_id, func, *args, **kwargs):
        """
        Try to acquire a stack lock and, if successful, run the given
        method in a sub-thread.  Release the lock when the thread
        finishes.

        :param cnxt: RPC context
        :param stack: Stack to be operated on
        :type stack: heat.engine.parser.Stack
        :param engine_id: The UUID of the engine/worker acquiring the lock
        :param func: Callable to be invoked in sub-thread
        :type func: function or instancemethod
        :param args: Args to be passed to func
        :param kwargs: Keyword-args to be passed to func.
        """
        lock = stack_lock.StackLock(cnxt, stack, engine_id)
        with lock.thread_lock(stack.id):
            th = self.start_with_acquired_lock(stack, lock,
                                               func, *args, **kwargs)
            return th

    def start_with_acquired_lock(self, stack, lock, func, *args, **kwargs):
        """
        Run the given method in a sub-thread and release the provided lock
        when the thread finishes.

        :param stack: Stack to be operated on
        :type stack: heat.engine.parser.Stack
        :param lock: The acquired stack lock
        :type lock: heat.engine.stack_lock.StackLock
        :param func: Callable to be invoked in sub-thread
        :type func: function or instancemethod
        :param args: Args to be passed to func
        :param kwargs: Keyword-args to be passed to func

        """
        def release(gt, *args):
            """
            Callback function that will be passed to GreenThread.link().
            """
            lock.release(*args)

        th = self.start(stack.id, func, *args, **kwargs)
        th.link(release, stack.id)
        return th

    def add_timer(self, stack_id, func, *args, **kwargs):
        """
        Define a periodic task, to be run in a separate thread, in the stack
        threadgroups.  Periodicity is cfg.CONF.periodic_interval
        """
        if stack_id not in self.groups:
            self.groups[stack_id] = threadgroup.ThreadGroup()
        self.groups[stack_id].add_timer(cfg.CONF.periodic_interval,
                                        func, *args, **kwargs)

    def add_event(self, stack_id, event):
        self.events[stack_id].append(event)

    def remove_event(self, gt, stack_id, event):
        for e in self.events.pop(stack_id, []):
            if e is not event:
                self.add_event(stack_id, e)

    def stop_timers(self, stack_id):
        if stack_id in self.groups:
            self.groups[stack_id].stop_timers()

    def stop(self, stack_id, graceful=False):
        '''Stop any active threads on a stack.'''
        if stack_id in self.groups:
            self.events.pop(stack_id, None)
            threadgroup = self.groups.pop(stack_id)
            threads = threadgroup.threads[:]

            threadgroup.stop(graceful)
            threadgroup.wait()

            # Wait for link()ed functions (i.e. lock release)
            links_done = dict((th, False) for th in threads)

            def mark_done(gt, th):
                links_done[th] = True

            for th in threads:
                th.link(mark_done, th)
            while not all(links_done.values()):
                eventlet.sleep()

    def send(self, stack_id, message):
        for event in self.events.pop(stack_id, []):
            event.send(message)


@profiler.trace_cls("rpc")
class EngineListener(service.Service):
    '''
    Listen on an AMQP queue named for the engine.  Allows individual
    engines to communicate with each other for multi-engine support.
    '''

    ACTIONS = (STOP_STACK, SEND) = ('stop_stack', 'send')

    def __init__(self, host, engine_id, thread_group_mgr):
        super(EngineListener, self).__init__()
        self.thread_group_mgr = thread_group_mgr
        self.engine_id = engine_id
        self.host = host

    def start(self):
        super(EngineListener, self).start()
        self.target = messaging.Target(
            server=self.host, topic=self.engine_id)
        server = rpc_messaging.get_rpc_server(self.target, self)
        server.start()

    def listening(self, ctxt):
        '''
        Respond affirmatively to confirm that the engine performing the
        action is still alive.
        '''
        return True

    def stop_stack(self, ctxt, stack_identity):
        '''Stop any active threads on a stack.'''
        stack_id = stack_identity['stack_id']
        self.thread_group_mgr.stop(stack_id)

    def send(self, ctxt, stack_identity, message):
        stack_id = stack_identity['stack_id']
        self.thread_group_mgr.send(stack_id, message)


@profiler.trace_cls("rpc")
class EngineService(service.Service):
    """
    Manages the running instances from creation to destruction.
    All the methods in here are called from the RPC backend.  This is
    all done dynamically so if a call is made via RPC that does not
    have a corresponding method here, an exception will be thrown when
    it attempts to call into this class.  Arguments to these methods
    are also dynamically added and will be named as keyword arguments
    by the RPC caller.
    """

    RPC_API_VERSION = '1.1'

    def __init__(self, host, topic, manager=None):
        super(EngineService, self).__init__()
        resources.initialise()
        self.host = host
        self.topic = topic

        # The following are initialized here, but assigned in start() which
        # happens after the fork when spawning multiple worker processes
        self.listener = None
        self.engine_id = None
        self.thread_group_mgr = None
        self.target = None

        if cfg.CONF.instance_user:
            warnings.warn('The "instance_user" option in heat.conf is '
                          'deprecated and will be removed in the Juno '
                          'release.', DeprecationWarning)

        if cfg.CONF.trusts_delegated_roles:
            warnings.warn('The default value of "trusts_delegated_roles" '
                          'option in heat.conf is changed to [] in Kilo '
                          'and heat will delegate all roles of trustor. '
                          'Please keep the same if you do not want to '
                          'delegate subset roles when upgrading.',
                          Warning)

    def start(self):
        self.engine_id = stack_lock.StackLock.generate_engine_id()
        self.thread_group_mgr = ThreadGroupManager()
        self.listener = EngineListener(self.host, self.engine_id,
                                       self.thread_group_mgr)
        LOG.debug("Starting listener for engine %s" % self.engine_id)
        self.listener.start()
        target = messaging.Target(
            version=self.RPC_API_VERSION, server=self.host,
            topic=self.topic)
        self.target = target
        server = rpc_messaging.get_rpc_server(target, self)
        server.start()
        self._client = rpc_messaging.get_rpc_client(
            version=self.RPC_API_VERSION)

        super(EngineService, self).start()

    def stop(self):
        # Stop rpc connection at first for preventing new requests
        LOG.info(_LI("Attempting to stop engine service..."))
        try:
            self.conn.close()
        except Exception:
            pass

        # Wait for all active threads to be finished
        for stack_id in self.thread_group_mgr.groups.keys():
            # Ignore dummy service task
            if stack_id == cfg.CONF.periodic_interval:
                continue
            LOG.info(_LI("Waiting stack %s processing to be finished"),
                     stack_id)
            # Stop threads gracefully
            self.thread_group_mgr.stop(stack_id, True)
            LOG.info(_LI("Stack %s processing was finished"), stack_id)

        # Terminate the engine process
        LOG.info(_LI("All threads were gone, terminating engine"))
        super(EngineService, self).stop()

    @request_context
    def identify_stack(self, cnxt, stack_name):
        """
        The identify_stack method returns the full stack identifier for a
        single, live stack given the stack name.

        :param cnxt: RPC context.
        :param stack_name: Name or UUID of the stack to look up.
        """
        if uuidutils.is_uuid_like(stack_name):
            s = db_api.stack_get(cnxt, stack_name, show_deleted=True)
            # may be the name is in uuid format, so if get by id returns None,
            # we should get the info by name again
            if not s:
                s = db_api.stack_get_by_name(cnxt, stack_name)
        else:
            s = db_api.stack_get_by_name(cnxt, stack_name)
        if s:
            stack = parser.Stack.load(cnxt, stack=s)
            return dict(stack.identifier())
        else:
            raise exception.StackNotFound(stack_name=stack_name)

    def _get_stack(self, cnxt, stack_identity, show_deleted=False):
        identity = identifier.HeatIdentifier(**stack_identity)

        s = db_api.stack_get(cnxt, identity.stack_id,
                             show_deleted=show_deleted,
                             eager_load=True)

        if s is None:
            raise exception.StackNotFound(stack_name=identity.stack_name)

        if cnxt.tenant_id not in (identity.tenant, s.stack_user_project_id):
            # The DB API should not allow this, but sanity-check anyway..
            raise exception.InvalidTenant(target=identity.tenant,
                                          actual=cnxt.tenant_id)

        if identity.path or s.name != identity.stack_name:
            raise exception.StackNotFound(stack_name=identity.stack_name)

        return s

    @request_context
    def show_stack(self, cnxt, stack_identity):
        """
        Return detailed information about one or all stacks.

        :param cnxt: RPC context.
        :param stack_identity: Name of the stack you want to show, or None
            to show all
        """
        if stack_identity is not None:
            db_stack = self._get_stack(cnxt, stack_identity, show_deleted=True)
            stacks = [parser.Stack.load(cnxt, stack=db_stack)]
        else:
            stacks = parser.Stack.load_all(cnxt)

        return [api.format_stack(stack) for stack in stacks]

    def get_revision(self, cnxt):
        return cfg.CONF.revision['heat_revision']

    @request_context
    def list_stacks(self, cnxt, limit=None, marker=None, sort_keys=None,
                    sort_dir=None, filters=None, tenant_safe=True,
                    show_deleted=False, show_nested=False):
        """
        The list_stacks method returns attributes of all stacks.  It supports
        pagination (``limit`` and ``marker``), sorting (``sort_keys`` and
        ``sort_dir``) and filtering (``filters``) of the results.

        :param cnxt: RPC context
        :param limit: the number of stacks to list (integer or string)
        :param marker: the ID of the last item in the previous page
        :param sort_keys: an array of fields used to sort the list
        :param sort_dir: the direction of the sort ('asc' or 'desc')
        :param filters: a dict with attribute:value to filter the list
        :param tenant_safe: if true, scope the request by the current tenant
        :param show_deleted: if true, show soft-deleted stacks
        :param show_nested: if true, show nested stacks
        :returns: a list of formatted stacks
        """
        stacks = parser.Stack.load_all(cnxt, limit, marker, sort_keys,
                                       sort_dir, filters, tenant_safe,
                                       show_deleted, resolve_data=False,
                                       show_nested=show_nested)
        return [api.format_stack(stack) for stack in stacks]

    @request_context
    def count_stacks(self, cnxt, filters=None, tenant_safe=True,
                     show_deleted=False, show_nested=False):
        """
        Return the number of stacks that match the given filters
        :param cnxt: RPC context.
        :param filters: a dict of ATTR:VALUE to match against stacks
        :param tenant_safe: if true, scope the request by the current tenant
        :param show_deleted: if true, count will include the deleted stacks
        :param show_nested: if true, count will include nested stacks
        :returns: a integer representing the number of matched stacks
        """
        return db_api.stack_count_all(cnxt, filters=filters,
                                      tenant_safe=tenant_safe,
                                      show_deleted=show_deleted,
                                      show_nested=show_nested)

    def _validate_deferred_auth_context(self, cnxt, stack):
        if cfg.CONF.deferred_auth_method != 'password':
            return

        if not stack.requires_deferred_auth():
            return

        if cnxt.username is None:
            raise exception.MissingCredentialError(required='X-Auth-User')
        if cnxt.password is None:
            raise exception.MissingCredentialError(required='X-Auth-Key')

    def _validate_new_stack(self, cnxt, stack_name, parsed_template):
        try:
            parsed_template.validate()
        except Exception as ex:
            raise exception.StackValidationFailed(message=six.text_type(ex))

        if db_api.stack_get_by_name(cnxt, stack_name):
            raise exception.StackExists(stack_name=stack_name)

        tenant_limit = cfg.CONF.max_stacks_per_tenant
        if db_api.stack_count_all(cnxt) >= tenant_limit:
            message = _("You have reached the maximum stacks per tenant, %d."
                        " Please delete some stacks.") % tenant_limit
            raise exception.RequestLimitExceeded(message=message)

        num_resources = len(parsed_template[parsed_template.RESOURCES])
        if num_resources > cfg.CONF.max_resources_per_stack:
            message = exception.StackResourceLimitExceeded.msg_fmt
            raise exception.RequestLimitExceeded(message=message)

    def _parse_template_and_validate_stack(self, cnxt, stack_name, template,
                                           params, files, args, owner_id=None):
        # If it is stack-adopt, use parameters from adopt_stack_data
        common_params = api.extract_args(args)

        if rpc_api.PARAM_ADOPT_STACK_DATA in common_params:
            params[rpc_api.STACK_PARAMETERS] = common_params[
                rpc_api.PARAM_ADOPT_STACK_DATA]['environment'][
                    rpc_api.STACK_PARAMETERS]

        env = environment.Environment(params)

        tmpl = templatem.Template(template, files=files, env=env)
        self._validate_new_stack(cnxt, stack_name, tmpl)

        stack = parser.Stack(cnxt, stack_name, tmpl,
                             owner_id=owner_id,
                             **common_params)

        self._validate_deferred_auth_context(cnxt, stack)
        stack.validate()
        return stack

    @staticmethod
    def _spin_wait_acquire_lock_or_timeout(engine_id, context, stack):
        timeout = stack._get_remaining_timeout_secs()
        lock = stack_lock.StackLock(context, stack, engine_id)
        start_time = datetime.datetime.now().replace(microsecond=0)
        current_time = datetime.datetime.now().replace(microsecond=0)
        while current_time - start_time <= datetime.timedelta(seconds=timeout):
            LOG.debug("==== Spin waiting for lock...")
            acquired_lock = lock.try_acquire()
            if acquired_lock is None:
                LOG.debug("==== acquired lock...")
                return lock
            else:
                eventlet.sleep(0.5)
                current_time = datetime.datetime.now().replace(microsecond=0)
        return

    @staticmethod
    def _handle_resource_notif(engine_id, context, request_id, stack_id,
                               resource_id, convg_status):
        stack = parser.Stack.load(context, stack_id=stack_id)
        lock = EngineService._spin_wait_acquire_lock_or_timeout(engine_id, context,
                                                       stack)
        if not lock:
            stack.handle_timeout()
        try:
            stack.process_resource_notif(request_id, resource_id, convg_status)
        finally:
            lock.release(stack_id)


    @request_context
    def converge_resource(self, context, request_id, stack_id, resource_id, timeout):
        stack = parser.Stack.load(context, stack_id=stack_id)
        self.thread_group_mgr.start(stack_id, stack.do_converge,
                                    request_id, resource_id, timeout)

    @request_context
    def notify_resource_observed(self, context, request_id, stack_id,
                                 resource_id, convg_status):
        # Just launch in a thread.
        # simply start a new thread with handle_resouce_notif as func
        self.thread_group_mgr.start(stack_id, self._handle_resource_notif,
                                    self.engine_id, context, request_id,
                                    stack_id, resource_id, convg_status)

    @request_context
    def create_stack(self, cnxt, stack_name, template, params, files, args,
                     owner_id=None):
        """
        The create_stack method creates a new stack using the template
        provided.
        Note that at this stage the template has already been fetched from the
        heat-api process if using a template-url.

        :param cnxt: RPC context.
        :param stack_name: Name of the stack you want to create.
        :param template: Template of stack you want to create.
        :param params: Stack Input Params
        :param files: Files referenced from the template
        :param args: Request parameters/args passed from API
        :param owner_id: parent stack ID for nested stacks, only expected when
                         called from another heat-engine (not a user option)
        """
        LOG.info(_LI('Creating stack %s'), stack_name)

        def _stack_create(stack):

            if not stack.stack_user_project_id:
                stack.create_stack_user_project_id()

            # Create/Adopt a stack, and create the periodic task if successful
            if stack.adopt_stack_data:
                if not cfg.CONF.enable_stack_adopt:
                    raise exception.NotSupported(feature='Stack Adopt')

                stack.adopt()
            else:
                stack.create_start()

        stack = self._parse_template_and_validate_stack(cnxt,
                                                        stack_name,
                                                        template,
                                                        params,
                                                        files,
                                                        args,
                                                        owner_id)

        stack.store()

        self.thread_group_mgr.start_with_lock(cnxt, stack, self.engine_id,
                                              _stack_create, stack)

        return dict(stack.identifier())

    @staticmethod
    def _start_update_with_lock(engine_id, context, stack, updated_stack,
                                event=None):
        lock = EngineService._spin_wait_acquire_lock_or_timeout(engine_id,
                                                                context,
                                                                stack)
        if not lock:
            stack.handle_timeout()
        try:
            stack.update(updated_stack, event=event)
        finally:
            lock.release(stack.id)

    @request_context
    def update_stack(self, cnxt, stack_identity, template, params,
                     files, args):
        """
        The update_stack method updates an existing stack based on the
        provided template and parameters.
        Note that at this stage the template has already been fetched from the
        heat-api process if using a template-url.

        :param cnxt: RPC context.
        :param stack_identity: Name of the stack you want to create.
        :param template: Template of stack you want to create.
        :param params: Stack Input Params
        :param files: Files referenced from the template
        :param args: Request parameters/args passed from API
        """
        # Get the database representation of the existing stack
        db_stack = self._get_stack(cnxt, stack_identity)
        LOG.info(_LI('Updating stack %s'), db_stack.name)

        current_stack = parser.Stack.load(cnxt, stack=db_stack)

        if current_stack.action == current_stack.SUSPEND:
            msg = _('Updating a stack when it is suspended')
            raise exception.NotSupported(feature=msg)

        if current_stack.action == current_stack.DELETE:
            msg = _('Updating a stack when it is deleting')
            raise exception.NotSupported(feature=msg)

        # Now parse the template and any parameters for the updated
        # stack definition.
        env = environment.Environment(params)
        if args.get(rpc_api.PARAM_EXISTING, None):
            env.patch_previous_parameters(
                current_stack.env,
                args.get(rpc_api.PARAM_CLEAR_PARAMETERS, []))
        tmpl = templatem.Template(template, files=files, env=env)
        # NOTE: set predecessor only if the earlier one realized.
        if current_stack.status == 'COMPLETE':
            tmpl.predecessor = current_stack.t.id
        else:
            tmpl.id = current_stack.t.id
        if len(tmpl[tmpl.RESOURCES]) > cfg.CONF.max_resources_per_stack:
            raise exception.RequestLimitExceeded(
                message=exception.StackResourceLimitExceeded.msg_fmt)
        stack_name = current_stack.name
        common_params = api.extract_args(args)
        common_params.setdefault(rpc_api.PARAM_TIMEOUT,
                                 current_stack.timeout_mins)
        common_params.setdefault(rpc_api.PARAM_DISABLE_ROLLBACK,
                                 current_stack.disable_rollback)

        updated_stack = parser.Stack(cnxt, stack_name, tmpl,
                                     env, **common_params)
        updated_stack.parameters.set_stack_id(current_stack.identifier())

        self._validate_deferred_auth_context(cnxt, updated_stack)
        updated_stack.validate()

        event = eventlet.event.Event()

        th = self.thread_group_mgr.start(current_stack.id,
                                         self._start_update_with_lock,
                                         self.engine_id, context,
                                         current_stack, updated_stack,
                                         event=event)

        th.link(self.thread_group_mgr.remove_event, current_stack.id, event)
        self.thread_group_mgr.add_event(current_stack.id, event)
        return dict(current_stack.identifier())

    @request_context
    def stack_cancel_update(self, cnxt, stack_identity):
        """Cancel currently running stack update.

        :param cnxt: RPC context.
        :param stack_identity: Name of the stack for which to cancel update.
        """
        # Get the database representation of the existing stack
        db_stack = self._get_stack(cnxt, stack_identity)

        current_stack = parser.Stack.load(cnxt, stack=db_stack)
        if current_stack.state != (current_stack.UPDATE,
                                   current_stack.IN_PROGRESS):
            msg = _("Cancelling update when stack is %s"
                    ) % str(current_stack.state)
            raise exception.NotSupported(feature=msg)
        LOG.info(_LI('Starting cancel of updating stack %s') % db_stack.name)
        # stop the running update and take the lock
        # as we cancel only running update, the acquire_result is
        # always some engine_id, not None
        lock = stack_lock.StackLock(cnxt, current_stack,
                                    self.engine_id)
        engine_id = lock.try_acquire()
        # Current engine has the lock
        if engine_id == self.engine_id:
            self.thread_group_mgr.send(current_stack.id, 'cancel')

        # Another active engine has the lock
        elif stack_lock.StackLock.engine_alive(cnxt, engine_id):
            cancel_result = self._remote_call(
                cnxt, engine_id, self.listener.SEND,
                stack_identity, rpc_api.THREAD_CANCEL)
            if cancel_result is None:
                LOG.debug("Successfully sent %(msg)s message "
                          "to remote task on engine %(eng)s" % {
                              'eng': engine_id, 'msg': 'cancel'})
            else:
                raise exception.EventSendFailed(stack_name=current_stack.name,
                                                engine_id=engine_id)

    @request_context
    def validate_template(self, cnxt, template, params=None):
        """
        The validate_template method uses the stack parser to check
        the validity of a template.

        :param cnxt: RPC context.
        :param template: Template of stack you want to create.
        :param params: Stack Input Params
        """
        LOG.info(_LI('validate_template'))
        if template is None:
            msg = _("No Template provided.")
            return webob.exc.HTTPBadRequest(explanation=msg)

        tmpl = templatem.Template(template)

        # validate overall template
        try:
            tmpl.validate()
        except Exception as ex:
            return {'Error': six.text_type(ex)}

        # validate resource classes
        tmpl_resources = tmpl[tmpl.RESOURCES]

        env = environment.Environment(params)

        for res in tmpl_resources.values():
            ResourceClass = env.get_class(res['Type'])
            if ResourceClass == resources.template_resource.TemplateResource:
                # we can't validate a TemplateResource unless we instantiate
                # it as we need to download the template and convert the
                # parameters into properties_schema.
                continue

            props = properties.Properties(ResourceClass.properties_schema,
                                          res.get('Properties', {}),
                                          context=cnxt)
            deletion_policy = res.get('DeletionPolicy', 'Delete')
            try:
                ResourceClass.validate_deletion_policy(deletion_policy)
                props.validate(with_value=False)
            except Exception as ex:
                return {'Error': six.text_type(ex)}

        # validate parameters
        tmpl_params = tmpl.parameters(None, {})
        tmpl_params.validate(validate_value=False, context=cnxt)
        is_real_param = lambda p: p.name not in tmpl_params.PSEUDO_PARAMETERS
        params = tmpl_params.map(api.format_validate_parameter, is_real_param)
        param_groups = parameter_groups.ParameterGroups(tmpl)

        result = {
            'Description': tmpl.get('Description', ''),
            'Parameters': params,
        }

        if param_groups.parameter_groups:
            result['ParameterGroups'] = param_groups.parameter_groups

        return result

    @request_context
    def authenticated_to_backend(self, cnxt):
        """
        Verify that the credentials in the RPC context are valid for the
        current cloud backend.
        """
        return clients.Clients(cnxt).authenticated()

    @request_context
    def get_template(self, cnxt, stack_identity):
        """
        Get the template.

        :param cnxt: RPC context.
        :param stack_identity: Name of the stack you want to see.
        """
        s = self._get_stack(cnxt, stack_identity, show_deleted=True)
        if s:
            return s.raw_template.template
        return None

    def _remote_call(self, cnxt, lock_engine_id, call, *args, **kwargs):
        timeout = cfg.CONF.engine_life_check_timeout
        self.cctxt = self._client.prepare(
            version='1.0',
            timeout=timeout,
            topic=lock_engine_id)
        try:
            self.cctxt.call(cnxt, call, *args, **kwargs)
        except messaging.MessagingTimeout:
            return False

    @request_context
    def delete_stack(self, cnxt, stack_identity):
        """
        The delete_stack method deletes a given stack.

        :param cnxt: RPC context.
        :param stack_identity: Name of the stack you want to delete.
        """

        st = self._get_stack(cnxt, stack_identity)
        LOG.info(_LI('Deleting stack %s'), st.name)
        stack = parser.Stack.load(cnxt, stack=st)
        # TODO: spin wait for lock in another thread
        lock = stack_lock.StackLock(cnxt, stack, self.engine_id)
        with lock.try_thread_lock(stack.id) as acquire_result:

            # Successfully acquired lock
            if acquire_result is None:
                self.thread_group_mgr.stop_timers(stack.id)
                self.thread_group_mgr.start_with_acquired_lock(stack, lock,
                                                               stack.delete_start)
                return

        # Current engine has the lock
        if acquire_result == self.engine_id:
            # give threads which are almost complete an opportunity to
            # finish naturally before force stopping them
            eventlet.sleep(0.2)
            self.thread_group_mgr.stop(stack.id)

        # Another active engine has the lock
        elif stack_lock.StackLock.engine_alive(cnxt, acquire_result):
            stop_result = self._remote_call(
                cnxt, acquire_result, self.listener.STOP_STACK,
                stack_identity=stack_identity)
            if stop_result is None:
                LOG.debug("Successfully stopped remote task on engine %s"
                          % acquire_result)
            else:
                raise exception.StopActionFailed(stack_name=stack.name,
                                                 engine_id=acquire_result)

        # There may be additional resources that we don't know about
        # if an update was in-progress when the stack was stopped, so
        # reload the stack from the database.
        st = self._get_stack(cnxt, stack_identity)
        stack = parser.Stack.load(cnxt, stack=st)

        self.thread_group_mgr.start_with_lock(cnxt, stack, self.engine_id,
                                              stack.delete_start)
        return None


    def list_resource_types(self, cnxt, support_status=None):
        """
        Get a list of supported resource types.

        :param cnxt: RPC context.
        """
        return resources.global_env().get_types(support_status)


    @request_context
    def list_events(self, cnxt, stack_identity, filters=None, limit=None,
                    marker=None, sort_keys=None, sort_dir=None):
        """
        The list_events method lists all events associated with a given stack.
        It supports pagination (``limit`` and ``marker``),
        sorting (``sort_keys`` and ``sort_dir``) and filtering(filters)
        of the results.

        :param cnxt: RPC context.
        :param stack_identity: Name of the stack you want to get events for
        :param filters: a dict with attribute:value to filter the list
        :param limit: the number of events to list (integer or string)
        :param marker: the ID of the last event in the previous page
        :param sort_keys: an array of fields used to sort the list
        :param sort_dir: the direction of the sort ('asc' or 'desc').
        """

        if stack_identity is not None:
            st = self._get_stack(cnxt, stack_identity, show_deleted=True)

            events = db_api.event_get_all_by_stack(cnxt, st.id, limit=limit,
                                                   marker=marker,
                                                   sort_keys=sort_keys,
                                                   sort_dir=sort_dir,
                                                   filters=filters)
        else:
            events = db_api.event_get_all_by_tenant(cnxt, limit=limit,
                                                    marker=marker,
                                                    sort_keys=sort_keys,
                                                    sort_dir=sort_dir,
                                                    filters=filters)

        stacks = {}

        def get_stack(stack_id):
            if stack_id not in stacks:
                stacks[stack_id] = parser.Stack.load(cnxt, stack_id)
            return stacks[stack_id]

        return [api.format_event(Event.load(cnxt,
                                            e.id, e,
                                            get_stack(e.stack_id)))
                for e in events]

    def _authorize_stack_user(self, cnxt, stack, resource_name):
        '''
        Filter access to describe_stack_resource for stack in-instance users
        - The user must map to a User resource defined in the requested stack
        - The user resource must validate OK against any Policy specified
        '''
        # first check whether access is allowed by context user_id
        if stack.access_allowed(cnxt.user_id, resource_name):
            return True

        # fall back to looking for EC2 credentials in the context
        try:
            ec2_creds = json.loads(cnxt.aws_creds).get('ec2Credentials')
        except (TypeError, AttributeError):
            ec2_creds = None

        if not ec2_creds:
            return False

        access_key = ec2_creds.get('access')
        return stack.access_allowed(access_key, resource_name)

    @request_context
    def describe_stack_resource(self, cnxt, stack_identity, resource_name):
        s = self._get_stack(cnxt, stack_identity)
        stack = parser.Stack.load(cnxt, stack=s)

        if cfg.CONF.heat_stack_user_role in cnxt.roles:
            if not self._authorize_stack_user(cnxt, stack, resource_name):
                LOG.warn(_LW("Access denied to resource %s"), resource_name)
                raise exception.Forbidden()

        if resource_name not in stack:
            raise exception.ResourceNotFound(resource_name=resource_name,
                                             stack_name=stack.name)

        resource = stack[resource_name]
        if resource.id is None:
            raise exception.ResourceNotAvailable(resource_name=resource_name)

        return api.format_stack_resource(stack[resource_name])

    @request_context
    def find_physical_resource(self, cnxt, physical_resource_id):
        """
        Return an identifier for the resource with the specified physical
        resource ID.

        :param cnxt: RPC context.
        :param physical_resource_id: The physical resource ID to look up.
        """
        rs = db_api.resource_get_by_physical_resource_id(cnxt,
                                                         physical_resource_id)
        if not rs:
            raise exception.PhysicalResourceNotFound(
                resource_id=physical_resource_id)

        stack = parser.Stack.load(cnxt, stack_id=rs.stack.id)
        resource = stack[rs.name]

        return dict(resource.identifier())

    @request_context
    def describe_stack_resources(self, cnxt, stack_identity, resource_name):
        s = self._get_stack(cnxt, stack_identity)

        stack = parser.Stack.load(cnxt, stack=s)

        return [api.format_stack_resource(resource)
                for name, resource in six.iteritems(stack)
                if resource_name is None or name == resource_name]

    @request_context
    def list_stack_resources(self, cnxt, stack_identity, nested_depth=0):
        s = self._get_stack(cnxt, stack_identity, show_deleted=True)
        stack = parser.Stack.load(cnxt, stack=s)
        depth = min(nested_depth, cfg.CONF.max_nested_stack_depth)

        return [api.format_stack_resource(resource, detail=False)
                for resource in stack.iter_resources(depth)]
