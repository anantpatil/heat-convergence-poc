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

from heat.common.i18n import _LI
from heat.db import api as db_api
from heat.engine import resource
from heat.engine import scheduler
from heat.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class StackUpdate(object):
    """
    A Task to perform the update of an existing stack to a new template.
    """

    def __init__(self, stack, rollback=False, error_wait_time=None):
        """Initialise with the existing stack and the new stack."""
        self.stack = stack
        self.context = self.stack.context
        self.stack_id = self.stack.id

        self.rollback = rollback
        self.error_wait_time = error_wait_time

        self.current_snippets = dict((n, r.frozen_definition())
                                     for n, r in self.stack.items())

    def __repr__(self):
        if self.rollback:
            return '%s Rollback' % str(self.stack)
        else:
            return '%s Update' % str(self.stack)

    @scheduler.wrappertask
    def __call__(self):
        """Return a co-routine that updates the stack."""

        # TODO: Rakesh: If rollback not enabled, delete the deleted resources first
        # and then update the resources to take care of user quota

        self.updater = scheduler.DependencyTaskGroup(
            self.context,
            self.stack_id,
            self._update_resource,
            error_wait_time=self.error_wait_time)

        self.gc = scheduler.DependencyTaskGroup(
            self.context,
            self.stack_id,
            self._reclaim_resource_if_needed,
            reverse=True)

        yield self.updater()

        # delete the old versions and delete resources from stack.
        db_api.update_resource_traversal(self.context, self.stack_id,
                                         traversed=False)
        LOG.debug("==== Running GC...")
        yield self.gc()

    def _update_resource(self, res_name):
        old_resource_db = db_api.resource_get_by_name_and_stack(
            self.context, res_name, self.stack_id)
        if res_name in self.current_snippets.keys():
            # Resource is in new template, update it if it's in old template
            # otherwise create a new one
            new_resource = self.stack.resources[res_name]
            if old_resource_db:
                # update this
                LOG.debug("==== Resource found in both old and new templates")
                old_resource = resource.Resource.load(old_resource_db,
                                                      self.stack)
                return self._process_resource_update(new_resource, old_resource)
            else:
                # new resource to be created
                LOG.debug("==== Resource found in new template only...creating it")
                return self._create_resource(new_resource)
        else:
            LOG.debug("==== Resource not found in new template...marking deleted.")
            self._create_delete_version(old_resource_db)
            return

    def copyof(self, db_resource):
        new_resource = resource.Resource.load(db_resource, self.stack)
        return new_resource

    def _create_delete_version(self, old_resource_db):
            '''
            Create a DELETE version in DB to be deleted in GC phase.
            '''
            old_resource = resource.Resource.load(old_resource_db,
                                                  self.stack)
            new_resource = self.copyof(old_resource_db)
            new_resource.resource_id = old_resource.resource_id
            new_resource.version = old_resource.version + 1
            # to create new version in DB, reset id to None
            new_resource.id = None
            new_resource.store_update(old_resource.DELETE,
                                      old_resource.INIT,
                                      "Resource not in new template")

            # set physical resource id to none for old resource
            old_resource.resource_id = None
            old_resource.store_update(old_resource.action,
                                      old_resource.status,
                                      old_resource.status_reason)
            db_api.update_resource_traversal(
                self.context, self.stack_id, traversed=True,
                res_name=old_resource.name)


    def _reclaim_resource_if_needed(self, res_name):
        LOG.debug("==== Reclaiming older versions and deleted resources")
        db_resource = db_api.resource_get_by_name_and_stack(
            self.context, res_name, self.stack_id)
        res = resource.Resource.load(db_resource,
                                     self.stack)
        # if resource is marked as DELETE, INIT, delete all versions
        # and edges from graph
        if (res.action, res.status) == (res.DELETE, res.INIT):
            return self._delete_all_versions(res)
        else:
            return self._delete_older_versions(res)

    @scheduler.wrappertask
    def _delete_all_versions(self, res):
        LOG.debug("==== Reclaiming all versions of deleted resource %s", res.name)
        # get the resource sorted by versions in descending order
        db_resources = db_api.resource_get_all_versions_by_name_and_stack(
            self.context, res.name, self.stack_id)
        LOG.debug("==== resources returned in order:")
        for res in db_resources:
            LOG.debug("==== res_name: %s version %s", res.name, res.version)
        for db_resource in db_resources:
            if db_resource.nova_instance:
                res = resource.Resource.load(db_resource, self.stack)
                yield res.destroy()
            else:
                # remove the DB entry
                LOG.debug("==== Removing resource from DB: %s "
                          "version: %s", db_resource.name, db_resource.version)
                db_api.resource_delete(self.context, db_resource.id)

        LOG.debug("==== Deleting all the edges for %s", res.name)
        self._delete_all_edges(res.name)

    def _delete_all_edges(self, res_name):
        '''
        Delete all the edges from the graph
        :param res_name:
        :return:
        '''
        db_api.resource_graph_delete_all_edges(self.context, self.stack_id, res_name)

    @scheduler.wrappertask
    def _delete_older_versions(self, res):
        '''
        Only delete older versions
        :param res:
        :return:
        '''
        LOG.debug("==== Deleting older version for %s: ", res.name)
        db_resources = db_api.resource_get_all_versions_by_name_and_stack(
            self.context, res.name, self.stack_id)
        if len(db_resources) == 1:
            LOG.debug("==== No older version were found for %s: ", res.name)
        else:
            for db_res in db_resources[1:]:
                if db_res.nova_instance:
                    # physically remove
                    LOG.debug("==== Physically removing resource: %s"
                              " with version: %s", db_res.name, db_res.version)
                    r = resource.Resource.load(db_res, self.stack)
                    yield r.destroy()
                else:
                    # remove the DB entry
                    LOG.debug("==== Removing resource from DB: %s "
                              "version: %s", db_res.name, db_res.version)
                    db_api.resource_delete(self.context, db_res.id)
        db_api.update_resource_traversal(
            self.context, self.stack_id, traversed=True, res_name=res.name)

    @scheduler.wrappertask
    def _create_resource(self, new_res):
        LOG.debug("==== Creating resource: %s", new_res.name)
        new_res.action = new_res.INIT
        new_res.status = new_res.COMPLETE
        yield new_res.create()

    @scheduler.wrappertask
    def _process_resource_update(self, new_resource, old_resource):
        res_name = new_resource.name
        new_resource.version = old_resource.version + 1
        LOG.debug("==== Processing update on resource %s: ", res_name)
        try:
            if new_resource.needs_update(old_resource.t):
                LOG.debug("==== Resource %s needs update: ", res_name)
                new_resource.id = None
                new_resource.action = new_resource.UPDATE
                new_resource.status = new_resource.INIT
                new_resource.resource_id = old_resource.resource_id
                new_resource.store()
                LOG.debug("==== Created new version for %s ", res_name)
                yield self._update_in_place(new_resource, old_resource.t)
            else:
                LOG.info("==== Resource %s doesn't need update: ", res_name)
                db_api.update_resource_traversal(
                    self.context, self.stack_id, traversed=True, res_name=res_name)
                return
        except resource.UpdateReplace:
            pass
        else:
            LOG.info(_LI("Resource %(res_name)s for stack %(stack_name)s "
                         "updated"), {'res_name': res_name,
                                      'stack_name': self.stack.name})
            # remove physical resource id from prev version
            old_resource.resource_id = None
            old_resource.store_update()
            return

        LOG.debug("==== UpdateReplace failed, creating new %s", res_name)
        yield self._create_resource(new_resource)

    @scheduler.wrappertask
    def _update_in_place(self, new_resource, old_rsrc_defn):
        # existing_snippet = self.current_snippets[existing_res.name]

        # Note the new resource snippet is resolved in the context
        # of the existing stack (which is the stack being updated)
        # but with the template of the new stack (in case the update
        # is switching template implementations)
        #new_snippet = new_res.t.reparse(self.stack,
                                        #self.new_template)
        existing_snippet = old_rsrc_defn
        return new_resource.update(new_resource.t, existing_snippet)
