=================================
HEAT CONVERGENCE Proof-of-Concept
=================================

This repo is for demonstrating proof-of-concept for adopting convergence
into Openstack Heat.

Convergence spec: https://review.openstack.org/#/c/95907/
Convergence PoC Wiki: https://wiki.openstack.org/wiki/Heat/ConvergenceDesign
Persisting dependency graph and resource versioning: https://review.openstack.org/#/c/123749/


Getting Started
---------------

Dummy Resource for testing
~~~~~~~~~~~~~~~~~~~~~~~~~~

New resource named "Dummy" is used for testing. The resource
create/update/delete simply sleeps for few seconds (between 1-5) and
returns as done. Generates a random string as physical resource ID.

This resource has three properties:
1. name_len: Length of name which is random string.
2. update_in_place_prop: Just change this property to simulate
update-in-place behaviour. If this property is not changed, the resource
is replaced with new resource.
3. fail_prop: To fail a resource just pass fail_prop as "yes" while
creating/updating.


How to run
~~~~~~~~~~
Set up a devstack environment, download the local.conf and stackrc from
this repo and run stack.sh. The setup would be very light-weight, only
heat API and heat engine would be running as service.

Review the contents of local.conf w.r.t. host IP address and no_proxy
etc. and run stack.sh.

This PoC requires Database and AMQP, since the implementation needs its
features. With out them convergence si difficult to achieve. The code in
Heat engine beomes simpler with these supporting infrastructure. Please
read more on the wiki page on why didn't had standalone PoC.

The sample-templates contain few templates which you can use to
create/update the stack. Note that we have not covered SNAPSHOT,
SUSPEND, ABANDON/ADOPT, and RESUME features.

Changes
~~~~~~~
All the code not needed for PoC is removed. service.py, stack.py and
resource.py has most of the changes and almost re-written.

TODO
~~~~
* Clean up all the not needed code for PoC.
* Add resource incrementally as they are taken up for convergence.
* Handle updates with no actual resource updates in it.
* Test concurrent update.
* Test rollback with concurrent update.
* Only load the resource and its children to while updating frozen
  resource definition in DB.
* SCHEDULED status of resource to take care of updates while resource is
  in transit to worker back and forth.(Done)
* Each update generates an new unique req_id. Workers panic after seeing
  new req_id and result in noop. (Done)
* Each notification and converge request should immedeatly spawn an
  eventlet thread to process the request, not blocking the RPC initi
  thread. This is required as the RPC is cast and not call.(Done)
* Remove stack lock and use DB tranasctions.
* If an update is issued while update in progress, update the graph and
  wait for notifications from worker for previous jobs. Trigger the next
  set of jobs then, after recieving the notifications instead of
  triggering right away.
* Rollback should look back, at older versions, instead of blindly
  issuing another update.
* Graph state should be integer type. Create enum for readabiliy.
* Act based on CONVG_STATE instead of again loading the resource from
  DB.
* Change update logic to delete the resources first if rollabck is not
  enabled. Deduce the sequence of GC from stack's disabled_rollback var.
* Rollback should look in current template and decide on what needs to
  be done Delete all the not found resources first and then update
  remaining.
* Determine if concurrent update is running by inspecting the incming
  request_id and only if so, run filter to find scheduled/in_progress
  versions of a resource.
* Decouple resource from stack, or make stack optional so that resource
  alone can be loaded by worker and converged.
* Rename RsourceGraph -> DependencyTaskGraph.
* Separate convergence worker for SoC.
* Update logic in service.py can completely move to stack's update
  method.
