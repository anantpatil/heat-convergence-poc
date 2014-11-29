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

New resource named "random_sleep" is used for testing. The resource
create/update/delete simply sleeps for few seconds (between 1-10) and
returns as done. Generates a random string as physical resource ID.

This resource update-in-place property. Just change this property to
emulate update-in-place behaviour. If this property is not changed, the
resource is replaced with new resource.

To fail a resource just pass fail_property as 1 while creating/updating.


How to run
----------
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
-------
All the code not needed for PoC is removed.
service.py, stack.py and resource.py has most of the changes and almost
re-written.
