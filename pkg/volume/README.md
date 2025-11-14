
VolumeManager is for the admin web to manage the volumes and the EC2 instances relationship.

We can create a volume via the admin web(VolumeProvider), and attach it to one or more EC2 instances.

After we attach the volume to the EC2 instances, we can use the VolumeManager to manage the relationship between the volume and the EC2 instances.

We have to let DEBS system know the current node has which volumes, so that we can serve the R/W request on the correct volume.