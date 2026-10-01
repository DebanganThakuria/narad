Narad stores each partition once, on the volume of the node that owns it.
A crash, restart or power loss loses nothing that got a `202`; losing a
volume loses the partitions on it. For a second copy, add a replica child
or take volume snapshots, as
[Back up and replicate topics](../operate/backups.md) shows.
