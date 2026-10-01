A `202 Accepted` means the node that answered has written your message to
its write-ahead log and fsynced it. From then on a crash, restart or power
loss of any node does not lose it. Consumers do not see it yet: the node
hands it to the partition's owner in the background, usually within
milliseconds. A produce that times out may or may not have been accepted,
so retrying it can create a duplicate.
