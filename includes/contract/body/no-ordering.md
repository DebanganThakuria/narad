Narad does not guarantee delivery order. Messages with the same key
usually arrive in the order they were produced, but redelivery, a broker
restart and routing around an unreachable partition owner all reorder
them. If you need a sequence, carry one in the payload.
