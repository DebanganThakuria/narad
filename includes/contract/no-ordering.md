!!! note "No ordering guarantee"
    Narad does not guarantee delivery order. Messages with the same key
    usually arrive in the order they were produced, but redelivery, a broker
    restart and routing around an unreachable partition owner all reorder
    them. If you need a sequence, carry one in the payload.
    [All five causes are in the delivery contract](../understand/delivery-contract.md#ordering).
