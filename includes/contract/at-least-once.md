!!! note "Delivery is at least once"
    Narad delivers a message until a consumer acks it, as long as the topic's
    retention still holds it, and it can deliver the same message more than
    once: after a lease expires, after a nack, or after a broker crash. Make
    every handler idempotent, for example by keying its work on an ID in the
    payload.
    [Every case is in the delivery contract](../understand/delivery-contract.md#at-least-once).
