# NATS Schema Registry Example

This example shows how to build a NATS Schema Registry using the NATS Service API and NATS KV Store.

This example allows you to register a schema, and then essentially become a "Gatekeeper" for your JetStream streams.

## Usage

Run a NATS server with JetStream enabled:
```bash
nats-server -js
```

Run the example against a local NATS server:
```bash
go run .
```

Register a schema (you can use the sample.json in this repo):
```bash
cat sample.json | nats req '$SCHEMA.REGISTER.my_cool_schema'
```

Publish a message to a stream that uses the schema:

```bash
nats req '$SCHEMA.VALIDATE.numbers.foobar' 1 # This should work, and the message is passed to numbers.foobar
nats req '$SCHEMA.VALIDATE.numbers.foobar' abc # This should fail
```


## TODO
I wrote this while on a stream, there is still plenty to add or improve:
- [ ] Use natscontext to support different server addresses and credentials
- [ ] Add a way to list all registered schemas
- [ ] Respond with more appropriate error codes that are JetStream compatible when validation fails
- [ ] Support a more graceful shutdown
- [ ] Make KV backing configurable
- [ ] Support more than just jsonschema
