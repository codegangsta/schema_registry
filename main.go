package main

import (
	"context"
	"log"
	"runtime"

	"github.com/invopop/jsonschema"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func main() {
	err := Connect()
	if err != nil {
		panic(err)
	}

	runtime.Goexit()
}

func Connect() error {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return err
	}

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      "schema_registry",
		Description: "Register and manages schemas.",
		History:     10,
	})
	if err != nil {
		return err
	}

	// Create our schema registry
	registry := NewSchemaRegistry(kv, nc)
	err = registry.Watch(context.Background())
	if err != nil {
		return err
	}

	svc, err := micro.AddService(nc, micro.Config{
		Name:        "schema_registry",
		Description: "Register and manage schemas. Validate payloads against schemas.",
		Version:     "0.0.1",
	})
	if err != nil {
		return err
	}

	reflector := jsonschema.Reflector{
		DoNotReference: true,
	}

	schema, err := reflector.Reflect(&Schema{}).MarshalJSON()
	if err != nil {
		return err
	}

	svc.AddEndpoint("register", micro.HandlerFunc(registry.RegisterSchema),
		micro.WithEndpointSubject("$SCHEMA.REGISTER.*"),
		micro.WithEndpointSchema(&micro.Schema{
			Request:  string(schema),
			Response: string(schema),
		}))

	svc.AddEndpoint("get", micro.HandlerFunc(registry.GetSchema),
		micro.WithEndpointSubject("$SCHEMA.GET.*"),
		micro.WithEndpointSchema(&micro.Schema{
			Response: string(schema),
		}))

	svc.AddEndpoint("unregister", micro.HandlerFunc(registry.UnregisterSchema),
		micro.WithEndpointSubject("$SCHEMA.UNREGISTER.*"))

	svc.AddEndpoint("update", micro.HandlerFunc(registry.UpdateSchema),
		micro.WithEndpointSubject("$SCHEMA.UPDATE.*"),
		micro.WithEndpointSchema(&micro.Schema{
			Request:  string(schema),
			Response: string(schema),
		}))

	svc.AddEndpoint("validate", micro.HandlerFunc(func(r micro.Request) {}),
		micro.WithEndpointSubject("$SCHEMA.VALIDATE.>"))

	// Schema validation needs to have more access to the NATS message, namely the reply subject,
	// so we need to use a raw subscription instead of the service API.
	_, err = nc.QueueSubscribe("$SCHEMA.VALIDATE.>", "schema_registry", registry.ValidatePayload)
	if err != nil {
		return err
	}

	log.Println("Connected to NATS for schema_registry", nc.ConnectedUrl())

	return nil
}
