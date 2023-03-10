package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/xeipuuv/gojsonschema"
)

type Schema struct {
	Name     string `json:"name"`
	Subject  string `json:"subject"`
	Revision uint64 `json:"revision,omitempty"`
	Type     string `json:"type"`
	Body     string `json:"body"`
}

type SchemaRegistry struct {
	// Contain nats kv and have methods for schema crud and validation
	kv nats.KeyValue
	nc *nats.Conn

	schemas   map[string]Schema
	schemasMu sync.RWMutex
}

func NewSchemaRegistry(kv nats.KeyValue, nc *nats.Conn) *SchemaRegistry {
	return &SchemaRegistry{
		nc:      nc,
		kv:      kv,
		schemas: map[string]Schema{},
	}
}

// Watch watches the kv store for changes and adds them to a
// local cache of schemas. It runs this in a goroutine and takes a context for
// cancelation.
func (reg *SchemaRegistry) Watch(c context.Context) error {
	// Watch the kv store for changes
	watcher, err := reg.kv.WatchAll()
	if err != nil {
		return err
	}

	// Run this in a goroutine
	go func() {
		for {
			select {
			case <-c.Done():
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					log.Println("Loaded initial schemas")
					continue
				}

				var schema Schema
				err := json.Unmarshal(entry.Value(), &schema)
				if err != nil {
					log.Printf("error unmarshaling schema: %v", err)
					continue
				}
				schema.Revision = entry.Revision()

				reg.schemasMu.Lock()
				reg.schemas[schema.Name] = schema
				reg.schemasMu.Unlock()
				log.Printf("Loaded schema: %q revision %d", schema.Name, schema.Revision)
			}
		}
	}()

	return nil
}

// Register subject: $SCHEMA.REGISTER.<schema_name>
func (reg *SchemaRegistry) RegisterSchema(r micro.Request) {
	var schema Schema
	err := json.Unmarshal(r.Data(), &schema)
	if err != nil {
		r.Error("400", err.Error(), nil)
		return
	}

	// Pull out the schema name from the subject
	parts := strings.Split(r.Subject(), ".")
	schema.Name = parts[len(parts)-1]

	// Put the schema in the kv store
	data, err := json.Marshal(schema)
	if err != nil {
		r.Error("400", err.Error(), nil)
		return
	}

	rev, err := reg.kv.Create(schema.Name, data)
	if err != nil {
		r.Error("500", err.Error(), nil)
		return
	}

	schema.Revision = rev
	r.RespondJSON(schema)
}

// Register subject: $SCHEMA.UNREGISTER.<schema_name>
func (reg *SchemaRegistry) UnregisterSchema(r micro.Request) {
	parts := strings.Split(r.Subject(), ".")
	name := parts[len(parts)-1]

	// remove the schema from the kv store
	err := reg.kv.Delete(name)
	if err != nil {
		r.Error("500", err.Error(), nil)
		return
	}

	r.Respond(nil)
}

// Get subject: $SCHEMA.GET.<schema_name>
func (reg *SchemaRegistry) GetSchema(r micro.Request) {
	parts := strings.Split(r.Subject(), ".")
	name := parts[len(parts)-1]

	// Get the schema from the kv store
	schema, ok := reg.schemas[name]
	if !ok {
		r.Error("404", "Not found", nil)
		return
	}
	r.RespondJSON(schema)
}

// Update subject: $SCHEMA.UPDATE.<schema_name>
func (reg *SchemaRegistry) UpdateSchema(r micro.Request) {
	var schema Schema
	err := json.Unmarshal(r.Data(), &schema)
	if err != nil {
		r.Error("400", err.Error(), nil)
		return
	}

	// Pull out the schema name from the subject
	parts := strings.Split(r.Subject(), ".")
	schema.Name = parts[len(parts)-1]

	// Put the schema in the kv store
	data, err := json.Marshal(schema)
	if err != nil {
		r.Error("400", err.Error(), nil)
		return
	}

	rev, err := reg.kv.Put(schema.Name, data)
	if err != nil {
		r.Error("500", err.Error(), nil)
		return
	}

	schema.Revision = rev
	r.RespondJSON(schema)
}

// Validate subject: $SCHEMA.VALIDATE.<subject>
func (reg *SchemaRegistry) ValidatePayload(m *nats.Msg) {
	reg.schemasMu.RLock()
	defer reg.schemasMu.RUnlock()

	// Pull out the subject from the request subject
	parts := strings.Split(m.Subject, ".")
	subject := strings.Join(parts[2:], ".")

	// find a schema that matches the subject
	for _, schema := range reg.schemas {
		if !SubjectsMatch(subject, schema.Subject) {
			continue
		}

		// validate the payload
		err := reg.validate(m.Data, schema)
		if err != nil {
			m.Respond([]byte(err.Error()))
			return
		}

		msg := nats.NewMsg(subject)
		msg.Reply = m.Reply
		msg.Data = m.Data
		msg.Header = m.Header
		if msg.Header == nil {
			msg.Header = nats.Header{}
		}
		msg.Header.Set("Schema-Name", schema.Name)
		msg.Header.Set("Schema-Revision", fmt.Sprintf("%d", schema.Revision))
		msg.Header.Set("Schema-Subject", schema.Subject)
		msg.Header.Set("Schema-Type", schema.Type)
		msg.Header.Set("Schema-Validated", "true")
		err = reg.nc.PublishMsg(msg)

		if err != nil {
			log.Printf("error publishing message: %v", err)
			m.Respond([]byte(err.Error()))
			return
		}

		return
	}

	errorMessage := fmt.Sprintf("could not find schema for subject %q", subject)
	fmt.Println(errorMessage)
	m.Respond([]byte(errorMessage))
}

func (reg *SchemaRegistry) validate(data []byte, schema Schema) error {
	dataBody := gojsonschema.NewStringLoader(string(data))
	schemaBody := gojsonschema.NewStringLoader(schema.Body)

	result, err := gojsonschema.Validate(schemaBody, dataBody)
	if err != nil {
		return err
	}
	if !result.Valid() {
		var errors []string
		for _, desc := range result.Errors() {
			errors = append(errors, desc.String())
		}
		return fmt.Errorf("invalid payload: %v", strings.Join(errors, ", "))
	}

	return nil
}

// SubjectsMatch returns true if the literal subject matches the wildcard subject.
// Subjects are case sensitive and can contain tokens delimited by the dot (.) character.
// The wildcard subject can contain the * wildcard.
// Examples: foo.bar, foo.bar.baz, foo.*.baz
// Wildcards can also have a catch all suffix of >
// Examples: foo.>, foo.bar.>
func SubjectsMatch(literal string, wildcard string) bool {
	if literal == wildcard {
		return true
	}

	lparts := strings.Split(literal, ".")
	wparts := strings.Split(wildcard, ".")

	for i, wpart := range wparts {
		if len(lparts) <= i {
			return false
		}
		if wpart == "*" {
			continue
		}
		if wpart == ">" {
			return true
		}
		if lparts[i] != wpart {
			return false
		}
	}

	return true
}
