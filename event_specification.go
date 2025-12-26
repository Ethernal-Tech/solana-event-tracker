package tracker

import (
	"crypto/sha256"
	"reflect"
)

// eventSpec defines the specification for a Solana (Anchor-based) program event that should
// be monitored and indexed by the indexer. Each eventSpec contains the necessary information
// to identify, deserialize, and process events emitted by Anchor-based Solana programs.
type eventSpec struct {
	// eventType holds a zero-value instance of the concrete Anchor-generated event type that
	// will be used for deserialization. It MUST be a non-pointer type to avoid potential race
	// conditions that can lead to data race.
	eventType any

	// name is the human-readable name of the event as defined in the Anchor program. It is
	// used to calculate event discriminant.
	name string

	// discriminant is the 8-byte Anchor event discriminator used to uniquely identify event
	// type in transaction logs.
	discriminant [8]byte
}

// ProgramEventSpecs represents a collection of event specifications that should be tracked for
// a particular Solana program.
type ProgramEventSpecs []eventSpec

// AddEventSpec adds a new event specification to the collection. The first parameter represents
// the Anchor program event that should be tracked and it must be a zero-value instance of the
// Anchor-generated type that implements `UnmarshalWithDecoder(decoder *binary.Decoder) error`
// method. If the type does not implement this method, deserialization will fail during indexing,
// so you should always use Anchor-generated bindings to ensure proper implementation. The name
// parameter must exactly match the event name defined in the Anchor program's #[event] attribute,
// as it is used to calculate the 8-byte discriminator for event identification.
func (s *ProgramEventSpecs) AddEventSpec(eventType any, name string) *ProgramEventSpecs {
	val := reflect.ValueOf(eventType)
	if val.Kind() == reflect.Ptr {
		eventType = val.Elem().Interface()
	}

	*s = append(*s, eventSpec{
		eventType:    eventType,
		name:         name,
		discriminant: calculateEventDiscriminant(name),
	})

	return s
}

func calculateEventDiscriminant(eventName string) [8]byte {
	// The discriminant is computed as the first 8 bytes of the SHA-256 hash of the string
	// "event:<event_name>". For example, if the event name is "TransactionExecutedEvent",
	// the discriminant is calculated as:
	//
	// SHA256("event:"+"TransactionExecutedEvent")[:8]

	preimage := "event:" + eventName

	hash := sha256.Sum256([]byte(preimage))

	return [8]byte(hash[:8])
}
