package storage_handler

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"

	tracker "github.com/Ethernal-Tech/solana-event-tracker"
	"github.com/gagliardetto/solana-go"
	bolt "go.etcd.io/bbolt"
)

type BoltStorageHandler struct {
	txMode bool
	db     *bolt.DB
}

// EventRecord represents a stored event with metadata
type EventRecord struct {
	ID        uint64                 `json:"id"`
	Slot      uint64                 `json:"slot"`
	Program   string                 `json:"program"`
	EventType string                 `json:"event_type"`
	Data      map[string]interface{} `json:"data"`
}

var (
	slotBucket              = []byte("slot")
	unprocessedEventsBucket = []byte("unprocessed_events")
	processedEventsBucket   = []byte("processed_events")
	eventIDCounterBucket    = []byte("event_id_counter")
)

func NewBoltStorageHandler(path string, txMode bool) (*BoltStorageHandler, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot open bolt db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(slotBucket)
		if err != nil {
			return fmt.Errorf("cannot create the slot bucket: %w", err)
		}

		// Create unprocessed events bucket
		_, err = tx.CreateBucketIfNotExists(unprocessedEventsBucket)
		if err != nil {
			return fmt.Errorf("cannot create the unprocessed events bucket: %w", err)
		}

		// Create processed events bucket
		_, err = tx.CreateBucketIfNotExists(processedEventsBucket)
		if err != nil {
			return fmt.Errorf("cannot create the processed events bucket: %w", err)
		}

		// Create event ID counter bucket
		_, err = tx.CreateBucketIfNotExists(eventIDCounterBucket)
		if err != nil {
			return fmt.Errorf("cannot create the event ID counter bucket: %w", err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &BoltStorageHandler{txMode, db}, nil
}

func (b *BoltStorageHandler) Close() {
	b.db.Close()
}

func encodeSlotValue(slot uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, slot)
	return b
}

func decodeSlotValue(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func (b *BoltStorageHandler) ReadSlot() (uint64, error) {
	var retValue uint64

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(slotBucket)
		if bucket == nil {
			return fmt.Errorf("cannot find slot bucket")
		}

		value := bucket.Get([]byte("current"))
		if value == nil {
			retValue = 0
		} else {
			retValue = decodeSlotValue(value)
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return retValue, nil
}

func (b *BoltStorageHandler) StoreSlot(tx tracker.StorageTransaction, slot uint64) error {
	storeFn := func(tx *bolt.Tx) error {
		bucket := tx.Bucket(slotBucket)
		if bucket == nil {
			return fmt.Errorf("cannot find slot bucket")
		}

		return bucket.Put([]byte("current"), encodeSlotValue(slot+1))
	}

	if tx == nil {
		return b.db.Update(storeFn)
	}

	if tx, ok := tx.(*bolt.Tx); ok {
		return storeFn(tx)
	}

	return fmt.Errorf("unknown storage transaction type")
}

func (b *BoltStorageHandler) StoreEvent(
	tx tracker.StorageTransaction,
	slot uint64,
	programID solana.PublicKey,
	eventName string,
	eventData any) error {
	storeFn := func(tx *bolt.Tx) error {
		eventsBucket := tx.Bucket(eventsBucket)
		if eventsBucket == nil {
			return fmt.Errorf("cannot find events bucket")
		}

		slotBucket, err := eventsBucket.CreateBucketIfNotExists(encodeSlotValue(slot))
		if err != nil {
			return fmt.Errorf("cannot create bucket for the slot %v: %w", slot, err)
		}

		programBucket, err := slotBucket.CreateBucketIfNotExists(programID.Bytes())
		if err != nil {
			return fmt.Errorf("cannot create bucket for the program %v: %w", programID, err)
		}

		eventTypeBucket, err := programBucket.CreateBucketIfNotExists([]byte(eventName))
		if err != nil {
			return fmt.Errorf("cannot create bucket for the event type %v: %w", eventName, err)
		}

		value, err := json.Marshal(reflect.ValueOf(eventData).Elem().Interface())
		if err != nil {
			return fmt.Errorf("cannot serialize (marshal) event data: %w", err)
		}

		hash := sha256.Sum256(value)
		key := hash[:]

		return eventTypeBucket.Put(key, value)
	}

	if tx == nil {
		return b.db.Update(storeFn)
	}

	if tx, ok := tx.(*bolt.Tx); ok {
		return storeFn(tx)
	}

	return fmt.Errorf("unknown storage transaction type")
}

func (b *BoltStorageHandler) UseTransactions() bool {
	return b.txMode
}

func (b *BoltStorageHandler) ApplyTransaction(
	slotFn func(tracker.StorageTransaction) error,
	eventFns []func(tracker.StorageTransaction) error) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		for _, fn := range eventFns {
			if err := fn(tx); err != nil {
				return err
			}
		}

		return slotFn(tx)
	})
}

// GetEventsByType retrieves all events of a specific type across all slots
func (b *BoltStorageHandler) GetEventsByType(eventName string, eventType any) ([]interface{}, error) {
	var results []interface{}

	err := b.db.View(func(tx *bolt.Tx) error {
		eventsBucket := tx.Bucket(eventsBucket)
		if eventsBucket == nil {
			return nil // No events stored yet
		}

		// Iterate through all slots
		return eventsBucket.ForEach(func(slotKey, _ []byte) error {
			slotBucket := eventsBucket.Bucket(slotKey)
			if slotBucket == nil {
				return nil
			}

			// Iterate through all programs in this slot
			return slotBucket.ForEach(func(programKey, _ []byte) error {
				programBucket := slotBucket.Bucket(programKey)
				if programBucket == nil {
					return nil
				}

				// Check if this event type exists
				eventTypeBucket := programBucket.Bucket([]byte(eventName))
				if eventTypeBucket == nil {
					return nil // This program doesn't have this event type
				}

				// Iterate through all events of this type
				return eventTypeBucket.ForEach(func(k, v []byte) error {
					// Create new instance of the event type
					eventPtr := reflect.New(reflect.TypeOf(eventType))
					if err := json.Unmarshal(v, eventPtr.Interface()); err != nil {
						return fmt.Errorf("failed to unmarshal event: %w", err)
					}
					results = append(results, eventPtr.Elem().Interface())
					return nil
				})
			})
		})
	})

	return results, err
}

// GetEventsBySlotRange retrieves all events within a slot range (inclusive)
func (b *BoltStorageHandler) GetEventsBySlotRange(startSlot, endSlot uint64) (map[string][]interface{}, error) {
	results := make(map[string][]interface{}) // eventName -> []events

	err := b.db.View(func(tx *bolt.Tx) error {
		eventsBucket := tx.Bucket(eventsBucket)
		if eventsBucket == nil {
			return nil
		}

		// Iterate through all slots
		return eventsBucket.ForEach(func(slotKey, _ []byte) error {
			slot := decodeSlotValue(slotKey)

			// Skip if outside range
			if slot < startSlot || slot > endSlot {
				return nil
			}

			slotBucket := eventsBucket.Bucket(slotKey)
			if slotBucket == nil {
				return nil
			}

			// Iterate through all programs
			return slotBucket.ForEach(func(programKey, _ []byte) error {
				programBucket := slotBucket.Bucket(programKey)
				if programBucket == nil {
					return nil
				}

				// Iterate through all event types
				return programBucket.ForEach(func(eventNameKey, _ []byte) error {
					eventName := string(eventNameKey)
					eventTypeBucket := programBucket.Bucket(eventNameKey)
					if eventTypeBucket == nil {
						return nil
					}

					// Iterate through all events
					return eventTypeBucket.ForEach(func(k, v []byte) error {
						// Store as raw JSON since we don't know the type
						var event map[string]interface{}
						if err := json.Unmarshal(v, &event); err != nil {
							return fmt.Errorf("failed to unmarshal event: %w", err)
						}

						event["_slot"] = slot // Add slot info
						results[eventName] = append(results[eventName], event)
						return nil
					})
				})
			})
		})
	})

	return results, err
}

// GetEventCountByType returns counts of events grouped by type
func (b *BoltStorageHandler) GetEventCountByType() (map[string]int, error) {
	counts := make(map[string]int)

	err := b.db.View(func(tx *bolt.Tx) error {
		eventsBucket := tx.Bucket(eventsBucket)
		if eventsBucket == nil {
			return nil
		}

		// Iterate through all slots
		return eventsBucket.ForEach(func(slotKey, _ []byte) error {
			slotBucket := eventsBucket.Bucket(slotKey)
			if slotBucket == nil {
				return nil
			}

			// Iterate through all programs
			return slotBucket.ForEach(func(programKey, _ []byte) error {
				programBucket := slotBucket.Bucket(programKey)
				if programBucket == nil {
					return nil
				}

				// Iterate through all event types
				return programBucket.ForEach(func(eventNameKey, _ []byte) error {
					eventName := string(eventNameKey)
					eventTypeBucket := programBucket.Bucket(eventNameKey)
					if eventTypeBucket == nil {
						return nil
					}

					// Count events of this type
					eventCount := 0
					_ = eventTypeBucket.ForEach(func(k, v []byte) error {
						eventCount++
						return nil
					})

					counts[eventName] += eventCount
					return nil
				})
			})
		})
	})

	return counts, err
}

// GetEventsSinceSlot retrieves all events from a specific slot onwards
func (b *BoltStorageHandler) GetEventsSinceSlot(startSlot uint64) ([]EventRecord, error) {
	var results []EventRecord

	err := b.db.View(func(tx *bolt.Tx) error {
		eventsBucket := tx.Bucket(eventsBucket)
		if eventsBucket == nil {
			return nil
		}

		return eventsBucket.ForEach(func(slotKey, _ []byte) error {
			slot := decodeSlotValue(slotKey)

			// Only process slots >= startSlot
			if slot < startSlot {
				return nil
			}

			slotBucket := eventsBucket.Bucket(slotKey)
			if slotBucket == nil {
				return nil
			}

			// Iterate through programs
			return slotBucket.ForEach(func(programKey, _ []byte) error {
				programBucket := slotBucket.Bucket(programKey)
				if programBucket == nil {
					return nil
				}

				// Iterate through event types
				return programBucket.ForEach(func(eventNameKey, _ []byte) error {
					eventTypeBucket := programBucket.Bucket(eventNameKey)
					if eventTypeBucket == nil {
						return nil
					}

					// Iterate through events
					return eventTypeBucket.ForEach(func(k, v []byte) error {
						var rawEvent map[string]interface{}
						if err := json.Unmarshal(v, &rawEvent); err != nil {
							return err
						}

						results = append(results, EventRecord{
							Slot:      slot,
							Program:   string(programKey),
							EventType: string(eventNameKey),
							Data:      rawEvent,
						})
						return nil
					})
				})
			})
		})
	})

	return results, err
}
