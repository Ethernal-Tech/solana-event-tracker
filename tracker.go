package tracker

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"

	binary "github.com/gagliardetto/binary"
)

type eventTrackerState string

var (
	active     eventTrackerState = "active"
	paused     eventTrackerState = "paused"
	inactive   eventTrackerState = "inactive"
	terminated eventTrackerState = "terminated"
)

// StorageTransaction represents a transaction object from the underlying storage backend. The
// actual (dynamic) type depends on the storage implementation being used (for example, *sql.Tx
// for relational DB or *bolt.Tx for Bolt DB ). See [ApplyTransaction] for more information on
// how transactions are managed during slot processing.
type StorageTransaction any

// StorageHandler defines the interface that must be implemented by any storage backend used
// with the [EventTracker]. The event tracker processes blocks and Anchor-based events from
// the Solana blockchain sequentially, and relies on this interface (that is, the underlying
// storage) to persist both the indexing progress (which slot should be processed next) and the
// actual event data that is extracted. Implementations of this interface can use any storage
// mechanism, such as a relational database, a key-value store, or a file-based system, as long
// as they correctly implement the methods described below.
type StorageHandler interface {
	// ReadSlot is invoked exactly once by the tracker during startup, that is, when the [Start]
	// method is called. This method must return the slot number from which the tracker should
	// begin monitoring and processing blocks (for example, if it returns 247, then slot 247
	// will be the first slot processed by the tracker upon startup). If the method returns an
	// error, the tracker will not even start and will terminate immediately.
	ReadSlot() (uint64, error)

	// StoreSlot is invoked by the tracker after every successfully processed slot. Note that a
	// slot does not necessarily contain a block, however, the method will still be invoked for
	// those empty slots. In transaction-like mode, the method is not invoked directly but rather
	// wrapped and passed to [ApplyTransaction]. The first argument is a transaction object from
	// the underlying storage backend, see [ApplyTransaction] for more information. The second
	// argument is the slot number that was just processed, and the implementation is responsible
	// for incrementing this value by one before storing it. This ensures that when [ReadSlot] is
	// invoked on the next startup, it returns correct starting slot. If the method returns an
	// error, the tracker will terminate immediately.
	StoreSlot(StorageTransaction, uint64) error

	// StoreEvent is invoked by the tracker after each successfully processed tracked event. In
	// transaction-like mode, the method is not invoked directly but rather wrapped and passed to
	// [ApplyTransaction]. The first argument is a transaction object from the underlying storage
	// backend, see [ApplyTransaction] for more information. The remaining arguments are, in order:
	// the slot number in which the event occurred, the public key (address) of the Solana program
	// that emitted the event, the event name as registered in the [ProgramEventSpecs] config, and
	// the deserialized event itself. If the method returns an error, the tracker will terminate
	// immediately.
	StoreEvent(StorageTransaction, uint64, solana.PublicKey, string, any) error

	// UseTransactions is invoked exactly once by the tracker during startup, that is, when the
	// [Start] method is called. This method should return true if the storage backend supports
	// a transaction-like (all-or-nothing) mode and the tracker should use it. Using transactional
	// writes is the recommended approach because it prevents data inconsistency that can occur
	// during system restarts. Without it, if the tracker processes a slot with multiple events
	// and successfully stores some of them before crashing, the slot number will not have been
	// updated. When the tracker starts again, it will process the same slot again, leading to
	// duplicate event entries in storage. By using transactions, either all events from a slot
	// are stored together with the updated slot, or none of them are.
	UseTransactions() bool

	// ApplyTransaction is invoked by the tracker after processing each slot, but only if the
	// invocation of [UseTransactions] returned true. This method receives two arguments: first,
	// a function that wraps the invocation of the [StoreSlot] method for the current slot, and
	// second, a list of functions where each one wraps a [StoreEvent] invocation for a single
	// tracked event found in that slot. Each wrapper function accepts a transaction object as
	// its argument, which it then passes to the underlying [StoreSlot] or [StoreEvent] method
	// call. So, the implementation should create/begin a transaction, call all wrapper functions
	// by passing the transaction object to each one, and then commit or rollback the transaction
	// after all invocations complete. This approach ensures that either all storage operations
	// for a given slot are persisted together atomically, or none of them are saved if any
	// operation fails. If this method returns an error, the tracker will terminate immediately.
	ApplyTransaction(func(StorageTransaction) error, []func(StorageTransaction) error) error
}

// SlotNotification represents a notification sent on the chSlot channel.
type SlotNotification struct {
	// SlotNumber is the number of the slot that was just processed.
	SlotNumber uint64

	// BlockExists indicates whether a block actually exists in this slot. True if a block exists,
	// false if the slot is empty.
	BlockExists bool
}

// EventNotification represents a notification sent on the chEvent channel.
type EventNotification struct {
	// SlotNumber is the number of the slot in which the tracked event was emitted.
	SlotNumber uint64

	// Program is the public key (address) of the Solana program that emitted the tracked event.
	Program solana.PublicKey

	// EventName is the name of the event, as registered in the corresponding ProgramEventSpecs.
	EventName string

	// EventData is the deserialized event payload. The dynamic type of the returned value is a
	// pointer, and the pointed data MUST be treated as read-only. Modifying it may result in a
	// data race. In order to modify it, create a deep copy.
	EventData any
}

// ErrorNotification represents a notification sent on the chError channel.
type ErrorNotification struct {
	// Error is the concrete error that occurred during event tracker execution.
	error

	// Terminated indicates whether this error will cause the event tracker to terminate. If
	// true, the tracker entered graceful termination process. A true value has the same effect
	// as calling the [Terminate] method, so all described in its documentation apply.
	Terminated bool
}

// EventTracker monitors the Solana blockchain for specific program events, emits notifications
// on a channels, and persists data to a storage.
type EventTracker struct {
	// client is the Solana RPC client used to fetch blocks from the blockchain network.
	client *rpc.Client

	// storage is responsible for persisting indexed data. For details, see the [StorageHandler]
	// interface documentation.
	storage StorageHandler

	// trackedPrograms defines which programs and events the tracker observes. Only events listed
	// in ProgramEventSpecs for programs present in this map are indexed.
	trackedPrograms map[solana.PublicKey]ProgramEventSpecs

	// commitment specifies the commitment level required for a block/slot to be considered for
	// indexing. Common values are "confirmed" and "finalized" (no chain reorganization). Other
	// levels should not be considered.
	commitment rpc.CommitmentType

	// Optional fields (settable through [NewEventTracker] constructor function):

	// logger records state changes and actions during the tracker's lifecycle. By default, no
	// logging is performed.
	logger Logger

	// eventSink is an optional output destination where the tracker writes event each time an
	// event is successfully indexed and processed. Write will be performed only if the golang
	// type representing the event implements `String(uint64, solana.PublicKey) string` method,
	// where the first argument is the slot number in which the event occurred, and the second
	// is the public key of the program that emitted it. By default, no output
	eventSink io.Writer

	// pollTime specifies the polling interval for checking new blocks/slots. By default, 500
	// milliseconds.
	pollTime time.Duration

	// notifications indicates whether the tracker should send notifications on chSlot, chEvent
	// and chError channels. When false, both channels remain nil and no notifications are sent.
	// When true, channels are created with buffer sizes specified through [WithNotifications].
	notifications bool

	// Internal fields (not settable through [NewEventTracker]constructor function):

	// state represents the current status of the tracker, e.g., active, inactive, or paused.
	state eventTrackerState

	// chEvent is a channel used to emit notification each time a tracked event is processed.
	chEvent chan EventNotification

	// chSlot is a channel used to emit notification each time a slot is processed.
	chSlot chan SlotNotification

	// chError is a channel used to emit notifications when an error occurs during event tracker
	// execution.
	chError chan ErrorNotification

	// chPause is a channel used to pause the tracker after completing the current slot.
	chPause chan struct{}

	// chTerminate is a channel used to terminate the tracker gracefully after processing
	// the current slot. Upon termination, chEvent and chSlot channels are closed.
	chTerminate chan struct{}

	// applyTx indicates whether operations on the storage are executed within a transaction. In
	// other words, whether the [(StorageHandler).ApplyTransaction] is invoked. It is set to the
	// return value of the [(StorageHandler).UseTransaction] method.
	applyTx bool

	mut sync.Mutex

	// delay between block fetches for rate limiting
	blockFetchDelay time.Duration
}

// NewEventTracker constructs a new EventTracker instance. None of the required arguments may
// be nil. The recommended commitment level is [rpc.CommitmentFinalized].
//
// Note: none of the provided arguments are concurrent-safe. They MUST NOT be modified directly
// after the tracker has been created. If you need to update the set of tracked programs/events,
// use the corresponding exposed methods provided by the tracker.
//
// The following optional configurations are available (see their documentation for details):
//  1. WithLogger (default: no logging)
//  2. WithEventSink (default: no writes)
//  3. WithPollTime (default: 500 milliseconds)
//  4. WithNotifications (default: notifications disabled)
//     5.
func NewEventTracker(
	client *rpc.Client,
	storage StorageHandler,
	trackedPrograms map[solana.PublicKey]ProgramEventSpecs,
	commitment rpc.CommitmentType,
	opts ...eventTrackerOption) (*EventTracker, error) {

	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if storage == nil {
		return nil, fmt.Errorf("storage (handler) cannot be nil")
	}
	if trackedPrograms == nil {
		return nil, fmt.Errorf("map of tracked programs cannot be nil")
	}
	if len(trackedPrograms) == 0 {
		return nil, fmt.Errorf("must track at least one program")
	}

	tracker := &EventTracker{
		client:          client,
		storage:         storage,
		trackedPrograms: trackedPrograms,
		commitment:      commitment,
		logger:          nil,
		eventSink:       nil,
		pollTime:        500 * time.Millisecond,
		notifications:   false,
		state:           inactive,
		chPause:         make(chan struct{}),
		chTerminate:     make(chan struct{}),
		blockFetchDelay: 100 * time.Millisecond,
	}

	for _, o := range opts {
		if err := o(tracker); err != nil {
			return nil, err
		}
	}

	return tracker, nil
}

// ChSlot returns a read-only channel that emits a notification each time a slot is successfully
// processed. A notification is emitted even if no block exists for that slot. If notifications
// are not enabled through [WithNotifications], a nil channel is returned.
func (t *EventTracker) ChSlot() <-chan SlotNotification {
	return (<-chan SlotNotification)(t.chSlot)
}

// ChEvent returns a read-only channel that emits a notification each time a tracked event is
// successfully processed. If notifications are not enabled through [WithNotifications], a nil
// channel is returned.
func (t *EventTracker) ChEvent() <-chan EventNotification {
	return (<-chan EventNotification)(t.chEvent)
}

// ChError returns a read-only channel that emits a notification each time an error occurs during
// event tracker execution. If notifications are not enabled through [WithNotifications], a nil
// channel is returned.
func (t *EventTracker) ChError() <-chan ErrorNotification {
	return (<-chan ErrorNotification)(t.chError)
}

// State returns the current state of the EventTracker.
func (t *EventTracker) State() eventTrackerState {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.state
}

// Pause pauses the running event tracker. If the method returns true, it means the tracker has
// entered the pausing process. Returning from the method does not mean the tracker is paused.
// Use the [State] method to check if it has actually reached the "paused" state. Calling [Start],
// [Terminate], or another [Pause] method before [State] returns "paused" is considered undefined
// behavior. Returning false means the event tracker cannot be paused because it is not in the
// active state.
func (t *EventTracker) Pause() bool {
	if t.State() != active {
		return false
	}

	t.chPause <- struct{}{}

	return true
}

// Terminate gracefully terminates the running event tracker. If the method returns true, it
// means the event tracker has entered the termination process. Returning from the method does
// not mean the tracker is terminated. Use the [State] method to check if it has actually reached
// the "inactive" state. Calling [Start], [Pause], or another [Terminate] method before [State]
// returns "inactive" is considered undefined behavior. Returning false means the event tracker
// cannot be terminated because it is not in the active state.
func (t *EventTracker) Terminate() bool {
	if t.State() != active {
		return false
	}

	t.chTerminate <- struct{}{}

	return true
}

func (t *EventTracker) setState(state eventTrackerState) {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.state = state
}

func (t *EventTracker) logDebug(format string, a ...any) {
	if t.logger == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	if leveled, ok := t.logger.(LeveledLogger); ok {
		leveled.Debug(msg)
	} else {
		t.logger.Log(msg)
	}
}

func (t *EventTracker) logInfo(format string, a ...any) {
	if t.logger == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	if leveled, ok := t.logger.(LeveledLogger); ok {
		leveled.Info(msg)
	} else {
		t.logger.Log(msg)
	}
}

func (t *EventTracker) logWarn(format string, a ...any) {
	if t.logger == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	if leveled, ok := t.logger.(LeveledLogger); ok {
		leveled.Warn(msg)
	} else {
		t.logger.Log(msg)
	}
}

func (t *EventTracker) logError(format string, a ...any) {
	if t.logger == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	if leveled, ok := t.logger.(LeveledLogger); ok {
		leveled.Error(msg)
	} else {
		t.logger.Log(msg)
	}
}

// log is kept for backward compatibility and calls logInfo.
func (t *EventTracker) log(format string, a ...any) {
	t.logInfo(format, a...)
}

func sendNotification[T any](ch chan<- T, v T) {
	select {
	case ch <- v:
	default:
	}
}

func (t *EventTracker) notify(notification any) {
	if t.notifications {
		switch value := notification.(type) {
		case SlotNotification:
			sendNotification(t.chSlot, value)
		case EventNotification:
			sendNotification(t.chEvent, value)
		case ErrorNotification:
			sendNotification(t.chError, value)
		}
	}
}

func (t *EventTracker) terminate() {
	if t.notifications {
		close(t.chEvent)
		close(t.chSlot)
		close(t.chError)
	}

	t.storage = nil
	t.setState(terminated)
	t.logInfo("Event tracker has been terminated")
}

// Start launches the event tracker in a background goroutine, transitioning from inactive to
// active state. The tracker processes blocks continuously until Terminate() or Pause() is called.
// Use Pause() to temporarily halt and Start() to resume processing.
// Once terminated, the tracker cannot be restarted.
func (t *EventTracker) Start() error {
	if t.client == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewEventTracker]")
	} else if t.storage == nil {
		return fmt.Errorf(
			"this event tracker instance is terminated, only paused tracker can be again started")
	}
	// prevent starting if already active
	if t.State() == active {
		return fmt.Errorf("tracker is already running")
	}

	currentSlot, err := t.storage.ReadSlot()
	if err != nil {
		return fmt.Errorf("cannot read starting slot: %w", err)
	}

	t.applyTx = t.storage.UseTransactions()
	// Transition to active state before starting the goroutine
	t.setState(active)
	go func() {
		t.logInfo("Starting indexing from slot %d", currentSlot)

		// Polling loop
		for {
			select { // before fetching new slot, check for pause/terminate signals
			case <-t.chPause:
				t.setState(paused)
				t.logInfo("Event tracker has been paused")
				return // exit goroutine
			case <-t.chTerminate:
				t.terminate()
				return // exit goroutine
			default: // continue with normal execution
			}

			t.logDebug("Checking chain head at slot %d", currentSlot)

			fetchedSlot, err := t.client.GetSlot(context.TODO(), t.commitment)
			if err != nil {
				t.notify(
					ErrorNotification{fmt.Errorf("failed to fetch slot %d: %w", currentSlot, err), false})
				t.logError("Failed to fetch slot %d: %s", currentSlot, err.Error())
				t.logInfo("I will try again in %d ms...", t.pollTime.Milliseconds())
				time.Sleep(t.pollTime)
				continue
			}

			if currentSlot > uint64(fetchedSlot) {
				t.logDebug(
					"Reached chain head, waiting for slot %d to be %s (currently last %s: %d)",
					currentSlot,
					t.commitment,
					t.commitment,
					fetchedSlot,
				)
				t.logDebug("I will try again in %d ms...", t.pollTime.Milliseconds())
				time.Sleep(t.pollTime)
				continue
			}

			// Catch-up loop: Process all available slots up to chain head
			for currentSlot <= uint64(fetchedSlot) {
				// Check for pause/terminate signals during catch-up
				select {
				case <-t.chPause:
					t.setState(paused)
					t.logInfo("Event tracker has been paused")

					return
				case <-t.chTerminate:
					t.terminate()
					return
				default:
				}
				// Process current slot - cannot interrupt (pause, terminate) during processing of a slot
				t.logDebug("Slot %d is %s, processing...", currentSlot, t.commitment)

				block, err := t.client.GetBlockWithOpts(context.TODO(), currentSlot, &rpc.GetBlockOpts{
					TransactionDetails:             rpc.TransactionDetailsFull,
					MaxSupportedTransactionVersion: new(uint64),
				})

				if err != nil {
					// Check if slot was skipped
					if isSkippedSlotError(err) {
						if err := t.storage.StoreSlot(nil, currentSlot); err != nil {
							t.notify(ErrorNotification{
								fmt.Errorf("failed to store skipped slot: %w", err), true})
							t.logError("Failed to store skipped slot: %s", err.Error())
							t.terminate()
							return
						}

						t.notify(SlotNotification{currentSlot, false})
						t.logDebug("Slot %d was skipped (no block produced), moving to next slot", currentSlot)

						currentSlot++
						continue
					}

					// Retry for other errors - break inner loop to re-fetch chain head
					t.notify(ErrorNotification{
						fmt.Errorf("failed to fetch block for slot %d: %w", currentSlot, err), false})
					t.logError("Failed to fetch block for slot %d: %s", currentSlot, err.Error())
					t.logInfo("I will try again in %d ms...", t.pollTime.Milliseconds())
					time.Sleep(t.pollTime)
					break // Break inner loop, outer loop will retry
				}

				if block == nil {
					if err := t.storage.StoreSlot(nil, currentSlot); err != nil {
						t.notify(ErrorNotification{
							fmt.Errorf("failed to store slot: %w", err), true})
						t.logError("Failed to store slot: %s", err.Error())
						t.terminate()
						return
					}

					t.notify(SlotNotification{currentSlot, false})
					t.logDebug("Slot %d is empty", currentSlot)

					currentSlot++
					continue
				}

				t.logInfo("Block in slot %d has %d transactions", currentSlot, len(block.Transactions))

				if !t.processBlock(currentSlot, block) {
					return
				}

				t.notify(SlotNotification{currentSlot, true})

				currentSlot++
				if t.blockFetchDelay > 0 {
					time.Sleep(t.blockFetchDelay) // Rate limiting between block fetches to avoid hitting RPC limits during catch-up
				}
			}

			if currentSlot > uint64(fetchedSlot) {
				// Sleep when caught up to chain head
				t.logDebug("Processed up to slot %d, waiting for new blocks...", currentSlot-1)
				time.Sleep(t.pollTime)
			}
		}
	}()

	return nil
}

func (t *EventTracker) processBlock(slot uint64, block *rpc.GetBlockResult) bool {
	// TODO: We should also check whether any of the tracked programs was called via a CPI.

	var eventFns []func(st StorageTransaction) error
	// Store event details for post-commit notifications in transaction mode
	var pendingNotifications []EventNotification

	trackedInTx := make(map[solana.PublicKey]bool)
	for txIndex, tx := range block.Transactions {
		transaction, err := tx.GetTransaction()
		if err != nil {
			t.notify(ErrorNotification{
				fmt.Errorf("failed to decode transaction %d: %s", txIndex+1, err), false})

			t.logWarn("Failed to decode transaction %d: %s", txIndex+1, err.Error())

			continue
		}

		if tx.Meta == nil {
			t.notify(ErrorNotification{
				fmt.Errorf("cannot read meta data for the transaction"), false})

			t.logWarn("Cannot read meta data for the transaction")

			continue
		}

		if len(tx.Meta.LogMessages) == 0 {
			continue
		}

		for _, instruction := range transaction.Message.Instructions {
			if int(instruction.ProgramIDIndex) >= len(transaction.Message.AccountKeys) {
				t.logWarn("Invalid ProgramIDIndex %d in transaction %d (only %d accounts)",
					instruction.ProgramIDIndex, txIndex+1, len(transaction.Message.AccountKeys))
				continue
			}
			programID := transaction.Message.AccountKeys[instruction.ProgramIDIndex]

			if _, ok := t.trackedPrograms[programID]; ok {
				trackedInTx[programID] = true
			}

			if _, ok := t.trackedPrograms[programID]; !ok {
				continue
			}

			for _, log := range tx.Meta.LogMessages {
				if !strings.Contains(log, "Program data: ") {
					continue
				}

				// Extract base64 data
				dataStart := strings.Index(log, "Program data: ")
				if dataStart == -1 {
					continue
				}
				base64Data := log[dataStart+14:] // len("Program data: ") = 14
				base64Data = strings.TrimSpace(base64Data)
				base64Data = strings.TrimRight(base64Data, "=") // Remove padding for RawStdEncoding

				decoded, err := base64.RawStdEncoding.DecodeString(base64Data)
				if err != nil {
					t.notify(ErrorNotification{
						fmt.Errorf("failed to decode log: %w", err), false})

					t.logWarn("Failed to decode log: %s", err.Error())

					continue
				}

				// Try to parse with each tracked program in this transaction
				for programID := range trackedInTx {
					parsed, name, err := t.parseEvent(decoded, programID)
					if err != nil {
						t.notify(ErrorNotification{
							fmt.Errorf("failed to parse event: %w", err), false})

						t.logWarn("Failed to parse event: %s", err.Error())

						continue
					}

					if parsed == nil {
						continue
					}

					// Found a matching event!
					if t.applyTx {
						pendingNotifications = append(pendingNotifications, EventNotification{
							SlotNumber: slot,
							Program:    programID,
							EventName:  name,
							EventData:  parsed,
						})

						eventFns = append(eventFns, func(st StorageTransaction) error {
							return t.storage.StoreEvent(
								st,
								slot,
								programID,
								name,
								parsed,
							)
						})
					} else {
						if err := t.storage.StoreEvent(nil, slot, programID, name, parsed); err != nil {
							t.notify(ErrorNotification{
								fmt.Errorf("failed to store event: %w", err), true})

							t.logError("Failed to store event: %s", err.Error())

							t.terminate()

							return false
						}

						t.notify(EventNotification{
							SlotNumber: slot,
							Program:    programID,
							EventName:  name,
							EventData:  parsed,
						})

						t.logInfo(fmt.Sprintf("Event of type %s emitted by %s at slot %d", name, programID, slot))
					}

					if str, ok := parsed.(interface {
						String(uint64, solana.PublicKey) string
					}); t.eventSink != nil && ok {
						if _, err := t.eventSink.Write([]byte(str.String(slot, programID))); err != nil {
							t.notify(ErrorNotification{
								fmt.Errorf("failed to write in event sink: %w", err), false})

							t.logWarn("Failed to write in event sink: %s", err.Error())
						}
					}

					break // Found the correct program, no need to try others
				}
			}
		}
	}

	if t.applyTx {
		slotFn := func(st StorageTransaction) error {
			return t.storage.StoreSlot(
				st,
				slot,
			)
		}

		if err := t.storage.ApplyTransaction(slotFn, eventFns); err != nil {
			t.notify(ErrorNotification{
				fmt.Errorf("failed to apply storage transaction: %w", err), true})

			t.logError("Failed to apply storage transaction: %s", err.Error())

			t.terminate()

			return false
		}

		// Send notifications AFTER successful transaction commit
		for _, notification := range pendingNotifications {
			t.notify(notification)
			t.logInfo(fmt.Sprintf("Event of type %s emitted by %s at slot %d",
				notification.EventName, notification.Program, notification.SlotNumber))
		}

		t.logDebug("Successfully stored %d events for slot %d", len(eventFns), slot)

		return true
	}

	if err := t.storage.StoreSlot(nil, slot); err != nil {
		t.notify(ErrorNotification{
			fmt.Errorf("failed to store slot: %w", err), true})

		t.logError("Failed to store slot: %s", err.Error())

		t.terminate()

		return false
	}

	return true
}

func (t *EventTracker) parseEvent(eventData []byte, programID solana.PublicKey) (any, string, error) {
	decoder := binary.NewBorshDecoder(eventData)
	discriminator, err := decoder.ReadDiscriminator()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get event discriminator: %w", err)
	}
	trackedEvents, _ := t.trackedPrograms[programID] // checked existence of programID in processBlock

	for _, event := range trackedEvents {
		if event.discriminant == discriminator {

			value := reflect.New(reflect.ValueOf(event.eventType).Type()).Interface()

			deserValue, ok := value.(interface {
				UnmarshalWithDecoder(decoder *binary.Decoder) (err error)
			})

			if !ok {
				return nil, "", fmt.Errorf(
					"event type %T for event %s (program %s) does not implement UnmarshalWithDecoder method",
					value, event.name, programID)
			}

			if err := deserValue.UnmarshalWithDecoder(decoder); err != nil {
				return nil, "", fmt.Errorf(
					"failed to unmarshal event %s (discriminator: %x, program: %s): %w",
					event.name, discriminator, programID, err)
			}

			return deserValue, event.name, nil
		}
	}

	// No matching discriminator found - this is normal if the log contains
	// data that doesn't match any tracked event types (e.g., program logs)
	return nil, "", nil
}

// helper that checks if the error indicates a skipped/missing slot
func isSkippedSlotError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "-32007") ||
		strings.Contains(errStr, "was skipped") ||
		strings.Contains(errStr, "missing due to ledger jump")
}
