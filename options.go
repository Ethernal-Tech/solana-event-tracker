package tracker

import (
	"fmt"
	"io"
)

type eventTrackerOption func(*EventTracker) error

// WithLogger configures the event tracker to log its state changes and actions during its
// lifecycle. By default, no logging is performed. You can use [DefaultLogger] to log to
// standard output using fmt formatting.
func WithLogger(logger Logger) eventTrackerOption {
	return func(t *EventTracker) error {
		t.logger = logger

		return nil
	}
}

// WithEventSink configures the event tracker to write event data to the provided [io.Writer]
// each time an event is successfully indexed and processed. Write will be performed only if
// the golang type representing the event implements `String(uint64, solana.PublicKey) string`
// method, where the first argument is the slot number in which the event occurred, and the
// second is the public key of the program that emitted it. By default, no events are written.
func WithEventSink(eventSink io.Writer) eventTrackerOption {
	return func(t *EventTracker) error {
		t.eventSink = eventSink

		return nil
	}
}

// WithPollTime configures the event tracker to use the provided polling interval, expressed
// in milliseconds. The polling time must be between 200 and 900000 milliseconds (15 minutes),
// inclusive. By default, the polling interval is set to 500 milliseconds.
func WithPollTime(pollTime uint64) eventTrackerOption {
	return func(t *EventTracker) error {
		if pollTime < 200 {
			return fmt.Errorf("poll time must be at least 200 milliseconds")
		} else if pollTime > 900000 {
			return fmt.Errorf("poll time must not exceed 900000 milliseconds (15 minutes)")
		}

		t.pollTime = pollTime

		return nil
	}
}

// WithNotifications configures the event tracker to send notifications on the chSlot and chEvent
// channels each time a slot or tracked event is processed, and on the chError channel each time
// an error occurs. The arguments define buffer sizes for the chSlot, chEvent, and chError channels
// in that order. If zero is provided for any channel, that channel will be unbuffered. By default,
// notifications are disabled.
func WithNotifications(slotBuffSize, eventBuffSize, errorBuffSize uint8) eventTrackerOption {
	return func(t *EventTracker) error {
		t.notifications = true

		t.chSlot = make(chan SlotNotification, slotBuffSize)
		t.chEvent = make(chan EventNotification, eventBuffSize)
		t.chError = make(chan ErrorNotification, errorBuffSize)

		return nil
	}
}
