/*

NotifyCond is a utility type for consumers using LISTEN / NOTIFY to avoid
polling the database for new work.


Usage


NotifyCond supports multiple concurrent channels, but it does not support
concurrent access to the same notification channel.  An attempt to do so might
result in undefined behaviour or panics.

An example of the intended usage pattern:

    package main

    import (
        "github.com/lib/pq"
        "github.com/lib/pq/notifycond"
        "database/sql"
        "time"
    )

    func work() {
        // Fetch and process work from the database.  It is crucial to process
        // *all* available work, not just one task.
        for {
            task := getWorkFromDatabase()
            if task == nil {
                return
            }

            go doWorkOnTask(task)
        }
    }

    func main() {
        listener := pq.NewListener("", 15 * time.Second, time.Minute, nil)
        ncond := notifycond.NewNotifyCond(listener)

        // It is important here that the order of operations is:
        //   1) Listen()
        //   2) Process *all* work
        //   3) Wait for a notification (possibly queued while in step 2)
        //   4) Go to 2
        //
        // Following this order guarantees that there will never be work
        // available in the database for extended periods of time without your
        // application knowing about it.
        sem, err := ncond.Listen("getwork")
        if err != nil {
            panic(err)
        }

        for {
            work()
            <-sem
        }
    }
 */
package notifycond

import (
	"github.com/lib/pq"
	"errors"
	"fmt"
	"sync"
	"time"
)

var errClosed = errors.New("NotifyCond has been closed")

type NotifyCond struct {
	listener *pq.Listener

	closeWaitGroup sync.WaitGroup
	closeChannel chan struct{}
	closed bool

	newPingIntervalChannel chan time.Duration
	broadcastOnPingTimeout bool

	lock sync.Mutex
	channels map[string] chan<- *pq.Notification
}

func NewNotifyCond(listener *pq.Listener) *NotifyCond {
	dispatcher := &NotifyCond{
		listener: listener,
		channels: make(map[string] chan<- *pq.Notification),
		newPingIntervalChannel: make(chan time.Duration, 1),
	}
	dispatcher.closeWaitGroup.Add(1)
	go dispatcher.mainDispatcherLoop()
	return dispatcher
}

func (s *NotifyCond) removeChannel(channel string, ch chan<- *pq.Notification) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Check that we're still in the channel list.  This should not happen
	// unless someone is misusing our interface.
	oldch, ok := s.channels[channel]
	if !ok {
		panic(fmt.Sprintf("channel %s not part of NotifyCond.channels", channel))
	}
	if oldch != ch {
		panic(fmt.Sprintf("unexpected channel %v in channel %s; expected %v", oldch, channel, ch))
	}
	delete(s.channels, channel)
}

// Listen starts listening on a notification channel.  The returned Go channel
// ("condition channel") will be guaranteed to have at least one notification
// in it any time one or more notifications have been received from the
// database since the last receive on that channel.
//
// It is not safe to call Listen if a concurrent Unlisten call on the same
// channel is in progress.  However, it is safe to Listen on a channel which
// was previously Unlistened by a different goroutine.
//
// If the channel is already active, pq.ErrChannelAlreadyOpen is returned.  If
// the NotifyCond has been closed, an error is returned.
func (s *NotifyCond) Listen(channel string) (<-chan *pq.Notification, error) {
	s.lock.Lock()

	if s.closed {
		s.lock.Unlock()
		return nil, errClosed
	}

	_, ok := s.channels[channel]
	if ok {
		s.lock.Unlock()
		return nil, pq.ErrChannelAlreadyOpen
	}
	ch := make(chan *pq.Notification, 1)
	s.channels[channel] = ch
	s.lock.Unlock()

	err := s.listener.Listen(channel)
	if err != nil {
		s.removeChannel(channel, ch)
		return nil, err
	}

	return ch, nil
}

// Unlisten stops listening on the supplied notification channel and closes the
// condition channel associated with it.  It is not safe to call Unlisten if a
// concurrent Listen call on that same channel is in progress, but it is safe
// to Unlisten a channel from a different goroutine than the one that
// previously executed Listen.  It is also safe to call Unlisten while a
// goroutine is waiting on the condition channel.  The channel will be closed
// gracefully.
//
// Returns pq.ErrChannelNotOpen if the channel is not currently active, or an
// error if the NotifyCond has been closed.
func (s *NotifyCond) Unlisten(channel string) error {
	s.lock.Lock()

	if s.closed {
		s.lock.Unlock()
		return errClosed
	}

	ch, ok := s.channels[channel]
	if !ok {
		s.lock.Unlock()
		return pq.ErrChannelNotOpen
	}
	s.lock.Unlock()

	err := s.listener.Unlisten(channel)
	if err != nil {
		return err
	}

	s.removeChannel(channel, ch)
	close(ch)

	return nil
}

// Calls Ping() on the underlying Listener.
func (s *NotifyCond) Ping() error {
	return s.listener.Ping()
}

// Controls the amount of time the connection is allowed to stay idle before
// the server is pinged via Listener.Ping().
func (s *NotifyCond) SetPingInterval(interval time.Duration) {
	s.newPingIntervalChannel <- interval
}

// Sets whether the nil *pq.Notification should be sent automatically when the
// server is pinged after inactivity.
func (s *NotifyCond) SetBroadcastOnPingTimeout(broadcastOnPingTimeout bool) {
	s.lock.Lock()
	s.broadcastOnPingTimeout = broadcastOnPingTimeout
	s.lock.Unlock()
}

func (s *NotifyCond) pingTimeout() {
	go func() {
		s.listener.Ping()
	}()

	// Grabbing the lock here is a bit wasteful, but ping timeouts are expected
	// to be quite rare anyway.
	s.lock.Lock()
	if s.broadcastOnPingTimeout {
		s.broadcast()
	}
	s.lock.Unlock()
}

// Close closes the NotifyCond and all of its associated channels.  It does not
// return until all condition channels have been closed.  Calling Close on a
// closed NotifyCond returns an error.
func (s *NotifyCond) Close() error {
	s.lock.Lock()
	if s.closed {
		s.lock.Unlock()
		return errClosed
	}
	s.closed = true
	s.closeChannel <- struct{}{}
	s.lock.Unlock()

	// wait for all channels to be closed
	s.closeWaitGroup.Wait()
	return s.listener.Close()
}

// Broadcast a nil *Notification to all listeners.  Caller must be holding
// s.lock.
func (s *NotifyCond) broadcast() {
	for channel := range s.channels {
		s.notify(channel, nil)
	}
}

// Sends a notification on a channel.  Caller must be holding s.lock.
func (s *NotifyCond) notify(channel string, n *pq.Notification) {
	ch, ok := s.channels[channel]
	if !ok {
		return
	}

	select {
		case ch <- n:

		default:
			// There's already a notification waiting in the channel; we can
			// ignore this one.
	}
}

func (s *NotifyCond) shutdown() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, ch := range s.channels {
		close(ch)
	}

	// let Close know we're done
	s.closeWaitGroup.Done()
}

// Runs in its own goroutine
func (s *NotifyCond) mainDispatcherLoop() {
	pingTimer := time.NewTimer(1)
	var pingInterval *time.Duration
	for {
		if pingInterval != nil {
			pingTimer.Reset(*pingInterval)
		} else {
			pingTimer.Stop()
		}

		select {
			case n := <-s.listener.Notify:
				s.lock.Lock()
				if n == nil {
					s.broadcast()
				} else {
					s.notify(n.Channel, n)
				}
				s.lock.Unlock()

			case <-s.closeChannel:
				s.shutdown()
				return

			case <-pingTimer.C:
				s.pingTimeout()

			case newPingInterval := <-s.newPingIntervalChannel:
				pingInterval = &newPingInterval
		}
	}
}
