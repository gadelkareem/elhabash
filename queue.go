package elhabash

import (
	"net/url"
	"sync"
)

// Queue offers methods to send Commands to the Fetcher, and to Stop the crawling process.
// It is safe to use from concurrent goroutines.
type Queue struct {
	commandsChannel chan Command

	// signal channels
	closed, cancelled, done chan struct{}

	wg sync.WaitGroup
}

// Close closes the Queue so that no more Commands can be sent. It blocks until
// the Fetcher drains all pending commandsChannel. After the call, the Fetcher is stopped.
// Attempts to enqueue new URLs after Close has been called will always result in
// a ErrQueueClosed error.
func (q *Queue) Close() error {
	// Make sure it is not already closed, as this is a run-time panic
	select {
	case <-q.closed:
		// Already closed, no-op
		return nil
	default:
		// Close the signal-channel
		close(q.closed)
		// Send a nil Command to make sure the processQueue method sees the close signal.
		q.commandsChannel <- nil
		// Wait for the Fetcher to drain.
		q.wg.Wait()
		// Unblock any callers waiting on queue.Block
		close(q.done)
		close(q.commandsChannel)
		return nil
	}
}

// Block blocks the current goroutine until the Queue is closed and all pending
// commandsChannel are drained.
func (q *Queue) Wait() {
	<-q.done
}

// Done returns a channel that is closed when the Queue is closed (either
// via Close or Cancel). Multiple calls always return the same channel.
func (q *Queue) Done() <-chan struct{} {
	return q.done
}

// Cancel closes the Queue and drains the pending commandsChannel without processing
// them, allowing for a fast "stop immediately"-ish operation.
func (q *Queue) Cancel() error {
	select {
	case <-q.cancelled:
		// already cancelled, no-op
		return nil
	default:
		// mark the queue as cancelled
		close(q.cancelled)
		// Close the Queue, that will wait for pending commandsChannel to drain
		// will unblock any callers waiting on queue.Block
		return q.Close()
	}
}

// Send enqueues a Command into the Fetcher. If the Queue has been closed, it
// returns ErrQueueClosed. The Command's URL must have a Host.
func (q *Queue) Send(c Command) error {
	if c == nil {
		return ErrEmptyHost
	}
	if u := c.Url(); u == nil || u.Host == "" {
		return ErrEmptyHost
	}
	select {
	case <-q.closed:
		return ErrQueueClosed
	default:
		q.commandsChannel <- c
	}
	return nil
}

// SendString enqueues a method and some URL strings into the Fetcher. It returns an error
// if the URL string cannot be parsed, or if the Queue has been closed.
// The first return value is the number of URLs successfully enqueued.
func (q *Queue) SendString(method string, rawUrl ...*url.URL) (int, error) {
	return q.sendWithMethod(method, rawUrl)
}

//// SendStringHead enqueues the URL strings to be fetched with a HEAD method.
//// It returns an error if the URL string cannot be parsed, or if the Queue has been closed.
//// The first return value is the number of URLs successfully enqueued.
func (q *Queue) SendStringHead(rawUrl ...*url.URL) (int, error) {
	return q.sendWithMethod("HEAD", rawUrl)
}

//// SendStringGet enqueues the URL strings to be fetched with a GET method.
//// It returns an error if the URL string cannot be parsed, or if the Queue has been closed.
//// The first return value is the number of URLs successfully enqueued.
func (q *Queue) SendStringGet(rawUrl ...*url.URL) (int, error) {
	return q.sendWithMethod("GET", rawUrl)
}

// Parses the URL strings and enqueues them as *Cmd. It returns the number of URLs
// successfully enqueued, and an error if the URL string cannot be parsed or
// the Queue has been closed.
func (q *Queue) sendWithMethod(method string, rawurls []*url.URL) (int, error) {
	for i, u := range rawurls {
		if err := q.Send(&Cmd{U: u, M: method}); err != nil {
			return i, err
		}
	}
	return len(rawurls), nil
}
