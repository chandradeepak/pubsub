package pubsub

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	defaultSize = 1000
)

// PubSub provides publish and subscribe capabilty for topics
type PubSub struct {	
	Topic              string
	Capacity           int
	msgChan            chan interface{}
	activeSubscription atomic.Value
	lock               sync.RWMutex
}

// New creates a new pub sub library for use
func New(topic string, capacity int) (*PubSub, error) {
	if topic == "" {
		return nil, errors.New("topic is empty")
	}

	if capacity <= 0 {
		capacity = defaultSize
	}

	ps := &PubSub{Capacity: capacity, Topic: topic}
	ps.msgChan = make(chan interface{}, capacity)

	var subs []*sub
	ps.activeSubscription.Store(subs)
	go ps.fanout()
	return ps, nil
}

// this function would take all the published messages and push to the subscriptions that are active
func (ps *PubSub) fanout() {
	for msg := range ps.msgChan {
		//send the message to all the subscriptions
		for _, sub := range ps.activeSubscription.Load().([]*sub) {
			sub.msgChan <- msg
		}
	}
}

// Pub is used to publish the message to a topic
func (ps *PubSub) Pub(msg interface{}) error {
	ps.msgChan <- msg
	return nil
}

// Sub is used to subscribe to a topic. It returns a channel from which we can read the data
func (ps *PubSub) Sub() <-chan interface{} {

	ch := make(chan interface{}, ps.Capacity)

	//synchronize with other potential writers
	ps.lock.Lock()
	defer ps.lock.Unlock()

	//load old subscriptions
	currentSubs := ps.activeSubscription.Load().([]*sub)

	//copy old subscriptions to the new list
	sb := newSub(ps.Topic, ch, ps.Capacity)
	var newsubs []*sub
	for _, oldsb := range currentSubs {
		newsubs = append(newsubs, oldsb)
	}
	newsubs = append(newsubs, sb)

	//store new subscriptions
	ps.activeSubscription.Store(newsubs)
	return ch
}

type sub struct {
	topic   string
	size    int
	msgChan chan interface{}
}

func newSub(topic string, msgChan chan interface{}, size int) *sub {
	s := &sub{topic: topic, msgChan: msgChan, size: size}
	return s
}

func (s *sub) close() {
	close(s.msgChan)
}