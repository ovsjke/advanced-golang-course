// SPDX-License-Identifier: Apache-2.0

// Package broadcaster delivers every message one publisher publishes to every
// current subscriber.
//
// The doc comments in this file are the contract. Where they and README.md
// disagree, this file wins; the grader is written against it.
package broadcaster

import (
	"context"
	"sync"
)

// Policy is what Publish does about a subscriber that has fallen behind (see
// Subscribe for when that is). Each policy's promise to a subscriber that
// stops reading is stated below; the grader checks exactly these.
type Policy int

const (
	// Block: Publish waits until that subscriber has room, or is
	// unsubscribed. Every subscriber gets every message; one that stops
	// reading stops the publisher.
	Block Policy = iota
	// DropNewest: that subscriber does not get the new message. A subscriber
	// that stops reading still gets the first buffer messages published after
	// it stopped; of later ones it may get some and miss others, in order.
	DropNewest
	// DropOldest: that subscriber loses its oldest undelivered message, and
	// gets the new one. A subscriber that stops reading keeps the newest.
	DropOldest
	// Disconnect: that subscriber is unsubscribed, and its channel closed. It
	// gets everything published before that, with no gaps, then the close.
	Disconnect
)

// SlowSubscriber is your slow-subscriber policy. Choose it, and defend the
// choice in this comment: what it costs the publisher, the other subscribers
// and the slow subscriber itself, and why that is the right trade. The grader
// tests the policy declared here.
//
// Выбрал такую политику, так как посчитал ее реализацию наиболее интересной
// По сути политика реализует жертву пропускной способностью, т.к. мы не прерывваем поставку сообщения,
// пока не придет команда отписки или закрытия бродкастера. Но взамен мы получаем гарантию доставки публикации.

// Каждая операция блокирует мьютексом разделяемый контекст, пока есть вероятность гонки. 
// Под мьютексами мы проверяем состояние контекста (bool), что гарантирует его актуальность на момент проверки

// Все решения в общем позволяют гарантировать доставку сообщения, вместе с тем, что сообщения получат именно те пользователи, 
// кто на момент его публикации имеет на него права.

// Текущую реализацию считаю рабочей, а компромиссы в виде пропускной способности и скорости обработки сообщений обусловены тем,
// что нам необходимо гарантировать доставку. И в таком решение ее цена такова.


const SlowSubscriber = Block

// Broadcaster fans messages out from one publisher to any number of
// subscribers. Add the fields you need.
//
// Publish and Close are called by one goroutine, the publisher, never
// concurrently with each other, and Publish is never called after Close.
// Subscribe, and Unsubscribe and C on a Subscription, may be called from any
// goroutine at any time, concurrently with each other and with Publish.
type Broadcaster[T any] struct {
	mu          sync.Mutex
	active      bool
	subscribers map[*Subscription[T]]struct{}
}

// Subscription is one subscriber's view of a Broadcaster. Add the fields you
// need.
type Subscription[T any] struct {
	ctx       context.Context
	mu        sync.Mutex
	active    bool
	cancel    context.CancelFunc
	channel   chan T
	broadcast *Broadcaster[T]
}

// New returns a Broadcaster with no subscribers.
func New[T any]() *Broadcaster[T] {
	return &Broadcaster[T]{
		subscribers: make(map[*Subscription[T]]struct{}),
		active:      true,
	}
}

// Subscribe adds a subscriber whose buffer holds buffer undelivered messages.
//
//   - buffer < 1 panics, before anything else happens.
//   - The subscription receives every message whose Publish call starts after
//     Subscribe returns, in publish order, each exactly once, for as long as
//     it keeps up. It never receives a message whose Publish call returned
//     before Subscribe was called.
//   - It has fallen behind once at least buffer messages are waiting for it.
//     SlowSubscriber applies to it at the earliest then, and at the latest
//     when 2*buffer+1 are waiting: memory per subscriber is bounded, however
//     slow it is.
//   - Delivery is prompt: a message waiting for a subscriber that is
//     receiving on C reaches it within 100 ms, and within 25 ms on average
//     across a run of messages (the grader's bounds).
//   - A subscription that has never fallen behind loses nothing when it ends:
//     every message published to it before Unsubscribe or Close returns has
//     been received, or is waiting in C when C is closed.
//   - Under DropNewest and DropOldest, a subscriber that catches up again
//     receives new messages again.
//   - Subscribe returns promptly, even while Publish is blocked on a slow
//     subscriber. The grader allows 100 ms for many Subscribe and Unsubscribe
//     calls in a row: they must never wait for a blocked Publish.
//   - After Close, Subscribe returns a subscription whose channel is closed.
func (b *Broadcaster[T]) Subscribe(buffer int) *Subscription[T] {
	if buffer < 1 {
		panic("buffer < 1")
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan T, buffer)
	sub := &Subscription[T]{
		ctx:       ctx,
		active:    true,
		cancel:    cancel,
		channel:   ch,
		broadcast: b,
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.active {
		close(sub.channel)
		sub.cancel()
		sub.active = false
		return sub
	}
	b.subscribers[sub] = struct{}{}
	return sub
}

// C returns the channel the subscription receives on; the same channel on
// every call. It is closed when the subscription ends: by Unsubscribe, by
// Close, or by the Disconnect policy. Messages already buffered in it may
// still be received after it is closed.
func (s *Subscription[T]) C() <-chan T {
	return s.channel
}

// Unsubscribe ends the subscription. It is safe to call from any goroutine,
// including the one reading C, concurrently with anything, and more than once.
//
//   - When it returns, C is closed, and no message whose Publish call starts
//     after that is delivered.
//   - It returns promptly even while Publish is blocked on this subscriber or
//     any other, and never waits for a blocked Publish; a Publish blocked on
//     this subscriber then proceeds. The grader allows 100 ms.
func (s *Subscription[T]) Unsubscribe() {
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return
	}
	s.active = false
	close(s.channel)
	s.broadcast.mu.Lock()
	delete(s.broadcast.subscribers, s)
	s.broadcast.mu.Unlock()
}

// Publish delivers v to every current subscriber, under SlowSubscriber for
// any that has fallen behind.
//
//   - Under DropNewest, DropOldest and Disconnect, Publish never waits for a
//     slow subscriber: it returns promptly whatever the subscribers do. The
//     grader allows 100 ms.
//   - Under Block, Publish waits only while a subscriber has fallen behind,
//     and stops waiting for it as soon as it has room or is unsubscribed.
//   - The number of goroutines the Broadcaster runs may grow with the
//     number of subscribers, never with the number of messages published.
func (b *Broadcaster[T]) Publish(v T) {
	var wg sync.WaitGroup
	b.mu.Lock()
	for sub := range b.subscribers {
		wg.Go(func() {
			sub.mu.Lock()
			defer sub.mu.Unlock()
			if !sub.active {
				return
			}
			select {
			case sub.channel <- v:
				return
			case <-sub.ctx.Done():
				return
			}
		})
	}
	b.mu.Unlock()
	wg.Wait()
}

// Close ends every subscription: every C is closed when Close returns, and
// later Subscribe calls get a closed subscription. When Close returns, no
// goroutine the Broadcaster started is still running.
func (b *Broadcaster[T]) Close() {
	b.mu.Lock()
	if !b.active {
		b.mu.Unlock()
		return
	}
	b.active = false
	subs := make([]*Subscription[T], len(b.subscribers))
	count := 0
	for key := range b.subscribers {
		subs[count] = key
		count++
	}
	b.mu.Unlock()
	for _, sub := range subs {
		sub.Unsubscribe()
	}
	return
}
