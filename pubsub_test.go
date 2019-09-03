package pubsub

import (
	"testing"
	"time"
)

func TestNewEmptyTopic(t *testing.T) {
	_, err := New("", 1000)
	if err == nil {
		t.Fatalf("expected err not be empty")
	}
}

func TestNew(t *testing.T) {
	_, err := New("Test", 1000)
	if err != nil {
		t.Fatal("expected err to be nil")
	}
}

func TestPub(t *testing.T) {
	ps, err := New("Test", 1000)
	if err != nil {
		t.Fatal("expected err to be nil")
	}
	ps.Pub("hi")
}

func TestConfig(t *testing.T) {
	ps, err := New("Test", 1000)
	if err != nil {
		t.Fatal("expected err to be nil")
	}
	if ps.Capacity != defaultSize {
		t.Fatal("Default pub size is not allocated")
	}
}

func TestSubAfterPub(t *testing.T) {
	ps, err := New("Test", 1000)
	if err != nil {
		t.Fatal("expected err to be nil")
	}
	ps.Pub("hi")
	time.Sleep(time.Second)
	msgChan := ps.Sub()
	t.Log("Subscription is sucessfull")

	if msgChan == nil {
		t.Fatal("the returned subscription channel is empty")
	}

	select {
	case msg := <-msgChan:
		t.Fatal("expected subscription not to work since subscription is done after publish", msg)

	case <-time.After(time.Second * 2):
		t.Log("timed out receiving data from subscription")

	}
}

func TestSubBeforePub(t *testing.T) {
	ps, err := New("Test", 1000)
	if err != nil {
		t.Fatal("expected err to be nil")
	}
	msgChan := ps.Sub()
	t.Log("Subscription is sucessfull")
	ps.Pub("hi")

	if msgChan == nil {
		t.Fatal("the returned subscription channel is empty")
	}

	select {
	case msg := <-msgChan:
		if msg != "hi" {
			t.Fatal("data received, but data is not what is published", msg)
		}

	case <-time.After(time.Second * 2):
		t.Log("timed out receiving data from subscription")
		t.Fail()

	}
}
