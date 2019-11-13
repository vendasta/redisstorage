package redisstorage

import (
	"net/url"
	"testing"
)

func TestQueue(t *testing.T) {
	s := &Storage{
		Address:  "127.0.0.1:6379",
		Password: "",
		DB:       0,
		Prefix:   "queue_test",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize client: " + err.Error())
		return
	}
	urls := []string{"http://example.com/", "http://go-colly.org/", "https://xx.yy/zz"}
	for _, u := range urls {
		if err := s.AddRequest([]byte(u)); err != nil {
			t.Error("failed to add request: " + err.Error())
			return
		}
	}
	if size, err := s.QueueSize(); size != 3 || err != nil {
		t.Error("invalid queue size")
		return
	}
	for _, u := range urls {
		if r, err := s.GetRequest(); err != nil || string(r) != u {
			if err != nil {
				t.Error("failed to get request: " + err.Error())
			} else {
				t.Error("failed to get request: " + string(r))
			}
			return
		}
	}

	requestId := uint64(2234)
	err := s.Visited(requestId)
	if err != nil {
		t.Error("error setting visited: " + err.Error())
	}
	visited, err := s.IsVisited(requestId)
	if err != nil {
		t.Error("error getting visited: " + err.Error())
	}
	if visited == false {
		t.Error("visited should be true")
	}

	parts, _ := url.Parse("http://www.vendasta.com")
	s.SetCookies(parts, "somecookie")
	s.Cookies(parts)

	err = s.Clear()
	if err != nil {
		t.Error("error clearing queue: " + err.Error())
	}
}
