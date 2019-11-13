package redisstorage

import (
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Storage implements the redis storage backend for Colly
type Storage struct {
	// Address is the redis server address
	Address string
	// Password is the password for the redis server
	Password string
	// DB is the redis database. Default is 0
	DB int
	// Prefix is an optional string in the keys. It can be used
	// to use one redis database for independent scraping tasks.
	Prefix string
	// Pool is the redis connection pool
	Pool *redis.Pool
	// Expires is when the item saved to redis will expire
	Expires time.Duration
	//ClientTimeOut is to set when the application client connections will timeout and close
	ClientTimeOut time.Duration

	mu sync.RWMutex // Only used for cookie methods.
}

// Init initializes the redis storage
func (s *Storage) Init() error {
	if s.Pool == nil {
		s.Pool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial(
					"tcp",
					s.Address,
					redis.DialConnectTimeout(time.Millisecond*50),
					redis.DialReadTimeout(time.Millisecond*250),
					redis.DialWriteTimeout(time.Millisecond*250),
				)
			},
			MaxIdle: 10,
		}
	}

	_, err := redis.DoWithTimeout(s.Pool.Get(), 250*time.Millisecond, "PING")
	if err != nil {
		return fmt.Errorf("redis connection error: %s", err.Error())
	}
	return nil
}

// Clear removes all entries from the storage
func (s *Storage) Clear() error {
	conn := s.Pool.Get()
	defer conn.Close()

	s.mu.Lock()
	defer s.mu.Unlock()
	keys, err := redis.Values(redis.DoWithTimeout(conn, time.Millisecond*250, "KEYS", s.getCookieID("*")))
	if err != nil {
		return err
	}
	keys2, err := redis.Values(redis.DoWithTimeout(conn, time.Millisecond*250, "KEYS", fmt.Sprintf("%s:request:*", s.Prefix)))
	if err != nil {
		return err
	}
	keys = append(keys, keys2...)
	keys = append(keys, s.getQueueID())
	_, err = redis.DoWithTimeout(conn, time.Millisecond*250, "DEL", keys...)
	return err
}

// Visited implements colly/storage.Visited()
func (s *Storage) Visited(requestID uint64) error {
	conn := s.Pool.Get()
	defer conn.Close()

	expiry := int(s.Expires.Seconds())
	if expiry <= 0 {
		expiry = 600
	}
	_, err := redis.DoWithTimeout(conn, time.Millisecond*250, "SETEX", s.getIDStr(requestID), expiry, "1")
	return err
}

// IsVisited implements colly/storage.IsVisited()
func (s *Storage) IsVisited(requestID uint64) (bool, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	_, err := redis.DoWithTimeout(conn, time.Millisecond*250, "GET", s.getIDStr(requestID))
	if err == redis.ErrNil {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// SetCookies implements colly/storage..SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	conn := s.Pool.Get()
	defer conn.Close()

	// TODO(js) Cookie methods currently have no way to return an error.

	// We need to use a write lock to prevent a race in the db:
	// if two callers set cookies in a very small window of time,
	// it is possible to drop the new cookies from one caller
	// ('last update wins' == best avoided).
	s.mu.Lock()
	defer s.mu.Unlock()

	expiry := int(s.Expires.Seconds())
	if expiry <= 0 {
		expiry = 600
	}
	_, err := redis.DoWithTimeout(conn, time.Millisecond*250, "SETEX", s.getCookieID(u.Host), expiry, cookies)
	if err != nil {
		log.Printf("SetCookies() .Set error %s", err)
		return
	}
}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	conn := s.Pool.Get()
	defer conn.Close()

	// TODO(js) Cookie methods currently have no way to return an error.

	s.mu.RLock()
	cookiesStr, err := redis.String(redis.DoWithTimeout(conn, time.Millisecond*250, "GET", s.getCookieID(u.Host)))
	s.mu.RUnlock()
	if err == redis.ErrNil {
		cookiesStr = ""
	} else if err != nil {
		log.Printf("Cookies() .Get error %s", err)
		return ""
	}
	return cookiesStr
}

// AddRequest implements queue.Storage.AddRequest() function
func (s *Storage) AddRequest(r []byte) error {
	conn := s.Pool.Get()
	defer conn.Close()

	_, err := redis.DoWithTimeout(conn, time.Millisecond*250, "RPUSH", s.getQueueID(), r)
	if err != nil {
		return err
	}
	return nil
}

// GetRequest implements queue.Storage.GetRequest() function
func (s *Storage) GetRequest() ([]byte, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	r, err := redis.Bytes(redis.DoWithTimeout(conn, time.Millisecond*250, "LPOP", s.getQueueID()))
	if err != nil {
		return nil, err
	}
	return r, err
}

// QueueSize implements queue.Storage.QueueSize() function
func (s *Storage) QueueSize() (int, error) {
	conn := s.Pool.Get()
	defer conn.Close()

	return redis.Int(redis.DoWithTimeout(conn, time.Millisecond*250, "LLEN", s.getQueueID()))
}

func (s *Storage) getIDStr(ID uint64) string {
	return fmt.Sprintf("%s:request:%d", s.Prefix, ID)
}

func (s *Storage) getCookieID(c string) string {
	return fmt.Sprintf("%s:cookie:%s", s.Prefix, c)
}

func (s *Storage) getQueueID() string {
	return fmt.Sprintf("%s:queue", s.Prefix)
}
