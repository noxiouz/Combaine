package slave

import (
	"net"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"
)

type fakeDiscovery struct {
	F func() ([]string, error)
}

func (f *fakeDiscovery) GetHosts() ([]string, error) {
	return f.F()
}

func TestResolverBadHosts(t *testing.T) {
	r := NewCocaineResolver(&fakeDiscovery{
		F: func() ([]string, error) {
			return []string{"Z", "A", "C"}, nil
		},
	})
	defer r.Close()
	ctx, cl := context.WithTimeout(
		context.WithValue(context.Background(), "session", "testid"),
		100*time.Millisecond)
	defer cl()
	_, err := r.Resolve(ctx, "parsing")
	assert.Error(t, err)
	assert.Equal(t, "context deadline exceeded", err.Error())
}

func TestResolverSlowServer(t *testing.T) {
	l, err := net.Listen("tcp", ":10053")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				time.Sleep(1 * time.Second)
				c.Close()
			}(conn)
		}
	}()

	r := NewCocaineResolver(&fakeDiscovery{
		F: func() ([]string, error) {
			return []string{"localhost"}, nil
		},
	})
	defer r.Close()
	ctx, cl := context.WithTimeout(
		context.WithValue(context.Background(), "session", "testid"),
		1200*time.Millisecond)
	defer cl()
	_, err = r.Resolve(ctx, "parsing")
	assert.Error(t, err)
	assert.Equal(t, "context deadline exceeded", err.Error())
}
