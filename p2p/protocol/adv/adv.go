package adv

import (
	"context"
	"net"
	"net/http"

	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	gostream "github.com/libp2p/go-libp2p-gostream"
)

const ProtocolID = "/legs/adv/0.0.1"

func Serve(ctx context.Context, host host.Host, handler http.Handler) error {
	l, err := gostream.Listen(host, ProtocolID)
	if err != nil {
		return err
	}

	server := http.Server{Handler: handler}
	errChan := make(chan error)
	go func() {
		errChan <- server.Serve(l)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return server.Close()
	}
}

func NewClient(ctx context.Context, host host.Host, peer peer.ID) (http.Client, error) {
	transport := http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return gostream.Dial(ctx, host, peer, ProtocolID)
		},
	}

	return http.Client{
		Transport: &transport,
	}, nil
}
