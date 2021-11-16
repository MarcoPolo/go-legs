package adv

import (
	"context"
	"testing"
	"time"

	nhttp "net/http"

	"github.com/filecoin-project/go-legs/http"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	blhost "github.com/libp2p/go-libp2p-blankhost"
	peer "github.com/libp2p/go-libp2p-core/peer"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	"github.com/multiformats/go-multiaddr"
)

func TestWithHttpAdv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))
	h2 := blhost.NewBlankHost(swarmt.GenSwarm(t, ctx))

	// Proviee multiaddrs for h1 to h2
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)

	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcSys := test.MkLinkSystem(srcStore)

	p, err := http.NewPublisher(context.Background(), srcStore, srcSys)
	if err != nil {
		t.Fatal(err)
	}

	rootLnk, err := test.Store(srcStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	go Serve(ctx, h1, p.(nhttp.Handler))

	client, err := NewClient(ctx, h2, h1.ID())
	if err != nil {
		t.Fatal(err)
	}

	notUsed, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	proto, _ := multiaddr.NewMultiaddr("/http")
	nlm := multiaddr.Join(notUsed, proto)

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstSys := test.MkLinkSystem(dstStore)
	subscriber, err := http.NewHTTPSubscriber(ctx, &client, nlm, &dstSys, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	cchan, cncl, err := subscriber.Sync(context.Background(), peer.NewPeerRecord().PeerID, cid.Undef, nil)
	defer cncl()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case rc := <-cchan:
		if !rc.Equals(rootLnk.(cidlink.Link).Cid) {
			t.Fatalf("didn't get expected cid. expected %s, got %s", rootLnk, rc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}
