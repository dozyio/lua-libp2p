package main

import (
	"context"
	"fmt"
	"os"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run . <bootstrap-multiaddr>")
		os.Exit(2)
	}
	bootstrapAddr, err := ma.NewMultiaddr(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ai, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer h.Close()
	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer kdht.Close()

	h.Peerstore().AddAddrs(ai.ID, ai.Addrs, time.Minute)
	if _, err := kdht.RoutingTable().TryAddPeer(ai.ID, true, false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	value, err := kdht.GetValue(ctx, routing.KeyForPublicKey(ai.ID))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pub, err := crypto.UnmarshalPublicKey(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	derived, err := peer.IDFromPublicKey(pub)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if derived != ai.ID {
		fmt.Fprintf(os.Stderr, "peer id mismatch: got %s want %s\n", derived, ai.ID)
		os.Exit(1)
	}
	fmt.Println("ok")
}
