package main

import (
	"context"
	"fmt"
	"os"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	peer "github.com/libp2p/go-libp2p/core/peer"
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
	found, err := kdht.FindPeer(ctx, ai.ID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if found.ID != ai.ID || len(found.Addrs) == 0 {
		fmt.Fprintln(os.Stderr, "unexpected find peer result")
		os.Exit(1)
	}
	fmt.Println("ok")
}
