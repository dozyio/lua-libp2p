package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	ma "github.com/multiformats/go-multiaddr"
)

type notifee struct{}

var seen sync.Map

func (notifee) HandlePeerFound(info peer.AddrInfo) {
	peerID := info.ID.String()
	if _, loaded := seen.LoadOrStore(peerID, true); loaded {
		return
	}
	fmt.Println("FOUND " + peerID)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	listen, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	h, err := libp2p.New(libp2p.ListenAddrs(listen))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer h.Close()

	svc := mdns.NewMdnsService(h, mdns.ServiceName, notifee{})
	if err := svc.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer svc.Close()

	fmt.Println(h.ID().String())
	<-ctx.Done()
}
