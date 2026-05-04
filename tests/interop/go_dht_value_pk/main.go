package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	routing "github.com/libp2p/go-libp2p/core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer h.Close()

	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer), dht.AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
		return addrs
	}))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer kdht.Close()

	pub, err := crypto.MarshalPublicKey(h.Peerstore().PubKey(h.ID()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	key := routing.KeyForPublicKey(h.ID())
	_ = kdht.PutValue(ctx, key, pub)

	addrs := h.Addrs()
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "host has no listen addresses")
		os.Exit(1)
	}
	peerPart, err := ma.NewMultiaddr("/p2p/" + h.ID().String())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println(addrs[0].Encapsulate(peerPart).String())
	fmt.Println(hex.EncodeToString([]byte(key)))

	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
}
