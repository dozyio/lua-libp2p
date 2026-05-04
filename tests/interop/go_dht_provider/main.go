package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	cid "github.com/ipfs/go-cid"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer h.Close()

	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer kdht.Close()

	digest, err := mh.Sum([]byte("lua-libp2p-dht-provider-interop"), mh.SHA2_256, -1)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	content := cid.NewCidV1(cid.Raw, digest)
	if err := kdht.Provide(ctx, content, false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

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
	fmt.Println(hex.EncodeToString(digest))

	<-ctx.Done()
	time.Sleep(100 * time.Millisecond)
}
