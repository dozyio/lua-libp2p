package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	cid "github.com/ipfs/go-cid"
	libp2p "github.com/libp2p/go-libp2p"
	peer "github.com/libp2p/go-libp2p/core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run . <bootstrap-multiaddr> <key-hex>")
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
	keyBytes, err := hex.DecodeString(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	content := cid.NewCidV1(cid.Raw, mh.Multihash(keyBytes))

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

	if err := h.Connect(ctx, *ai); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := kdht.RoutingTable().TryAddPeer(ai.ID, true, false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	providerCh := kdht.FindProvidersAsync(ctx, content, 1)
	select {
	case provider, ok := <-providerCh:
		if !ok {
			fmt.Fprintln(os.Stderr, "expected at least one provider")
			os.Exit(1)
		}
		if provider.ID != ai.ID {
			fmt.Fprintf(os.Stderr, "provider mismatch: got %s want %s\n", provider.ID, ai.ID)
			os.Exit(1)
		}
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, ctx.Err())
		os.Exit(1)
	}

	fmt.Println("ok")
}
