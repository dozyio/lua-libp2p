package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ping "github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

func main() {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer h.Close()

	_ = ping.NewPingService(h)

	connected := make(chan struct{}, 1)
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			if c.Stat().Direction == network.DirInbound && !c.Stat().Limited {
				select {
				case connected <- struct{}{}:
				default:
				}
			}
		},
	})

	addrInfo := peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
	full, err := peer.AddrInfoToP2pAddrs(&addrInfo)
	if err != nil || len(full) == 0 {
		fmt.Fprintln(os.Stderr, "failed to build p2p addr")
		os.Exit(1)
	}
	fmt.Println(full[0].String())
	_ = bufio.NewWriter(os.Stdout).Flush()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-connected:
		// Keep the host alive long enough for the Lua side to register the new
		// direct connection and verify a ping over it.
		time.Sleep(3 * time.Second)
		fmt.Println("ok")
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "timeout waiting for unilateral direct connection")
		os.Exit(1)
	}
}
