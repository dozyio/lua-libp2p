package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/host/autonat"
	"github.com/multiformats/go-multiaddr"
)

func printAddrs(label string, addrs []multiaddr.Multiaddr) {
	fmt.Println(label)
	for _, a := range addrs {
		fmt.Printf("  %s\n", a.String())
	}
}

func connectBootstrapPeers(ctx context.Context, h host.Host) {
	peers := dht.GetDefaultBootstrapPeerAddrInfos()
	connected := 0
	for _, p := range peers {
		if err := h.Connect(ctx, p); err == nil {
			connected++
		}
	}
	fmt.Printf("bootstrap peers connected: %d/%d\n", connected, len(peers))
}

func countDHTPeers(h host.Host) int {
	seen := map[peer.ID]struct{}{}
	for _, c := range h.Network().Conns() {
		seen[c.RemotePeer()] = struct{}{}
	}
	return len(seen)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "host init failed: %v\n", err)
		os.Exit(1)
	}
	defer h.Close()

	fmt.Printf("peer id: %s\n", h.ID())
	printAddrs("initial addrs:", h.Addrs())
	fmt.Println("waiting for NAT mapping + AutoNAT updates (Ctrl+C to stop)...")

	autoNAT, err := autonat.New(h)
	if err != nil {
		fmt.Fprintf(os.Stderr, "autonat init failed: %v\n", err)
		os.Exit(1)
	}
	defer autoNAT.Close()

	kad, err := dht.New(ctx, h)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kad-dht init failed: %v\n", err)
		os.Exit(1)
	}
	defer kad.Close()

	if err := kad.Bootstrap(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "kad-dht bootstrap failed: %v\n", err)
	}
	connectBootstrapPeers(ctx, h)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			fmt.Println("shutting down")
			return
		case <-ticker.C:
			reachability := autoNAT.Status()
			fmt.Printf("reachability: %s\n", reachability.String())
			fmt.Printf("connected peers: %d\n", countDHTPeers(h))
			if reachability == network.ReachabilityPublic {
				fmt.Println("autonat: node appears publicly reachable")
			} else if reachability == network.ReachabilityPrivate {
				fmt.Println("autonat: node appears behind NAT/private")
			}
			printAddrs("current addrs:", h.Addrs())
		}
	}
}
