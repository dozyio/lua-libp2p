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
	"github.com/libp2p/go-libp2p/core/protocol"
	holepunch_pb "github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	pbio "github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	dcutrID      = protocol.ID("/libp2p/dcutr")
	maxFrameSize = 4 * 1024
)

func main() {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer h.Close()

	pid := h.ID()
	addrs := peer.AddrInfo{ID: pid, Addrs: h.Addrs()}
	full, err := peer.AddrInfoToP2pAddrs(&addrs)
	if err != nil || len(full) == 0 {
		fmt.Fprintln(os.Stderr, "failed to build p2p addr")
		os.Exit(1)
	}
	fmt.Println(full[0].String())
	_ = bufio.NewWriter(os.Stdout).Flush()

	done := make(chan error, 1)
	h.SetStreamHandler(dcutrID, func(s network.Stream) {
		defer s.Close()
		_ = s.SetDeadline(time.Now().Add(5 * time.Second))
		r := pbio.NewDelimitedReader(s, maxFrameSize)
		w := pbio.NewDelimitedWriter(s)

		req := &holepunch_pb.HolePunch{}
		err := r.ReadMsg(req)
		if err != nil {
			done <- err
			return
		}
		if req.GetType() != holepunch_pb.HolePunch_CONNECT {
			done <- fmt.Errorf("expected CONNECT")
			return
		}

		obs := make([][]byte, 0, len(h.Addrs()))
		for _, a := range h.Addrs() {
			if !isPublicOrLoopback(a) {
				continue
			}
			obs = append(obs, a.Bytes())
		}
		if len(obs) == 0 {
			obs = append(obs, full[0].Bytes())
		}
		resp := &holepunch_pb.HolePunch{
			Type:    holepunch_pb.HolePunch_CONNECT.Enum(),
			ObsAddrs: obs,
		}
		if err := w.WriteMsg(resp); err != nil {
			done <- err
			return
		}

		syncMsg := &holepunch_pb.HolePunch{}
		err = r.ReadMsg(syncMsg)
		if err != nil {
			done <- err
			return
		}
		if syncMsg.GetType() != holepunch_pb.HolePunch_SYNC {
			done <- fmt.Errorf("expected SYNC")
			return
		}
		done <- nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "timeout waiting for dcutr stream")
		os.Exit(1)
	}
}

func isPublicOrLoopback(a ma.Multiaddr) bool {
	value, err := a.ValueForProtocol(ma.P_IP4)
	if err == nil {
		if value == "127.0.0.1" {
			return true
		}
		return true
	}
	return false
}
