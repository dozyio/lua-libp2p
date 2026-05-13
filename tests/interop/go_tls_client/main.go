package main

import (
  "context"
  "crypto/rand"
  "fmt"
  "io"
  "net"
  "os"

  ic "github.com/libp2p/go-libp2p/core/crypto"
  "github.com/libp2p/go-libp2p/core/peer"
  "github.com/libp2p/go-libp2p/core/protocol"
  libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
)

func main() {
  if len(os.Args) != 3 {
    fmt.Fprintln(os.Stderr, "usage: go run . <host:port> <expected-peer-id>")
    os.Exit(2)
  }

  conn, err := net.Dial("tcp", os.Args[1])
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer conn.Close()

  priv, _, err := ic.GenerateEd25519Key(rand.Reader)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  expectedPeerID, err := peer.Decode(os.Args[2])
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  tpt, err := libp2ptls.New(protocol.ID(libp2ptls.ID), priv, nil)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  secConn, err := tpt.SecureOutbound(context.Background(), conn, expectedPeerID)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  if _, err := secConn.Write([]byte("hello")); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  buf := make([]byte, 5)
  if _, err := io.ReadFull(secConn, buf); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  if string(buf) != "world" {
    fmt.Fprintln(os.Stderr, "unexpected response payload")
    os.Exit(1)
  }

  fmt.Println("ok")
}
