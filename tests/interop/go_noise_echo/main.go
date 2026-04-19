package main

import (
  "context"
  "crypto/rand"
  "fmt"
  "io"
  "net"
  "os"
  "time"

  ic "github.com/libp2p/go-libp2p/core/crypto"
  "github.com/libp2p/go-libp2p/core/protocol"
  noise "github.com/libp2p/go-libp2p/p2p/security/noise"
)

func main() {
  listenAddr := os.Getenv("NOISE_ECHO_ADDR")
  if listenAddr == "" {
    listenAddr = "127.0.0.1:0"
  }

  ln, err := net.Listen("tcp", listenAddr)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer ln.Close()

  fmt.Println(ln.Addr().String())

  conn, err := ln.Accept()
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

  tpt, err := noise.New(protocol.ID(noise.ID), priv, nil)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  secConn, err := tpt.SecureInbound(context.Background(), conn, "")
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  buf := make([]byte, 5)
  if _, err := io.ReadFull(secConn, buf); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  if string(buf) != "hello" {
    fmt.Fprintln(os.Stderr, "unexpected request payload")
    os.Exit(1)
  }

  if _, err := secConn.Write([]byte("world")); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  time.Sleep(200 * time.Millisecond)
}
