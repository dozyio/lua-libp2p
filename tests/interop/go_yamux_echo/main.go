package main

import (
  "bufio"
  "fmt"
  "io"
  "net"
  "os"
  "time"

  yamux "github.com/libp2p/go-yamux/v5"
)

func main() {
  listenAddr := os.Getenv("YAMUX_ECHO_ADDR")
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
  _ = bufio.NewWriter(os.Stdout).Flush()

  conn, err := ln.Accept()
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer conn.Close()

  session, err := yamux.Server(conn, nil, nil)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer session.Close()

  stream, err := session.AcceptStream()
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer stream.Close()

  req := make([]byte, 5)
  if _, err := io.ReadFull(stream, req); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  if string(req) != "hello" {
    fmt.Fprintln(os.Stderr, "unexpected request payload")
    os.Exit(1)
  }

  if _, err := stream.Write([]byte("world")); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  time.Sleep(250 * time.Millisecond)
}
