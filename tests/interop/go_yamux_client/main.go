package main

import (
  "context"
  "fmt"
  "io"
  "net"
  "os"

  yamux "github.com/libp2p/go-yamux/v5"
)

func main() {
  if len(os.Args) != 2 {
    fmt.Fprintln(os.Stderr, "usage: go run . <host:port>")
    os.Exit(2)
  }

  conn, err := net.Dial("tcp", os.Args[1])
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer conn.Close()

  session, err := yamux.Client(conn, nil, nil)
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer session.Close()

  stream, err := session.OpenStream(context.Background())
  if err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  defer stream.Close()

  if _, err := stream.Write([]byte("hello")); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }

  buf := make([]byte, 5)
  if _, err := io.ReadFull(stream, buf); err != nil {
    fmt.Fprintln(os.Stderr, err)
    os.Exit(1)
  }
  if string(buf) != "world" {
    fmt.Fprintln(os.Stderr, "unexpected response payload")
    os.Exit(1)
  }

  fmt.Println("ok")
}
