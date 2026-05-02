package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	natpmp "github.com/jackpal/go-nat-pmp"
)

func main() {
	var gateway string
	var internalPort int
	var externalPort int
	var ttl int

	flag.StringVar(&gateway, "gateway", "", "NAT-PMP gateway IPv4 (required)")
	flag.IntVar(&internalPort, "internal-port", 4001, "internal TCP port")
	flag.IntVar(&externalPort, "external-port", 4001, "requested external TCP port")
	flag.IntVar(&ttl, "ttl", 3600, "mapping lifetime seconds")
	flag.Parse()

	if gateway == "" {
		fmt.Fprintln(os.Stderr, "--gateway is required")
		os.Exit(2)
	}
	gwIP := net.ParseIP(gateway)
	if gwIP == nil {
		fmt.Fprintf(os.Stderr, "invalid --gateway IP: %s\n", gateway)
		os.Exit(2)
	}

	client := natpmp.NewClient(gwIP)

	start := time.Now()
	extResp, err := client.GetExternalAddress()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetExternalAddress failed after %s: %v\n", time.Since(start).Round(time.Millisecond), err)
		os.Exit(1)
	}
	fmt.Printf("external ip: %d.%d.%d.%d\n", extResp.ExternalIPAddress[0], extResp.ExternalIPAddress[1], extResp.ExternalIPAddress[2], extResp.ExternalIPAddress[3])
	fmt.Printf("epoch: %d\n", extResp.SecondsSinceStartOfEpoc)

	start = time.Now()
	mapResp, err := client.AddPortMapping("tcp", internalPort, externalPort, ttl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "AddPortMapping failed after %s: %v\n", time.Since(start).Round(time.Millisecond), err)
		os.Exit(1)
	}
	fmt.Printf("mapping ok: internal=%d external=%d ttl=%d\n", mapResp.InternalPort, mapResp.MappedExternalPort, mapResp.PortMappingLifetimeInSeconds)
	fmt.Printf("mapping epoch: %d\n", mapResp.SecondsSinceStartOfEpoc)
}
