package network

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/grandcat/zeroconf"
)

const MDNSService = "_aether._tcp"

// StartMDNSAdvertiser broadcasts the local peer's existence over mDNS.
func StartMDNSAdvertiser(clientID string, port int) (*zeroconf.Server, error) {
	server, err := zeroconf.Register(clientID, MDNSService, "local.", port, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to register mDNS service: %w", err)
	}
	log.Printf("[mDNS] Broadcasting local P2P presence as %s on port %d", clientID, port)
	return server, nil
}

// DiscoverLocalPeers scans the local network for other Aether peers.
func DiscoverLocalPeers(timeout time.Duration) (map[string]string, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create mDNS resolver: %w", err)
	}

	entries := make(chan *zeroconf.ServiceEntry)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := resolver.Browse(ctx, MDNSService, "local.", entries); err != nil {
		return nil, fmt.Errorf("mDNS browse failed: %w", err)
	}

	peers := make(map[string]string)

	for {
		select {
		case <-ctx.Done():
			return peers, nil
		case entry, ok := <-entries:
			if !ok {
				return peers, nil
			}
			if len(entry.AddrIPv4) > 0 {
				ip := entry.AddrIPv4[0].String()
				peers[entry.Instance] = ip
				log.Printf("[mDNS] Found local peer: %s at %s:%d", entry.Instance, ip, entry.Port)
			}
		}
	}
}
