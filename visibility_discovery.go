package cdc

import (
	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/dnsdiscovery"
)

var newDNSResolver = dnsdiscovery.Replicas

func applyVisibilityDiscovery(cfg *config.Config) {
	d := cfg.VisibilityGuard.Discovery.DNS
	if d == nil {
		return
	}

	cfg.VisibilityGuard.ResolveReplicas = newDNSResolver(dnsdiscovery.Options{
		Nameserver: d.Nameserver,
		Names:      d.Names,
		Port:       d.Port,
	})
	cfg.VisibilityGuard.RefreshInterval = d.RefreshInterval
}
