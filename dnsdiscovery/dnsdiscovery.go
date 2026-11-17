package dnsdiscovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
)

type (
	Options struct {
		Nameserver string

		Names []string

		Port int
	}

	lookupFunc func(ctx context.Context, network, host string) ([]netip.Addr, error)

	resolver struct {
		invalid error
		lookup  lookupFunc
		port    string
		names   []string
	}

	netResolver struct{ *net.Resolver }

	sourceResult struct {
		err     error
		members []config.ReplicaMember
	}
)

func Replicas(opts Options) func(ctx context.Context) ([]config.ReplicaMember, error) {
	r := &resolver{names: append([]string(nil), opts.Names...), port: strconv.Itoa(opts.Port)}
	switch {
	case len(opts.Names) == 0:
		r.invalid = errors.New("dnsdiscovery: no name configured, no cluster is proven")
	case opts.Port < 1 || opts.Port > 65535:
		r.invalid = fmt.Errorf("dnsdiscovery: port %d is not in 1-65535", opts.Port)
	case opts.Nameserver != "":
		if _, err := netip.ParseAddrPort(opts.Nameserver); err != nil {
			r.invalid = fmt.Errorf("dnsdiscovery: nameserver %q must be a literal ip:port: %w", opts.Nameserver, err)
		}
	}

	if r.invalid == nil {
		r.lookup = newNetResolver(opts.Nameserver).lookup
	}

	if opts.Nameserver == "" {
		logger.Warn("dnsdiscovery: no nameserver is set, answers come from the system resolver; Consul cuts a UDP answer to 3 records (udp_answer_limit) when a forwarder queries without EDNS, so a standby can be missing silently. Set a nameserver (a Consul agent DNS port) or confirm the path returns complete answers")
	}

	return r.resolve
}

func (n netResolver) lookup(ctx context.Context, network, host string) ([]netip.Addr, error) {

	return n.LookupNetIP(ctx, network, host)
}

func newNetResolver(nameserver string) netResolver {
	r := &net.Resolver{PreferGo: true, StrictErrors: true}
	if nameserver != "" {
		r.Dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", nameserver)
		}
	}

	return netResolver{r}
}

func (r *resolver) resolve(ctx context.Context) ([]config.ReplicaMember, error) {
	if r.invalid != nil {
		return nil, r.invalid
	}

	results := make([]sourceResult, len(r.names))
	var wg sync.WaitGroup
	for i, name := range r.names {
		wg.Add(1)
		go func() {
			defer wg.Done()

			results[i].members, results[i].err = r.source(ctx, name)
		}()
	}

	wg.Wait()

	var errs []error
	for _, res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	members := make([]config.ReplicaMember, 0)
	owner := make(map[string]string)
	for _, res := range results {
		for _, m := range res.members {
			if first, dup := owner[m.Addr]; dup {
				return nil, fmt.Errorf("dnsdiscovery: %s is listed under %s and under %s, two names resolved to the same standby", m.Addr, first, m.Source)
			}

			owner[m.Addr] = m.Source
		}

		members = append(members, res.members...)
	}

	return members, nil
}

func (r *resolver) source(ctx context.Context, name string) ([]config.ReplicaMember, error) {

	fqdn := strings.TrimSuffix(name, ".") + "."

	families := [3]string{"ip4", "ip6", "ip"}
	var (
		answers [3][]netip.Addr
		errs    [3]error
		wg      sync.WaitGroup
	)
	for i, network := range families {
		wg.Add(1)
		go func() {
			defer wg.Done()

			answers[i], errs[i] = r.lookup(ctx, network, fqdn)
		}()
	}

	wg.Wait()

	var failures []error
	for i, err := range errs {
		if err == nil || isNotFound(err) || (families[i] == "ip6" && isFilteredOut(err)) {
			continue
		}

		failures = append(failures, fmt.Errorf("dnsdiscovery name %s: %s lookup: %w", name, families[i], err))
	}

	if len(failures) > 0 {
		return nil, errors.Join(failures...)
	}

	members := make([]config.ReplicaMember, 0, len(answers[0])+len(answers[1]))
	seen := make(map[netip.Addr]bool)
	for _, family := range answers[:2] {
		for _, a := range family {
			a = a.Unmap()
			if seen[a] {
				continue
			}

			seen[a] = true
			members = append(members, config.ReplicaMember{
				Addr:   net.JoinHostPort(a.String(), r.port),
				Source: name,
			})
		}
	}

	if familyRecords := len(answers[0]) + len(answers[1]); familyRecords > 0 && len(answers[2]) == 0 {
		return nil, fmt.Errorf("dnsdiscovery name %s: the unfiltered lookup found no record while the ip4 or ip6 lookup returned %d; the cross-check disagrees with the family lookups", name, familyRecords)
	}

	for _, a := range answers[2] {
		if !seen[a.Unmap()] {
			return nil, fmt.Errorf("dnsdiscovery name %s: the unfiltered lookup returned %s, which the ip4 and ip6 lookups did not report (an IPv4-mapped AAAA record is dropped by the ip6 lookup); the set would be incomplete", name, a)
		}
	}

	if len(members) == 0 {
		return nil, fmt.Errorf("dnsdiscovery name %s: no A or AAAA record; an empty answer is a source failure, not zero replicas", name)
	}

	return members, nil
}

func isFilteredOut(err error) bool {
	var addrErr *net.AddrError
	return errors.As(err, &addrErr) && addrErr.Err == "no suitable address found"
}

func isNotFound(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}
