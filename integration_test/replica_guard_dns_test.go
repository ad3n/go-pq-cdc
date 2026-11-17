package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/dnsdiscovery"
	"github.com/ad3n/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const dnsTypeA = 1

type dnsResponder struct {
	lastQuery time.Time
	answers   map[string][]netip.Addr
	resolves  map[string]int
	addr      string
	mu        sync.Mutex
	down      bool
}

func newDNSResponder(t *testing.T) *dnsResponder {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	d := &dnsResponder{addr: ln.Addr().String(), answers: map[string][]netip.Addr{}, resolves: map[string]int{}}
	go func() {
		for {
			c, aErr := ln.Accept()
			if aErr != nil {
				return
			}

			go d.serve(c)
		}
	}()
	return d
}

func (d *dnsResponder) set(name string, ips ...netip.Addr) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.answers[name+"."] = ips
}

func (d *dnsResponder) setDown(down bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.down = down
}

func (d *dnsResponder) resolveCount(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.resolves[name+"."] / 2
}

func (d *dnsResponder) waitStale(interval time.Duration) {
	for {
		d.mu.Lock()
		age := time.Since(d.lastQuery)
		d.mu.Unlock()
		if age > interval+50*time.Millisecond {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (d *dnsResponder) serve(c net.Conn) {
	defer c.Close()

	for {
		var size [2]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}

		msg := make([]byte, binary.BigEndian.Uint16(size[:]))
		if _, err := io.ReadFull(c, msg); err != nil {
			return
		}

		var labels []string
		i := 12
		for msg[i] != 0 {
			labels = append(labels, string(msg[i+1:i+1+int(msg[i])]))
			i += 1 + int(msg[i])
		}

		name, qtype, end := strings.Join(labels, ".")+".", binary.BigEndian.Uint16(msg[i+1:]), i+5

		d.mu.Lock()
		d.lastQuery = time.Now()
		if qtype == dnsTypeA {
			d.resolves[name]++
		}

		ips, known := d.answers[name]
		down := d.down
		d.mu.Unlock()
		if down {
			continue
		}

		var rcode uint16
		if !known {
			rcode = 3
		}

		if qtype != dnsTypeA {
			ips = nil
		}

		resp := binary.BigEndian.AppendUint16(nil, 0)
		resp = append(resp, msg[:2]...)
		resp = binary.BigEndian.AppendUint16(resp, 0x8180|rcode)
		resp = binary.BigEndian.AppendUint16(resp, 1)
		resp = binary.BigEndian.AppendUint16(resp, uint16(len(ips)))
		resp = append(resp, 0, 0, 0, 0)
		resp = append(resp, msg[12:end]...)
		for _, ip := range ips {

			resp = append(resp, 0xC0, 12, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4)
			resp = append(resp, ip.AsSlice()...)
		}

		binary.BigEndian.PutUint16(resp, uint16(len(resp)-2))
		if _, err := c.Write(resp); err != nil {
			return
		}
	}
}

func requireContainerNetwork(ctx context.Context, t *testing.T) {
	t.Helper()
	ip, err := Container.ContainerIP(ctx)
	require.NoError(t, err)
	addr := net.JoinHostPort(ip, "5432")
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", addr)
	if err == nil {
		_ = conn.Close()
		return
	}

	if os.Getenv("CI") != "" {
		t.Fatalf("CI is set and the primary's Docker network address %s is not reachable from the test process: %v", addr, err)
	}

	t.Skipf("the primary's Docker network address %s is not reachable from the test process (%v): Docker Desktop does not route container IPs to a macOS host; run make test/integration/linux RUN=TestReplicaGuardDNS", addr, err)
}

func startGatedStandby(ctx context.Context, t *testing.T, name, applyDelay string) (netip.Addr, *pgxpool.Pool, testcontainers.Container) {
	t.Helper()
	_, pool, container := startStandbyContainer(ctx, t, name, applyDelay)
	ip, err := container.ContainerIP(ctx)
	require.NoError(t, err)
	addr, err := netip.ParseAddr(ip)
	require.NoError(t, err)
	return addr, pool, container
}

func dnsGuardConfig(t *testing.T, slot string, failMode config.VisibilityFailMode, timeout, refresh time.Duration, nameserver string, names ...string) config.Config {
	t.Helper()
	body := fmt.Sprintf(`visibilityGuard:
  enabled: true
  failMode: %s
  timeout: %s
  discovery:
    dns:
      refreshInterval: %s
      port: 5432
      nameserver: %s
      names: [%s]
`, failMode, timeout, refresh, nameserver, strings.Join(names, ", "))
	path := filepath.Join(t.TempDir(), "visibility_guard.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	fromFile, err := config.ReadConfigYAML(path)
	require.NoError(t, err)
	cdcCfg := Config
	cdcCfg.Slot.Name = slot
	cdcCfg.VisibilityGuard = fromFile.VisibilityGuard
	return cdcCfg
}

func requireNoEvent(t *testing.T, events <-chan replicaEvent, window time.Duration) {
	t.Helper()
	select {
	case ev := <-events:
		t.Fatalf("event %d was dispatched while the gate must still hold (visible on standbys: %v)", ev.id, ev.visibleOn)
	case <-time.After(window):
	}
}

func insertBook(ctx context.Context, t *testing.T, primary *pgxpool.Pool, id int, name string) time.Time {
	t.Helper()
	committedAt := time.Now()
	_, err := primary.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", id, name)
	require.NoError(t, err)
	return committedAt
}

func slotPID(ctx context.Context, t *testing.T, primary *pgxpool.Pool, slot string) int {
	t.Helper()
	var pid int
	require.NoError(t, primary.QueryRow(ctx, "SELECT coalesce(active_pid, 0) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&pid))
	return pid
}

func requireRunning(t *testing.T, g *guardedConnector) {
	t.Helper()
	select {
	case <-g.done:
		t.Fatal("the connector stopped")
	default:
	}
}

func requireStartFails(ctx context.Context, t *testing.T, g *guardedConnector) {
	t.Helper()
	assert.Error(t, g.start(ctx, 3*time.Second), "the stream must not become ready")
	select {
	case <-g.done:
	case <-time.After(5 * time.Second):
		t.Fatal("startup must fail, the connector kept running")
	}
}

func lagGaugeLabels(t *testing.T) string {
	t.Helper()
	resp, err := http.Get("http://localhost:" + strconv.Itoa(Config.Metric.Port) + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "go_pq_cdc_visibility_replica_lag_bytes{") {
			lines = append(lines, line)
		}
	}

	return strings.Join(lines, "\n")
}

func TestReplicaGuardDNS(t *testing.T) {
	const applyDelay = 3 * time.Second
	const refreshInterval = 200 * time.Millisecond
	const nameA = "replica.books.service.services.region-a.test"
	const nameB = "replica.books.service.services.region-b.test"
	ctx := context.Background()
	requireContainerNetwork(ctx, t)

	ipA, standbyA, _ := startGatedStandby(ctx, t, "standby_dns_a", "0")
	ipB, standbyB, _ := startGatedStandby(ctx, t, "standby_dns_b", applyDelay.String())
	addrA, addrB := net.JoinHostPort(ipA.String(), "5432"), net.JoinHostPort(ipB.String(), "5432")

	primaryCfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	primary, err := pgxpool.New(ctx, primaryCfg.DSNWithoutSSL())
	require.NoError(t, err)
	t.Cleanup(primary.Close)
	waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyB)

	dns := newDNSResponder(t)
	guardCfg := func(t *testing.T, slot string, failMode config.VisibilityFailMode, timeout, refresh time.Duration, names ...string) config.Config {
		t.Helper()
		return dnsGuardConfig(t, slot, failMode, timeout, refresh, dns.addr, names...)
	}

	connect := func(t *testing.T, cfg config.Config, standbys ...*pgxpool.Pool) *guardedConnector {
		t.Helper()
		t.Cleanup(func() { dropReplicationSlot(ctx, t, cfg.Slot.Name) })
		return newGuardedConnector(ctx, t, cfg, standbys...)
	}

	requireHeldAndVisible := func(t *testing.T, ev replicaEvent, committedAt time.Time, id int32) {
		t.Helper()
		assert.Equal(t, id, ev.id)
		assert.Equal(t, []bool{true, true}, ev.visibleOn, "the row must be readable on both standbys when the handler runs")
		assert.GreaterOrEqual(t, ev.dispatchedAt.Sub(committedAt), applyDelay-500*time.Millisecond, "dispatch must wait for the delayed standby")
		assert.Less(t, ev.dispatchedAt.Sub(committedAt), 10*time.Second)
	}

	t.Run("1_two_names_two_regions", func(t *testing.T) {
		dns.set(nameA, ipA)
		dns.set(nameB, ipB)

		t.Run("control: only region A configured, the row is not readable on region B (the incident)", func(t *testing.T) {
			g := connect(t, guardCfg(t, "slot_test_dns_1_control", config.VisibilityFailClosed, 0, refreshInterval, nameA), standbyA, standbyB)
			require.NoError(t, g.start(ctx, 15*time.Second))
			waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyB)

			committedAt := insertBook(ctx, t, primary, 99101, "control")
			ev := nextEvent(t, g.events, 20*time.Second)
			assert.Equal(t, int32(99101), ev.id)
			assert.Equal(t, []bool{true, false}, ev.visibleOn, "region A is gated; region B is not and does not have the row yet")
			assert.Less(t, ev.dispatchedAt.Sub(committedAt), applyDelay-time.Second, "nothing waits for region B")
			labels := lagGaugeLabels(t)
			assert.Contains(t, labels, `replica="`+addrA+`"`, "standby A is polled at the address its name answered with")
			assert.NotContains(t, labels, `replica="`+addrB+`"`)
		})

		t.Run("both names configured: held until region B replayed", func(t *testing.T) {
			g := connect(t, guardCfg(t, "slot_test_dns_1_both", config.VisibilityFailClosed, 0, refreshInterval, nameA, nameB), standbyA, standbyB)
			require.NoError(t, g.start(ctx, 15*time.Second))
			waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyB)

			committedAt := insertBook(ctx, t, primary, 99102, "both")
			requireHeldAndVisible(t, nextEvent(t, g.events, 20*time.Second), committedAt, 99102)
			labels := lagGaugeLabels(t)
			assert.Contains(t, labels, `replica="`+addrA+`"`)
			assert.Contains(t, labels, `replica="`+addrB+`"`)
		})
	})

	t.Run("2_member_added_while_streaming", func(t *testing.T) {
		dns.set(nameA, ipA)
		g := connect(t, guardCfg(t, "slot_test_dns_2", config.VisibilityFailClosed, 0, refreshInterval, nameA), standbyA, standbyB)
		require.NoError(t, g.start(ctx, 15*time.Second))
		waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyB)

		insertBook(ctx, t, primary, 99201, "before")
		ev := nextEvent(t, g.events, 20*time.Second)
		assert.Equal(t, []bool{true, false}, ev.visibleOn, "standby B is not in the answer yet")
		waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyB)

		dns.set(nameA, ipA, ipB)
		dns.waitStale(refreshInterval)
		committedAt := insertBook(ctx, t, primary, 99202, "after")
		requireHeldAndVisible(t, nextEvent(t, g.events, 20*time.Second), committedAt, 99202)
	})

	t.Run("3_member_removed_while_streaming", func(t *testing.T) {
		ipC, standbyC, containerC := startGatedStandby(ctx, t, "standby_dns_c", applyDelay.String())
		addrC := net.JoinHostPort(ipC.String(), "5432")
		dns.set(nameA, ipA, ipC)
		const slot = "slot_test_dns_3"
		g := connect(t, guardCfg(t, slot, config.VisibilityFailClosed, 0, refreshInterval, nameA), standbyA, standbyC)
		require.NoError(t, g.start(ctx, 15*time.Second))
		waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyC)
		pid := slotPID(ctx, t, primary, slot)
		require.NotZero(t, pid)

		committedAt := insertBook(ctx, t, primary, 99301, "listed")
		ev := nextEvent(t, g.events, 20*time.Second)
		assert.Equal(t, []bool{true, true}, ev.visibleOn)
		assert.GreaterOrEqual(t, ev.dispatchedAt.Sub(committedAt), applyDelay-500*time.Millisecond, "standby C is gated")
		assert.Contains(t, lagGaugeLabels(t), `replica="`+addrC+`"`, "a listed standby has a lag series")

		stopTimeout := 5 * time.Second
		require.NoError(t, containerC.Stop(ctx, &stopTimeout))
		dns.set(nameA, ipA)
		dns.waitStale(refreshInterval)
		resolvesBefore := dns.resolveCount(nameA)

		insertBook(ctx, t, primary, 99302, "removed")
		ev = nextEvent(t, g.events, 20*time.Second)
		assert.Equal(t, int32(99302), ev.id)
		assert.True(t, ev.visibleOn[0], "standby A is still gated")
		assert.Equal(t, resolvesBefore+1, dns.resolveCount(nameA), "the delisted, stopped standby is dropped with exactly one resolve")
		insertBook(ctx, t, primary, 99303, "removed-2")
		assert.Equal(t, int32(99303), nextEvent(t, g.events, 20*time.Second).id)

		labels := lagGaugeLabels(t)
		assert.NotContains(t, labels, `replica="`+addrC+`"`, "the lag series of a delisted standby is deleted")
		assert.Contains(t, labels, `replica="`+addrA+`"`, "the series of a standby that stayed is kept")
		requireRunning(t, g)
		assert.Equal(t, pid, slotPID(ctx, t, primary, slot), "the stream was not restarted")
	})

	t.Run("4_promoted_member", func(t *testing.T) {
		ipD, standbyD, _ := startGatedStandby(ctx, t, "standby_dns_d", applyDelay.String())
		addrD := net.JoinHostPort(ipD.String(), "5432")

		t.Run("promoted, then delisted: dropped by the wait's one re-resolve, the stream goes on", func(t *testing.T) {
			dns.set(nameA, ipA, ipD)
			const slot = "slot_test_dns_4"

			g := connect(t, guardCfg(t, slot, config.VisibilityFailClosed, 0, 5*time.Minute, nameA), standbyA, standbyD)
			require.NoError(t, g.start(ctx, 15*time.Second))
			waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyD)
			pid := slotPID(ctx, t, primary, slot)
			require.NotZero(t, pid)

			committedAt := insertBook(ctx, t, primary, 99401, "listed")
			ev := nextEvent(t, g.events, 20*time.Second)
			assert.Equal(t, []bool{true, true}, ev.visibleOn)
			assert.GreaterOrEqual(t, ev.dispatchedAt.Sub(committedAt), applyDelay-500*time.Millisecond, "standby D is gated")
			waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyD)

			var promoted bool
			require.NoError(t, standbyD.QueryRow(ctx, "SELECT pg_promote(true, 60)").Scan(&promoted))
			require.True(t, promoted)
			dns.set(nameA, ipA)
			resolvesBefore := dns.resolveCount(nameA)

			insertBook(ctx, t, primary, 99402, "promoted")
			ev = nextEvent(t, g.events, 20*time.Second)
			assert.Equal(t, int32(99402), ev.id)
			assert.True(t, ev.visibleOn[0], "standby A is still gated")
			insertBook(ctx, t, primary, 99403, "promoted-2")
			assert.Equal(t, int32(99403), nextEvent(t, g.events, 20*time.Second).id)

			assert.Equal(t, resolvesBefore+1, dns.resolveCount(nameA), "exactly one re-resolve found the promotion; the dropped standby is not asked about again")
			labels := lagGaugeLabels(t)
			assert.NotContains(t, labels, `replica="`+addrD+`"`, "the lag series of the dropped standby is deleted")
			assert.Contains(t, labels, `replica="`+addrA+`"`)
			requireRunning(t, g)
			assert.Equal(t, pid, slotPID(ctx, t, primary, slot), "the stream was not restarted")
		})

		t.Run("promoted and still listed: startup fails until the name delists it", func(t *testing.T) {
			require.ErrorIs(t, replication.WaitReplayed(ctx, standbyD, 1, replication.WaitOptions{Timeout: time.Second}), replication.ErrNotStandby,
				"the promoted standby answers the guard's poll as not in recovery")
			dns.set(nameA, ipA, ipD)
			listed := connect(t, guardCfg(t, "slot_test_dns_4_listed", config.VisibilityFailClosed, 0, refreshInterval, nameA), standbyA)
			requireStartFails(ctx, t, listed)
			listed.connector.Close()

			dns.set(nameA, ipA)
			g := connect(t, guardCfg(t, "slot_test_dns_4_delisted", config.VisibilityFailClosed, 0, refreshInterval, nameA), standbyA)
			require.NoError(t, g.start(ctx, 15*time.Second))
			waitStandbyCaughtUp(ctx, t, primary, standbyA)
			insertBook(ctx, t, primary, 99404, "delisted")
			ev := nextEvent(t, g.events, 20*time.Second)
			assert.Equal(t, int32(99404), ev.id)
			assert.True(t, ev.visible)
			assert.NotContains(t, lagGaugeLabels(t), `replica="`+addrD+`"`)
		})
	})

	holdsThroughSourceFailure := func(t *testing.T, slot string, id int32, breakSource, fixSource func()) {
		t.Helper()
		dns.set(nameA, ipA)
		dns.set(nameB, ipB)
		g := connect(t, guardCfg(t, slot, config.VisibilityFailClosed, 20*time.Second, refreshInterval, nameA, nameB), standbyA, standbyB)
		require.NoError(t, g.start(ctx, 15*time.Second))
		waitStandbyCaughtUp(ctx, t, primary, standbyA, standbyB)

		dns.waitStale(refreshInterval)
		breakSource()
		resolvesBefore := dns.resolveCount(nameB)
		insertBook(ctx, t, primary, int(id), "outage")
		requireNoEvent(t, g.events, applyDelay+time.Second)
		assert.GreaterOrEqual(t, dns.resolveCount(nameB)-resolvesBefore, 2, "the refresh is retried until the source answers")
		requireRunning(t, g)

		fixSource()
		ev := nextEvent(t, g.events, 15*time.Second)
		assert.Equal(t, id, ev.id)
		assert.Equal(t, []bool{true, true}, ev.visibleOn, "the held event is dispatched after both standbys replayed it")
		assert.NoError(t, ev.waitErr)
	}

	t.Run("5_source_outage_fail_closed", func(t *testing.T) {
		holdsThroughSourceFailure(t, "slot_test_dns_5", 99501, func() { dns.setDown(true) }, func() { dns.setDown(false) })
	})

	t.Run("6_same_address_under_both_names", func(t *testing.T) {
		dns.set(nameA, ipA)
		dns.set(nameB, ipA)
		_, err := dnsdiscovery.Replicas(dnsdiscovery.Options{Names: []string{nameA, nameB}, Port: 5432, Nameserver: dns.addr})(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), addrA+" is listed under "+nameA+" and under "+nameB)

		g := connect(t, guardCfg(t, "slot_test_dns_6", config.VisibilityFailClosed, 0, refreshInterval, nameA, nameB), standbyA)
		requireStartFails(ctx, t, g)
	})

	t.Run("7_empty_answer", func(t *testing.T) {
		t.Run("at startup: the source failed, it is not zero replicas", func(t *testing.T) {
			dns.set(nameA, ipA)
			dns.set(nameB)
			_, err := dnsdiscovery.Replicas(dnsdiscovery.Options{Names: []string{nameA, nameB}, Port: 5432, Nameserver: dns.addr})(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "name "+nameB+": no A or AAAA record")

			g := connect(t, guardCfg(t, "slot_test_dns_7_start", config.VisibilityFailClosed, 0, refreshInterval, nameA, nameB), standbyA)
			requireStartFails(ctx, t, g)
		})

		t.Run("while streaming: the refresh fails, it is not zero replicas", func(t *testing.T) {
			holdsThroughSourceFailure(t, "slot_test_dns_7_stream", 99701, func() { dns.set(nameB) }, func() { dns.set(nameB, ipB) })
		})
	})
}
