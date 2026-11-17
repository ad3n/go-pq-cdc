package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	sourceAnswer struct {
		err     error
		members []config.ReplicaMember
	}

	fakeSource struct {
		answers []sourceAnswer
		calls   atomic.Int32
	}

	discoveryMetric struct {
		*countingMetric
		deleted []string
	}
)

func (s *fakeSource) resolve(context.Context) ([]config.ReplicaMember, error) {
	n := int(s.calls.Add(1)) - 1
	if n >= len(s.answers) {
		n = len(s.answers) - 1
	}

	return s.answers[n].members, s.answers[n].err
}

func answer(members ...config.ReplicaMember) sourceAnswer {
	return sourceAnswer{members: append([]config.ReplicaMember{}, members...)}
}

func member(addr string, timeline int32) config.ReplicaMember {
	return config.ReplicaMember{Addr: addr, Source: "replica.a.service.consul", Timeline: timeline}
}

func (m *discoveryMetric) DeleteVisibilityReplica(replica string) {
	m.deleted = append(m.deleted, replica)
	m.lags.Delete(replica)
}

func testDiscoveryGuard(t *testing.T, src *fakeSource, dialers map[string]*dialer) (*replicaGuard, *discoveryMetric, error) {
	t.Helper()
	g, cm := testReplicaGuard(nil)
	m := &discoveryMetric{countingMetric: cm}
	g.metric = m
	g.cfg.ResolveReplicas = src.resolve
	g.cfg.RefreshInterval = time.Hour
	g.connect = func(addr string) func(context.Context) (replicaConn, error) {
		d, ok := dialers[addr]
		require.True(t, ok, "no dialer for %s", addr)
		return d.dial
	}
	return g, m, g.start(context.Background())
}

func due(g *replicaGuard) { g.resolvedAt = g.resolvedAt.Add(-2 * g.refreshInterval()) }

func names(g *replicaGuard) []string {
	var out []string
	for _, r := range g.replicas {
		out = append(out, r.name)
	}

	return out
}

func healthy(rows ...replicaRow) *dialer {
	return &dialer{conns: []*scriptedReplica{{rows: rows}}}
}

func TestReplicaDiscoveryRefreshReconcilesAddedAndRemovedReplicas(t *testing.T) {
	gone, kept, joined := healthy(standby("0/30")), healthy(standby("0/30")), healthy(standby("0/30"))
	src := &fakeSource{answers: []sourceAnswer{
		answer(member("gone:5432", 7), member("kept:5432", 7)),
		answer(member("kept:5432", 7), member("joined:5432", 7)),
	}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"gone:5432": gone, "kept:5432": kept, "joined:5432": joined})
	require.NoError(t, err)
	assert.Equal(t, int32(1), gone.dials.Load(), "start dials and checks every resolved replica")
	assert.Equal(t, int32(0), joined.dials.Load())

	require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second)))
	assert.Equal(t, int32(1), src.calls.Load(), "a fresh set is not refreshed")

	due(g)
	require.NoError(t, g.wait(context.Background(), 201, 0x20, time.Now().Add(time.Second)))
	assert.Equal(t, int32(2), src.calls.Load(), "one due refresh")
	assert.Equal(t, []string{"kept:5432", "joined:5432"}, names(g))
	assert.True(t, gone.conns[0].closed, "a removed replica's connection is closed")
	assert.Equal(t, []string{"gone:5432"}, m.deleted, "a removed replica's lag series is deleted")
	assert.Equal(t, int32(1), kept.dials.Load(), "a kept replica keeps its connection")
	assert.Equal(t, int32(1), joined.dials.Load(), "an added replica is dialled by its first poll")
	assert.Equal(t, int32(1), joined.conns[0].calls.Load())
}

func TestReplicaDiscoveryAddedReplicaIsFullyChecked(t *testing.T) {
	foreign := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/30")}, systemID: 42}}}
	src := &fakeSource{answers: []sourceAnswer{answer(), answer(member("foreign:5432", 7))}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"foreign:5432": foreign})
	require.NoError(t, err)

	due(g)
	err = g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second))
	require.ErrorIs(t, err, errClusterMismatch, "still listed after the re-resolve: classified as without discovery")
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	assert.Equal(t, int32(0), foreign.conns[0].calls.Load(), "no replay poll on a foreign cluster")
	assert.Equal(t, int32(3), src.calls.Load(), "start, due refresh, one re-resolve")
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestReplicaDiscoveryPromotedReplicaIsDroppedAfterReResolve(t *testing.T) {
	promoted := healthy(standby("0/10"), replicaRow{recovery: false, timeline: 7})
	lagging := healthy(standby("0/10"), standby("0/10"), standby("0/10"), standby("0/30"))
	src := &fakeSource{answers: []sourceAnswer{
		answer(member("promoted:5432", 7), member("lagging:5432", 7)),
		answer(member("lagging:5432", 7)),
	}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"promoted:5432": promoted, "lagging:5432": lagging})
	require.NoError(t, err)

	require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second)))
	assert.Equal(t, int32(2), src.calls.Load(), "start and one re-resolve")
	assert.Equal(t, []string{"lagging:5432"}, names(g))
	assert.True(t, promoted.conns[0].closed)
	assert.Equal(t, []string{"promoted:5432"}, m.deleted)
	assert.Equal(t, int32(1), lagging.dials.Load(), "the round stops between polls, never during one")
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestReplicaDiscoveryPromotedReplicaStillListedIsFatal(t *testing.T) {
	promoted := healthy(standby("0/10"), replicaRow{recovery: false, timeline: 7})
	src := &fakeSource{answers: []sourceAnswer{answer(member("promoted:5432", 7))}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"promoted:5432": promoted})
	require.NoError(t, err)

	err = g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second))
	require.ErrorIs(t, err, ErrNotStandby, "source and reality disagree")
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorContains(t, err, "replica promoted:5432")
	assert.Equal(t, int32(2), src.calls.Load())
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestReplicaDiscoveryReResolvesAtMostOncePerWait(t *testing.T) {
	broken := func() *dialer {
		return &dialer{conns: []*scriptedReplica{
			{rows: []replicaRow{standby("0/10"), {err: io.ErrUnexpectedEOF}}},
			{rows: []replicaRow{{err: io.ErrUnexpectedEOF}}},
		}}
	}
	decommissioned, restarting := broken(), broken()
	src := &fakeSource{answers: []sourceAnswer{
		answer(member("decommissioned:5432", 7), member("restarting:5432", 7)),
		answer(member("restarting:5432", 7)),
	}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"decommissioned:5432": decommissioned, "restarting:5432": restarting})
	require.NoError(t, err)

	err = g.wait(context.Background(), 200, 0x20, time.Now().Add(50*time.Millisecond))
	require.ErrorIs(t, err, ErrVisibilityTimeout, "a connection error of a listed replica is retried until the deadline")
	require.ErrorContains(t, err, "replica restarting:5432")
	assert.Equal(t, int32(2), src.calls.Load(), "start and exactly one re-resolve")
	assert.Equal(t, []string{"restarting:5432"}, names(g))
	assert.Greater(t, restarting.dials.Load(), int32(2), "redialled until the deadline")
	assert.Equal(t, int32(1), m.timeouts.Load())
}

func TestReplicaDiscoveryDueRefreshFailingUntilTheDeadlineTimesOut(t *testing.T) {
	boom := errors.New("resolver unreachable")
	a := healthy(standby("0/30"))
	src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), {err: boom}}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a})
	require.NoError(t, err)
	_, cached := g.replicas[0].cachedAge(0x20, time.Now())
	require.True(t, cached, "precondition: the cache alone would certify")

	due(g)
	err = g.wait(context.Background(), 200, 0x20, time.Now().Add(50*time.Millisecond))
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorIs(t, err, boom)
	assert.Greater(t, src.calls.Load(), int32(3), "retried with the poll backoff")
	assert.Equal(t, int32(1), a.conns[0].calls.Load(), "only the startup poll: the stale set never certifies")
	assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load())
	assert.Equal(t, int32(1), m.timeouts.Load())
	assert.Equal(t, []string{"a:5432"}, names(g), "a failed refresh changes nothing")

	due(g)
	src.answers = []sourceAnswer{{err: boom}, {err: boom}, answer(member("a:5432", 7))}
	src.calls.Store(0)
	require.NoError(t, g.wait(context.Background(), 201, 0x20, time.Now().Add(time.Second)), "a refresh that succeeds inside the budget certifies")
	assert.Equal(t, int32(3), src.calls.Load())
}

func TestGateDiscoveryRefreshFailureFollowsFailMode(t *testing.T) {
	for _, mode := range []config.VisibilityFailMode{config.VisibilityFailClosed, config.VisibilityFailOpen} {
		t.Run(string(mode), func(t *testing.T) {
			gs := newGatedStream(t, guardCfg(mode), [][]string{{"f", "100:201:"}})
			src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), {err: errors.New("resolver unreachable")}}}
			g, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": healthy(standby("0/30"))})
			require.NoError(t, err)
			g.metric, g.cfg.Timeout, g.cfg.FailMode = gs.m, gs.s.config.VisibilityGuard.Timeout, mode
			gs.s.replicas = g
			due(g)

			err = gs.run(txMsg(200, 2, 2))
			assert.Equal(t, int32(1), gs.m.timeouts.Load())
			if mode == config.VisibilityFailClosed {
				require.ErrorIs(t, err, ErrVisibilityTimeout)
				assert.Empty(t, gs.received)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, []uint32{200}, gs.received)
			assert.Equal(t, int32(1), gs.m.failOpens.Load())
		})
	}
}

func TestReplicaDiscoveryResolverSlowerThanTheIntervalNeverCertifies(t *testing.T) {
	a := healthy(standby("0/30"))
	src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7))}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a})
	require.NoError(t, err)
	const latency, budget = 10 * time.Millisecond, 150 * time.Millisecond
	g.cfg.RefreshInterval = latency / 2
	g.cfg.ResolveReplicas = func(ctx context.Context) ([]config.ReplicaMember, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(latency):
		}

		return src.resolve(ctx)
	}
	due(g)

	err = g.wait(context.Background(), 200, 0x20, time.Now().Add(budget))
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorContains(t, err, "took longer than the refresh interval")
	assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load(), "a stale set never certifies")
	assert.Equal(t, int32(1), m.timeouts.Load())
	refreshes := src.calls.Load() - 1
	assert.GreaterOrEqual(t, refreshes, int32(1), "retried")
	assert.LessOrEqual(t, refreshes, int32(budget/latency)+1, "one slow call per attempt, no spin")
}

func TestReplicaDiscoverySetThatAgedDuringTheWaitIsRefreshedBeforeCertifying(t *testing.T) {
	const interval = 40 * time.Millisecond

	lagging := func() *dialer {
		rows := make([]replicaRow, 7, 8)
		for i := range rows {
			rows[i] = standby("0/10")
		}

		return healthy(append(rows, standby("0/30"))...)
	}

	t.Run("an unchanged set certifies after one extra resolve", func(t *testing.T) {
		a := lagging()
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7))}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a})
		require.NoError(t, err)
		g.cfg.RefreshInterval = interval

		require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(2*time.Second)))
		assert.Equal(t, int32(2), src.calls.Load(), "start and exactly one refresh, no loop")
		assert.Equal(t, int32(1), a.dials.Load(), "a kept member keeps its connection")
		assert.Equal(t, int32(8), a.conns[0].calls.Load(), "certified once, not polled again")
		assert.Equal(t, int32(0), m.timeouts.Load())
	})

	t.Run("a replica the source added meanwhile holds the wait until it replayed", func(t *testing.T) {
		a, behind := lagging(), healthy(standby("0/10"), standby("0/10"), standby("0/30"))
		src := &fakeSource{answers: []sourceAnswer{
			answer(member("a:5432", 7)),
			answer(member("a:5432", 7), member("b:5432", 7)),
		}}
		g, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a, "b:5432": behind})
		require.NoError(t, err)
		g.cfg.RefreshInterval = interval

		require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(2*time.Second)))
		assert.GreaterOrEqual(t, src.calls.Load(), int32(2), "refreshed before certifying")
		assert.Equal(t, []string{"a:5432", "b:5432"}, names(g))
		assert.Equal(t, int32(1), behind.dials.Load(), "the added replica is dialled and checked")
		assert.Equal(t, int32(3), behind.conns[0].calls.Load(), "and polled until it replayed the commit")
		assert.Equal(t, int32(8), a.conns[0].calls.Load(), "the certified member is not polled again")
	})
}

func TestReplicaDiscoveryEmptySetGatesOnPrimaryOnlyWithWarn(t *testing.T) {
	const warning = "replica guard resolved no replicas, gating on the primary only"
	t.Run("at start", func(t *testing.T) {
		logs.reset()
		src := &fakeSource{answers: []sourceAnswer{answer()}}
		g, m, err := testDiscoveryGuard(t, src, nil)
		require.NoError(t, err)
		rec, ok := logs.find(warning)
		require.True(t, ok)
		assert.Equal(t, slog.LevelWarn, rec.level)

		require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second)))
		assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load())
	})
	t.Run("after a refresh", func(t *testing.T) {
		a := healthy(standby("0/10"))
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), answer()}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a})
		require.NoError(t, err)
		logs.reset()

		due(g)
		require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second)), "the lagging replica left the set")
		rec, ok := logs.find(warning)
		require.True(t, ok)
		assert.Equal(t, slog.LevelWarn, rec.level)
		assert.True(t, a.conns[0].closed)
		assert.Equal(t, []string{"a:5432"}, m.deleted)
	})
}

func TestReplicaDiscoveryTimelineRule(t *testing.T) {
	t.Run("known and different blocks until the deadline", func(t *testing.T) {
		a := healthy(standby("0/30"))
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 6))}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a})
		require.NoError(t, err, "R19 never fails start")

		err = g.wait(context.Background(), 200, 0x20, time.Now().Add(50*time.Millisecond))
		require.ErrorIs(t, err, ErrVisibilityTimeout)
		require.ErrorIs(t, err, errTimelineBlocked)
		assert.Equal(t, int32(2), src.calls.Load(), "the blocked pass spends the one optional re-resolve")
		assert.Greater(t, a.conns[0].calls.Load(), int32(3), "kept polling: blocked, not failed")
		assert.Equal(t, int32(0), m.cached.Load(), "the replay cache does not bypass the rule")
	})
	t.Run("unblocked by a re-resolve that reports the session's timeline", func(t *testing.T) {
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 6)), answer(member("a:5432", 7))}}
		g, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": healthy(standby("0/30"))})
		require.NoError(t, err)
		require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second)))
		assert.Equal(t, int32(2), src.calls.Load())
	})
	t.Run("unknown timeline turns the rule off", func(t *testing.T) {
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 0))}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": healthy(standby("0/30"))})
		require.NoError(t, err)
		require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second)))
		assert.Equal(t, int32(1), src.calls.Load(), "no re-resolve")
		assert.Equal(t, int32(1), m.cached.Load())
	})
}

func TestReplicaDiscoveryInvalidAnswerIsRejected(t *testing.T) {
	for name, members := range map[string][]config.ReplicaMember{
		"not host:port":     {member("standby1", 7)},
		"empty address":     {member("", 7)},
		"listed twice":      {member("standby1:5432", 7), member("standby1:5432", 7)},
		"negative timeline": {member("standby1:5432", -1)},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateMembers(members))
			d := healthy(standby("0/30"))
			src := &fakeSource{answers: []sourceAnswer{answer(members...)}}
			_, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"standby1:5432": d})
			require.ErrorContains(t, err, "resolve replicas: invalid answer")
			assert.Equal(t, int32(0), d.dials.Load(), "a bad entry fails the answer, nothing is dialled")
		})
	}

	require.NoError(t, validateMembers([]config.ReplicaMember{member("standby1:5432", 7), member("[::1]:5433", 0)}))

	t.Run("on refresh it is returned at once, not retried into a timeout", func(t *testing.T) {
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), answer(member("a:5432", 7), member("a:5432", 7))}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": healthy(standby("0/30"))})
		require.NoError(t, err)
		due(g)
		err = g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second))
		require.ErrorIs(t, err, errInvalidAnswer)
		require.NotErrorIs(t, err, ErrVisibilityTimeout)
		require.ErrorContains(t, err, "listed twice")
		assert.Equal(t, int32(2), src.calls.Load(), "start and one refresh, no retry")
		assert.Equal(t, int32(0), m.timeouts.Load())
	})
}

func TestReplicaDiscoveryNilAnswerIsInvalid(t *testing.T) {
	require.Error(t, validateMembers(nil))
	require.NoError(t, validateMembers([]config.ReplicaMember{}))

	t.Run("at start", func(t *testing.T) {
		src := &fakeSource{answers: []sourceAnswer{{}}}
		_, _, err := testDiscoveryGuard(t, src, nil)
		require.ErrorIs(t, err, errInvalidAnswer)
		require.ErrorContains(t, err, "nil slice")
	})
	t.Run("on refresh the set is kept and nothing certifies", func(t *testing.T) {
		src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), {}}}
		g, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": healthy(standby("0/30"))})
		require.NoError(t, err)
		due(g)
		err = g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second))
		require.ErrorIs(t, err, errInvalidAnswer)
		assert.Equal(t, []string{"a:5432"}, names(g))
	})
}

func TestReplicaDiscoveryAnswerAfterTheResolveContextEndedIsDiscarded(t *testing.T) {
	a := healthy(standby("0/10"))
	src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7))}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": a})
	require.NoError(t, err)

	for name, late := range map[string][]config.ReplicaMember{
		"nil":       nil,
		"empty set": {},
		"other set": {member("b:5432", 7)},
	} {
		t.Run(name, func(t *testing.T) {
			g.cfg.ResolveReplicas = func(ctx context.Context) ([]config.ReplicaMember, error) {
				<-ctx.Done()
				return late, nil
			}
			due(g)
			err := g.wait(context.Background(), 200, 0x20, time.Now().Add(40*time.Millisecond))
			require.ErrorIs(t, err, ErrVisibilityTimeout, "the late answer is a failed resolve, retried to the deadline")
			assert.Equal(t, []string{"a:5432"}, names(g), "no set is installed")
			assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load(), "the stale set did not certify")
		})
	}

	t.Run("cancelled by the caller", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		g.cfg.ResolveReplicas = func(context.Context) ([]config.ReplicaMember, error) {
			cancel()
			return []config.ReplicaMember{}, nil
		}
		_, _, err := g.resolve(ctx, time.Time{})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestReplicaDiscoveryFailingReResolveNeverDropsAMember(t *testing.T) {
	boom := errors.New("resolver unreachable")
	t.Run("a server answer stays fatal", func(t *testing.T) {

		promoted := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10"), {recovery: false, timeline: 7}, standby("0/30")}}}}
		src := &fakeSource{answers: []sourceAnswer{answer(member("promoted:5432", 7)), {err: boom}}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"promoted:5432": promoted})
		require.NoError(t, err)

		err = g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second))
		require.ErrorIs(t, err, ErrNotStandby)
		require.NotErrorIs(t, err, ErrVisibilityTimeout)
		assert.Equal(t, int32(2), src.calls.Load())
		assert.Equal(t, []string{"promoted:5432"}, names(g))
		assert.Empty(t, m.deleted)
	})
	t.Run("a connection error ends in the timeout path", func(t *testing.T) {
		broken := &dialer{conns: []*scriptedReplica{
			{rows: []replicaRow{standby("0/10"), {err: io.ErrUnexpectedEOF}}},
			{rows: []replicaRow{{err: io.ErrUnexpectedEOF}}},
		}}
		src := &fakeSource{answers: []sourceAnswer{answer(member("broken:5432", 7)), {err: boom}}}
		g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"broken:5432": broken})
		require.NoError(t, err)

		err = g.wait(context.Background(), 200, 0x20, time.Now().Add(50*time.Millisecond))
		require.ErrorIs(t, err, ErrVisibilityTimeout)
		assert.Equal(t, int32(2), src.calls.Load())
		assert.Equal(t, []string{"broken:5432"}, names(g))
		assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load(), "nothing certified")
		assert.Empty(t, m.deleted)
	})
}

func TestReplicaDiscoveryInvalidReResolveAnswerNeverDropsAMember(t *testing.T) {
	for name, invalid := range map[string]sourceAnswer{
		"listed twice": answer(member("other:5432", 7), member("other:5432", 7)),
		"nil slice":    {},
	} {
		t.Run(name, func(t *testing.T) {

			promoted := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10"), {recovery: false, timeline: 7}, standby("0/30")}}}}
			src := &fakeSource{answers: []sourceAnswer{answer(member("promoted:5432", 7)), invalid}}
			g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"promoted:5432": promoted, "other:5432": healthy(standby("0/30"))})
			require.NoError(t, err)

			err = g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Second))
			require.ErrorIs(t, err, ErrNotStandby)
			require.NotErrorIs(t, err, ErrVisibilityTimeout)
			require.ErrorContains(t, err, "replica promoted:5432")
			require.ErrorIs(t, err, errInvalidAnswer)
			assert.Equal(t, int32(2), src.calls.Load(), "start and the one re-resolve")
			assert.Equal(t, []string{"promoted:5432"}, names(g), "no member dropped, none added")
			assert.Empty(t, m.deleted)
			assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load(), "nothing certified")
			assert.Equal(t, int32(0), m.timeouts.Load())
		})
	}
}

func TestReplicaDiscoveryInvalidReResolveAfterConnErrorFailsAtOnce(t *testing.T) {
	broken := &dialer{conns: []*scriptedReplica{
		{rows: []replicaRow{standby("0/10"), {err: io.ErrUnexpectedEOF}}},
		{rows: []replicaRow{{err: io.ErrUnexpectedEOF}}},
	}}
	src := &fakeSource{answers: []sourceAnswer{answer(member("broken:5432", 7)), {}}}
	g, m, err := testDiscoveryGuard(t, src, map[string]*dialer{"broken:5432": broken})
	require.NoError(t, err)

	start := time.Now()
	err = g.wait(context.Background(), 200, 0x20, start.Add(5*time.Second))
	require.ErrorIs(t, err, errInvalidAnswer)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	assert.Less(t, time.Since(start), 2*time.Second, "at once, well before the deadline")
	assert.Equal(t, int32(2), src.calls.Load(), "start and the one re-resolve")
	assert.Equal(t, []string{"broken:5432"}, names(g), "no member dropped, none added")
	assert.Empty(t, m.deleted)
	assert.Equal(t, int32(0), m.cached.Load()+m.polled.Load(), "nothing certified")
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestGateDiscoveryInvalidReResolveAfterConnErrorIsNotAFailOpenPass(t *testing.T) {
	for _, mode := range []config.VisibilityFailMode{config.VisibilityFailClosed, config.VisibilityFailOpen} {
		t.Run(string(mode), func(t *testing.T) {
			gs := newGatedStream(t, guardCfg(mode), [][]string{{"f", "100:201:"}})
			broken := &dialer{conns: []*scriptedReplica{
				{rows: []replicaRow{standby("0/10"), {err: io.ErrUnexpectedEOF}}},
				{rows: []replicaRow{{err: io.ErrUnexpectedEOF}}},
			}}
			src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), {}}}
			g, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": broken})
			require.NoError(t, err)
			g.metric, g.cfg.Timeout, g.cfg.FailMode = gs.m, gs.s.config.VisibilityGuard.Timeout, mode
			gs.s.replicas = g

			err = gs.run(txMsg(200, 2, 0x20))
			require.ErrorIs(t, err, errInvalidAnswer)
			require.NotErrorIs(t, err, ErrVisibilityTimeout)
			assert.Empty(t, gs.received, "nothing is dispatched")
			assert.Equal(t, int32(0), gs.m.timeouts.Load())
			assert.Equal(t, int32(0), gs.m.failOpens.Load())
			assert.Equal(t, int32(2), src.calls.Load(), "start and the one re-resolve")
		})
	}
}

func TestGateDiscoveryInvalidAnswerIsNotAFailOpenPass(t *testing.T) {
	for _, mode := range []config.VisibilityFailMode{config.VisibilityFailClosed, config.VisibilityFailOpen} {
		t.Run(string(mode), func(t *testing.T) {
			gs := newGatedStream(t, guardCfg(mode), [][]string{{"f", "100:201:"}})
			src := &fakeSource{answers: []sourceAnswer{answer(member("a:5432", 7)), {}}}
			g, _, err := testDiscoveryGuard(t, src, map[string]*dialer{"a:5432": healthy(standby("0/30"))})
			require.NoError(t, err)
			g.metric, g.cfg.Timeout, g.cfg.FailMode = gs.m, gs.s.config.VisibilityGuard.Timeout, mode
			gs.s.replicas = g
			due(g)

			err = gs.run(txMsg(200, 2, 2))
			require.ErrorIs(t, err, errInvalidAnswer)
			require.NotErrorIs(t, err, ErrVisibilityTimeout)
			assert.Empty(t, gs.received, "nothing is dispatched")
			assert.Equal(t, int32(0), gs.m.timeouts.Load())
			assert.Equal(t, int32(0), gs.m.failOpens.Load())
			assert.Equal(t, int32(2), src.calls.Load(), "not retried")
		})
	}
}

func TestReplicaDiscoveryResolverErrorFailsStart(t *testing.T) {
	src := &fakeSource{answers: []sourceAnswer{{err: errors.New("no leader in the answer")}}}
	_, _, err := testDiscoveryGuard(t, src, nil)
	require.ErrorContains(t, err, "resolve replicas: no leader in the answer")
}

func TestDescribeMembersGroupsBySource(t *testing.T) {
	got := describeMembers([]config.ReplicaMember{
		{Addr: "a1:5432", Source: "http://a:8008", Timeline: 7},
		{Addr: "b1:5432", Source: "http://b:8008"},
		{Addr: "a2:5432", Source: "http://a:8008", Timeline: 7},
	})
	assert.Equal(t, "http://a:8008: a1:5432 (timeline 7), a2:5432 (timeline 7); http://b:8008: b1:5432 (timeline unknown)", got)
}
