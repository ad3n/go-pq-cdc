package replication

import (
	"context"
	goerrors "errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/internal/metric"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type (
	Querier interface {
		QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	}

	WaitOptions struct {
		Timeout      time.Duration
		PollInterval time.Duration
	}

	replicaState struct {
		server   string
		replay   pq.LSN
		timeline int32
	}

	replicaGuard struct {
		metric metric.Metric

		resolvedAt time.Time

		connect  func(addr string) func(ctx context.Context) (replicaConn, error)
		systemID string
		replicas []*replica
		cfg      config.VisibilityGuardConfig
		timeline int32
	}

	memberError struct {
		err  error
		name string
	}

	replicaConn interface {
		Querier
		IsClosed() bool
		Close(ctx context.Context) error
	}

	replica struct {
		observedAt time.Time
		dial       func(ctx context.Context) (replicaConn, error)
		conn       replicaConn
		name       string
		server     string
		source     string
		replayed   pq.LSN
		timeline   int32
	}
)

var (
	ErrNotStandby = goerrors.New("server is not in recovery (primary, promoted standby, or pooler routed to one)")

	errReplicaConn = goerrors.New("replica connection")

	errClusterMismatch = goerrors.New("replica cluster mismatch")

	errTimelineBlocked = goerrors.New("source reports the replica on another timeline")

	errRoundStopped = goerrors.New("replica wait stopped for a re-resolve")

	errNoTimeLeft = goerrors.New("no time left before the gate deadline")

	errInvalidAnswer = goerrors.New("invalid answer")
)

const replicaPollSQL = "SELECT pg_is_in_recovery(), coalesce(pg_last_wal_replay_lsn()::text, ''), (pg_control_recovery()).min_recovery_end_timeline, coalesce(host(inet_server_addr()) || ':' || inet_server_port(), '')"

const replicaSystemSQL = "SELECT (pg_control_system()).system_identifier"

const replayCacheTTL = time.Second

const resolveTimeout = time.Second

func WaitReplayed(ctx context.Context, q Querier, commitLSN pq.LSN, opts WaitOptions) error {
	if commitLSN == 0 {
		return goerrors.New("commitLSN is 0 (snapshot event?)")
	}

	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}

	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Millisecond
	}

	poll := func(ctx context.Context) (replicaState, error) { return pollReplica(ctx, q) }
	return waitReplayed(ctx, poll, commitLSN, time.Now().Add(opts.Timeout), opts.PollInterval)
}

func pollReplica(ctx context.Context, q Querier) (replicaState, error) {
	var inRecovery bool
	var replay string
	var st replicaState
	if err := q.QueryRow(ctx, replicaPollSQL).Scan(&inRecovery, &replay, &st.timeline, &st.server); err != nil {
		return replicaState{}, err
	}

	if !inRecovery || replay == "" {
		return replicaState{}, ErrNotStandby
	}

	lsn, err := pq.ParseLSN(replay)
	if err != nil {
		return replicaState{}, fmt.Errorf("replay position %q: %w", replay, err)
	}

	st.replay = lsn
	return st, nil
}

func waitReplayed(ctx context.Context, poll func(context.Context) (replicaState, error), commitLSN pq.LSN, deadline time.Time, delay time.Duration) error {
	for {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		st, err := poll(pollCtx)
		cancel()
		if err == nil && st.replay > commitLSN {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if isAnswer(err) {
			return err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			if err != nil {
				return fmt.Errorf("%w: %w", ErrVisibilityTimeout, err)
			}

			return fmt.Errorf("%w: replay %s has not passed commit %s", ErrVisibilityTimeout, st.replay, commitLSN)
		}

		var sleep time.Duration
		sleep, delay = backoff(delay, remaining)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func isAnswer(err error) bool {
	return err != nil && !goerrors.Is(err, errReplicaConn) && !goerrors.Is(err, errTimelineBlocked) &&
		!goerrors.Is(err, context.DeadlineExceeded)
}

func isMemberError(err error) bool {
	return err != nil && !goerrors.Is(err, context.DeadlineExceeded) && !goerrors.Is(err, context.Canceled)
}

func (r *replica) cachedAge(commitLSN pq.LSN, now time.Time) (time.Duration, bool) {
	if r.observedAt.IsZero() || r.conn == nil || r.conn.IsClosed() || r.replayed <= commitLSN {
		return 0, false
	}

	return ageWithin(r.observedAt, now, replayCacheTTL)
}

func ageWithin(at, now time.Time, ttl time.Duration) (time.Duration, bool) {
	age := now.Sub(at)
	wall := now.Round(0).Sub(at.Round(0))
	if age < 0 || wall < 0 || age >= ttl || wall >= ttl {
		return 0, false
	}

	return age, true
}

func (r *replica) forget() { r.replayed, r.observedAt, r.server = 0, time.Time{}, "" }

func openReplicaGuard(ctx context.Context, cfg config.Config, system *pq.IdentifySystemResult, m metric.Metric) (*replicaGuard, error) {
	g := &replicaGuard{metric: m, cfg: cfg.VisibilityGuard, timeline: system.Timeline, systemID: system.SystemID,
		connect: func(addr string) func(context.Context) (replicaConn, error) {
			dsn := cfg.ReplicaDSN(addr)
			return func(ctx context.Context) (replicaConn, error) { return pgx.Connect(ctx, dsn) }
		},
	}
	if err := g.start(ctx); err != nil {
		_ = g.close(ctx)
		return nil, err
	}

	return g, nil
}

func (g *replicaGuard) start(ctx context.Context) error {
	if g.cfg.ResolveReplicas == nil {
		for _, hostPort := range g.cfg.Replicas {
			g.replicas = append(g.replicas, &replica{name: hostPort, dial: g.connect(hostPort)})
		}

		return g.open(ctx)
	}

	members, at, err := g.resolve(ctx, time.Time{})
	if err != nil {
		return fmt.Errorf("resolve replicas: %w", err)
	}

	g.apply(ctx, members, at)
	return g.open(ctx)
}

func (g *replicaGuard) open(ctx context.Context) error {
	for _, r := range g.replicas {
		if _, err := g.poll(ctx, r, 0); err != nil {
			return fmt.Errorf("replica %s: %w", r.name, err)
		}
	}

	return nil
}

func (g *replicaGuard) checkSystemID(ctx context.Context, r *replica) error {
	want, err := strconv.ParseUint(g.systemID, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: parse replication session system identifier %q: %w", errClusterMismatch, g.systemID, err)
	}

	var got int64
	if err = r.conn.QueryRow(ctx, replicaSystemSQL).Scan(&got); err != nil {
		return err
	}

	if uint64(got) != want {
		return fmt.Errorf("%w: system identifier %d is not the replication session's %d (different cluster?)", errClusterMismatch, uint64(got), want)
	}

	return nil
}

func (g *replicaGuard) wait(ctx context.Context, xid uint32, commitLSN pq.LSN, deadline time.Time) error {
	start := time.Now()
	certified := make(map[*replica]string, len(g.replicas))
	if err := g.certify(ctx, commitLSN, deadline, certified); err != nil {
		if goerrors.Is(err, ErrVisibilityTimeout) {
			g.metric.VisibilityTimeoutIncrement()
		}

		return err
	}

	passed := make([]string, 0, len(g.replicas))
	for _, r := range g.replicas {
		passed = append(passed, certified[r])
	}

	waited := time.Since(start)
	args := []any{"xid", xid, "commitLSN", commitLSN.String(), "replicas", strings.Join(passed, ", "),
		"started_at", start.UTC().Format(time.RFC3339Nano), "wait_ms", float64(waited.Microseconds()) / 1000}
	if waited >= slowVisibilityWait {
		logger.Info("replica guard slow wait completed", args...)
	} else {
		logger.Debug("replica guard wait completed", args...)
	}

	return nil
}

func (g *replicaGuard) certify(ctx context.Context, commitLSN pq.LSN, deadline time.Time, certified map[*replica]string) error {
	if g.cfg.ResolveReplicas == nil {
		_, err := g.round(ctx, commitLSN, deadline, certified, false)
		return err
	}

	ask := true
	stale := !g.fresh(time.Now())
	for {
		if stale {
			if err := g.refresh(ctx, deadline); err != nil {
				return err
			}
		}

		asked, err := g.round(ctx, commitLSN, deadline, certified, ask)
		if len(asked) > 0 {
			ask = false
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if err = g.reResolve(ctx, deadline, asked); err != nil {
				return err
			}

			_, err = g.round(ctx, commitLSN, deadline, certified, false)
		}

		if err != nil || g.fresh(time.Now()) {
			return err
		}

		logger.Debug("replica guard set aged past the refresh interval during the wait, refreshing before it certifies",
			"refresh_interval", g.refreshInterval().String())
		stale = true
	}
}

func (g *replicaGuard) round(ctx context.Context, commitLSN pq.LSN, deadline time.Time, certified map[*replica]string, ask bool) ([]memberError, error) {
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := make(chan struct{})
	var stopOnce sync.Once
	passed := make([]string, len(g.replicas))
	errs := make([]error, len(g.replicas))
	asked := make([]error, len(g.replicas))
	var wg sync.WaitGroup
	for i, r := range g.replicas {
		blocked := g.timelineBlocks(r)
		if _, ok := certified[r]; ok && !blocked {
			continue
		}

		delete(certified, r)
		if age, ok := r.cachedAge(commitLSN, start); ok && !blocked {
			passed[i] = fmt.Sprintf("%s server=%s replay=%s cached_age_ms=%.1f", r.name, r.server, r.replayed, float64(age.Microseconds())/1000)
			g.metric.VisibilityReplicaCheck(r.name, true)
			continue
		}

		wg.Go(func() {
			var last replicaState
			poll := func(ctx context.Context) (replicaState, error) {
				select {
				case <-stop:
					return replicaState{}, errRoundStopped
				default:
				}

				st, err := g.poll(ctx, r, commitLSN)
				if err == nil {
					last = st
					if st.replay > commitLSN && g.timelineBlocks(r) {
						err = fmt.Errorf("%w: timeline %d, replication session's %d", errTimelineBlocked, r.timeline, g.timeline)
					}
				}

				if ask && isMemberError(err) {
					asked[i] = err
					stopOnce.Do(func() { close(stop) })
					return st, errRoundStopped
				}

				return st, err
			}
			if err := waitReplayed(ctx, poll, commitLSN, deadline, g.cfg.PollInterval); err != nil {
				if goerrors.Is(err, errRoundStopped) {
					return
				}

				errs[i] = fmt.Errorf("replica %s: %w", r.name, err)
				if !goerrors.Is(err, ErrVisibilityTimeout) {
					cancel()
				}

				return
			}

			passed[i] = fmt.Sprintf("%s server=%s replay=%s", r.name, last.server, last.replay)
			g.metric.VisibilityReplicaCheck(r.name, false)
		})
	}

	wg.Wait()
	var held []memberError
	for i, r := range g.replicas {
		if passed[i] != "" {
			certified[r] = passed[i]
		}

		if asked[i] != nil {
			held = append(held, memberError{name: r.name, err: asked[i]})
		}
	}

	return held, worstWaitError(errs)
}

func (g *replicaGuard) reResolve(ctx context.Context, deadline time.Time, asked []memberError) error {
	members, at, err := g.resolve(ctx, deadline)
	if err != nil {
		logger.Warn("replica guard re-resolve failed, replica errors are classified as without discovery", "error", err)
	} else {
		g.apply(ctx, members, at)
	}

	listed := make(map[string]bool, len(members))
	for _, m := range members {
		listed[m.Addr] = true
	}

	var fatal error
	for _, a := range asked {
		if err == nil && !listed[a.name] {
			logger.Info("replica guard dropped a replica the source no longer lists", "replica", a.name, "error", a.err)
			continue
		}

		if isAnswer(a.err) {
			fatal = fmt.Errorf("replica %s: %w", a.name, a.err)
			break
		}
	}

	if goerrors.Is(err, errInvalidAnswer) {

		return fmt.Errorf("replica set not re-resolved: %w", goerrors.Join(err, fatal))
	}

	return fatal
}

func (g *replicaGuard) fresh(now time.Time) bool {
	_, ok := ageWithin(g.resolvedAt, now, g.refreshInterval())
	return ok
}

func (g *replicaGuard) refresh(ctx context.Context, deadline time.Time) error {
	delay := g.cfg.PollInterval
	var cause error
	for attempt := 0; ; attempt++ {
		members, at, err := g.resolve(ctx, deadline)
		if err == nil {
			g.apply(ctx, members, at)
			if g.fresh(time.Now()) {
				return nil
			}

			err = fmt.Errorf("the answer took longer than the refresh interval %s", g.refreshInterval())
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if goerrors.Is(err, errInvalidAnswer) {
			return fmt.Errorf("replica set not refreshed: %w", err)
		}

		if attempt == 0 {
			logger.Warn("replica guard refresh failed, retrying until the gate deadline", "error", err)
		}

		remaining := time.Until(deadline)

		if cause == nil || !goerrors.Is(err, errNoTimeLeft) && (remaining > 0 || !goerrors.Is(err, context.DeadlineExceeded)) {
			cause = err
		}

		if remaining <= 0 {
			return fmt.Errorf("%w: replica set older than %s and not refreshed: %w", ErrVisibilityTimeout, g.refreshInterval(), cause)
		}

		var sleep time.Duration
		sleep, delay = backoff(delay, remaining)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func (g *replicaGuard) refreshInterval() time.Duration {
	if g.cfg.RefreshInterval > 0 {
		return g.cfg.RefreshInterval
	}

	return config.DefaultReplicaRefreshInterval
}

func (g *replicaGuard) resolve(ctx context.Context, deadline time.Time) ([]config.ReplicaMember, time.Time, error) {
	at := time.Now()
	bound := at.Add(resolveTimeout)
	if !deadline.IsZero() && deadline.Before(bound) {
		if !deadline.After(at) {
			return nil, at, errNoTimeLeft
		}

		bound = deadline
	}

	rctx, cancel := context.WithDeadline(ctx, bound)
	defer cancel()

	members, err := g.cfg.ResolveReplicas(rctx)
	if err != nil {
		return nil, at, err
	}

	if err = rctx.Err(); err != nil {
		return nil, at, fmt.Errorf("answer discarded, resolve ended first: %w", err)
	}

	if err = validateMembers(members); err != nil {
		return nil, at, fmt.Errorf("%w: %w", errInvalidAnswer, err)
	}

	return members, at, nil
}

func validateMembers(members []config.ReplicaMember) error {

	if members == nil {
		return goerrors.New("nil slice: an empty answer must be a non-nil empty slice")
	}

	var err error
	seen := make(map[string]bool, len(members))
	for _, m := range members {
		if _, _, e := net.SplitHostPort(m.Addr); e != nil {
			err = goerrors.Join(err, fmt.Errorf("%q must be host:port", m.Addr))
		}

		if seen[m.Addr] {
			err = goerrors.Join(err, fmt.Errorf("%q listed twice", m.Addr))
		}

		seen[m.Addr] = true
		if m.Timeline < 0 {
			err = goerrors.Join(err, fmt.Errorf("%q: negative timeline %d", m.Addr, m.Timeline))
		}
	}

	return err
}

func (g *replicaGuard) apply(ctx context.Context, members []config.ReplicaMember, at time.Time) {
	left := make(map[string]*replica, len(g.replicas))
	for _, r := range g.replicas {
		left[r.name] = r
	}

	changed := g.resolvedAt.IsZero()
	next := make([]*replica, 0, len(members))
	var added, removed []string
	for _, m := range members {
		r, ok := left[m.Addr]
		if ok {
			delete(left, m.Addr)
			changed = changed || r.timeline != m.Timeline || r.source != m.Source
		} else {
			r = &replica{name: m.Addr, dial: g.connect(m.Addr)}
			added = append(added, m.Addr)
		}

		r.timeline, r.source = m.Timeline, m.Source
		next = append(next, r)
	}

	for _, r := range g.replicas {
		if left[r.name] == nil {
			continue
		}

		if r.conn != nil && !r.conn.IsClosed() {
			_ = r.conn.Close(ctx)
		}

		g.metric.DeleteVisibilityReplica(r.name)
		removed = append(removed, r.name)
	}

	g.replicas, g.resolvedAt = next, at
	if !changed && len(added) == 0 && len(removed) == 0 {
		return
	}

	args := []any{"replicas", describeMembers(members), "added", added, "removed", removed}
	if len(members) == 0 {
		logger.Warn("replica guard resolved no replicas, gating on the primary only", args...)
		return
	}

	logger.Info("replica guard replica set changed", args...)
}

func describeMembers(members []config.ReplicaMember) string {
	var sources []string
	bySource := make(map[string][]string, len(members))
	for _, m := range members {
		if _, ok := bySource[m.Source]; !ok {
			sources = append(sources, m.Source)
		}

		timeline := "unknown"
		if m.Timeline != 0 {
			timeline = strconv.Itoa(int(m.Timeline))
		}

		bySource[m.Source] = append(bySource[m.Source], fmt.Sprintf("%s (timeline %s)", m.Addr, timeline))
	}

	groups := make([]string, 0, len(sources))
	for _, s := range sources {
		group := strings.Join(bySource[s], ", ")
		if s != "" {
			group = s + ": " + group
		}

		groups = append(groups, group)
	}

	return strings.Join(groups, "; ")
}

func (g *replicaGuard) timelineBlocks(r *replica) bool {
	return r.timeline != 0 && r.timeline != g.timeline
}

func worstWaitError(errs []error) error {
	rank := func(err error) int {
		switch {
		case goerrors.Is(err, ErrVisibilityTimeout):
			return 1
		case goerrors.Is(err, context.Canceled):
			return 0
		}

		return 2
	}
	var worst error
	for _, err := range errs {
		if err != nil && (worst == nil || rank(err) > rank(worst)) {
			worst = err
		}
	}

	return worst
}

func (g *replicaGuard) poll(ctx context.Context, r *replica, commitLSN pq.LSN) (replicaState, error) {
	if r.conn == nil || r.conn.IsClosed() {
		r.forget()
		conn, err := r.dial(ctx)
		if err != nil {
			return replicaState{}, fmt.Errorf("%w: dial: %w", errReplicaConn, err)
		}

		r.conn = conn

		if err = g.checkSystemID(ctx, r); err != nil {
			return replicaState{}, g.answerOrDrop(ctx, r, err)
		}
	}

	start := time.Now()
	st, err := pollReplica(ctx, r.conn)
	if err != nil {
		return replicaState{}, g.answerOrDrop(ctx, r, err)
	}

	if st.timeline > g.timeline {
		return replicaState{}, fmt.Errorf("timeline %d is ahead of the replication session's %d (followed a newer primary?)", st.timeline, g.timeline)
	}

	var lag pq.LSN
	if commitLSN > st.replay {
		lag = commitLSN - st.replay
	}

	g.metric.SetVisibilityReplicaLag(r.name, float64(lag))
	r.replayed, r.observedAt, r.server = st.replay, start, st.server
	return st, nil
}

func (g *replicaGuard) answerOrDrop(ctx context.Context, r *replica, err error) error {
	var pgErr *pgconn.PgError
	if goerrors.As(err, &pgErr) {
		if pgErr.Code != postgresAdminShutdown && pgErr.Code != postgresCrashShutdown && !strings.HasPrefix(pgErr.Code, "08") {
			return err
		}
	} else if goerrors.Is(err, ErrNotStandby) || goerrors.Is(err, errClusterMismatch) {
		return err
	}

	_ = r.conn.Close(ctx)
	r.conn = nil
	r.forget()
	return fmt.Errorf("%w: %w", errReplicaConn, err)
}

func (g *replicaGuard) close(ctx context.Context) error {
	var errs []error
	for _, r := range g.replicas {
		if r.conn != nil && !r.conn.IsClosed() {
			errs = append(errs, r.conn.Close(ctx))
		}
	}

	return goerrors.Join(errs...)
}
