package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/internal/metric"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/ad3n/go-pq-cdc/pq/message"
	"github.com/ad3n/go-pq-cdc/pq/message/format"
	"github.com/avast/retry-go/v4"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrorSlotInUse    = errors.New("replication slot in use")
	ErrorNotConnected = errors.New("stream is not connected")
)

const (
	StandbyStatusUpdateByteID = 'r'
	streamShutdownTimeout     = 30 * time.Second

	sinkStallWarnInterval = 30 * time.Second
	messageSinkStallMsg   = "postgres message sink is blocked on a full message channel, the consumer is not draining it; the listener or its downstream is slow or stuck"
	messageSinkResumedMsg = "postgres message sink resumed"

	standbyStatusWriteTimeout = 5 * time.Second
	postgresAdminShutdown     = "57P01"
	postgresCrashShutdown     = "57P02"
)

type (
	ListenerContext struct {
		Context context.Context
		Message any
		Ack     func() error

		Xid uint32

		CommitLSN pq.LSN
	}

	ListenerFunc func(ctx *ListenerContext)

	Message struct {
		message  any
		walStart int64

		xid uint32

		commitLSN pq.LSN
	}

	Streamer interface {
		Connect(ctx context.Context) error
		Open(ctx context.Context) error
		Close(ctx context.Context) error
		GetSystemInfo() *pq.IdentifySystemResult
		GetMetric() metric.Metric
		OpenFromSnapshotLSN()
	}

	stream struct {
		metric              metric.Metric
		conn                pq.Connection
		guard               *visibilityGuard
		replicas            *replicaGuard
		cancel              context.CancelFunc
		system              *pq.IdentifySystemResult
		relation            map[uint32]*format.Relation
		messageCH           chan *Message
		listenerFunc        ListenerFunc
		sinkEnd             chan struct{}
		processEnd          chan struct{}
		statusUpdateEnd     chan struct{}
		mu                  *sync.RWMutex
		config              config.Config
		lastXLogPos         pq.LSN
		confirmedXLogPos    pq.LSN
		acked               atomic.Uint64
		messageCloseOnce    sync.Once
		connMu              sync.Mutex
		closed              atomic.Bool
		sinkStarted         atomic.Bool
		processStarted      atomic.Bool
		statusUpdateStarted atomic.Bool
		statusUpdateFailed  atomic.Bool
		openFromSnapshotLSN bool
	}

	messageBuffer struct {
		ctx     context.Context
		pending *Message
		outCh   chan<- *Message

		progress  func() (acked uint64, confirmedLSN pq.LSN)
		sent      uint64
		commitLSN pq.LSN

		stallWarnInterval time.Duration
		xid               uint32
		inTx              bool
	}

	streamTxBuffer struct {
		txns map[uint32][]*Message

		subXacts  map[uint32][]subXact
		activeXid uint32
		streaming bool
	}

	subXact struct {
		xid   uint32
		index int
	}

	writeDeadliner interface {
		SetWriteDeadline(t time.Time) error
	}
)

func (m *Message) transactionScoped() bool {
	logical, ok := m.message.(*format.LogicalDecodingMessage)
	return !ok || logical.Flags&1 != 0
}

func NewStream(dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	s := &stream{
		conn:         pq.NewConnectionTemplate(dsn),
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan *Message, metric.MessageBufferCapacity),
		listenerFunc: listenerFunc,

		lastXLogPos: 0,
		sinkEnd:     make(chan struct{}, 1),
		processEnd:  make(chan struct{}, 1),

		statusUpdateEnd: make(chan struct{}),
		mu:              &sync.RWMutex{},
	}

	if m != nil {
		m.SetMessageBufferSource(func() int { return len(s.messageCH) })
	}

	return s
}

func (s *stream) Connect(ctx context.Context) error {

	s.connMu.Lock()
	defer s.connMu.Unlock()

	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "stream connection")
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return errors.Wrap(err, "identify system")
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {

	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}

		return errors.Wrap(err, "replication setup")
	}

	if s.config.VisibilityGuard.Enabled {
		guard, err := openVisibilityGuard(ctx, s.config.DSN(), s.config.VisibilityGuard, s.system.Timeline, s.metric)
		if err != nil {
			return errors.Wrap(err, "visibility guard")
		}

		s.guard = guard
		logger.Info("visibility guard enabled",
			"failMode", s.config.VisibilityGuard.FailMode,
			"timeout", s.config.VisibilityGuard.Timeout,
			"pollInterval", s.config.VisibilityGuard.PollInterval,
		)

		if len(s.config.VisibilityGuard.Replicas) > 0 || s.config.VisibilityGuard.ResolveReplicas != nil {
			replicas, err := openReplicaGuard(ctx, s.config, s.system, s.metric)
			if err != nil {
				_ = s.guard.close(ctx)
				s.guard = nil
				return errors.Wrap(err, "replica guard")
			}

			s.replicas = replicas
			if s.config.VisibilityGuard.ResolveReplicas != nil {
				logger.Info("replica guard enabled", "discovery", true, "refreshInterval", replicas.refreshInterval())
			} else {
				logger.Info("replica guard enabled", "replicas", s.config.VisibilityGuard.Replicas)
			}
		}
	}

	s.sinkStarted.Store(true)
	s.processStarted.Store(true)
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	go s.sink(runCtx)
	go s.process(runCtx)

	if s.config.StatusUpdateEvery() > 0 {
		s.startStatusUpdateLoop(runCtx)
	}

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := s.lastXLogPos
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "fetch snapshot LSN")
		}

		replicationStartLsn = snapshotLSN
	}

	if err := replication.StartWithOptions(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn, StartOptions{
		ProtoVersion:    s.config.Slot.ProtoVersion,
		LogicalMessages: s.config.Slot.LogicalMessages,
	}); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name, "lsn", replicationStartLsn.String())
	} else {
		logger.Info("replication started from confirmed_flush_lsn", "slot", s.config.Slot.Name)
	}

	return nil
}

func (b *messageBuffer) send(msg *Message) bool {
	select {
	case b.outCh <- msg:
		b.sent++
		return true
	default:
	}

	return b.sendBlocked(msg)
}

func (b *messageBuffer) sendBlocked(msg *Message) bool {
	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	interval := b.stallWarnInterval
	if interval <= 0 {
		interval = sinkStallWarnInterval
	}

	start := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	warned := false
	for {
		select {
		case b.outCh <- msg:
			b.sent++
			if warned {
				logger.Info(messageSinkResumedMsg, "blocked", time.Since(start))
			}

			return true
		case <-ctx.Done():
			return false
		case <-ticker.C:
			warned = true
			b.warnStalled(time.Since(start))
		}
	}
}

func (b *messageBuffer) warnStalled(blocked time.Duration) {
	var acked uint64
	var confirmedLSN pq.LSN
	if b.progress != nil {
		acked, confirmedLSN = b.progress()
	}

	logger.Warn(messageSinkStallMsg,
		"blocked", blocked,
		"buffered", len(b.outCh),
		"capacity", cap(b.outCh),
		"sent", b.sent,
		"acked", acked,
		"confirmedLSN", confirmedLSN.String())
}

func (b *messageBuffer) flush() {
	if b.pending != nil {
		if b.send(b.pending) {
			b.pending = nil
		}
	}
}

func (b *messageBuffer) flushWithLSN(lsn pq.LSN) {
	if b.pending != nil {
		if !b.send(&Message{
			message:   b.pending.message,
			walStart:  int64(lsn),
			xid:       b.pending.xid,
			commitLSN: b.pending.commitLSN,
		}) {
			return
		}

		b.pending = nil
	}
}

func (b *messageBuffer) discard() {
	b.pending = nil
}

func (b *messageBuffer) buffer(msg *Message) {
	b.flush()
	b.pending = msg
}

func (s *streamTxBuffer) startTx(xid uint32) {
	if s.txns == nil {
		s.txns = make(map[uint32][]*Message)
		s.subXacts = make(map[uint32][]subXact)
	}

	if _, ok := s.txns[xid]; !ok {
		s.txns[xid] = nil
	}

	s.activeXid = xid
	s.streaming = true
}

func (s *streamTxBuffer) append(msg *Message, xid uint32) {
	if msg == nil {
		return
	}

	if xid != s.activeXid && !s.hasSubXact(xid) {
		s.subXacts[s.activeXid] = append(s.subXacts[s.activeXid], subXact{xid: xid, index: len(s.txns[s.activeXid])})
	}

	s.txns[s.activeXid] = append(s.txns[s.activeXid], msg)
}

func (s *streamTxBuffer) hasSubXact(xid uint32) bool {
	subs := s.subXacts[s.activeXid]

	for i := len(subs) - 1; i >= 0; i-- {
		if subs[i].xid == xid {
			return true
		}
	}

	return false
}

func (s *streamTxBuffer) stopTx() {
	s.streaming = false
}

func (s *streamTxBuffer) flushTx(xid uint32, send func(*Message) bool, commitLSN, endLSN pq.LSN) {
	s.streaming = false
	msgs := s.txns[xid]
	n := len(msgs)
	for i, msg := range msgs {
		msg.commitLSN = commitLSN
		out := msg
		if i == n-1 {
			out = &Message{
				message:   msg.message,
				walStart:  int64(endLSN),
				xid:       msg.xid,
				commitLSN: commitLSN,
			}
		}

		if !send(out) {
			return
		}
	}

	delete(s.txns, xid)
	delete(s.subXacts, xid)
}

func (s *streamTxBuffer) abortTx(xid, subXid uint32) {
	s.streaming = false
	if subXid == xid {
		delete(s.txns, xid)
		delete(s.subXacts, xid)
		return
	}

	subs := s.subXacts[xid]
	for i := len(subs) - 1; i >= 0; i-- {
		if subs[i].xid == subXid {
			s.txns[xid] = s.txns[xid][:subs[i].index]
			s.subXacts[xid] = subs[:i]
			return
		}
	}

}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	buf := &messageBuffer{outCh: s.messageCH, progress: s.messageBufferProgress}
	buf.ctx = ctx
	streamBuf := &streamTxBuffer{}
	corrupted := s.sinkLoop(ctx, buf, streamBuf)
	s.messageCloseOnce.Do(func() {
		close(s.messageCH)
	})

	s.sinkEnd <- struct{}{}
	if !s.closed.Load() {

		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), streamShutdownTimeout)
		defer cancel()

		if err := s.Close(closeCtx); err != nil {
			logger.Error("replication stream shutdown failed", "error", err)
		}

		if corrupted {
			panic("corrupted connection")
		}
	}
}

func (s *stream) messageBufferProgress() (uint64, pq.LSN) {
	return s.acked.Load(), s.LoadConfirmedXLogPos()
}

func (s *stream) sinkLoop(ctx context.Context, buf *messageBuffer, streamBuf *streamTxBuffer) (corrupted bool) {
	for {

		if s.statusUpdateFailed.Load() {
			return true
		}

		msgCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), time.Now().Add(300*time.Millisecond))

		s.connMu.Lock()
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		s.connMu.Unlock()
		cancel()

		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				return false
			}

			if handleReplicationConnectionTermination(err) {

				return true
			}

			if pgconn.Timeout(err) {
				if s.LoadXLogPos() > 0 {
					if err = s.sendRunStandbyStatusUpdate(ctx); err != nil {
						if !handleReplicationConnectionTermination(err) {
							logger.Error("send stand by status update", "error", err)
						}

						return true
					}

					logger.Debug("send stand by status update")
				}

				continue
			}

			logger.Error("receive message error", "error", err)
			return true
		}

		copyData, ok := s.extractCopyData(rawMsg)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			if err := s.handleKeepalive(ctx, copyData.Data[1:], buf, streamBuf); err != nil {
				handleReplicationConnectionTermination(err)
				return true
			}
		case message.XLogDataByteID:
			s.handleXLogData(copyData.Data[1:], buf, streamBuf)
		}
	}
}

func (s *stream) extractCopyData(rawMsg pgproto3.BackendMessage) (*pgproto3.CopyData, bool) {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		res, _ := errMsg.MarshalJSON()
		logger.Error("receive postgres wal error: " + string(res))
		return nil, false
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		logger.Warn(fmt.Sprintf("received unexpected message: %T", rawMsg))
		return nil, false
	}

	return msg, true
}

func (s *stream) handleKeepalive(ctx context.Context, data []byte, buf *messageBuffer, streamBuf *streamTxBuffer) error {
	pkm, err := format.NewPrimaryKeepaliveMessage(data)
	if err != nil {
		logger.Error("decode primary keepalive message", "error", err)
		return nil
	}

	if pkm.ServerWALEnd > 0 {
		s.UpdateXLogPos(pkm.ServerWALEnd)
		logger.Debug("updated xlog position from keepalive", "serverWALEnd", pkm.ServerWALEnd.String())

		if ctx.Err() == nil && !buf.inTx && buf.pending == nil &&
			len(streamBuf.txns) == 0 && buf.sent == s.acked.Load() {
			s.UpdateConfirmedXLogPos(pkm.ServerWALEnd)
		}
	}

	if pkm.ReplyRequested {
		if err = s.sendRunStandbyStatusUpdate(ctx); err != nil {
			logger.Error("standby status update", "error", err)
			return err
		}

		logger.Debug("standby status update sent on keepalive request")
	}

	return nil
}

func (s *stream) handleXLogData(data []byte, buf *messageBuffer, streamBuf *streamTxBuffer) {
	xld, err := ParseXLogData(data)
	if err != nil {
		logger.Error("parse xLog data", "error", err)
		return
	}

	logger.Debug("wal received",
		"walDataLen", len(xld.WALData),
		"walStart", xld.WALStart,
		"walEnd", xld.ServerWALEnd,
		"serverTime", xld.ServerTime,
	)

	s.UpdateXLogPos(xld.ServerWALEnd)
	s.metric.SetCDCLatency(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds())

	decodedMsg, err := message.New(xld.WALData, streamBuf.streaming, xld.ServerTime, s.relation)
	if err != nil {
		logger.Warn("wal data message parsing error, message dropped", "error", err, "walStart", xld.WALStart)
		return
	}

	if decodedMsg == nil {
		return
	}

	switch m := decodedMsg.(type) {
	case *format.Insert:
		m.LSN = xld.WALStart
	case *format.Update:
		m.LSN = xld.WALStart
	case *format.Delete:
		m.LSN = xld.WALStart
	}

	s.dispatchMessage(decodedMsg, xld, buf, streamBuf)
}

func (s *stream) dispatchMessage(decodedMsg any, xld XLogData, buf *messageBuffer, streamBuf *streamTxBuffer) {

	if m, ok := decodedMsg.(*format.LogicalDecodingMessage); ok && m.Flags&1 == 0 && !streamBuf.streaming {

		walStart := int64(0)
		if len(streamBuf.txns) == 0 {
			walStart = int64(xld.WALStart)
		}

		buf.send(&Message{message: m, walStart: walStart})
		return
	}

	switch msg := decodedMsg.(type) {
	case *format.Begin:
		buf.discard()
		buf.xid = msg.Xid
		buf.commitLSN = msg.FinalLSN
		buf.inTx = true

	case *format.Commit:
		buf.flushWithLSN(msg.TransactionEndLSN)
		buf.inTx = false

	case *format.StreamStart:

		streamBuf.startTx(msg.Xid)

	case *format.StreamStop:

		streamBuf.stopTx()

	case *format.StreamCommit:

		streamBuf.flushTx(msg.Xid, buf.send, msg.CommitLSN, msg.TransactionEndLSN)

	case *format.StreamAbort:

		streamBuf.abortTx(msg.Xid, msg.SubXid)

	default:

		m := &Message{
			message:  decodedMsg,
			walStart: int64(xld.WALStart),
		}
		if streamBuf.streaming {
			m.xid = streamBuf.activeXid
			streamBuf.append(m, messageXid(decodedMsg))
		} else {
			m.xid = buf.xid
			m.commitLSN = buf.commitLSN
			buf.buffer(m)
		}
	}
}

func messageXid(msg any) uint32 {
	switch m := msg.(type) {
	case *format.Insert:
		return m.XID
	case *format.Update:
		return m.XID
	case *format.Delete:
		return m.XID
	case *format.Truncate:
		return m.XID
	case *format.Relation:
		return m.XID
	case *format.LogicalDecodingMessage:
		return m.XID
	}

	return 0
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")
	err := s.processLoop(ctx)
	s.processEnd <- struct{}{}
	if err == nil || s.closed.Load() {
		return
	}

	logger.Error("visibility guard failed, restarting stream", "error", err)
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), streamShutdownTimeout)
	defer cancel()

	if cErr := s.Close(closeCtx); cErr != nil {
		logger.Error("replication stream shutdown failed", "error", cErr)
	}

	panic(err)
}

func (s *stream) processLoop(ctx context.Context) error {

	var lastGated struct {
		commitLSN pq.LSN
		xid       uint32
		gated     bool
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("postgres message process stopped")
			return nil
		case msg, ok := <-s.messageCH:
			if !ok {
				logger.Info("postgres message process stopped")
				return nil
			}

			if msg == nil {
				continue
			}

			var done atomic.Bool
			ackFunc := func() error {
				s.UpdateConfirmedXLogPos(pq.LSN(msg.walStart))

				if done.CompareAndSwap(false, true) {
					s.acked.Add(1)
				}

				return nil
			}

			if s.isHeartbeatMessage(msg.message) {
				if err := ackFunc(); err != nil {
					logger.Error("heartbeat auto-ack failed", "error", err)
				}

				continue
			}

			if s.guard != nil && msg.transactionScoped() && (!lastGated.gated || msg.xid != lastGated.xid || msg.commitLSN != lastGated.commitLSN) {
				if err := s.gate(ctx, msg.xid, msg.commitLSN); err != nil {
					if ctx.Err() != nil {

						logger.Info("postgres message process stopped")
						return nil
					}

					return err
				}

				lastGated.xid, lastGated.commitLSN, lastGated.gated = msg.xid, msg.commitLSN, true
			}

			lCtx := &ListenerContext{
				Context:   ctx,
				Message:   msg.message,
				Ack:       ackFunc,
				Xid:       msg.xid,
				CommitLSN: msg.commitLSN,
			}

			switch lCtx.Message.(type) {
			case *format.Insert:
				s.metric.InsertOpIncrement(1)
			case *format.Delete:
				s.metric.DeleteOpIncrement(1)
			case *format.Update:
				s.metric.UpdateOpIncrement(1)
			case *format.LogicalDecodingMessage:
				s.metric.LogicalMessageIncrement(1)
			}

			start := time.Now().UTC()
			s.listenerFunc(lCtx)
			s.metric.SetProcessLatency(time.Since(start).Nanoseconds())
		}
	}
}

func (s *stream) gate(ctx context.Context, xid uint32, commitLSN pq.LSN) error {
	if xid == 0 || commitLSN == 0 {

		return fmt.Errorf("%w: message without a decoded BEGIN (xid %d, commitLSN %s)", ErrVisibilityGuard, xid, commitLSN)
	}

	start := time.Now()
	err := s.guard.wait(ctx, xid)
	if err == nil && s.replicas != nil {
		err = s.replicas.wait(ctx, xid, commitLSN, start.Add(s.config.VisibilityGuard.Timeout))
	}

	s.metric.ObserveVisibilityWait(time.Since(start))
	switch {
	case err == nil:
		return nil
	case goerrors.Is(err, ErrVisibilityTimeout) && s.config.VisibilityGuard.FailMode == config.VisibilityFailOpen:
		logger.Warn("visibility guard timeout, dispatching anyway (failMode: open)", "xid", xid, "error", err)
		s.metric.VisibilityFailOpenIncrement()
		return nil
	default:
		return fmt.Errorf("%w: %w", ErrVisibilityGuard, err)
	}
}

func (s *stream) isHeartbeatMessage(msg any) bool {
	if !s.config.IsHeartbeatEnabled() {
		return false
	}

	hbSchema := s.config.Heartbeat.Table.Schema
	hbTable := s.config.Heartbeat.Table.Name

	switch m := msg.(type) {
	case *format.Insert:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Update:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Delete:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	}

	return false
}

func (s *stream) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, streamShutdownTimeout)
		defer cancel()
	}

	var errs []error

	if s.cancel != nil {
		s.cancel()
	}

	s.connMu.Lock()
	if !s.conn.IsClosed() {
		s.flushFinalStandbyStatusUpdateLocked(ctx)
		if err := s.conn.Close(ctx); err != nil {
			errs = append(errs, err)
		}

		logger.Info("postgres connection closed")
	}

	s.connMu.Unlock()

	if s.sinkStarted.Load() {
		select {
		case <-s.sinkEnd:
			logger.Info("postgres message sink stopped")
		case <-ctx.Done():
			logger.Warn("timed out waiting for postgres message sink", "error", ctx.Err())
			errs = append(errs, errors.Wrap(ctx.Err(), "wait for postgres message sink"))
		}
	}

	if s.processStarted.Load() {
		select {
		case <-s.processEnd:
			logger.Info("postgres message process stopped")
		case <-ctx.Done():
			logger.Warn("timed out waiting for postgres message process", "error", ctx.Err())
			errs = append(errs, errors.Wrap(ctx.Err(), "wait for postgres message process"))
		}
	}

	if s.statusUpdateStarted.Load() {
		select {
		case <-s.statusUpdateEnd:
			logger.Info("postgres standby status update loop stopped")
		case <-ctx.Done():
			logger.Warn("timed out waiting for standby status update loop", "error", ctx.Err())
			errs = append(errs, errors.Wrap(ctx.Err(), "wait for standby status update loop"))
		}
	}

	if s.guard != nil {
		if err := s.guard.close(ctx); err != nil {
			errs = append(errs, errors.Wrap(err, "close visibility guard"))
		}

		logger.Info("visibility guard connection closed")
	}

	if s.replicas != nil {
		if err := s.replicas.close(ctx); err != nil {
			errs = append(errs, errors.Wrap(err, "close replica guard"))
		}

		logger.Info("replica guard connections closed")
	}

	return goerrors.Join(errs...)
}

func isReplicationConnectionTerminationError(err error) bool {
	if goerrors.Is(err, io.EOF) ||
		goerrors.Is(err, io.ErrUnexpectedEOF) ||
		goerrors.Is(err, net.ErrClosed) ||
		goerrors.Is(err, syscall.ECONNRESET) ||
		goerrors.Is(err, syscall.EPIPE) {
		return true
	}

	return false
}

func isPostgresShutdownError(err error) bool {
	var pgErr *pgconn.PgError
	if goerrors.As(err, &pgErr) {
		switch pgErr.Code {
		case postgresAdminShutdown, postgresCrashShutdown:
			return true
		default:
			return false
		}
	}

	return false
}

func handleReplicationConnectionTermination(err error) bool {
	if isPostgresShutdownError(err) {
		logger.Info("postgres replication connection closed", "error", err)
		return true
	}

	if isReplicationConnectionTerminationError(err) {
		if goerrors.Is(err, io.ErrUnexpectedEOF) {
			logger.Error("postgres replication connection terminated unexpectedly", "error", err)
		} else {
			logger.Info("postgres replication connection closed", "error", err)
		}

		return true
	}

	return false
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
}

func (s *stream) UpdateXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastXLogPos < l {
		s.lastXLogPos = l
	}
}

func (s *stream) LoadXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastXLogPos
}

func (s *stream) UpdateConfirmedXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.confirmedXLogPos < l {
		s.confirmedXLogPos = l
	}
}

func (s *stream) LoadConfirmedXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.confirmedXLogPos
}

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

func (s *stream) fetchSnapshotLSN(ctx context.Context) (pq.LSN, error) {
	logger.Info("fetching snapshot LSN from database", "slotName", s.config.Slot.Name)

	var snapshotLSN pq.LSN

	err := retry.Do(
		func() error {

			conn, err := pq.NewConnection(ctx, s.config.DSN())
			if err != nil {
				return errors.Wrap(err, "create connection for snapshot LSN query")
			}
			defer conn.Close(ctx)

			query := fmt.Sprintf(`
				SELECT snapshot_lsn, completed
				FROM cdc_snapshot_job
				WHERE slot_name = '%s'
			`, s.config.Slot.Name)

			resultReader := conn.Exec(ctx, query)
			results, err := resultReader.ReadAll()
			if err != nil {
				resultReader.Close()
				return errors.Wrap(err, "execute snapshot LSN query")
			}

			if err = resultReader.Close(); err != nil {
				return errors.Wrap(err, "close result reader")
			}

			if len(results) == 0 || len(results[0].Rows) == 0 {
				return retry.Unrecoverable(errors.New("no snapshot job found for slot: " + s.config.Slot.Name))
			}

			row := results[0].Rows[0]

			completed := string(row[1]) == "true" || string(row[1]) == "t"
			if !completed {
				return errors.New("snapshot job not completed yet for slot: " + s.config.Slot.Name)
			}

			lsnStr := string(row[0])
			if lsnStr == "" {
				return retry.Unrecoverable(errors.New("empty snapshot LSN result"))
			}

			snapshotLSN, err = pq.ParseLSN(lsnStr)
			if err != nil {
				return retry.Unrecoverable(errors.Wrap(err, "parse snapshot LSN: "+lsnStr))
			}

			return nil
		},
		retry.Context(ctx),
		retry.Attempts(0),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			logger.Error("error in snapshot LSN fetch, retrying",
				"attempt", n+1,
				"error", err,
				"slotName", s.config.Slot.Name)
		}),
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to fetch snapshot LSN")
	}

	logger.Info("fetched snapshot LSN from database", "slotName", s.config.Slot.Name, "snapshotLSN", snapshotLSN.String())
	return snapshotLSN, nil
}

func (s *stream) sendStandbyStatusUpdate(ctx context.Context) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	return s.writeStandbyStatusUpdate(ctx)
}

func (s *stream) sendRunStandbyStatusUpdate(ctx context.Context) error {
	return s.sendStandbyStatusUpdate(context.WithoutCancel(ctx))
}

func (s *stream) writeStandbyStatusUpdate(ctx context.Context) error {
	if d, ok := s.conn.(writeDeadliner); ok {
		deadline := time.Now().Add(standbyStatusWriteTimeout)
		if ctxDeadline, hasDeadline := ctx.Deadline(); hasDeadline && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}

		if err := d.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set standby status write deadline")
		}
		defer func() {
			_ = d.SetWriteDeadline(time.Time{})
		}()
	}

	return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()), uint64(s.LoadConfirmedXLogPos()))
}

func (s *stream) startStatusUpdateLoop(ctx context.Context) {
	s.statusUpdateStarted.Store(true)
	go func() {
		defer close(s.statusUpdateEnd)

		s.statusUpdateLoop(ctx)
	}()
}

func (s *stream) statusUpdateLoop(ctx context.Context) {
	interval := s.config.StatusUpdateEvery()
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			if s.closed.Load() || s.LoadXLogPos() == 0 {
				continue
			}

			if err := s.sendRunStandbyStatusUpdate(ctx); err != nil {
				if s.closed.Load() || ctx.Err() != nil {
					return
				}

				logger.Error("periodic standby status update failed, restarting the stream", "error", err)
				s.statusUpdateFailed.Store(true)
				if s.cancel != nil {
					s.cancel()
				}

				return
			}

			logger.Debug("periodic standby status update sent")
		}
	}
}

func (s *stream) flushFinalStandbyStatusUpdateLocked(ctx context.Context) {
	if s.LoadConfirmedXLogPos() == 0 {
		return
	}

	if err := s.writeStandbyStatusUpdate(ctx); err != nil {
		logger.Warn("final standby status update failed, updates may duplicate on restart", "error", err)
		return
	}

	logger.Debug("final standby status update sent")
}

func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, walReceivedPosition, walFlushedPosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walReceivedPosition)
	data = AppendUint64(data, walFlushedPosition)
	data = AppendUint64(data, walFlushedPosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.UTC().UnixMicro() - microSecFromUnixEpochToY2K)
}
