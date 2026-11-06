package heartbeat

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
)

type Heartbeat struct {
	conn      pq.Connection
	ctx       context.Context
	cancel    context.CancelFunc
	dsn       string
	cfg       config.HeartbeatConfig
	mu        sync.Mutex
	workers   sync.WaitGroup
	closeOnce sync.Once
	closed    atomic.Bool
}

func (h *Heartbeat) EnsureTable(ctx context.Context, conn pq.Connection) error {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)

	exists, err := pq.TableExists(ctx, conn, h.cfg.Table.Schema, h.cfg.Table.Name)
	if err != nil {
		return fmt.Errorf("check heartbeat table existence: %w", err)
	}

	if !exists {
		if err := h.createTable(ctx, conn); err != nil {
			return err
		}

		logger.Info("heartbeat table created", "table", schema+"."+table)
	}

	if err := h.insertInitialRow(ctx, conn); err != nil {
		return err
	}

	logger.Info("heartbeat table ready to use", "table", schema+"."+table)
	return nil
}

func (h *Heartbeat) Run(ctx context.Context) {
	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return
	}

	h.workers.Add(1)
	h.mu.Unlock()
	defer func() {
		h.closed.Store(true)
		h.cancel()
		h.closeConnection(context.Background())
		h.workers.Done()
	}()

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(h.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	logger.Debug("heartbeat loop started",
		"interval", h.cfg.Interval,
		"table", h.cfg.Table.Schema+"."+h.cfg.Table.Name)

	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("heartbeat loop stopped", "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := h.execute(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}

				logger.Error("heartbeat execution failed", "error", err)
				continue
			}

			logger.Debug("heartbeat query executed")
		}
	}
}

func (h *Heartbeat) Close(ctx context.Context) {
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		h.cancel()
		h.closeConnection(ctx)
		h.workers.Wait()
	})
}

func (h *Heartbeat) closeConnection(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.conn != nil {
		h.conn.Close(ctx)
		h.conn = nil
	}
}

func (h *Heartbeat) createTable(ctx context.Context, conn pq.Connection) error {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)
	constraint := quoteIdentifier(h.cfg.Table.Name + "_single_row")

	sql := fmt.Sprintf(`
		CREATE TABLE %s.%s (
			id INTEGER PRIMARY KEY DEFAULT 1,
			last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT %s CHECK (id = 1)
		)`, schema, table, constraint)

	if err := pq.ExecSQL(ctx, conn, sql); err != nil {
		return fmt.Errorf("create heartbeat table failed: %w", err)
	}

	return nil
}

func (h *Heartbeat) insertInitialRow(ctx context.Context, conn pq.Connection) error {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)

	sql := fmt.Sprintf(`
		INSERT INTO %s.%s (id) VALUES (1) ON CONFLICT DO NOTHING`, schema, table)

	if err := pq.ExecSQL(ctx, conn, sql); err != nil {
		return fmt.Errorf("insert heartbeat row failed: %w", err)
	}

	return nil
}

func (h *Heartbeat) execute(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed.Load() {
		return context.Canceled
	}

	if h.conn == nil {
		conn, err := pq.NewConnection(ctx, h.dsn)
		if err != nil {
			return fmt.Errorf("heartbeat connection (re)establish failed: %w", err)
		}

		h.conn = conn
	}

	query := h.query()
	resultReader := h.conn.Exec(ctx, query)
	if resultReader == nil {
		return fmt.Errorf("heartbeat exec returned nil resultReader")
	}
	defer func() {
		if err := resultReader.Close(); err != nil {
			logger.Error("heartbeat result reader close failed", "error", err)
		}
	}()

	rows, err := resultReader.ReadAll()
	if err != nil {
		h.conn.Close(ctx)
		h.conn = nil
		return fmt.Errorf("heartbeat query failed: %w", err)
	}

	if len(rows) == 0 {
		if err := h.insertInitialRow(ctx, h.conn); err != nil {
			return fmt.Errorf("recreate heartbeat row failed: %w", err)
		}
	}

	return nil
}

func (h *Heartbeat) query() string {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)
	return fmt.Sprintf(`UPDATE %s.%s SET last_heartbeat = NOW() WHERE id = 1 RETURNING 1`, schema, table)
}

func New(dsn string, cfg config.HeartbeatConfig) *Heartbeat {
	ctx, cancel := context.WithCancel(context.Background())
	return &Heartbeat{
		ctx:    ctx,
		cancel: cancel,
		dsn:    dsn,
		cfg:    cfg,
	}
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
