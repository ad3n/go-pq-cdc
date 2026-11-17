package cdc

import (
	"context"
	goerrors "errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ad3n/go-pq-cdc/pq/heartbeat"
	"github.com/ad3n/go-pq-cdc/pq/message/format"
	"github.com/ad3n/go-pq-cdc/pq/snapshot"

	"github.com/ad3n/go-pq-cdc/pq/timescaledb"

	"github.com/ad3n/go-pq-cdc/config"
	"github.com/ad3n/go-pq-cdc/internal/http"
	"github.com/ad3n/go-pq-cdc/internal/metric"
	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq"
	"github.com/ad3n/go-pq-cdc/pq/publication"
	"github.com/ad3n/go-pq-cdc/pq/replication"
	"github.com/ad3n/go-pq-cdc/pq/slot"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	Connector interface {
		Start(ctx context.Context)
		WaitUntilReady(ctx context.Context) error
		Close()
		GetConfig() *config.Config
		SetMetricCollectors(collectors ...prometheus.Collector)
	}

	connector struct {
		heartbeat          *heartbeat.Heartbeat
		prometheusRegistry metric.Registry
		server             http.Server
		stream             replication.Streamer
		timescaleDB        *timescaledb.TimescaleDB
		slot               *slot.Slot
		cancelCh           chan os.Signal
		readyCh            chan struct{}
		cfg                *config.Config
		snapshotter        *snapshot.Snapshotter
		listenerFunc       replication.ListenerFunc
		once               sync.Once
		closeOnce          sync.Once
	}
)

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc replication.ListenerFunc) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJSON(configFilePath)
	}

	if strings.HasSuffix(configFilePath, ".yml") || strings.HasSuffix(configFilePath, ".yaml") {
		cfg, err = config.ReadConfigYAML(configFilePath)
	}

	if err != nil {
		return nil, err
	}

	return NewConnector(ctx, cfg, listenerFunc)
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}

	cfg.Print()

	logger.InitLogger(cfg.Logger.Logger)
	applyVisibilityDiscovery(&cfg)

	if cfg.IsSnapshotOnlyMode() {
		return newSnapshotOnlyConnector(ctx, cfg, listenerFunc)
	}

	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	hb, publicationInfo, err := setupHeartbeatAndPublication(ctx, cfg, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, err
	}

	conn.Close(ctx)

	m := metric.NewMetric(cfg.Slot.Name)

	snapshotTables, err := cfg.GetSnapshotTables(publicationInfo)
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot tables")
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	stream := replication.NewStream(cfg.ReplicationDSN(), cfg, m, listenerFunc)

	sl := slot.NewSlot(cfg.ReplicationDSN(), cfg.DSN(), cfg.Slot, m, stream.(slot.XLogUpdater))

	prometheusRegistry := metric.NewRegistry(m)

	tdb, err := initializeTimescaleDB(ctx, cfg)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		snapshotter.Close(cleanupCtx)
		sl.Close(cleanupCtx)
		stream.Close(cleanupCtx)
		if hb != nil {
			hb.Close(cleanupCtx)
		}

		return nil, err
	}

	return &connector{
		cfg:                &cfg,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, sl),
		slot:               sl,
		heartbeat:          hb,
		timescaleDB:        tdb,
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
	}, nil
}

func newSnapshotOnlyConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {

	m := metric.NewMetric("snapshot_only")

	snapshotTables, err := cfg.GetSnapshotTables(nil)
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot tables")
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	prometheusRegistry := metric.NewRegistry(m)

	logger.Info("snapshot-only mode enabled", "tables", len(snapshotTables))

	return &connector{
		cfg:                &cfg,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, nil),
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
	}, nil
}

func setupHeartbeatAndPublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*heartbeat.Heartbeat, *publication.Config, error) {
	var hb *heartbeat.Heartbeat
	if cfg.IsHeartbeatEnabled() {
		hb = heartbeat.New(cfg.DSN(), cfg.Heartbeat)
		if err := hb.EnsureTable(ctx, conn); err != nil {
			return nil, nil, errors.Wrap(err, "create heartbeat table")
		}
	}

	publicationInfo, err := initializePublication(ctx, cfg, conn)
	if err != nil {
		return nil, nil, err
	}

	if err := cfg.ValidateHeartbeatInPublication(publicationInfo); err != nil {
		return nil, nil, err
	}

	logger.Info("publication", "info", publicationInfo)
	return hb, publicationInfo, nil
}

func initializeTimescaleDB(ctx context.Context, cfg config.Config) (*timescaledb.TimescaleDB, error) {
	if !cfg.ExtensionSupport.EnableTimeScaleDB {
		return nil, nil
	}

	tdb, err := timescaledb.NewTimescaleDB(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	if _, err = tdb.FindHyperTables(ctx); err != nil {
		tdb.Close(context.Background())
		return nil, err
	}

	return tdb, nil
}

func initializePublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*publication.Config, error) {
	pub := publication.New(cfg.Publication, conn)
	if cfg.Publication.CreateIfNotExists {
		if err := pub.ApplyReplicaIdentities(ctx); err != nil {
			return nil, err
		}
	} else if err := pub.CheckReplicaIdentities(ctx); err != nil {
		logger.Error("replica identity check failed; take manual action to ALTER TABLE ... REPLICA IDENTITY to match the config", "error", err)
	}

	return pub.Create(ctx)
}

func initializeSnapshot(ctx context.Context, cfg config.Config, tables publication.Tables, m metric.Metric) (*snapshot.Snapshotter, error) {
	if !cfg.Snapshot.Enabled {
		return nil, nil
	}

	return snapshot.New(ctx, cfg.Snapshot, tables, cfg.DSN(), m)
}

func (c *connector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

	if c.cfg.IsSnapshotOnlyMode() {

		if !c.shouldTakeSnapshotOnly(ctx) {
			logger.Info("snapshot-only already completed, exiting")
			return
		}

		if err := c.executeSnapshotOnly(ctx); err != nil {
			logger.Error("snapshot-only execution failed", "error", err)
			return
		}

		logger.Info("snapshot-only completed successfully, exiting")
		return
	}

	if c.cfg.Snapshot.Enabled && c.shouldTakeSnapshot(ctx) {
		if err := c.prepareSnapshotAndSlot(ctx); err != nil {
			logger.Error("snapshot preparation failed", "error", err)
			return
		}
	} else {

		logger.Info("creating replication slot for CDC")
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			logger.Error("slot creation failed", "error", err)
			return
		}

		logger.Info("slot info", "info", slotInfo)
	}

	if err := c.slot.Connect(ctx); err != nil {
		logger.Error("slot connection failed", "error", err)
		return
	}

	c.CaptureSlot(ctx)

	if err := c.stream.Connect(ctx); err != nil {
		logger.Error("stream connection failed", "error", err)
		return
	}

	err := c.stream.Open(ctx)
	if err != nil {
		if goerrors.Is(err, replication.ErrorSlotInUse) {
			logger.Info("capture failed")
			c.Start(ctx)
			return
		}

		logger.Error("postgres stream open", "error", err)
		return
	}

	logger.Info("slot captured")
	go c.slot.Metrics(ctx)

	if c.heartbeat != nil {
		go c.heartbeat.Run(ctx)
	}

	if c.timescaleDB != nil {
		go c.timescaleDB.SyncHyperTables(ctx)
	}

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	close(c.readyCh)

	<-c.cancelCh
	logger.Debug("cancel channel triggered")
}

func (c *connector) shouldTakeSnapshot(ctx context.Context) bool {
	if !c.cfg.Snapshot.Enabled {
		return false
	}

	switch c.cfg.Snapshot.Mode {
	case config.SnapshotModeNever:
		return false
	case config.SnapshotModeInitial:

		if c.cfg.Snapshot.Resnapshot {
			logger.Info("resnapshot enabled, cleaning metadata for slot", "slotName", c.cfg.Slot.Name)
			if err := c.snapshotter.CleanupJobForSlot(ctx, c.cfg.Slot.Name); err != nil {
				logger.Warn("failed to cleanup job for resnapshot", "error", err)
			}

			return true
		}

		job, err := c.snapshotter.LoadJob(ctx, c.cfg.Slot.Name)
		if err != nil {
			logger.Debug("failed to load snapshot job state, will take snapshot", "error", err)
			return true
		}

		return job == nil || !job.Completed
	default:
		logger.Warn("invalid snapshot mode, skipping snapshot", "mode", c.cfg.Snapshot.Mode)
		return false
	}
}

func (c *connector) prepareSnapshotAndSlot(ctx context.Context) error {
	return c.retryOperation(ctx, "snapshot", 3, func() error {

		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			return errors.Wrap(err, "create slot")
		}

		logger.Debug("replication slot created, WAL preserved", "slotName", slotInfo.Name, "restartLSN", slotInfo.RestartLSN.String())

		err = c.snapshotter.Prepare(ctx, c.cfg.Slot.Name)
		if err != nil {
			return errors.Wrap(err, "prepare snapshot")
		}

		c.stream.OpenFromSnapshotLSN()

		if err := c.executeSnapshotWithRetry(ctx); err != nil {

			if c.isSnapshotInvalidationError(err) {
				log.Fatal(err)
			}

			return errors.Wrap(err, "execute snapshot")
		}

		logger.Info("snapshot completed successfully")
		return nil
	})
}

func (c *connector) executeSnapshotOnly(ctx context.Context) error {
	slotName := c.getSnapshotOnlySlotName()

	logger.Info("starting snapshot-only execution", "slotName", slotName)

	err := c.snapshotter.Prepare(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "prepare snapshot")
	}

	if err := c.snapshotter.Execute(ctx, c.snapshotHandler, slotName); err != nil {
		return errors.Wrap(err, "execute snapshot")
	}

	logger.Info("snapshot data collection completed")
	return nil
}

func (c *connector) getSnapshotOnlySlotName() string {
	if c.cfg.Snapshot.ID != "" {
		return c.cfg.Snapshot.ID
	}

	return fmt.Sprintf("snapshot_only_%s", c.cfg.Database)
}

func (c *connector) shouldTakeSnapshotOnly(ctx context.Context) bool {
	slotName := c.getSnapshotOnlySlotName()

	if c.cfg.Snapshot.Resnapshot {
		logger.Info("resnapshot enabled, cleaning metadata for slot", "slotName", slotName)
		if err := c.snapshotter.CleanupJobForSlot(ctx, slotName); err != nil {
			logger.Warn("failed to cleanup job for resnapshot", "error", err)
		}

		return true
	}

	job, err := c.snapshotter.LoadJob(ctx, slotName)
	if err != nil {
		logger.Debug("failed to load snapshot job, will take snapshot", "error", err)
		return true
	}

	if job == nil || !job.Completed {
		return true
	}

	logger.Info("snapshot-only already completed, skipping", "slotName", slotName)
	return false
}

func (c *connector) executeSnapshotWithRetry(ctx context.Context) error {
	const (
		maxRetries        = 5
		initialDelay      = 10 * time.Second
		maxDelay          = 60 * time.Second
		backoffMultiplier = 2
	)

	retryDelay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.snapshotter.Execute(ctx, c.snapshotHandler, c.cfg.Slot.Name)
		if err == nil {
			return nil
		}

		if !c.isSnapshotInvalidationError(err) {

			return err
		}

		if attempt >= maxRetries {
			return errors.Wrap(err, "snapshot execution failed after maximum retries")
		}

		c.logRetryAttempt(attempt, maxRetries, retryDelay)

		if waitErr := c.waitWithContext(ctx, retryDelay); waitErr != nil {
			return waitErr
		}

		retryDelay = c.calculateNextDelay(retryDelay, maxDelay, backoffMultiplier)
	}

	return errors.New("snapshot execution failed: unexpected exit from retry loop")
}

func (c *connector) isSnapshotInvalidationError(err error) bool {
	if err == nil {
		return false
	}

	if goerrors.Is(err, snapshot.ErrSnapshotInvalidated) {
		return true
	}

	return strings.Contains(err.Error(), "snapshot invalidated")
}

func (c *connector) logRetryAttempt(attempt, maxRetries int, delay time.Duration) {
	logger.Warn("[snapshot] snapshot invalidated, coordinator likely restarted",
		"attempt", attempt,
		"maxRetries", maxRetries,
		"waitTime", delay)

	logger.Info("[snapshot] waiting for coordinator to create new snapshot",
		"retryIn", delay)
}

func (c *connector) waitWithContext(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

func (c *connector) calculateNextDelay(currentDelay, maxDelay time.Duration, multiplier int) time.Duration {
	nextDelay := currentDelay * time.Duration(multiplier)
	if nextDelay > maxDelay {
		return maxDelay
	}

	return nextDelay
}

func (c *connector) retryOperation(ctx context.Context, operationName string, maxRetries int, operation func() error) error {
	var lastErr error
	retryDelay := 5 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if attempt > 1 {
			logger.Info("retrying operation", "operation", operationName, "attempt", attempt, "maxRetries", maxRetries)
		}

		if err := operation(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if goerrors.Is(err, context.Canceled) || goerrors.Is(err, context.DeadlineExceeded) {
				return err
			}

			lastErr = err
			logger.Warn("operation failed", "operation", operationName, "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("waiting before retry", "retryDelay", retryDelay.String())
				if err := c.waitWithContext(ctx, retryDelay); err != nil {
					return err
				}
			}

			continue
		}

		return nil
	}

	return errors.Wrapf(lastErr, "%s failed after %d retries", operationName, maxRetries)
}

func (c *connector) snapshotHandler(event *format.Snapshot) error {
	c.listenerFunc(&replication.ListenerContext{
		Message: event,
		Ack: func() error {
			return nil
		},
	})
	return nil
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.cancelCh:
		return context.Canceled
	}
}

func (c *connector) Close() {

	c.closeOnce.Do(func() {

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		logger.Debug("[connector] closing connector")

		signal.Stop(c.cancelCh)
		close(c.cancelCh)

		if c.snapshotter != nil {
			c.snapshotter.Close(ctx)
		}

		if c.slot != nil {
			c.slot.Close(ctx)
		}

		if c.heartbeat != nil {
			c.heartbeat.Close(ctx)
		}

		if c.timescaleDB != nil {
			c.timescaleDB.Close(ctx)
		}

		if c.stream != nil {
			if err := c.stream.Close(ctx); err != nil {
				logger.Error("replication stream shutdown failed", "error", err)
			}
		}

		c.server.Shutdown()

		logger.Info("[connector] connector closed successfully")
	})
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func (c *connector) CaptureSlot(ctx context.Context) {
	logger.Info("slot capturing...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		info, err := c.slot.Info(ctx)
		if err != nil {
			if goerrors.Is(err, slot.ErrorSlotClosed) {
				return
			}

			logger.Warn("slot info failed on capture slot", "error", err)
			continue
		}

		if info.Active {
			continue
		}

		logger.Debug("capture slot", "slotInfo", info)
		break
	}
}
