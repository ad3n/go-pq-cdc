package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/ad3n/go-pq-cdc/logger"
	"github.com/ad3n/go-pq-cdc/pq/publication"
	"github.com/ad3n/go-pq-cdc/pq/slot"
)

const defaultSchema = "public"

const DefaultStatusUpdateInterval = 10 * time.Second

const DefaultReplicaRefreshInterval = 10 * time.Second

type (
	Config struct {
		Logger      LoggerConfig       `json:"logger" yaml:"logger"`
		Host        string             `json:"host" yaml:"host"`
		Username    string             `json:"username" yaml:"username"`
		Password    string             `json:"password" yaml:"password"`
		Database    string             `json:"database" yaml:"database"`
		Publication publication.Config `json:"publication" yaml:"publication"`
		Heartbeat   HeartbeatConfig    `json:"heartbeat" yaml:"heartbeat"`
		Slot        slot.Config        `json:"slot" yaml:"slot"`

		StatusUpdateInterval *time.Duration        `json:"statusUpdateInterval" yaml:"statusUpdateInterval"`
		VisibilityGuard      VisibilityGuardConfig `json:"visibilityGuard" yaml:"visibilityGuard"`
		Snapshot             SnapshotConfig        `json:"snapshot" yaml:"snapshot"`
		Port                 int                   `json:"port" yaml:"port"`
		Metric               MetricConfig          `json:"metric" yaml:"metric"`
		DebugMode            bool                  `json:"debugMode" yaml:"debugMode"`
		ExtensionSupport     ExtensionSupport      `json:"extensionSupport" yaml:"extensionSupport"`
	}

	VisibilityGuardConfig struct {
		ResolveReplicas func(ctx context.Context) ([]ReplicaMember, error) `json:"-" yaml:"-"`
		Discovery       VisibilityDiscoveryConfig                          `json:"discovery" yaml:"discovery"`
		FailMode        VisibilityFailMode                                 `json:"failMode" yaml:"failMode"`
		Replicas        []string                                           `json:"replicas" yaml:"replicas"`
		Timeout         time.Duration                                      `json:"timeout" yaml:"timeout"`
		PollInterval    time.Duration                                      `json:"pollInterval" yaml:"pollInterval"`
		RefreshInterval time.Duration                                      `json:"-" yaml:"-"`
		Enabled         bool                                               `json:"enabled" yaml:"enabled"`
	}

	VisibilityDiscoveryConfig struct {
		DNS *DNSDiscoveryConfig `json:"dns" yaml:"dns"`
	}

	DNSDiscoveryConfig struct {
		Nameserver string `json:"nameserver" yaml:"nameserver"`

		Names []string `json:"names" yaml:"names"`

		RefreshInterval time.Duration `json:"refreshInterval" yaml:"refreshInterval"`

		Port int `json:"port" yaml:"port"`
	}

	ReplicaMember struct {
		Addr string

		Source string

		Timeline int32
	}

	VisibilityFailMode string

	MetricConfig struct {
		Port int `json:"port" yaml:"port"`
	}

	LoggerConfig struct {
		Logger   logger.Logger `json:"-" yaml:"-"`
		LogLevel slog.Level    `json:"level" yaml:"level"`
	}

	ExtensionSupport struct {
		EnableTimeScaleDB bool `json:"enableTimeScaleDB" yaml:"enableTimescaleDB"`
	}

	HeartbeatConfig struct {
		Table    publication.Table `json:"table" yaml:"table"`
		Interval time.Duration     `json:"interval" yaml:"interval"`
	}

	SnapshotConfig struct {
		Mode              SnapshotMode       `json:"mode" yaml:"mode"`
		InstanceID        string             `json:"instanceId" yaml:"instanceId"`
		ID                string             `json:"id" yaml:"id"`
		QueryCondition    string             `json:"queryCondition,omitempty" yaml:"queryCondition,omitempty"`
		Tables            publication.Tables `json:"tables" yaml:"tables"`
		ChunkSize         int64              `json:"chunkSize" yaml:"chunkSize"`
		ClaimTimeout      time.Duration      `json:"claimTimeout" yaml:"claimTimeout"`
		HeartbeatInterval time.Duration      `json:"heartbeatInterval" yaml:"heartbeatInterval"`
		Enabled           bool               `json:"enabled" yaml:"enabled"`
		Resnapshot        bool               `json:"resnapshot" yaml:"resnapshot"`
	}

	SnapshotMode string
)

const (
	VisibilityFailClosed VisibilityFailMode = "closed"

	VisibilityFailOpen VisibilityFailMode = "open"
)

const (
	SnapshotModeInitial      SnapshotMode = "initial"
	SnapshotModeNever        SnapshotMode = "never"
	SnapshotModeSnapshotOnly SnapshotMode = "snapshot_only"
)

func (v *VisibilityGuardConfig) setDefault(mainPort int) {
	if !v.Enabled {
		return
	}

	if v.FailMode == "" {
		v.FailMode = VisibilityFailClosed
	}

	if v.Timeout == 0 {
		v.Timeout = 10 * time.Second
	}

	if v.PollInterval == 0 {
		v.PollInterval = 5 * time.Millisecond
	}

	if v.ResolveReplicas != nil && v.RefreshInterval == 0 {
		v.RefreshInterval = DefaultReplicaRefreshInterval
	}

	if d := v.Discovery.DNS; d != nil {
		if d.RefreshInterval == 0 {
			d.RefreshInterval = DefaultReplicaRefreshInterval
		}

		if d.Port == 0 {
			d.Port = mainPort
		}
	}
}

func (v *VisibilityGuardConfig) Validate() error {
	if !v.Enabled {
		if len(v.Replicas) > 0 {
			return errors.New("visibilityGuard.replicas requires visibilityGuard.enabled: true")
		}

		if v.ResolveReplicas != nil {
			return errors.New("visibilityGuard.ResolveReplicas requires visibilityGuard.enabled: true")
		}

		if v.Discovery.DNS != nil {
			return errors.New("visibilityGuard.discovery.dns requires visibilityGuard.enabled: true")
		}

		return nil
	}

	var err error
	if v.ResolveReplicas != nil && len(v.Replicas) > 0 {
		err = errors.Join(err, errors.New("visibilityGuard.replicas and visibilityGuard.ResolveReplicas are mutually exclusive"))
	}

	if v.RefreshInterval < 0 {
		err = errors.Join(err, errors.New("visibilityGuard.RefreshInterval must not be negative"))
	}

	if d := v.Discovery.DNS; d != nil {
		if len(v.Replicas) > 0 {
			err = errors.Join(err, errors.New("visibilityGuard.discovery.dns and visibilityGuard.replicas are mutually exclusive"))
		}

		if v.ResolveReplicas != nil {
			err = errors.Join(err, errors.New("visibilityGuard.discovery.dns and visibilityGuard.ResolveReplicas are mutually exclusive"))
		}

		if v.RefreshInterval != 0 {
			err = errors.Join(err, errors.New("visibilityGuard.discovery.dns and visibilityGuard.RefreshInterval are mutually exclusive"))
		}

		err = errors.Join(err, d.validate())
	}

	seen := make(map[string]bool, len(v.Replicas))
	for _, r := range v.Replicas {
		if _, _, e := net.SplitHostPort(r); e != nil {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.replicas: %q must be host:port", r))
		}

		if seen[r] {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.replicas: %q listed twice", r))
		}

		seen[r] = true
	}

	if v.FailMode != VisibilityFailClosed && v.FailMode != VisibilityFailOpen {
		err = errors.Join(err, errors.New("visibilityGuard.failMode must be 'closed' or 'open'"))
	}

	if v.Timeout <= 0 {
		err = errors.Join(err, errors.New("visibilityGuard.timeout must be greater than 0"))
	}

	if v.PollInterval <= 0 {
		err = errors.Join(err, errors.New("visibilityGuard.pollInterval must be greater than 0"))
	}

	return err
}

func (d *DNSDiscoveryConfig) validate() error {
	var err error
	if len(d.Names) == 0 {
		err = errors.Join(err, errors.New("visibilityGuard.discovery.dns.names must list at least one name"))
	}

	seen := make(map[string]bool, len(d.Names))
	for _, name := range d.Names {
		if !validDNSName(name) {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.discovery.dns.names: %q is not a valid DNS name", name))
		}

		key := strings.ToLower(strings.TrimSuffix(name, "."))
		if seen[key] {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.discovery.dns.names: %q listed twice (names are compared case-insensitively, a trailing dot is ignored)", name))
		}

		seen[key] = true
	}

	if d.Port < 1 || d.Port > 65535 {
		err = errors.Join(err, fmt.Errorf("visibilityGuard.discovery.dns.port must be in 1-65535, got %d", d.Port))
	}

	if d.Nameserver != "" {
		if _, pErr := netip.ParseAddrPort(d.Nameserver); pErr != nil {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.discovery.dns.nameserver: %q must be a literal ip:port (a host name is not accepted)", d.Nameserver))
		}
	}

	if d.RefreshInterval <= 0 {
		err = errors.Join(err, errors.New("visibilityGuard.discovery.dns.refreshInterval must be greater than 0"))
	}

	return err
}

func validDNSName(name string) bool {
	name = strings.TrimSuffix(name, ".")
	if name == "" || len(name) > 253 {
		return false
	}

	if _, err := netip.ParseAddr(name); err == nil {
		return false
	}

	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}

		for _, c := range label {
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
				return false
			}
		}
	}

	return true
}

func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) ReplicationDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) ReplicaDSN(hostPort string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), hostPort, c.Database)
}

func (c *Config) DSNWithoutSSL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) SetDefault() {
	if c.Port == 0 {
		c.Port = 5432
	}

	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}

	if c.Heartbeat.Table.Name != "" {
		if c.Heartbeat.Interval == 0 {
			c.Heartbeat.Interval = 100 * time.Millisecond
		}

		if c.Heartbeat.Table.Schema == "" {
			c.Heartbeat.Table.Schema = defaultSchema
		}
	}

	if c.Slot.SlotActivityCheckerInterval == 0 {
		c.Slot.SlotActivityCheckerInterval = 1000
	}

	if c.Slot.ProtoVersion == 0 {
		c.Slot.ProtoVersion = 2
	}

	if c.StatusUpdateInterval == nil {
		interval := DefaultStatusUpdateInterval
		c.StatusUpdateInterval = &interval
	}

	if c.Logger.Logger == nil {
		c.Logger.Logger = logger.NewSlog(c.Logger.LogLevel)
	}

	c.VisibilityGuard.setDefault(c.Port)

	for tableID, table := range c.Publication.Tables {
		if table.Schema == "" {
			c.Publication.Tables[tableID].Schema = defaultSchema
		}
	}

	if c.Snapshot.Enabled {
		if c.Snapshot.Mode == "" {
			c.Snapshot.Mode = SnapshotModeNever
		}

		if c.Snapshot.ChunkSize == 0 {
			c.Snapshot.ChunkSize = 8_000
		}

		if c.Snapshot.ClaimTimeout == 0 {
			c.Snapshot.ClaimTimeout = 30 * time.Second
		}

		if c.Snapshot.HeartbeatInterval == 0 {
			c.Snapshot.HeartbeatInterval = 5 * time.Second
		}

		for tableID, table := range c.Snapshot.Tables {
			if table.Schema == "" {
				c.Snapshot.Tables[tableID].Schema = defaultSchema
			}
		}
	}
}

func (c *Config) StatusUpdateEvery() time.Duration {
	if c.StatusUpdateInterval == nil {
		return DefaultStatusUpdateInterval
	}

	return *c.StatusUpdateInterval
}

func (c *Config) IsSnapshotOnlyMode() bool {
	return c.Snapshot.Enabled && c.Snapshot.Mode == SnapshotModeSnapshotOnly
}

func (c *Config) IsHeartbeatEnabled() bool {
	return c.Heartbeat.Table.Name != ""
}

func (c *Config) GetSnapshotTables(publicationInfo *publication.Config) (publication.Tables, error) {

	if c.IsSnapshotOnlyMode() {
		if len(c.Snapshot.Tables) == 0 {
			return nil, errors.New("snapshot.tables must be specified for snapshot_only mode")
		}

		return c.Snapshot.Tables, nil
	}

	if len(c.Snapshot.Tables) > 0 {
		return c.validateSnapshotSubset(publicationInfo.Tables)
	}

	return c.mergePublicationTableConfig(publicationInfo.Tables), nil
}

func (c *Config) validateSnapshotSubset(pubTables publication.Tables) (publication.Tables, error) {
	if len(pubTables) == 0 {
		return nil, errors.New("publication has no tables defined. Either specify tables in publication.tables or query an existing publication")
	}

	pubMap := make(map[string]publication.Table)
	for _, t := range pubTables {
		key := t.Schema + "." + t.Name
		pubMap[key] = t
	}

	validatedTables := make(publication.Tables, 0, len(c.Snapshot.Tables))
	for _, st := range c.Snapshot.Tables {
		key := st.Schema + "." + st.Name
		pubTable, exists := pubMap[key]
		if !exists {
			return nil, fmt.Errorf(
				"snapshot table '%s' not found in publication '%s'. "+
					"For snapshot+CDC mode, snapshot.tables must be a subset of publication tables",
				key, c.Publication.Name,
			)
		}

		mergedTable := pubTable
		if st.SnapshotPartitionStrategy != "" {
			mergedTable.SnapshotPartitionStrategy = st.SnapshotPartitionStrategy
		}

		if st.QueryCondition != "" {
			mergedTable.QueryCondition = st.QueryCondition
		}

		validatedTables = append(validatedTables, mergedTable)
	}

	return validatedTables, nil
}

func (c *Config) ValidateHeartbeatInPublication(pubInfo *publication.Config) error {
	if !c.IsHeartbeatEnabled() || pubInfo == nil {
		return nil
	}

	if c.Publication.AllTables || pubInfo.AllTables {
		return nil
	}

	schema := c.Heartbeat.Table.Schema
	if schema == "" {
		schema = defaultSchema
	}

	name := c.Heartbeat.Table.Name
	if !pubInfo.Tables.Contains(schema, name) {
		return fmt.Errorf(
			"heartbeat table %s.%s is not included in publication %q; add it to publication.tables so heartbeat changes reach the replication slot",
			schema, name, pubInfo.Name,
		)
	}

	return nil
}

func (c *Config) Validate() error {
	var err error
	if isEmpty(c.Host) {
		err = errors.Join(err, errors.New("host cannot be empty"))
	}

	if isEmpty(c.Username) {
		err = errors.Join(err, errors.New("username cannot be empty"))
	}

	if isEmpty(c.Password) {
		err = errors.Join(err, errors.New("password cannot be empty"))
	}

	if isEmpty(c.Database) {
		err = errors.Join(err, errors.New("database cannot be empty"))
	}

	if c.StatusUpdateInterval != nil && *c.StatusUpdateInterval < 0 {
		err = errors.Join(err, errors.New("statusUpdateInterval must be greater than or equal to 0"))
	}

	if !c.IsSnapshotOnlyMode() {
		if cErr := c.Publication.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}

		if cErr := c.Slot.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}

		if cErr := c.VisibilityGuard.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}

	if cErr := c.Snapshot.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	if c.IsHeartbeatEnabled() {
		if c.Heartbeat.Interval <= 0 {
			err = errors.Join(err, errors.New("heartbeat.interval must be greater than 0 when heartbeat table is configured"))
		}

		if !c.IsSnapshotOnlyMode() && !c.Publication.AllTables && len(c.Publication.Tables) > 0 {
			if hErr := c.ValidateHeartbeatInPublication(&publication.Config{
				Name:   c.Publication.Name,
				Tables: c.Publication.Tables,
			}); hErr != nil {
				err = errors.Join(err, hErr)
			}
		}
	}

	return err
}

func (c *Config) Print() {
	cfg := *c
	cfg.Password = "*******"
	b, _ := json.Marshal(cfg)
	fmt.Println("used config: " + string(b))
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func (c *Config) mergePublicationTableConfig(pubInfoTables publication.Tables) publication.Tables {
	if len(c.Publication.Tables) == 0 {
		return pubInfoTables
	}

	userConfigMap := make(map[string]publication.Table)
	for _, t := range c.Publication.Tables {
		key := t.Schema + "." + t.Name
		userConfigMap[key] = t
	}

	result := make(publication.Tables, len(pubInfoTables))
	for i, t := range pubInfoTables {
		result[i] = t
		key := t.Schema + "." + t.Name
		if userTable, exists := userConfigMap[key]; exists {
			if userTable.SnapshotPartitionStrategy != "" {
				result[i].SnapshotPartitionStrategy = userTable.SnapshotPartitionStrategy
			}

			if userTable.QueryCondition != "" {
				result[i].QueryCondition = userTable.QueryCondition
			}
		}
	}

	return result
}

func (s *SnapshotConfig) Validate() error {
	if !s.Enabled {
		return nil
	}

	validModes := []SnapshotMode{SnapshotModeInitial, SnapshotModeNever, SnapshotModeSnapshotOnly}
	isValid := false
	for _, mode := range validModes {
		if s.Mode == mode {
			isValid = true
			break
		}
	}

	if !isValid {
		return errors.New("snapshot mode must be 'initial', 'never', or 'snapshot_only'")
	}

	if s.ChunkSize <= 0 {
		return errors.New("snapshot chunk size must be greater than 0")
	}

	if s.ClaimTimeout <= 0 {
		return errors.New("snapshot claim timeout must be greater than 0")
	}

	if s.HeartbeatInterval <= 0 {
		return errors.New("snapshot heartbeat interval must be greater than 0")
	}

	if s.Mode == SnapshotModeSnapshotOnly && len(s.Tables) == 0 {
		return errors.New("snapshot.tables must be specified for snapshot_only mode")
	}

	if s.QueryCondition != "" {
		if err := publication.ValidateQueryCondition(s.QueryCondition); err != nil {
			return err
		}
	}

	for _, t := range s.Tables {
		if t.QueryCondition != "" {
			if err := publication.ValidateQueryCondition(t.QueryCondition); err != nil {
				return fmt.Errorf("snapshot.tables %s.%s: %w", t.Schema, t.Name, err)
			}
		}
	}

	return nil
}
