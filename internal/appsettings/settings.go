package appsettings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

const (
	UploadThroughputLimitKey         = "upload_throughput_limit_mbit"
	MaxIncrementalChainDepthKey      = "max_incremental_chain_depth"
	UploadThroughputLimitEnv         = "UPLOAD_THROUGHPUT_LIMIT_MBIT"
	MaxIncrementalChainDepthEnv      = "MAX_INCREMENTAL_CHAIN_DEPTH"
	DefaultUploadThroughputLimitMbps = "45"
	DefaultMaxIncrementalChainDepth  = "30"
)

var ErrEnvironmentOverride = errors.New("setting is controlled by an environment variable")

type Source string

const (
	SourceEnvironment Source = "Environment"
	SourceDatabase    Source = "Database"
	SourceDefault     Source = "Default"
)

type Setting struct {
	Key                   string
	Value                 string
	DefaultValue          string
	Source                Source
	EnvironmentVariable   string
	EnvironmentControlled bool
}

type Snapshot struct {
	UploadThroughputLimit               Setting
	UploadThroughputLimitBytesPerSecond int64
	MaxIncrementalChainDepth            Setting
	MaxIncrementalChainDepthValue       int64
}

type Overrides struct {
	UploadThroughputLimitMbps string
	MaxIncrementalChainDepth  string
}

type Manager struct {
	database  *sql.DB
	overrides Overrides
	mu        sync.RWMutex
	current   Snapshot
}

func New(ctx context.Context, database *sql.DB, overrides Overrides) (*Manager, error) {
	if database == nil {
		return nil, fmt.Errorf("settings database is required")
	}
	manager := &Manager{database: database, overrides: overrides}
	current, err := manager.load(ctx)
	if err != nil {
		return nil, err
	}
	manager.current = current
	return manager, nil
}

func (m *Manager) Current() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

func (m *Manager) Set(ctx context.Context, key, value string) error {
	value = strings.TrimSpace(value)
	m.mu.Lock()
	defer m.mu.Unlock()

	setting, parsed, err := resolveSetting(key, value, SourceDatabase)
	if err != nil {
		return err
	}
	if m.environmentControlled(key) {
		return fmt.Errorf("%s: %w", environmentVariable(key), ErrEnvironmentOverride)
	}
	if err := db.New(m.database).UpsertAppSetting(ctx, db.UpsertAppSettingParams{Key: key, Value: value}); err != nil {
		return fmt.Errorf("save setting %s: %w", key, err)
	}
	m.apply(setting, parsed)
	return nil
}

func (m *Manager) Reset(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.environmentControlled(key) {
		return fmt.Errorf("%s: %w", environmentVariable(key), ErrEnvironmentOverride)
	}
	setting, parsed, err := resolveSetting(key, defaultValue(key), SourceDefault)
	if err != nil {
		return err
	}
	if err := db.New(m.database).DeleteAppSetting(ctx, key); err != nil {
		return fmt.Errorf("reset setting %s: %w", key, err)
	}
	m.apply(setting, parsed)
	return nil
}

func (m *Manager) load(ctx context.Context) (Snapshot, error) {
	rows, err := db.New(m.database).ListAppSettings(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("list app settings: %w", err)
	}
	stored := make(map[string]string, len(rows))
	for _, row := range rows {
		stored[row.Key] = row.Value
	}

	var snapshot Snapshot
	for _, key := range []string{UploadThroughputLimitKey, MaxIncrementalChainDepthKey} {
		value := defaultValue(key)
		source := SourceDefault
		if storedValue, ok := stored[key]; ok {
			value = storedValue
			source = SourceDatabase
		}
		if override := strings.TrimSpace(m.override(key)); override != "" {
			value = override
			source = SourceEnvironment
		}
		setting, parsed, err := resolveSetting(key, value, source)
		if err != nil {
			return Snapshot{}, fmt.Errorf("load setting %s from %s: %w", key, source, err)
		}
		applySnapshot(&snapshot, setting, parsed)
	}
	return snapshot, nil
}

func (m *Manager) apply(setting Setting, parsed int64) {
	applySnapshot(&m.current, setting, parsed)
}

func applySnapshot(snapshot *Snapshot, setting Setting, parsed int64) {
	switch setting.Key {
	case UploadThroughputLimitKey:
		snapshot.UploadThroughputLimit = setting
		snapshot.UploadThroughputLimitBytesPerSecond = parsed
	case MaxIncrementalChainDepthKey:
		snapshot.MaxIncrementalChainDepth = setting
		snapshot.MaxIncrementalChainDepthValue = parsed
	}
}

func resolveSetting(key, value string, source Source) (Setting, int64, error) {
	value = strings.TrimSpace(value)
	var parsed int64
	var err error
	switch key {
	case UploadThroughputLimitKey:
		parsed, err = ParseThroughputLimitMbps(value)
	case MaxIncrementalChainDepthKey:
		parsed, err = ParseMaxIncrementalChainDepth(value)
	default:
		return Setting{}, 0, fmt.Errorf("unknown setting %q", key)
	}
	if err != nil {
		return Setting{}, 0, err
	}
	return Setting{
		Key:                   key,
		Value:                 value,
		DefaultValue:          defaultValue(key),
		Source:                source,
		EnvironmentVariable:   environmentVariable(key),
		EnvironmentControlled: source == SourceEnvironment,
	}, parsed, nil
}

func ParseThroughputLimitMbps(value string) (int64, error) {
	value = strings.TrimSpace(value)
	mbps, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(mbps) || math.IsInf(mbps, 0) {
		return 0, fmt.Errorf("must be a finite number")
	}
	if mbps < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	if mbps > 0 && mbps < 0.1 {
		return 0, fmt.Errorf("positive limit must be at least 0.1 Mbps")
	}
	bytesPerSecond := mbps * 1_000_000 / 8
	if bytesPerSecond > math.MaxInt64 {
		return 0, fmt.Errorf("limit is too large")
	}
	return int64(math.Round(bytesPerSecond)), nil
}

func ParseMaxIncrementalChainDepth(value string) (int64, error) {
	value = strings.TrimSpace(value)
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be a whole number")
	}
	if parsed < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	return parsed, nil
}

func (m *Manager) environmentControlled(key string) bool {
	return strings.TrimSpace(m.override(key)) != ""
}

func (m *Manager) override(key string) string {
	switch key {
	case UploadThroughputLimitKey:
		return m.overrides.UploadThroughputLimitMbps
	case MaxIncrementalChainDepthKey:
		return m.overrides.MaxIncrementalChainDepth
	default:
		return ""
	}
}

func defaultValue(key string) string {
	switch key {
	case UploadThroughputLimitKey:
		return DefaultUploadThroughputLimitMbps
	case MaxIncrementalChainDepthKey:
		return DefaultMaxIncrementalChainDepth
	default:
		return ""
	}
}

func environmentVariable(key string) string {
	switch key {
	case UploadThroughputLimitKey:
		return UploadThroughputLimitEnv
	case MaxIncrementalChainDepthKey:
		return MaxIncrementalChainDepthEnv
	default:
		return ""
	}
}
