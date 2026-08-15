package storage

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
)

var safeComponent = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

type KeyBuilder struct {
	RootPrefix string
}

func (b KeyBuilder) DatasetPrefix(source, pool, dataset string) string {
	parts := []string{strings.Trim(b.RootPrefix, "/"), "sources", encodeComponent(source), "pools", encodeComponent(pool), "datasets"}
	for _, part := range strings.Split(dataset, "/") {
		if part == "" {
			continue
		}
		parts = append(parts, encodeComponent(part))
	}
	return path.Join(parts...)
}

func (b KeyBuilder) DatasetMetadataKey(source, pool, dataset string) string {
	return path.Join(b.DatasetPrefix(source, pool, dataset), "@dataset.json")
}

func (b KeyBuilder) SnapshotPrefix(source, pool, dataset, snapshot string) string {
	return path.Join(b.DatasetPrefix(source, pool, dataset), "@snapshots", encodeComponent(snapshot))
}

func (b KeyBuilder) ChunkKey(source, pool, dataset, snapshot string, index int64) string {
	return path.Join(b.SnapshotPrefix(source, pool, dataset, snapshot), "chunks", fmt.Sprintf("%012d.zfschunk", index))
}

func (b KeyBuilder) ManifestKey(source, pool, dataset, snapshot string) string {
	return path.Join(b.SnapshotPrefix(source, pool, dataset, snapshot), "manifest.json")
}

func (b KeyBuilder) CatalogBackupPrefix() string {
	return path.Join(strings.Trim(b.RootPrefix, "/"), "@catalog-backups")
}

func encodeComponent(component string) string {
	if safeComponent.MatchString(component) && !strings.HasPrefix(component, "@") {
		return component
	}
	escaped := url.PathEscape(component)
	return strings.ReplaceAll(escaped, "@", "%40")
}
