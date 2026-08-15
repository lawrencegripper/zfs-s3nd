package restoreplan

import (
	"context"
	"fmt"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

type Plan struct {
	SnapshotID         string
	Chain              []db.ListSnapshotRestoreChainRow
	Snapshot           db.ListSnapshotRestoreChainRow
	Parent             *catalog.SnapshotRef
	Next               *catalog.SnapshotRef
	RemainingAfterNext int64
}

func Build(ctx context.Context, repo *catalog.Repository, snapshotID string) (Plan, error) {
	chain, err := repo.RestoreChain(ctx, snapshotID)
	if err != nil {
		return Plan{}, err
	}
	if len(chain) == 0 {
		return Plan{}, fmt.Errorf("snapshot %s not found", snapshotID)
	}
	plan := Plan{SnapshotID: snapshotID, Chain: chain, Snapshot: chain[len(chain)-1]}
	if len(chain) > 1 {
		parent := catalog.SnapshotRef{ID: chain[len(chain)-2].ID, Name: chain[len(chain)-2].Name}
		plan.Parent = &parent
	}
	if next, remaining, ok, err := repo.NextCommittedChild(ctx, snapshotID); err != nil {
		return Plan{}, err
	} else if ok {
		plan.Next = &next
		plan.RemainingAfterNext = remaining
	}
	return plan, nil
}

func (p Plan) ValidateStreamable() error {
	if p.Snapshot.Status != "committed" || !p.Snapshot.ManifestObjectKey.Valid || p.Snapshot.ManifestObjectKey.String == "" {
		return fmt.Errorf("snapshot %s is not committed or has no manifest", p.Snapshot.Name)
	}
	return nil
}

func (p Plan) ManifestKey() string {
	if !p.Snapshot.ManifestObjectKey.Valid {
		return ""
	}
	return p.Snapshot.ManifestObjectKey.String
}

func (p Plan) ParentRequirement() string {
	if p.Parent == nil {
		return ""
	}
	return fmt.Sprintf("restore requires parent snapshot %s (%s) before receiving %s.\n", p.Parent.ID, p.Parent.Name, p.SnapshotID)
}

func (p Plan) CLINextRestore() string {
	if p.Next == nil {
		return ""
	}
	if p.RemainingAfterNext > 0 {
		return fmt.Sprintf("\nnext restore: zfs-s3nd restore-stream %s | zfs recv -F <target> (plus %d more)\n", p.Next.ID, p.RemainingAfterNext)
	}
	return fmt.Sprintf("\nnext restore: zfs-s3nd restore-stream %s | zfs recv -F <target>\n", p.Next.ID)
}

func (p Plan) SSHNextRestore() string {
	if p.Next == nil {
		return ""
	}
	if p.RemainingAfterNext > 0 {
		return fmt.Sprintf("\nnext restore: ssh <zfs-s3nd> restore-stream %s | zfs recv -F <target> (plus %d more)\n", p.Next.ID, p.RemainingAfterNext)
	}
	return fmt.Sprintf("\nnext restore: ssh <zfs-s3nd> restore-stream %s | zfs recv -F <target>\n", p.Next.ID)
}
