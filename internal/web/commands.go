package web

import (
	"fmt"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"regexp"
	"strings"
)

func restoreCommands(snapshot db.GetSnapshotDetailRow, chain []db.ListSnapshotRestoreChainRow, sshCommandPrefix string) []string {
	return restoreCommandsFor(snapshot.PoolName, snapshot.DatasetPath, chain, sshCommandPrefix)
}

var sshUserAtDestinationRE = regexp.MustCompile(`(^|\s)[^\s@]+@([^\s]+)$`)

func restoreCommandsFor(poolName, datasetPath string, chain []db.ListSnapshotRestoreChainRow, sshCommandPrefix string) []string {
	sshCommandPrefix = sshUserAtDestinationRE.ReplaceAllString(strings.TrimSpace(sshCommandPrefix), `${1}restore@${2}`)
	target := poolName + "/restored-" + strings.ReplaceAll(datasetPath, "/", "-")
	commands := make([]string, 0, len(chain))
	for _, item := range chain {
		if item.Status != "committed" || !item.ManifestObjectKey.Valid || item.ManifestObjectKey.String == "" {
			return nil
		}
		commands = append(commands, fmt.Sprintf("%s restore-stream %q | zfs recv -F %q", sshCommandPrefix, item.ID, target))
	}
	return commands
}

func nasKeygenScript() string {
	return strings.TrimSpace(`mkdir -p ~/.ssh
ssh-keygen -t ed25519 -f ~/.ssh/zfs-s3nd -N '' -C zfs-s3nd
cat ~/.ssh/zfs-s3nd.pub`)
}

func (s *Server) uploadSSHCommandPrefix() string {
	prefix := strings.TrimSpace(s.restoreSSHCommandPrefix)
	if prefix == "" {
		prefix = "ssh [named_source]@<ssh-host>"
	}
	if prefix == "ssh" {
		return "ssh"
	}
	if strings.HasPrefix(prefix, "ssh ") {
		return "ssh " + strings.TrimSpace(strings.TrimPrefix(prefix, "ssh "))
	}
	return prefix
}

func firstUploadScript(sshCommand string) string {
	return strings.TrimSpace(fmt.Sprintf(`zfs snapshot tank/photos@zs3-first
zfs send tank/photos@zs3-first | %s`, sshCommand))
}
