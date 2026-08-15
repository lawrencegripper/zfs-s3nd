package sshserver

import (
	"fmt"
	"strings"
)

type StateCommand struct {
	Source  string
	Pool    string
	Dataset string
}

func ParseStateCommand(command string) (StateCommand, error) {
	return ParseStateCommandForSource(command, "")
}

func ParseStateCommandForSource(command, source string) (StateCommand, error) {
	fields := strings.Fields(command)
	if len(fields) == 0 || fields[0] != "state" {
		return StateCommand{}, fmt.Errorf("unsupported command")
	}
	if source == "" {
		return StateCommand{}, fmt.Errorf("ssh username is required for state")
	}
	if len(fields) != 3 {
		return StateCommand{}, fmt.Errorf("usage: state <pool> <dataset>")
	}
	cmd := StateCommand{Source: source, Pool: fields[1], Dataset: fields[2]}
	if err := validateName("source", cmd.Source); err != nil {
		return StateCommand{}, err
	}
	if err := validateName("pool", cmd.Pool); err != nil {
		return StateCommand{}, err
	}
	if err := validateDataset(cmd.Dataset); err != nil {
		return StateCommand{}, err
	}
	return cmd, nil
}

func validateName(kind, value string) error {
	if value == "" || strings.Contains(value, "/") || strings.HasPrefix(value, "@") {
		return fmt.Errorf("invalid %s %q", kind, value)
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' || r == ':') {
			return fmt.Errorf("invalid %s %q", kind, value)
		}
	}
	return nil
}

func validateDataset(value string) error {
	if value == "" || strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") {
		return fmt.Errorf("invalid dataset %q", value)
	}
	for _, part := range strings.Split(value, "/") {
		if err := validateName("dataset component", part); err != nil {
			return fmt.Errorf("invalid dataset %q: %w", value, err)
		}
	}
	return nil
}
