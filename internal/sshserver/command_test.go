package sshserver

import "testing"

func TestParseStateCommand(t *testing.T) {
	cmd, err := ParseStateCommandForSource("state tank home/lg", "nas-home")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cmd.Source != "nas-home" || cmd.Pool != "tank" || cmd.Dataset != "home/lg" {
		t.Fatalf("unexpected command: %+v", cmd)
	}
}

func TestParseStateCommandRejectsShell(t *testing.T) {
	if _, err := ParseStateCommandForSource("sh -c whoami", "nas-home"); err == nil {
		t.Fatal("expected unsupported command")
	}
}

func TestParseStateCommandRejectsBadDataset(t *testing.T) {
	if _, err := ParseStateCommandForSource("state tank /bad", "nas-home"); err == nil {
		t.Fatal("expected invalid dataset")
	}
}
