package sshserver

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestStallTimeoutReaderFailsWhenNoBytesArrive(t *testing.T) {
	pipeReader, _ := io.Pipe()
	reader := newStallTimeoutReader(pipeReader, 20*time.Millisecond, func() { _ = pipeReader.CloseWithError(errReceiveStalled) })
	defer reader.Stop()

	done := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, errReceiveStalled) {
			t.Fatalf("read error got %v want %v", err, errReceiveStalled)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not unblock after timeout")
	}
}

func TestStallTimeoutReaderResetsWhenBytesArrive(t *testing.T) {
	pipeReader, pipeWriter := io.Pipe()
	reader := newStallTimeoutReader(pipeReader, 50*time.Millisecond, func() { _ = pipeReader.CloseWithError(errReceiveStalled) })
	defer reader.Stop()

	go func() {
		defer pipeWriter.Close()
		for _, b := range []byte("abc") {
			_, _ = pipeWriter.Write([]byte{b})
			time.Sleep(20 * time.Millisecond)
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if string(data) != "abc" {
		t.Fatalf("data got %q want abc", data)
	}
}
