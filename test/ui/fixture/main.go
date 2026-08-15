package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/ingest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
	"github.com/lawrencegripper/zfs-s3nd/internal/web"
)

func main() {
	databasePath := os.Getenv("UI_TEST_DATABASE_PATH")
	if databasePath == "" {
		databasePath = "./.ui-test/catalog.db"
	}
	_ = os.Remove(databasePath)
	_ = os.Remove(databasePath + "-shm")
	_ = os.Remove(databasePath + "-wal")

	cat, err := catalog.Open(databasePath)
	if err != nil {
		panic(err)
	}
	defer cat.Close()
	if err := cat.Migrate(); err != nil {
		panic(err)
	}

	store := storage.NewMemoryStore()
	server := web.New("127.0.0.1:3100", cat, slog.Default())
	server.SetStore(store)
	settingsManager, err := appsettings.New(context.Background(), cat.DB(), appsettings.Overrides{})
	if err != nil {
		panic(err)
	}
	server.SetSettingsManager(settingsManager)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /__fixture/seed-dataset", func(w http.ResponseWriter, r *http.Request) {
		result, err := (ingest.Service{Repo: catalog.NewRepository(cat.DB()), Store: store, Keys: storage.KeyBuilder{RootPrefix: "ui-test"}, ChunkSize: 5}).Receive(context.Background(), ingest.Request{Source: "nas-home", Pool: "tank", Dataset: "photos", Snapshot: "daily-001"}, bytes.NewReader([]byte("test-zfs-stream")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := cat.DB().ExecContext(r.Context(), `UPDATE snapshots SET stream_validation_status = 'succeeded', chain_validation_status = 'succeeded' WHERE id = ?`, result.SnapshotID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", server)
	if err := http.ListenAndServe("127.0.0.1:3100", mux); err != nil {
		panic(err)
	}
}
