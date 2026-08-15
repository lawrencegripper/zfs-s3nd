import {
  bucket,
  defineRailway,
  github,
  group,
  preserve,
  ref,
  service,
  volume,
} from "railway/iac";

const repo = "lawrencegripper/zfs-s3nd";

export default defineRailway((_ctx, makeProject) => {
  const backups = bucket("zfs-s3nd-backups", { region: "ams" });
  const catalogData = volume("catalog-data", {
    region: "europe-west4-drams3a",
    sizeMB: 1024,
  });

  const source = github(repo, { branch: "main" });

  const app = service("zfs-s3nd", {
    source,
    start: "zfs-s3nd serve",
    healthcheck: "/healthz",
    healthcheckTimeout: 30,
    volumeMounts: {
      "/data": catalogData,
    },
    tcp: [2222],
    deploy: {
      limitOverride: {
        containers: {
          cpu: 6,
          memoryBytes: 12 * 1024 * 1024 * 1024,
        },
      },
    },
    env: {
      DATABASE_PATH: "/data/catalog.db",
      PORT: "3000",
      SSH_PORT: "2222",
      STORAGE_ENCRYPTION_KEY: preserve(),
      STORAGE_ROOT_PREFIX: "zfs-s3nd/v1",
      CATALOG_BACKUP_INTERVAL: preserve(),
      MAX_INCREMENTAL_CHAIN_DEPTH: "30",
      UPLOAD_THROUGHPUT_LIMIT_MBIT: "45",
      RAILWAY_DEPLOYMENT_DRAINING_SECONDS: "21600",
      SHUTDOWN_GRACE_PERIOD: "5h55m",
      WEB_ADMIN_PASSWORD: preserve(),
      S3_BUCKET: ref(backups, "BUCKET"),
      S3_ENDPOINT: ref(backups, "ENDPOINT"),
      S3_REGION: ref(backups, "REGION"),
      S3_ACCESS_KEY_ID: ref(backups, "ACCESS_KEY_ID"),
      S3_SECRET_ACCESS_KEY: ref(backups, "SECRET_ACCESS_KEY"),
      S3_FORCE_PATH_STYLE: "false",
    },
  });

  return makeProject("zfs-s3nd", {
    resources: [
      group("App", [app]),
      group("Storage", [backups, catalogData]),
    ],
  });
});
