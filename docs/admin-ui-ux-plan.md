# Admin UI UX plan

Goal: make the admin UI guide users through the backup target lifecycle without turning it into a JavaScript app. Prefer server-rendered HTML, simple CSS, and small progressive enhancements only where they clearly help.

## User journeys

### 1. First setup / out-of-box experience

Intent: “I just deployed this. What do I do next?”

- Show a getting-started panel until at least one SSH key/source exists.
- Offer two SSH-key paths:
  - import public SSH keys from a GitHub username, like Ubuntu server first-run setup;
  - generate a dedicated NAS key and paste the public key.
- After a key exists but before the first snapshot, show the first `zfs snapshot` / `zfs send | ssh ...` command.
- Keep the private key on the NAS; the app should only receive public keys.

Status: GitHub key import and first-upload empty states are implemented.

### 2. Daily dashboard

Intent: “Are my backups healthy?”

Dashboard should prioritize:

- attention required: failed chain validations, failed uploads, stale jobs, old/missing catalog backup;
- current activity: active uploads with bytes/rate/heartbeat;
- backup coverage: datasets, committed snapshots, latest uploads;
- short links to Datasets, Activity, Settings, and a human-readable Status page backed by health/readiness checks.

Avoid making the dashboard a dumping ground for every settings form.

### 3. Datasets and snapshots

Intent: “What backup data do I have, and can I restore it?”

- Datasets list should summarize latest snapshot, latest chain-valid snapshot, failed validations, stored bytes.
- Snapshot detail should emphasize restore-chain validity as the headline.
- Restore commands should be numbered and explain parent/dependency ordering.
- Destructive actions should sit in a danger-zone area, not inline as casual table buttons.

### 4. Activity

Intent: “What happened recently?”

- Separate Activity page for uploads, validations, cleanup, catalog backups, failures.
- Keep logs/operations searchable enough for support, but avoid exposing secrets.

### 5. Settings

Intent: “Manage access and integration.”

- Move SSH keys/sources and API tokens out of the main dashboard.
- Keep GitHub import and paste-key forms here.
- Explain devices in user terms: the SSH username groups backups from a NAS/source.

## Visual and interaction design

- Server-rendered HTML and simple CSS first.
- Shared layout/nav across pages.
- Status badges:
  - green: succeeded/healthy/chain-valid;
  - yellow: pending/running/unknown;
  - red: failed/attention needed.
- Human-readable Status page should show readiness as a checklist with tick/cross indicators and error details for misconfiguration (for example object storage/S3 failures).
- Clear empty states with next actions.
- Monospace command blocks for copy/paste commands.
- Avoid a SPA and avoid client-side state.

## htmx guidance

htmx is a good fit only for narrow progressive enhancements:

- polling active uploads every few seconds;
- polling validation jobs/status;
- replacing a row/card after queueing validation or cleanup.

Do not use htmx for routing, global state, or making every table dynamic.

Recommended order:

1. Shared layout/nav and basic CSS polish. Status: implemented.
2. Split Dashboard / Datasets / Activity / Settings pages. Status: implemented.
3. Move settings forms off the dashboard. Status: implemented for normal settings; first-run dashboard keeps a guided key-import shortcut.
4. Improve dataset/snapshot restore and danger-zone UX. Status: implemented, including dataset-level validation trigger.
5. Add htmx only for active upload and validation polling if the static pages feel stale.
