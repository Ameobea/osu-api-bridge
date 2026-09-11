# osu! diffcalc

Private sidecar for canonical osu!standard star-rating and performance-point calculations. It uses the official `ppy.osu.Game` and `ppy.osu.Game.Rulesets.Osu` NuGet packages rather than an independent PP implementation.

The API listens on loopback port 4512 and Prometheus metrics listen on loopback port 4513. `osu-api-bridge` is the public facade; do not publish either diffcalc listener directly.

The current pin is `ppy.osu.Game` / `ppy.osu.Game.Rulesets.Osu` `2026.730.0` (difficulty version `20260706`). On 2026-08-27 it was checked against osu!'s live beatmap-attributes endpoint using beatmap 75 with NM, HD, and HDCL; the returned star values matched within the live API's float precision.

## Runtime model

- Loads gzipped `.osu` files directly from `fetched_beatmaps` in MySQL.
- Downloads and stores a missing beatmap from `https://osu.ppy.sh/osu/{id}`, serialized and rate-limited to protect the upstream.
- Caches parsed beatmaps by estimated bytes and difficulty results by beatmap/mod settings.
- Coalesces concurrent cache misses, bounds CPU-heavy calculations, and applies per-item timeouts.
- Authenticates `/v1/*` with a constant-time comparison of `X-Diffcalc-Key`.
- Keeps metric labels bounded; no beatmap IDs, request IDs, or mod strings are metric labels.

## Configuration

Copy `.env.example` to `.env` for deployment. The required values are:

- `ConnectionStrings__OsuTrack`: MySqlConnector connection string.
- `DiffCalc__ApiKey`: shared secret of at least 32 characters.

All settings in `appsettings.json` can be overridden using normal ASP.NET environment variable syntax, for example `DiffCalc__MaxBatchSize=128`.

The database account needs these privileges:

```sql
GRANT SELECT ON osutrack.fetched_beatmaps TO 'diffcalc'@'localhost';
GRANT INSERT ON osutrack.fetched_beatmaps TO 'diffcalc'@'localhost';
```

The insert uses `INSERT IGNORE` so concurrent discovery of the same beatmap is harmless without granting `UPDATE`. If the service user connects from Docker over host networking, adjust the MariaDB host portion of the account accordingly.

## Internal API

`POST /v1/calculate` accepts up to 256 independent calculations. Accuracy is a percentage from 0 through 100. Omit `score` for difficulty-only work. Bridge callers also send a bounded `operation` value (`hiscores`, `simulate_single`, or `simulate_batch`) for workload-level metrics; it is optional for backwards compatibility and defaults to `unknown`.

```json
{
  "operation": "simulate_batch",
  "calculations": [
    {
      "request_id": "example",
      "beatmap_id": 75,
      "mods": [{ "acronym": "HD" }, { "acronym": "DT" }],
      "is_classic": true,
      "score": {
        "accuracy": 98.5,
        "max_combo": 500,
        "statistics": { "great": 490, "ok": 8, "meh": 1, "miss": 1 }
      }
    }
  ]
}
```

Every result carries its request ID and either difficulty/performance attributes or a per-item error. The envelope also reports the pinned osu! package version and difficulty calculator version, which makes algorithm drift visible to callers and dashboards.

Health endpoints are `GET /health/live` and `GET /health/ready`. The readiness check includes MySQL. Prometheus scrapes `GET http://127.0.0.1:4513/metrics`.

## Updating the PP algorithm

The two `ppy.osu.*` package versions in `DiffCalc.csproj` must stay identical. To update:

1. Identify the package version used by the live osu! deployment; do not assume the newest package is already live.
2. Update both package references and `OsuCalculationEngine.OsuGamePackageVersion`.
3. Run the build and contract/integration tests against representative classic and lazer scores.
4. Deploy diffcalc, verify health and calculation error/latency metrics, then roll the bridge if the contract changed.

## Local verification and deployment

```sh
dotnet restore
dotnet build --configuration Release
dotnet run
curl http://127.0.0.1:4512/health/ready
```

Deploy from this directory with phost. The supplied `.phost.toml` builds its own image, uses host networking, and reads secrets from the ignored `.env` file.
