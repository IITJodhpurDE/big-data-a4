# Bigtable Assignment (A4) — Weather Data

Minimal instructions for running the Bigtable solution used in this assignment.

## What it does
- Creates a table `weather_g24ai2066` with column family `sensor`.
- Loads hourly weather readings from three CSVs: Portland (PDX), SeaTac (SEA), Vancouver (YVR).
- Runs four queries required by the assignment and prints results to stdout.

## Files
- `src/main/java/Bigtable.java` — main program and queries.
- `src/main/resources/data/seatac.csv`
- `src/main/resources/data/vancouver.csv`
- `src/main/resources/data/portland.csv`
- `Assig-4.md` — assignment brief.

## Configuration
- Project/Instance defaults are set in code:
  - Project ID: `capstone-473802`
  - Instance ID: `assignment-4`
- You can override via environment variables or system properties:
  - `BT_PROJECT_ID` or `-Dbt.projectId=...`
  - `BT_INSTANCE_ID` or `-Dbt.instanceId=...`
- Authentication uses Application Default Credentials (ADC):
  - Preferred: `gcloud auth application-default login`
  - Or set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON.

## How to run
This repo intentionally excludes build scaffolding from the assignment commit. Use one of the following:

1) IDE (recommended for reviewers)
- Open as a Java project.
- Ensure dependency `com.google.cloud:google-cloud-bigtable:2.39.0` is on the classpath.
- Run the `Bigtable` main class.

2) Gradle wrapper (if present locally)
```bash
./gradlew run
```

The program will:
- Delete the table only if it exists, then (re)create it.
- Load CSVs (first reading per hour only).
- Print the answers for queries 1–4.

## Notes
- Row key format: `station#date#hour` (e.g., `YVR#2022-10-01#10`).
- Data loader tolerates missing values (e.g., windspeed `M`).
- Safe if the table is deleted externally: on each run we check existence before deletion/creation.

## Troubleshooting
- ADC not found: run `gcloud auth application-default login` and retry.
- Permission denied: verify the account has Bigtable Admin and Reader roles for the instance.
