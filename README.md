# Async Task Service

A Spring Boot service that receives tasks through Kafka and runs them asynchronously with a configurable worker pool. It stores task progress and results in PostgreSQL, exposes them through a REST API, and supports multiple instances sharing one database.

## Database credentials

Set `DB_PASSWORD` or `POSTGRES_PASSWORD`, or supply `spring.datasource.password` through deployment configuration. There is no default database password: a missing, empty or blank password prevents startup. Docker Compose requires `POSTGRES_PASSWORD` and passes it to both PostgreSQL and the application. Keep real passwords in the environment or a secret store, outside the repository.

## API access

Spring Security uses HTTP Basic with two roles:

| Role | Read tasks | Manually retry FAILED tasks | Actuator / Swagger outside prod |
| --- | --- | --- | --- |
| USER | Own tasks only | Own tasks only | Denied |
| ADMIN | All tasks | All tasks | Allowed |

On the management port, `GET /actuator/health` and its `/readiness` and `/liveness` probes allow unauthenticated monitoring without exposing health details. Other Actuator endpoints require ADMIN. Authentication failures return `401`; insufficient roles or an invalid CSRF token return `403`. Unknown and other users' tasks both return the same `404`, including lookups by event ID. Task responses are not cached.

Accounts are configured under `api.security.users` in `application.yml`. The default configuration has one user and one administrator. Their passwords must come from `TASK_API_PASSWORD` and `TASK_API_ADMIN_PASSWORD`; there are no default passwords. Spring's `PasswordEncoder` hashes them before creating the in-memory accounts. Add more users to the same configuration list as needed. A configuration change requires an application restart. Never commit real passwords or log Authorization headers. Use HTTPS when the API is accessed outside localhost.

For local Docker Compose:

```sh
cp .env.example .env
# Fill POSTGRES_PASSWORD, TASK_API_PASSWORD and TASK_API_ADMIN_PASSWORD in .env, then:
docker compose up --build -d
docker compose port app 8080
```

Compose uses the `prod` profile by default. Only the API port is published, on loopback. Set `TASK_API_URL` to the address printed by the last command, including `http://`, and export `TASK_API_USERNAME` with the username from `.env`. Curl prompts for the password:

```sh
curl -u "$TASK_API_USERNAME" "$TASK_API_URL/api/tasks/42"
```

## Task ownership

Kafka task messages must include `ownerUsername`, exactly matching an authenticated API username:

```json
{
  "eventId": "11111111-1111-1111-1111-111111111111",
  "ownerUsername": "alice",
  "name": "My task",
  "duration": 1000
}
```

Ownership is stored in PostgreSQL and checked in the read and retry services. Reusing an event ID with a different owner is rejected. The migration keeps existing tasks without an owner; only administrators can access those tasks. New messages without an owner fail validation and go to DLT.

The Kafka producer is a trusted service: it must set `ownerUsername` from the authenticated user, rather than accept an arbitrary username from an external client. End users must not have direct Kafka write access. Compose is a local development setup; production Kafka needs restricted network access and TLS/SASL/ACLs, as described in the Obsidian task API notes. HTTP authentication does not authenticate Kafka publishers.

The consumer and DLT producer build their client configuration from Spring Boot `KafkaProperties`. Common `spring.kafka.security.protocol`, `spring.kafka.ssl.*` and `spring.kafka.properties.*` settings apply to both; settings under `spring.kafka.consumer` and `spring.kafka.producer` can customize each client separately. The service keeps its explicit transaction, acknowledgement and DLT serialization settings.

## Manual retry

`POST /api/tasks/{id}/retry` resets a FAILED task to NEW with a fresh attempt budget. Ownership is preserved. It returns `204` on success, `409 TASK_NOT_RETRYABLE` for other states, and `404` for unknown or inaccessible tasks. Successful retry requests log the actor and task ID.

CSRF protection remains enabled for HTTP Basic. First obtain a token and session cookie, then send both with the POST, using the same account:

```sh
curl -u "$TASK_API_USERNAME" -c /tmp/task-api.cookies "$TASK_API_URL/api/csrf"
# Copy the returned token into the header below:
curl -u "$TASK_API_USERNAME" -b /tmp/task-api.cookies \
  -H 'X-CSRF-TOKEN: <token>' -X POST "$TASK_API_URL/api/tasks/42/retry"
```

The cookie alone does not replace HTTP Basic credentials. Outside `prod`, authenticated administrators can inspect the API contract at `/v3/api-docs` or `/swagger-ui/index.html`.

## Management port and production profile

Actuator uses a separate HTTP server on port `9091` (`MANAGEMENT_SERVER_PORT` can change it). Actuator routes are absent from the API port, even with administrator credentials.

Activate `prod` with `SPRING_PROFILES_ACTIVE=prod` or `--spring.profiles.active=prod`. This profile disables both Swagger UI and OpenAPI JSON/YAML, including for administrators, and binds the management server to `127.0.0.1` by default. Set `TASK_API_ADMIN_USERNAME` to the configured administrator username, then query local metrics:

```sh
curl -u "$TASK_API_ADMIN_USERNAME" http://127.0.0.1:9091/actuator/metrics
```

In Compose, management listens on the container network at `http://app:9091`; this port is not published on the host. A monitoring container on `app-network` can use it with ADMIN credentials. Keep management on a private network and do not forward `/actuator` through the public gateway. Swagger can be enabled for local development by selecting a non-production profile; it still requires ADMIN.

## Worker dispatch

`TaskPoller` owns queue polling and worker loops. `MaintenanceScheduler` schedules recovery and cleanup. `WorkerLifecycle` owns executor admission, the shared stopping flag and graceful shutdown of workers and Kafka listeners.

Each poll (`worker.poll-delay-ms`, 1 second by default) starts at most one probe worker. A successful claim starts another worker if the pool has capacity; an empty claim ends that worker. Each worker owns an executor slot before claiming, and a full pool leaves tasks in PostgreSQL. An idle instance makes one claim query per poll, while a backlog can fill the worker pool. Existing workers may each make one final empty claim as the queue drains.

## Task handlers and at-least-once execution

`TaskHandler.handleStep(run, elapsedMillis, stepMillis)` performs one bounded step. `TaskStatusService` calls it repeatedly and manages progress, heartbeat, lease and final status. The default `DemoTaskHandler` only waits for `stepMillis`; replace it with your business implementation. Handlers must keep each call within the `stepMillis` budget, honor interruption and leave status updates to the service; set I/O timeouts accordingly.

Execution is **at-least-once**: work may repeat after a crash, lease expiry, an automatic retry or a manual retry. For example, a worker can create a report and crash before saving COMPLETED, so the next attempt creates the report again. Automatic retries stop when `max_attempts` is exhausted; successful completion is not guaranteed.

Make business effects idempotent: use a stable operation key such as `create-report:<eventId>`, with downstream deduplication or a database UNIQUE constraint committed atomically with the effect. `run.eventId()` stays the same across attempts and manual retries; `executionId` changes. Different business operations need distinct stable keys. `elapsedMillis` and `stepMillis` describe the current attempt and are not durable resume checkpoints or deduplication keys; a retry starts from the first step.

Execution ID and lease checks protect our task row, while external effects need their own deduplication. Keep operation keys for the supported replay/retry window, including Kafka/DLT replay: task cleanup removes the task row and its local event deduplication. The service supplies the handler contract; each business handler implements its own idempotency.

## Queue metrics

Each instance refreshes `task.queue.new` and `task.queue.oldest.ready.age` with a 60-second delay between refreshes (`worker.metrics-delay-ms`). Scrapes read the cached sample without querying PostgreSQL; a failed refresh exposes NaN. The query uses the literal `WHERE status = 'NEW'`, allowing PostgreSQL to use `idx_tasks_new_id` and exclude completed tasks from the scan. The exact count still scales with the number of NEW tasks.

## Task outcome counters

`task.completed` counts confirmed transitions to COMPLETED. `task.failed` counts confirmed transitions to FAILED, including exhausted handler attempts and lease recovery. Returning a task to NEW for retry does not increment either counter. Counters are updated after the transactional service returns; rejected updates from old workers and rolled-back changes do not count.

These are process-local counters of transitions, not the current number of rows in each status. They reset on application restart, and a manual retry can produce another terminal transition. A crash between commit and increment can miss an event, so use these counters for monitoring rather than a durable audit log.

## Worker heartbeats

Claiming a task grants a lease configured by `worker.lease-seconds` (30 seconds by default). Successful progress heartbeats renew it; a temporary database or transaction error does not discard the attempt while its last confirmed lease remains valid. The worker retries the heartbeat after the next work step. A rejected heartbeat (no matching active execution) stops work immediately.

The worker tracks the confirmed lease with a local monotonic deadline, starting before the claim transaction or heartbeat call so database/commit delays consume that budget. An unsuccessful heartbeat never extends it. Work stops at the deadline without writing a terminal status; the recovery job then handles the expired attempt. Database updates still require the matching execution ID and a live lease.

`worker.retry-backoff-seconds` sets the delay before automatic retry after a handler failure or lease expiry (30 seconds by default). Claim, heartbeat and the worker's local deadline share the configured lease; failure handling and recovery share the configured backoff. Lease must be positive; backoff may be zero. Settings take effect on application restart.

## Kafka listener health

`taskKafka` contributes to `/actuator/health` and `/actuator/health/readiness` on the management port. Both report DOWN/503 if there are no registered listeners, or if any listener or concurrent child consumer is stopped or paused. A running consumer with no assigned partitions is healthy: extra instances can legitimately be idle. Health details stay private.

The `task.kafka.listener.ready` gauge is 1 when all registered listeners are running and not paused, and 0 otherwise. Administrators can read it at `/actuator/metrics/task.kafka.listener.ready`; Prometheus exposes `task_kafka_listener_ready`.

Infrastructure database errors, including read-only SQLState `25006` during failover, connection failures, exhausted resources and system errors, pause consumption for `kafka.listener.database-retry-delay-ms` (10 seconds by default). The offset is left uncommitted and the same record is retried automatically after the pause. Data errors in SQLState classes 22/23 still go to DLT. Permanent authentication, schema and unclassified SQL errors stop the listener and require operator attention; health and the gauge expose that stopped state.

Use `/actuator/health/readiness` to decide whether an instance can accept work. Use `/actuator/health/liveness` for process restarts: database/listener outages do not make liveness fail, so a shared outage does not restart every instance.

## Task retention and cleanup

Cleanup runs every twelve hours (00:00 and 12:00 UTC). Configure `worker.cleanup.retention-days` for COMPLETED tasks (30 days), `failed-retention-days` for FAILED tasks (90 days), and `batch-size` (500 rows). Recent terminal tasks and all NEW/IN_PROGRESS tasks are preserved. After retention expires, a FAILED task can no longer be manually retried.

Each delete selects the oldest eligible rows with `FOR UPDATE SKIP LOCKED` and commits in its own transaction with a five-second timeout. Rows being retried or changed by another transaction are skipped. A run makes at most ten rounds for each terminal status (20 batches total); remaining rows are picked up on the next run. Interrupting cleanup stops new batches, and an error in a later batch does not roll back earlier committed batches.

ShedLock uses the shared PostgreSQL `shedlock` table and database time. The `task-cleanup` lock is held for at least one minute and has a ten-minute crash lease; another instance skips a run while it is held. The scheduling pool has four threads so cleanup does not monopolize the scheduler used for worker polling and listener resumes. ShedLock coordinates execution; cleanup remains safe to repeat after a crash.

Migration V4 builds the `(status, finished_at, id)` index with `CREATE INDEX CONCURRENTLY`, outside a transaction. It temporarily disables `statement_timeout` only on the migration connection and restores its previous value in `finally`. Flyway uses a session-level PostgreSQL advisory lock for this migration. If a concurrent index build fails and leaves an invalid index, inspect/remove that invalid index before Flyway repair and retry.
