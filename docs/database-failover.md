
# Database Replication, Recovery, and Failover

## 1. Purpose

This document describes the database replication architecture and the planned manual recovery procedure for the Distributed Banking Platform deployed on Kubernetes.

Each banking shard has a PostgreSQL primary database and a physical standby database. The standby provides a replacement database if the primary becomes unavailable.

This document distinguishes between:

- **Implemented and verified:** persistent storage, database configuration, physical streaming replication, and replication checks.
- **Planned recovery procedure:** primary failure handling, standby promotion, application rerouting, and restoration of replication.

Standby promotion and the complete failover procedure have not yet been experimentally verified.

## 2. Current Database Architecture

The platform uses three primary PostgreSQL databases:

| Database | Kubernetes StatefulSet | Database name | Purpose |
|---|---|---|---|
| Shard A primary | `shard-a-db` | `shard_a` | Stores Shard A accounts and transactions |
| Shard B primary | `shard-b-db` | `shard_b` | Stores Shard B accounts and transactions |
| Coordinator database | `coordinator-db` | `coordinator` | Stores distributed transaction records and decisions |

Each banking shard also has a PostgreSQL standby:

| Shard | Primary Service | Standby StatefulSet | Standby Service |
|---|---|---|---|
| Shard A | `shard-a-db` | `shard-a-standby` | `shard-a-standby` |
| Shard B | `shard-b-db` | `shard-b-standby` | `shard-b-standby` |

The primary and standby databases use separate PersistentVolumeClaims. Persistent storage allows database files to survive ordinary Pod deletion and recreation.

The Coordinator database currently has no standby configured.

### 2.1 Application Database Connections

The banking application replicas connect to their shard databases through Kubernetes Service names.

The current ConfigMaps contain:

**Shard A (`shard-a-config`):**
- `DB_HOST=shard-a-db`
- `DB_NAME=shard_a`
- `DB_PORT=5432`
- `DB_USER=bankuser`

**Shard B (`shard-b-config`):**
- `DB_HOST=shard-b-db`
- `DB_NAME=shard_b`
- `DB_PORT=5432`
- `DB_USER=bankuser`

Both application replicas belonging to the same shard use the same database configuration.

The application database connection helpers create new Psycopg connections for operations. They do not implement an explicit connection pool or automatic primary discovery.

### 2.2 Physical Replication

PostgreSQL physical streaming replication is configured between each shard primary and its standby.

The primary databases use:

- `wal_level=replica`
- `max_wal_senders=10`
- `max_replication_slots=10`
- SCRAM-SHA-256 authentication
- Replication slots named `shard_a_standby` and `shard_b_standby`

The standby initialization process uses `pg_basebackup` with streaming WAL transfer and a physical replication slot.

The following checks have been performed successfully:

1. Both standby Pods reached the Running and Ready state.
2. `pg_is_in_recovery()` returned `true` on both standbys.
3. The primary databases reported their standby connections in `pg_stat_replication` with state `streaming`.
4. A temporary test table and marker row created on each primary appeared on its standby.
5. Dropping the test table on the primary was also reflected on the standby.

These tests confirmed that physical replication was functioning at the time of verification.

## 3. Failure Detection and Recovery Principles

A primary database failure must not result in two writable databases for the same shard.

The recovery procedure must follow these principles:

1. Do not promote a standby until the old primary has been fenced.
2. Do not allow applications to write to the old primary after promotion.
3. Verify the standby's recovery state and available transaction data before promotion.
4. Preserve the Coordinator's durable COMMIT or ABORT decisions.
5. Do not assume that an unavailable transaction was aborted or committed.
6. Verify account balances and transaction records before resuming normal traffic.
7. Restore a primary-standby replication relationship after recovery.

A Pod becoming unavailable does not, by itself, prove that its PostgreSQL process has stopped. In particular, Kubernetes scaling or Pod deletion alone must not be treated as sufficient fencing when a node is unreachable.

The operator must establish that the old primary cannot continue accepting writes before promoting the standby.

## 4. Planned Manual Primary Recovery Procedure

The following procedure applies independently to Shard A and Shard B. The example names below use Shard A; the equivalent resources should be used when recovering Shard B.

### Step 1: Detect and Identify the Failure

Identify which primary database has failed.

Inspect the relevant StatefulSet, Pod, Service, and events. Determine whether the failure is caused by:

- PostgreSQL process failure;
- Pod failure;
- Node failure;
- Storage failure; or
- Network connectivity problems.

Do not immediately promote the standby merely because the primary is temporarily unreachable.

### Step 2: Stop New Banking Requests

Stop new requests from reaching the affected shard while recovery is being performed.

Allow existing operations to settle where possible. Inspect the Coordinator's transaction records and determine whether any cross-shard transactions are unresolved.

The Gateway and affected banking application replicas must not continue sending new operations to a database whose status is uncertain.

### Step 3: Fence the Old Primary

Ensure that the old primary cannot accept writes.

This may require stopping its PostgreSQL process, shutting down the affected Pod or node, or applying an appropriate infrastructure-level fencing mechanism.

Scaling down the old primary StatefulSet may be part of the procedure after its shutdown has been confirmed.

**Important:** Scaling a StatefulSet to zero is not, by itself, proof that an unreachable node has stopped running the old database process. Promotion must not proceed until the old primary is prevented from becoming a second writable primary.

### Step 4: Inspect the Standby

Before promotion, connect to the standby and check:

- Whether PostgreSQL is still in recovery mode.
- Whether WAL replay has progressed successfully.
- The latest received and replayed WAL positions.
- Whether the expected account and transaction records are present.
- Whether prepared transactions are present and consistent with available Coordinator decisions.

The recovery state can be checked with:

`SELECT pg_is_in_recovery();`

The expected result before promotion is `true`.

The operator must also compare the standby's transaction state with the Coordinator's durable transaction records wherever possible.

### Step 5: Evaluate Possible Data Loss

The current replication configuration is asynchronous.

Consequently, a primary may fail before its most recent WAL records have reached the standby. A standby may therefore be missing transactions that were recently committed on the primary.

The operator must identify this risk and record the available evidence about the last received and replayed WAL positions.

The system must not claim that every acknowledged transaction is guaranteed to survive a primary failure under the current asynchronous configuration.

If transaction outcomes cannot be established safely, keep the affected shard unavailable until the uncertainty is resolved. Do not manually modify account balances to compensate for missing or uncertain transactions.

### Step 6: Promote the Standby

Only after the old primary has been fenced and the standby has been inspected should the standby be promoted.

PostgreSQL provides the `pg_promote()` function for this purpose:

`SELECT pg_promote();`

After promotion, verify:

`SELECT pg_is_in_recovery();`

The expected result on the promoted database is `false`.

The promoted standby is now the writable primary for that shard.

### Step 7: Redirect Applications to the Promoted Database

The banking applications currently use the stable Service names `shard-a-db` and `shard-b-db`.

The planned approach is to preserve the existing application database host configuration and redirect the affected primary Service to the promoted standby Pod.

For example, after the old Shard A primary has been fenced and the Shard A standby has been promoted, the `shard-a-db` Service selector can be changed to select the promoted standby Pod.

The current primary and standby Pods use different labels:

- Old Shard A primary: `app: shard-a-db`
- Shard A standby: `app: shard-a-standby`

The equivalent labels for Shard B are:

- Old Shard B primary: `app: shard-b-db`
- Shard B standby: `app: shard-b-standby`

After changing the Service selector, inspect its endpoints to confirm that it points only to the promoted database.

The application Deployments for the affected shard must then be restarted so that their database connections are re-established.

This Service-selector approach is a planned routing strategy and must be tested before it is considered a verified failover mechanism.

### Step 8: Verify Database and Banking Consistency

Before restoring normal traffic, verify:

1. The promoted database is writable.
2. The old primary is not accepting writes.
3. The primary Service resolves to the promoted database only.
4. The affected banking application replicas can connect to the promoted database.
5. Account balances and account ownership remain correct.
6. Transaction records are consistent with the available recovery evidence.
7. No unexpected prepared transactions remain unresolved.
8. Cross-shard transactions follow the Coordinator's durable COMMIT or ABORT decisions.

Do not report a transfer as successful if its final outcome is still unknown.

Do not manually repair balances or independently infer a transaction's outcome from the absence of a prepared transaction.

### Step 9: Restore the Banking Services

After the database and application checks succeed, restore the affected banking application replicas and allow the Gateway to route requests to them.

Perform controlled tests of:

- Balance inquiries;
- Deposits and withdrawals;
- Same-shard transfers; and
- Cross-shard transfers.

Verify the resulting database records and balances, not only the HTTP responses.

### Step 10: Rebuild the Standby

After promotion, the old primary must not simply be restarted as an independent writable database.

It must be reinitialized as a standby of the new primary.

The preferred approach is to create a replacement standby using a fresh base backup from the promoted primary, with a separate persistent volume and an appropriately configured replication slot and authentication.

An existing old-primary data directory must not be reused blindly because it may belong to a different PostgreSQL timeline.

After the replacement standby starts, verify:

- `pg_is_in_recovery()` returns `true` on the standby.
- The new primary reports the standby in `pg_stat_replication`.
- The replication state is `streaming`.
- Test changes on the primary are reflected on the standby.

Only after these checks should the shard be considered to have restored its primary-standby configuration.

## 5. Coordinator and Two-Phase Commit Recovery

Database failover and distributed transaction recovery are related but separate responsibilities.

The Coordinator stores the durable decisions for cross-shard transactions. Its application code includes a recovery loop that checks transactions with `COMMIT_DECIDED` or `ABORT_DECIDED` status and retries sending the recorded decision to both participants.

The Coordinator Dockerfile starts the application using:

`CMD ["python", "app.py"]`

The application starts its recovery thread when executed directly. The recovery loop runs periodically and retries unresolved decisions.

This mechanism provides application-level 2PC recovery. It does not:

- Promote PostgreSQL standbys;
- Detect and fence failed database primaries;
- Redirect Kubernetes database Services; or
- Guarantee that asynchronous replication has preserved every recent transaction.

During database recovery, the operator must consult the Coordinator's durable decision before resolving an uncertain cross-shard transaction.

A recorded COMMIT decision must not be replaced with ABORT merely because a participant is temporarily unavailable.

If a transaction's outcome cannot be established, it must remain unresolved rather than being reported as successful or failed without evidence.

The Coordinator database is currently a single PostgreSQL instance and does not have a configured standby in this deployment.

## 6. Current Limitations and Risks

The current implementation has the following limitations:

1. **Manual promotion:** Standby promotion is not automated.
2. **Manual routing:** Kubernetes Service redirection must be performed by an operator.
3. **Asynchronous replication:** Recent committed transactions may not have reached the standby before primary failure.
4. **No automatic database discovery:** Banking applications use configured database Service names and do not independently identify the current primary.
5. **No explicit application retry or connection-pool failover:** Database connection recovery is not implemented as an automatic failover mechanism in the connection helper.
6. **Coordinator database availability:** The Coordinator database is a single instance without a standby.
7. **Replication configuration reproducibility:** Some primary `pg_hba.conf` replication changes were applied directly to the running database storage and must be incorporated into reproducible deployment configuration.

These limitations must be considered during failure experiments and when interpreting recovery results.

## 7. Conclusion

The project deployment has persistent PostgreSQL primary and standby databases for both banking shards, with physical streaming replication verified through database-level checks.

The current system does not provide automatic primary promotion or automatic database failover. The documented recovery design therefore uses controlled manual fencing, standby inspection, promotion, Service redirection, consistency verification, and standby reconstruction.

The procedure must be experimentally validated before it is considered a fully operational recovery mechanism.

