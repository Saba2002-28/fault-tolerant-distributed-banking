# Fault-Tolerant Distributed Banking on Kubernetes

A fault-tolerant distributed banking platform developed as part of **Distributed Systems – Assignment 3**. This project extends the original Assignment 2 banking system by deploying it on Kubernetes (Minikube) and introducing persistent database storage, PostgreSQL primary-standby replication, failure recovery, workload-driven autoscaling, and rolling updates.

The system uses two independent banking shards, replicated banking application services, a shard-aware Gateway, and a Transaction Coordinator implementing the Two-Phase Commit (2PC) protocol.

## Overview

The main objective of this project is to extend an existing distributed banking platform while preserving its transaction correctness guarantees under application failures, database failures, concurrent operations, scaling, and deployment updates.

The platform supports:

- Account balance inquiries
- Deposits
- Withdrawals
- Same-shard transfers
- Cross-shard transfers
- Concurrent banking operations
- Idempotent transaction requests
- Distributed transaction coordination using 2PC
- Persistent PostgreSQL databases with primary-standby replication
- Kubernetes-based application deployment and recovery

Clients communicate exclusively with the Gateway. They do not need to know which shard, application replica, or database handles their requests.

## Architecture

The application retains the distributed architecture developed in Assignment 2. Assignment 3 extends its deployment and infrastructure using Kubernetes resources.

### System components

| Component | Responsibility |
|---|---|
| Banking Client | Provides the interface for banking operations and communicates with the Gateway. |
| Gateway | Determines account ownership, routes requests to the appropriate shard, and selects an available application replica. |
| Transaction Coordinator | Coordinates cross-shard transactions using the Two-Phase Commit protocol and stores transaction states persistently. |
| Bank A1 and A2 | Application replicas responsible for accounts belonging to Shard A. |
| Bank B1 and B2 | Application replicas responsible for accounts belonging to Shard B. |
| Shard A Primary Database | Authoritative PostgreSQL database for Shard A. |
| Shard A Standby Database | Receives replicated WAL data from the Shard A primary. |
| Shard B Primary Database | Authoritative PostgreSQL database for Shard B. |
| Shard B Standby Database | Receives replicated WAL data from the Shard B primary. |
| Coordinator Database | Persistently stores transaction states and coordination decisions. |

### Logical application architecture

```text
                         Banking Client
                               |
                               v
                            Gateway
                               |
                 +-------------+-------------+
                 |                           |
              Shard A                     Shard B
                 |                           |
           +-----+-----+               +-----+-----+
           |           |               |           |
         Bank A1    Bank A2          Bank B1    Bank B2
           |           |               |           |
           +-----+-----+               +-----+-----+
                 |                           |
           Shard A Primary             Shard B Primary
                 |                           |
           WAL Replication              WAL Replication
                 |                           |
           Shard A Standby             Shard B Standby

                            Gateway
                               |
                               v
                    Transaction Coordinator
                               |
                               v
                     Coordinator Database
```

The diagram represents the logical application and database relationships. Kubernetes Services provide internal service discovery and communication between the application components.

### Kubernetes deployment architecture

The system is deployed in the `banking-assignment3` namespace on Minikube.

The deployment uses the following Kubernetes resource types:

| Kubernetes resource | Purpose |
|---|---|
| Deployment | Manages stateless application components and their replicas. |
| StatefulSet | Manages PostgreSQL database instances with stable identities and persistent storage. |
| Service | Provides stable network endpoints for application and database communication. |
| Headless Service | Provides database Pod discovery through stable DNS identities. |
| PersistentVolumeClaim (PVC) | Requests persistent storage for PostgreSQL data. |
| Secret | Stores database credentials. |
| ConfigMap | Provides application configuration. |
| HorizontalPodAutoscaler (HPA) | Adjusts application replica counts based on resource utilization. |
| NodePort Service | Exposes the Gateway outside the Kubernetes cluster. |

The Gateway is exposed through the `gateway-external` NodePort Service. Internal components communicate through Kubernetes Services rather than relying on individual Pod IP addresses.

## Technologies

- Python – Banking application, Gateway, Coordinator, and client logic
- Docker – Application containerization
- Kubernetes – Container orchestration
- Minikube – Local Kubernetes cluster
- PostgreSQL 17 – Banking databases and transaction coordination storage
- StatefulSets – Database workload management
- PersistentVolumeClaims – Persistent database storage
- Kubernetes Services – Service discovery and communication
- Horizontal Pod Autoscaler – Workload-driven application scaling
- HTTP/REST APIs – Communication between system components
- Two-Phase Commit (2PC) – Distributed transaction coordination
- PostgreSQL Streaming Replication – Primary-standby database replication

## Core Features

### 1. Data Sharding

The system uses deterministic range-based sharding.

Each account belongs to exactly one authoritative shard.

| Shard | Account range | Primary database | Standby database |
|---|---|---|---|
| Shard A | A0001–A1000 | `shard-a-db-0` | `shard-a-standby-0` |
| Shard B | A1001–A2000 | `shard-b-db-0` | `shard-b-standby-0` |

The Gateway determines the responsible shard before routing a request to an application replica.

### 2. Application Replication

Each banking shard has two application replicas:

- Shard A: Bank A1 and Bank A2
- Shard B: Bank B1 and Bank B2

The application replicas belonging to the same shard share its authoritative database.

The application workloads are managed by Kubernetes Deployments. Kubernetes Services provide stable endpoints for communication with the replicas.

### 3. Shard-aware Gateway and Service Discovery

The Gateway performs two routing steps:

1. Determines which shard owns the requested account.
2. Selects an available application replica within that shard.

Kubernetes Services provide stable communication endpoints even when individual Pods are replaced and receive new IP addresses.

The Gateway is exposed externally through a NodePort Service, while internal communication uses ClusterIP and headless Services as appropriate.

### 4. Database Persistence

Each database instance is managed through a StatefulSet and uses a PersistentVolumeClaim for storage.

The deployment includes five PostgreSQL instances:

- Shard A primary
- Shard A standby
- Shard B primary
- Shard B standby
- Coordinator database

Each database has a persistent storage claim.

The database data is stored independently of the lifecycle of its PostgreSQL Pod. This allows a replacement Pod to reuse its associated persistent storage.

### 5. Primary-Standby Database Replication

Each banking shard has a PostgreSQL primary and a standby database.

The system uses PostgreSQL streaming replication to transfer Write-Ahead Log (WAL) records from the primary to the standby.

The replication configuration includes:

- Primary database instances
- Standby database instances
- WAL streaming
- PostgreSQL replication slots
- Standby initialization using `pg_basebackup`
- Recovery configuration for standby instances

Replication status can be checked through PostgreSQL system views, including `pg_stat_replication` on the primary and `pg_stat_wal_receiver` on the standby.

The deployed configuration uses asynchronous replication. Consequently, the standby may temporarily lag behind the primary, and asynchronous replication alone does not guarantee zero data loss following every possible primary failure.

### 6. Database Failover and Recovery

The project includes a controlled primary database failure and recovery experiment.

During the Shard A failure experiment:

1. The original primary database was stopped.
2. The standby database was promoted using PostgreSQL's `pg_promote()`.
3. The promoted database was verified as writable.
4. The application was reconnected to the promoted database.
5. Banking operations were tested after recovery.
6. The original primary was rebuilt as a standby.
7. Replication was restored and verified.

The experiment demonstrated controlled database failover and restoration of replication.

**Limitation:** The tested failover procedure was manually controlled. It should not be interpreted as fully automatic primary failure detection and promotion.

### 7. Self-healing and Application Availability

Kubernetes Deployments and StatefulSets maintain the desired number of application and database Pods.

When a managed Pod terminates, Kubernetes can create a replacement according to the workload's desired state.

Readiness probes help Kubernetes determine whether a container is ready to receive traffic. Liveness probes can be used to detect and restart unhealthy containers.

Application availability and database availability are separate concerns: replacing an application Pod does not, by itself, recover an unavailable database.

### 8. Autoscaling

The project uses Horizontal Pod Autoscaling to adjust application replica counts according to CPU utilization.

The HPA experiment was performed on the `bank-a1` Deployment, with:

- Minimum replicas: 1
- Maximum replicas: 2
- CPU utilization target: 50%

A workload generator was used to produce requests and observe scaling behavior.

The experiment verified that the application replica count increased under load and that banking correctness was preserved during subsequent transaction tests.

### 9. Rolling Updates and Rollback

Kubernetes Deployments support rolling updates to replace application Pods gradually.

The project tested an application update under concurrent banking load and also tested recovery from an unsuccessful image update.

The rollback procedure used Kubernetes Deployment rollout history and `kubectl rollout undo` to restore the previous working version.

Transaction and account-balance checks were performed to verify that the deployment change did not produce incorrect banking results.

### 10. Idempotent Client Requests

Mutating banking operations require an `Idempotency-Key` HTTP header.

The key identifies a logical transaction and allows repeated requests to be recognized as retries rather than independent operations.

This mechanism helps prevent duplicate transaction effects when a client resends a request after a timeout or uncertain response.

A retry of the same logical transaction must reuse its original idempotency key. A new transaction must use a new key.

### 11. Local and Distributed Transactions

Transfers between accounts belonging to the same shard are performed using a local database transaction.

Transfers between accounts belonging to different shards are coordinated using the Two-Phase Commit protocol.

This preserves the transaction coordination design inherited from Assignment 2.

## Distributed Transactions and Recovery

The system uses Two-Phase Commit (2PC) for cross-shard transfers.

### Phase 1: Prepare

- The Coordinator generates a unique transaction ID.
- It sends PREPARE requests to both participating shards.
- Each participant validates the transaction and prepares its changes.
- Each participant returns a YES or NO vote.

### Phase 2: Commit or Abort

- If both participants vote YES, the Coordinator durably records a COMMIT decision and sends COMMIT requests.
- If any participant votes NO, the Coordinator records an ABORT decision and sends ABORT requests.
- The transaction is marked COMPLETED after the participants confirm the final decision.

### Transaction states

The Coordinator maintains persistent transaction states, including:

- INITIATED
- PREPARING
- PREPARED
- COMMIT_DECIDED
- ABORT_DECIDED
- COMPLETED

These states allow the Coordinator to recover unresolved transactions after an interruption.

A durable COMMIT decision must not be replaced by ABORT during recovery.

## Concurrency Control and Correctness

The banking application supports concurrent requests through multiple application replicas sharing their shard's database.

The system's correctness requirements include:

- Each account has exactly one authoritative shard.
- At most one database instance per shard is writable at a time.
- Acknowledged transactions must be durable.
- Retried requests must not create duplicate transaction effects.
- Cross-shard transfers must not remain permanently partially committed.
- Unresolved distributed transactions must remain recoverable.
- A successful response must not be reported when the transaction outcome is unknown.
- Account balances must not require manual repair after recovery.

The experiments use account balances, transaction records, and database state to verify correctness rather than relying exclusively on HTTP responses or Kubernetes Pod status.

## Project Deployment

### Prerequisites

Install the following software:

- Windows 11 or a compatible Linux environment
- WSL2 with Ubuntu (for the Windows-based setup)
- Docker Desktop
- Minikube
- kubectl
- Python 3
- Required Python packages listed in the project requirements files

The following instructions assume that Minikube is running through the Docker driver inside WSL2.

### 1. Clone the repository

```bash
git clone https://github.com/<your-github-username>/fault-tolerant-distributed-banking.git
cd fault-tolerant-distributed-banking
```

### 2. Start Minikube

```bash
minikube start --driver=docker --memory=2048 --cpus=2
```

Verify the cluster:

```bash
minikube status
kubectl get nodes
```

### 3. Create the Kubernetes namespace

```bash
kubectl create namespace banking-assignment3
```

If the namespace already exists, Kubernetes will report that it already exists. You can continue with the deployment.

### 4. Configure environment variables and Secrets

Configure the required database credentials and application settings using the provided Kubernetes configuration files.

Database passwords and other sensitive credentials should be supplied through Kubernetes Secrets.

Do not commit real passwords, API keys, or local `.env` files to GitHub. If an environment file is required for documentation, provide a `.env.example` containing placeholder values only.

### 5. Deploy the Kubernetes resources

Apply the Kubernetes manifests included in the repository.

For example, if the manifests are organized in a `k8s/` directory:

```bash
kubectl apply -f k8s/ -n banking-assignment3
```

The exact directory should match the repository's manifest organization.

### 6. Verify the deployment

Check the Pods:

```bash
kubectl get pods -n banking-assignment3
```

Check the Services:

```bash
kubectl get services -n banking-assignment3
```

Check the StatefulSets:

```bash
kubectl get statefulsets -n banking-assignment3
```

Check the persistent volume claims:

```bash
kubectl get pvc -n banking-assignment3
```

Check the Horizontal Pod Autoscalers:

```bash
kubectl get hpa -n banking-assignment3
```

### 7. Access the Gateway

The Gateway is exposed through the `gateway-external` NodePort Service.

To retrieve its Minikube URL:

```bash
minikube service gateway-external -n banking-assignment3 --url
```

Alternatively, create a local port-forward:

```bash
kubectl port-forward service/gateway 5000:5000 -n banking-assignment3
```

Keep the port-forward terminal running while using the client.

### 8. Run the Banking Client

Open another terminal in the project directory:

```bash
python3 client.py
```

The client provides an interactive menu for balance inquiries, deposits, withdrawals, and transfers.

The Gateway URL is configured in the client source code.

### 9. Inspect application logs

Gateway:

```bash
kubectl logs deployment/gateway -n banking-assignment3
```

Transaction Coordinator:

```bash
kubectl logs deployment/coordinator -n banking-assignment3
```

Banking application replicas:

```bash
kubectl logs deployment/bank-a1 -n banking-assignment3
kubectl logs deployment/bank-a2 -n banking-assignment3
kubectl logs deployment/bank-b1 -n banking-assignment3
kubectl logs deployment/bank-b2 -n banking-assignment3
```

Use the actual Deployment names from the Kubernetes manifests if they differ.

### 10. Stop Minikube

```bash
minikube stop
```

To delete the Minikube cluster:

```bash
minikube delete
```

**Warning:** Deleting the cluster can remove its Kubernetes resources and locally managed persistent storage. Back up any important database data before doing so.

## Database Verification

### Connect to a primary database

Shard A:

```bash
kubectl exec -it shard-a-db-0 -n banking-assignment3 -- sh
```

Then:

```bash
psql -U bankuser -d shard_a
```

Shard B:

```bash
kubectl exec -it shard-b-db-0 -n banking-assignment3 -- sh
```

Then:

```bash
psql -U bankuser -d shard_b
```

### Verify primary and standby roles

Run the following SQL command inside each PostgreSQL session:

```sql
SELECT pg_is_in_recovery();
```

Expected results under normal operation:

| Database Pod | Expected result | Role |
|---|---|---|
| `shard-a-db-0` | `false` | Primary |
| `shard-a-standby-0` | `true` | Standby |
| `shard-b-db-0` | `false` | Primary |
| `shard-b-standby-0` | `true` | Standby |

### Verify replication on the primary

Run on each primary:

```sql
SELECT
    application_name,
    state,
    sync_state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn
FROM pg_stat_replication;
```

A connected standby should normally appear with:

```text
state = streaming
```

### Verify replication on the standby

Run on each standby:

```sql
SELECT
    status,
    written_lsn,
    flushed_lsn,
    latest_end_lsn
FROM pg_stat_wal_receiver;
```

A healthy streaming connection should report:

```text
status = streaming
```

## Testing and Experiments

The assignment specifies seven experiments to evaluate fault tolerance, transaction correctness, and Kubernetes behavior.

The following table summarizes the experiments performed and their evidence.

| Experiment | Description | Observed result |
|---|---|---|
| 1. Application Pod Failure | Verify that healthy application replicas continue serving requests after a Pod failure. | Requires final documented evidence. |
| 2. Primary Database Failure | Promote the Shard A standby, verify banking operations, rebuild the former primary as a standby, and restore replication. | Controlled failover and replication recovery verified. |
| 3. Concurrent Transactions | Execute 50 deposits and 50 withdrawals concurrently against account A0001. | All 100 operations succeeded; balance and shard totals remained consistent. |
| 4. Coordinator Interruption | Interrupt the Coordinator after a durable COMMIT decision and verify recovery. | Transaction recovered and both participants committed; no unresolved prepared transactions remained. |
| 5. Autoscaling Under Load | Generate workload and observe HPA behavior on Bank A1. | HPA scaled from 1 to 2 replicas while banking correctness was preserved. |
| 6. Rolling Update Under Load | Perform an application update during concurrent deposits and test rollback after an invalid image update. | All 180 deposits succeeded; rollback restored the previous working version. |
| 7. Repeated Client Requests | Retry sequential and concurrent requests using idempotency keys. | No duplicate transaction effects were observed. |

### Selected experimental results

#### Concurrent transactions

- 50 successful deposits
- 50 successful withdrawals
- 100 total operations
- 10 concurrent workers
- No failed operations
- Final account balance remained unchanged
- Primary and standby balances and shard totals matched

#### Primary database failure and recovery

- Shard A primary was stopped.
- The standby was promoted using `pg_promote()`.
- The promoted database was verified as writable.
- Banking operations were tested after failover.
- The original primary was rebuilt as a standby.
- Replication was restored.
- Final WAL LSN positions matched.

The experiment demonstrated controlled recovery rather than automatic failover.

#### Coordinator interruption

- The Coordinator was interrupted after recording a durable COMMIT decision.
- Kubernetes recreated the Coordinator Pod.
- The Coordinator recovered the transaction from its persistent state.
- Both participating shards completed the transaction.
- No unresolved prepared transactions remained.

#### Autoscaling under load

The Bank A1 HPA experiment used a read-only workload for 120 seconds.

Recorded results:

- 2,612 successful HTTP requests
- 21.68 requests per second
- Average response time: 460.61 ms
- Maximum response time: 2,873.61 ms
- Maximum observed CPU utilization: 324%
- Replica count increased from 1 to 2

A subsequent correctness check executed:

- 100 deposits
- 100 withdrawals

All operations succeeded, and the account balances remained consistent.

#### Rolling update and rollback

- 180 deposits of $0.01 were submitted during the rolling update experiment.
- All requests succeeded.
- The final account balance was $466.90.
- An invalid image update produced an ImagePullBackOff condition.
- `kubectl rollout undo` restored the previous working application version.

#### Repeated client requests

Sequential and concurrent retries were tested using idempotency keys.

The verification checked that repeated requests did not create additional transaction records or duplicate balance changes.

## Security and Configuration

The Kubernetes deployment uses ConfigMaps and Secrets to separate application configuration from sensitive credentials.

The assignment also requires at least one enforced communication restriction, such as a Kubernetes NetworkPolicy.

Network restrictions should be documented together with an actual connectivity test demonstrating both permitted and denied communication paths.

## Limitations and Future Improvements

The following limitations should be considered when interpreting the implementation:

- Database failover was tested using a controlled manual promotion procedure, not fully automatic failover management.
- PostgreSQL replication is asynchronous, so a primary failure can potentially expose a replication lag window.
- The demonstrated HPA workload test focused on Bank A1 rather than every application Deployment.
- The core implementation uses a local Minikube cluster rather than a production multi-node Kubernetes environment.
- Additional monitoring, automated failover orchestration, and disaster-recovery mechanisms could further improve resilience.

---

Project Structure

The main project components are organized into the following directories and files:

```text

distributed-banking-platform/
│
├── banking-shard/
│   ├── app/
│   │   ├── database.py
│   │   ├── main.py
│   │   ├── operations.py
│   │   └── __init__.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── config/
│   └── shard-map.json
│
├── coordinator/
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── schema.sql
│
├── database/
│   ├── shard-a.sql
│   └── shard-b.sql
│
├── docs/
│   ├── database-failover.md
│   └── architecture.md
│
├── gateway/
│   ├── app.py
│   ├── shard_router.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── k8s/
│   ├── application/
│   │   ├── services/
│   │   │  ├── gateway-external.md
│   │   │  └── services.md
│   │   └── deployments.md
│   └── database/
│   │   ├── databases.md
│   │   ├── shard-a-rebuilt-standby.md
│   │   └── standbys.md
│
├── testscripts/
│   ├── experiment1/
│   │   ├── experiment1_pod_failure.py
│   │   └── guide.txt
│   │
│   ├── experiment2/
│   │   ├── experiment2_shard-a-failover.sh
│   │   └── experiment2_guide.txt
│   │
│   ├── experiment3/
│   │   ├── experiment_guide.txt
│   │   └── experiment3_concurrent_transactions.py
│   │
│   ├── experiment4/
│   │   └── Experiment4_guide.txt
│   │       
│   ├── experiment5/
│   │   ├── concurrency_test_wd.py
│   │   ├── day5_autoscaling_load_test.py
│   │   └── experiment 5 guide.txt
│   │
│   ├── experiment6/
│   │   ├── rolling_wrtie_test.py
│   │   └── experiment6 guide.txt
│   │
│   └── experiment7/
│       └── experiment7_idempotency.py
│
├── .gitignore
├── client.py
├── docker-compose.yml
└── README.md

```


## Repository Contents

The repository contains the source code and configuration required to build, deploy, and test the distributed banking platform.

The main deliverables include:

- Banking application source code
- Gateway and Transaction Coordinator source code
- Banking client
- Dockerfiles and application dependencies
- Kubernetes manifests
- Database initialization and replication configuration
- Persistent storage configuration
- Kubernetes Services and workload definitions
- Autoscaling and resource configuration
- Test scripts and experiment evidence
- Project documentation

Refer to the repository directories for the exact organization of these files.
Links to Docker Hub repositories:


## Acknowledgements

Developed as an academic project for the Distributed Systems course.
