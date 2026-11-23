#!/usr/bin/env bash
#
# Day 4 / Task 4 — Assignment 3, Experiment 2
# Controlled Shard A primary-database failure, promotion, rebuild, and
# normalization back to the canonical primary/standby names.
#
# Run from the Assignment 3 project root:
#   chmod +x scripts/day4_experiment2_shard_a_failover.sh
#   ./scripts/day4_experiment2_shard_a_failover.sh
#
# This is a controlled/manual failover demonstration, not automatic failover.
# It makes two real deposits (10 and 5) on every successful run.
# It clears PostgreSQL data-directory CONTENTS on two explicitly named PVCs,
# but never deletes either PVC.
#
set -Eeuo pipefail

NAMESPACE="banking-assignment3"
PROJECT_ROOT="$(pwd)"
MANIFEST="k8s/database/shard-a-rebuilt-standby.yaml"
BACKUP_DIR="${HOME}/assignment3-db-backups"
STAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_BEFORE="${BACKUP_DIR}/shard_a_before_failover_${STAMP}.dump"
BACKUP_REBUILD="${BACKUP_DIR}/shard_a_new_primary_before_rebuild_${STAMP}.dump"
ACCOUNT_ID="A0001"
PHASE="initialization"
PF_PIDS=()
PF_LOGS=()

log()  { printf '\n\033[1;34m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\n\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup_port_forwards() {
  local pid
  for pid in "${PF_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  for f in "${PF_LOGS[@]:-}"; do
    [[ -n "$f" ]] && rm -f "$f"
  done
}
on_exit() {
  local rc=$?
  cleanup_port_forwards
  if [[ $rc -ne 0 ]]; then
    warn "Script stopped during phase: ${PHASE} (exit code ${rc})."
    warn "The script has NOT deleted any PVC. Do not simply rerun it."
    warn "First inspect Pods, StatefulSets, Services/endpoints, PVCs, and PostgreSQL roles."
    warn "The database may intentionally be left unavailable rather than risk two writable primaries."
  fi
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

need() { command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"; }

sql() {
  local pod="$1"; shift
  local database="shard_a"
  [[ "$pod" == shard-b-* ]] && database="shard_b"
  kubectl exec -n "$NAMESPACE" "$pod" -- \
    psql -X -v ON_ERROR_STOP=1 -U bankuser -d "$database" -tA -c "$*"
}
wait_pod_ready() {
  local pod="$1"
  log "Waiting for Pod ${pod} to become Ready..."
  kubectl wait --for=condition=Ready "pod/${pod}" -n "$NAMESPACE" --timeout=300s
}
wait_deployment() {
  local deployment="$1"
  log "Waiting for Deployment ${deployment} rollout..."
  kubectl rollout status "deployment/${deployment}" -n "$NAMESPACE" --timeout=300s
}
wait_statefulset() {
  local sts="$1"
  log "Waiting for StatefulSet ${sts} rollout..."
  kubectl rollout status "statefulset/${sts}" -n "$NAMESPACE" --timeout=420s
}
assert_role() {
  local pod="$1" expected="$2" actual
  actual="$(sql "$pod" "SELECT pg_is_in_recovery();")"
  [[ "$actual" == "$expected" ]] || die "${pod}: expected pg_is_in_recovery()=${expected}, got '${actual}'."
  log "${pod}: PostgreSQL role check passed (pg_is_in_recovery()=${actual})."
}
assert_replication_streaming() {
  local pod="$1" rows
  rows="$(sql "$pod" "SELECT count(*) FROM pg_stat_replication WHERE state='streaming';")"
  [[ "$rows" =~ ^[1-9][0-9]*$ ]] || die "${pod}: no streaming standby found."
  log "${pod}: ${rows} streaming replication connection(s) found."
}
wait_endpoint() {
  local service="$1" expected_label="$2" i output
  for i in $(seq 1 60); do
    output="$(kubectl get endpoints "$service" -n "$NAMESPACE" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)"
    if [[ -n "$output" ]]; then
      log "Service ${service} has endpoint(s): ${output}"
      return 0
    fi
    sleep 2
  done
  die "Service ${service} has no ready endpoint after waiting."
}
start_port_forward() {
  local service="$1" local_port="$2" remote_port="$3"
  local logfile
  logfile="$(mktemp)"
  PF_LOGS+=("$logfile")
  kubectl port-forward -n "$NAMESPACE" "service/${service}" "${local_port}:${remote_port}" >"$logfile" 2>&1 &
  local pid=$!
  PF_PIDS+=("$pid")
  for _ in $(seq 1 40); do
    if curl -sS --max-time 1 "http://127.0.0.1:${local_port}/health" >/dev/null 2>&1; then
      log "Port-forward to ${service} is ready on localhost:${local_port}."
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$logfile" >&2
      die "Port-forward for ${service} exited unexpectedly."
    fi
    sleep 1
  done
  cat "$logfile" >&2
  die "Timed out waiting for ${service} port-forward on port ${local_port}."
}
stop_port_forwards() {
  cleanup_port_forwards
  PF_PIDS=()
  PF_LOGS=()
}

scale_down_applications() {
  kubectl scale deployment gateway coordinator bank-a1 bank-a2 \
    --replicas=0 -n "$NAMESPACE"
  for d in gateway coordinator bank-a1 bank-a2; do
    kubectl wait --for=delete pod -l "app=${d}" -n "$NAMESPACE" --timeout=180s 2>/dev/null || true
  done
}
scale_up_shard_a_apps() {
  kubectl scale deployment bank-a1 bank-a2 --replicas=1 -n "$NAMESPACE"
  wait_deployment bank-a1
  wait_deployment bank-a2
}
clean_pvc_data_dir() {
  local pod_name="$1" pvc_name="$2"
  local confirmation
  [[ "$pvc_name" == "data-shard-a-db-0" || "$pvc_name" == "data-shard-a-standby-0" ]] ||
    die "Refusing to clean unexpected PVC name: ${pvc_name}"
  kubectl get pvc "$pvc_name" -n "$NAMESPACE" >/dev/null ||
    die "PVC ${pvc_name} does not exist."
  [[ "$(kubectl get pvc "$pvc_name" -n "$NAMESPACE" -o jsonpath='{.status.phase}')" == "Bound" ]] ||
    die "PVC ${pvc_name} is not Bound."
  if kubectl get pod "$pod_name" -n "$NAMESPACE" >/dev/null 2>&1; then
    die "Temporary Pod ${pod_name} already exists; inspect it before continuing."
  fi

  warn "About to erase files INSIDE ${pvc_name} at /data/pgdata."
  warn "The PVC object itself will NOT be deleted."
  read -r -p "Type CLEAN ${pvc_name} to continue: " confirmation
  [[ "$confirmation" == "CLEAN ${pvc_name}" ]] || die "PVC cleanup was not confirmed."

  kubectl run "$pod_name" -n "$NAMESPACE" --image=postgres:17 --restart=Never \
    --overrides="{\"spec\":{\"securityContext\":{\"runAsUser\":999,\"runAsGroup\":999,\"fsGroup\":999},\"containers\":[{\"name\":\"cleaner\",\"image\":\"postgres:17\",\"imagePullPolicy\":\"IfNotPresent\",\"command\":[\"bash\",\"-c\",\"set -e; echo 'Before cleanup:'; ls -la /data/pgdata; find /data/pgdata -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; echo 'After cleanup:'; ls -la /data/pgdata\"],\"volumeMounts\":[{\"name\":\"data\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"data\",\"persistentVolumeClaim\":{\"claimName\":\"${pvc_name}\"}}]}}"
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${pod_name}" -n "$NAMESPACE" --timeout=180s
  kubectl logs "$pod_name" -n "$NAMESPACE"
  kubectl delete pod "$pod_name" -n "$NAMESPACE" --wait=true
}

show_shard_a_state() {
  log "Shard A account aggregates:"
  kubectl exec -n "$NAMESPACE" shard-a-db-0 -- psql -U bankuser -d shard_a -c \
    "SELECT COUNT(*) AS account_count, SUM(balance) AS total_balance, MIN(balance) AS minimum_balance, MAX(balance) AS maximum_balance FROM accounts;"
  log "Shard A participant/transfer/prepared-transaction counts:"
  kubectl exec -n "$NAMESPACE" shard-a-db-0 -- psql -U bankuser -d shard_a -c \
    "SELECT 'participant_decisions' AS table_name, COUNT(*) AS record_count FROM participant_decisions UNION ALL SELECT 'prepared_transfers', COUNT(*) FROM prepared_transfers UNION ALL SELECT 'pg_prepared_xacts', COUNT(*) FROM pg_prepared_xacts;"
  kubectl exec -n "$NAMESPACE" shard-a-db-0 -- psql -U bankuser -d shard_a -c \
    "SELECT decision, COUNT(*) AS record_count FROM participant_decisions GROUP BY decision ORDER BY decision;"
}

# ------------------------------ Preflight ------------------------------
PHASE="preflight"
need kubectl
need curl
[[ -f "$MANIFEST" ]] || die "Run this script from the Assignment 3 project root; missing ${MANIFEST}."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || die "Namespace ${NAMESPACE} does not exist."
kubectl get nodes
kubectl get pods -n "$NAMESPACE"
kubectl get pvc -n "$NAMESPACE"

for pvc in data-coordinator-db-0 data-shard-a-db-0 data-shard-a-standby-0 data-shard-b-db-0 data-shard-b-standby-0; do
  phase="$(kubectl get pvc "$pvc" -n "$NAMESPACE" -o jsonpath='{.status.phase}')"
  [[ "$phase" == "Bound" ]] || die "PVC ${pvc} is ${phase}, not Bound."
done

assert_role shard-a-db-0 f
assert_role shard-a-standby-0 t
assert_role shard-b-db-0 f
assert_role shard-b-standby-0 t
assert_replication_streaming shard-a-db-0
assert_replication_streaming shard-b-db-0

# Ensure all app/database components are currently available before the experiment.
for pod in shard-a-db-0 shard-a-standby-0 shard-b-db-0 shard-b-standby-0 coordinator-db-0; do
  kubectl wait --for=condition=Ready "pod/${pod}" -n "$NAMESPACE" --timeout=180s
done
for d in bank-a1 bank-a2 bank-b1 bank-b2 coordinator gateway; do
  wait_deployment "$d"
done

mkdir -p "$BACKUP_DIR"
[[ ! -e "$BACKUP_BEFORE" && ! -e "$BACKUP_REBUILD" ]] || die "A backup filename already exists; refusing to overwrite."
cat <<EOF

This run will:
  1. Back up Shard A before failure.
  2. Stop Shard A primary, promote its standby, and test it.
  3. Make a real deposit of 10 through bank-a1.
  4. Rebuild the old primary as a standby.
  5. Test Coordinator and Gateway; make a real deposit of 5 through Gateway.
  6. Promote the rebuilt node back to canonical primary.
  7. Rebuild the other node as canonical standby.
  8. Restore bank-a1, bank-a2, Coordinator, and Gateway.

It will clear only the contents of these two data directories, after separate
interactive confirmations and backups:
  - data-shard-a-db-0
  - data-shard-a-standby-0

It will NOT delete PVCs. Each successful run increases ${ACCOUNT_ID}'s balance
by 15 (10 + 5). Backup files will be saved under:
  ${BACKUP_DIR}

EOF
read -r -p "Type RUN-EXPERIMENT-2 to proceed: " answer
[[ "$answer" == "RUN-EXPERIMENT-2" ]] || die "Experiment cancelled by user."

# ------------------------------ Backup ------------------------------
PHASE="pre-failure-backup"
log "Creating pre-failure custom-format backup: ${BACKUP_BEFORE}"
kubectl exec -n "$NAMESPACE" shard-a-db-0 -- \
  pg_dump -U bankuser -d shard_a -Fc > "$BACKUP_BEFORE"
[[ -s "$BACKUP_BEFORE" ]] || die "Pre-failure backup is empty."
ls -lh "$BACKUP_BEFORE"
listing_file="$(mktemp)"
kubectl exec -i -n "$NAMESPACE" shard-a-db-0 -- pg_restore --list < "$BACKUP_BEFORE" > "$listing_file"
head -n 20 "$listing_file"
rm -f "$listing_file"

log "Recording primary and standby WAL positions."
kubectl exec -n "$NAMESPACE" shard-a-db-0 -- psql -U bankuser -d shard_a -c \
  "SELECT pg_current_wal_lsn() AS primary_current_lsn, state, sync_state, sent_lsn, flush_lsn, replay_lsn FROM pg_stat_replication;"
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT pg_is_in_recovery() AS is_standby, pg_last_wal_replay_lsn() AS last_replayed_lsn, pg_last_xact_replay_timestamp() AS last_replayed_transaction;"

# ------------------------------ Failover ------------------------------
PHASE="controlled-primary-failure"
log "Scaling down Gateway, Coordinator, and Shard A application replicas."
scale_down_applications
kubectl get pods -n "$NAMESPACE"

log "Stopping the old Shard A primary StatefulSet."
kubectl scale statefulset shard-a-db -n "$NAMESPACE" --replicas=0
kubectl wait --for=delete pod/shard-a-db-0 -n "$NAMESPACE" --timeout=180s
kubectl get pods -n "$NAMESPACE"

log "Promoting shard-a-standby-0."
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- \
  psql -U bankuser -d shard_a -v ON_ERROR_STOP=1 -c "SELECT pg_promote();"
assert_role shard-a-standby-0 f

log "Pointing the shard-a-db Service at the promoted standby."
kubectl patch svc shard-a-db -n "$NAMESPACE" \
  -p '{"spec":{"selector":{"app":"shard-a-standby"}}}'
wait_endpoint shard-a-db "shard-a-standby"

log "Restoring Shard A application replicas."
scale_up_shard_a_apps
kubectl get pods -n "$NAMESPACE"

log "Checking database state after promotion."
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT COUNT(*) AS account_count, SUM(balance) AS total_balance, MIN(balance) AS minimum_balance, MAX(balance) AS maximum_balance FROM accounts;"
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT 'participant_decisions' AS table_name, COUNT(*) AS record_count FROM participant_decisions UNION ALL SELECT 'prepared_transfers', COUNT(*) FROM prepared_transfers UNION ALL SELECT 'pg_prepared_xacts', COUNT(*) FROM pg_prepared_xacts;"
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT decision, COUNT(*) AS record_count FROM participant_decisions GROUP BY decision ORDER BY decision;"

log "Testing bank-a1 directly and depositing 10."
start_port_forward bank-a1 5001 5000
curl -fsS http://127.0.0.1:5001/health
printf '\n'
curl -fsS "http://127.0.0.1:5001/accounts/${ACCOUNT_ID}/balance"
printf '\n'
curl -fsS -X POST "http://127.0.0.1:5001/accounts/${ACCOUNT_ID}/deposit" \
  -H "Content-Type: application/json" -d '{"amount":10}'
printf '\n'
stop_port_forwards
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT account_id, balance FROM accounts WHERE account_id = '${ACCOUNT_ID}';"

# ------------------------------ Rebuild old primary as standby ------------------------------
PHASE="rebuild-old-primary-as-standby"
log "Creating physical replication slot on the promoted primary."
slot_exists="$(sql shard-a-standby-0 "SELECT count(*) FROM pg_replication_slots WHERE slot_name='shard_a_rebuilt_standby';")"
if [[ "$slot_exists" == "0" ]]; then
  kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- \
    psql -U bankuser -d shard_a -v ON_ERROR_STOP=1 -c \
    "SELECT pg_create_physical_replication_slot('shard_a_rebuilt_standby');"
else
  log "Replication slot shard_a_rebuilt_standby already exists; leaving it in place."
fi

# The initContainer only runs pg_basebackup when PG_VERSION is absent.
# Clean the old primary's data directory deliberately and only after backup.
clean_pvc_data_dir "shard-a-db-pvc-cleaner" "data-shard-a-db-0"

log "Applying the supplied shard-a-rebuilt-standby manifest."
kubectl apply -f "$MANIFEST"
wait_pod_ready shard-a-db-0
assert_role shard-a-db-0 t
assert_role shard-a-standby-0 f
assert_replication_streaming shard-a-standby-0
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT application_name, state, sync_state, client_addr, sent_lsn, replay_lsn FROM pg_stat_replication;"
kubectl get endpoints shard-a-db -n "$NAMESPACE" -o wide
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT COUNT(*) AS account_count, SUM(balance) AS total_balance, MIN(balance) AS minimum_balance, MAX(balance) AS maximum_balance FROM accounts;"

# ------------------------------ Restore Coordinator and Gateway ------------------------------
PHASE="restore-coordinator-and-gateway"
log "Restoring Coordinator."
kubectl scale deployment coordinator -n "$NAMESPACE" --replicas=1
wait_deployment coordinator
kubectl get pods -n "$NAMESPACE" -l app=coordinator -o wide
start_port_forward coordinator 5002 5000
curl -fsS http://127.0.0.1:5002/health
printf '\n'
kubectl logs deployment/coordinator -n "$NAMESPACE" --tail=50
stop_port_forwards

log "Restoring Gateway."
kubectl scale deployment gateway -n "$NAMESPACE" --replicas=1
wait_deployment gateway
kubectl get pods -n "$NAMESPACE" -l app=gateway -o wide
start_port_forward gateway 8080 5000
curl -fsS http://127.0.0.1:8080/health
printf '\n'
curl -fsS "http://127.0.0.1:8080/accounts/${ACCOUNT_ID}/balance"
printf '\n'
log "Depositing 5 through Gateway."
curl -fsS -X POST "http://127.0.0.1:8080/accounts/${ACCOUNT_ID}/deposit" \
  -H "Content-Type: application/json" -d '{"amount":5}'
printf '\n'
stop_port_forwards

log "Verifying the promoted primary and rebuilt standby contain the same account balance."
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT account_id, balance FROM accounts WHERE account_id = '${ACCOUNT_ID}';"
kubectl exec -n "$NAMESPACE" shard-a-db-0 -- psql -U bankuser -d shard_a -c \
  "SELECT pg_is_in_recovery(), account_id, balance FROM accounts WHERE account_id = '${ACCOUNT_ID}';"


# ------------------------------ Final restoration and verification ------------------------------

# ------------------------------ Final restoration and verification ------------------------------
PHASE="final-restoration-and-verification"

log "Restoring all application Deployments."
kubectl scale deployment bank-a1 bank-a2 bank-b1 bank-b2 --replicas=1 -n "$NAMESPACE"
kubectl scale deployment coordinator gateway --replicas=1 -n "$NAMESPACE"

for d in bank-a1 bank-a2 bank-b1 bank-b2 coordinator gateway; do
  wait_deployment "$d"
done

log "Final Gateway health and balance check."
kubectl get service gateway -n "$NAMESPACE"
start_port_forward gateway 5000 5000

curl -fsS http://127.0.0.1:5000/health
printf '\n'

curl -fsS "http://127.0.0.1:5000/accounts/${ACCOUNT_ID}/balance"
printf '\n'

stop_port_forwards

# ------------------------------ Verify failover result ------------------------------

log "Verifying Shard A database roles after failover."

assert_role shard-a-standby-0 f
assert_role shard-a-db-0 t
assert_replication_streaming shard-a-standby-0

log "Verifying Shard A account balance on the promoted primary."
kubectl exec -n "$NAMESPACE" shard-a-standby-0 -- psql -U bankuser -d shard_a -c \
  "SELECT account_id, balance FROM accounts WHERE account_id = '${ACCOUNT_ID}';"

log "Verifying the same account balance on the rebuilt standby."
kubectl exec -n "$NAMESPACE" shard-a-db-0 -- psql -U bankuser -d shard_a -c \
  "SELECT pg_is_in_recovery(), account_id, balance FROM accounts WHERE account_id = '${ACCOUNT_ID}';"

log "Verifying Shard B roles remain unchanged."
assert_role shard-b-db-0 f
assert_role shard-b-standby-0 t
assert_replication_streaming shard-b-db-0

log "Final Pods, StatefulSets, Services/endpoints, and PVCs:"
kubectl get pods -n "$NAMESPACE" -o wide
kubectl get statefulsets -n "$NAMESPACE"
kubectl get services,endpoints shard-a-db shard-a-standby -n "$NAMESPACE"
kubectl get pvc -n "$NAMESPACE"

PHASE="completed"

printf '\n\033[1;32mSUCCESS: Experiment 2 PASSED.\033[0m\n'
printf 'Shard A failover was demonstrated successfully.\n'
printf 'Current primary: shard-a-standby-0\n'
printf 'Current standby: shard-a-db-0\n'
printf 'Both databases were verified, and streaming replication is active.\n'
printf 'Gateway and application Deployments have been restored.\n'
printf '\n'
printf 'Backups:\n  %s\n  %s\n' "$BACKUP_BEFORE" "$BACKUP_REBUILD"
printf '\n'
printf 'Reminder: this run performed deposits totaling 15 on account %s.\n' "$ACCOUNT_ID"
printf 'Canonical database roles have NOT been normalized by this script.\n'
printf 'Follow the manual normalization procedure after recording the experiment.\n'
