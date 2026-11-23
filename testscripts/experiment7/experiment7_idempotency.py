import json
import time
import uuid
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from decimal import Decimal

GATEWAY_URL = "http://localhost:5000"
COORDINATOR_URL = "http://localhost:5001"

FROM_ACCOUNT = "A0500"
TO_ACCOUNT = "A1500"
AMOUNT = Decimal("10.00")

SEQUENTIAL_KEY = f"experiment7-sequential-{uuid.uuid4()}"
CONCURRENT_KEY = f"experiment7-concurrent-{uuid.uuid4()}"

TRANSFER = {
    "from_account_id": FROM_ACCOUNT,
    "to_account_id": TO_ACCOUNT,
    "amount": float(AMOUNT)
}


def http_request(url, method="GET", body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    request = Request(url, data=data, headers=headers or {}, method=method)

    try:
        with urlopen(request, timeout=60) as response:
            content = response.read().decode()
            return response.status, json.loads(content) if content else {}
    except HTTPError as error:
        content = error.read().decode()
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            result = {"error": content}
        return error.code, result
    except (URLError, TimeoutError, OSError) as error:
        return 0, {"error": str(error)}


def send_transfer(transfer, key):
    return http_request(
        f"{GATEWAY_URL}/accounts/transfer",
        method="POST",
        body=transfer,
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": key
        }
    )


def get_balance(account_id):
    status, body = http_request(
        f"{GATEWAY_URL}/accounts/{account_id}/balance"
    )
    if status != 200:
        raise RuntimeError(f"Could not read {account_id} balance: {status} {body}")
    return Decimal(body["balance"])


def print_result(label, result):
    status, body = result
    print(f"\n{label} — HTTP {status}")
    print(json.dumps(body, indent=2))


def kubectl_psql(pod, database, sql):
    result = subprocess.run(
        [
            "kubectl", "exec", "-n", "banking-assignment3", pod,
            "--", "psql", "-U", "bankuser", "-d", database,
            "-X", "-A", "-t", "-F", "|", "-c", sql
        ],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return result.stdout.strip()


def wait_for_completion(transaction_id, timeout=90):
    if not transaction_id:
        return None

    deadline = time.time() + timeout
    last_status = None

    while time.time() < deadline:
        status, body = http_request(
            f"{COORDINATOR_URL}/transactions/{transaction_id}"
        )
        if status == 200:
            last_status = body
            if body.get("status") == "COMPLETED":
                return body
        time.sleep(2)

    return last_status


def sql_keys(keys):
    return ", ".join("'" + key + "'" for key in keys)


def verify_database(keys):
    print("\n========== COORDINATOR DATABASE ==========")

    key_list = sql_keys(keys)
    rows = kubectl_psql(
        "coordinator-db-0", "coordinator",
        f"""
        SELECT
            t.idempotency_key,
            t.transaction_id,
            t.status,
            COALESCE(t.decision, 'NULL'),
            COUNT(p.shard_id),
            COUNT(p.shard_id) FILTER (WHERE p.status = 'COMMITTED')
        FROM distributed_transactions t
        LEFT JOIN transaction_participants p
            ON p.transaction_id = t.transaction_id
        WHERE t.idempotency_key IN ({key_list})
        GROUP BY t.idempotency_key, t.transaction_id, t.status, t.decision
        ORDER BY t.idempotency_key;
        """
    )

    print("key | transaction_id | status | decision | participants | committed")
    print(rows or "NO MATCHING TRANSACTIONS")

    transaction_ids = []
    for line in rows.splitlines():
        fields = line.split("|")
        if len(fields) >= 6:
            transaction_ids.append(fields[1])

    if not transaction_ids:
        return False

    ids = ", ".join("'" + value + "'" for value in transaction_ids)

    print("\n========== SHARD A DECISIONS ==========")
    shard_a = kubectl_psql(
        "shard-a-db-0", "shard_a",
        f"SELECT transaction_id, decision FROM participant_decisions WHERE transaction_id IN ({ids}) ORDER BY transaction_id;"
    )
    print(shard_a or "NO DECISION RECORDS")

    print("\n========== SHARD B DECISIONS ==========")
    shard_b = kubectl_psql(
        "shard-b-db-0", "shard_b",
        f"SELECT transaction_id, decision FROM participant_decisions WHERE transaction_id IN ({ids}) ORDER BY transaction_id;"
    )
    print(shard_b or "NO DECISION RECORDS")

    return True


def main():
    print("EXPERIMENT 7: REPEATED CLIENT REQUESTS / IDEMPOTENCY")
    print(f"Source: {FROM_ACCOUNT}")
    print(f"Destination: {TO_ACCOUNT}")
    print(f"Amount per logical transfer: ${AMOUNT}")
    print(f"Sequential key: {SEQUENTIAL_KEY}")
    print(f"Concurrent key: {CONCURRENT_KEY}")

    print("\n========== INITIAL BALANCES ==========")
    initial_from = get_balance(FROM_ACCOUNT)
    initial_to = get_balance(TO_ACCOUNT)
    print(f"{FROM_ACCOUNT}: ${initial_from}")
    print(f"{TO_ACCOUNT}: ${initial_to}")
    print(f"Combined: ${initial_from + initial_to}")

    print("\n========== TEST 1: SEQUENTIAL RETRY ==========")
    first = send_transfer(TRANSFER, SEQUENTIAL_KEY)
    print_result("First request", first)

    second = send_transfer(TRANSFER, SEQUENTIAL_KEY)
    print_result("Repeated request", second)

    print("\n========== TEST 2: DIFFERENT PAYLOAD ==========")
    different_transfer = dict(TRANSFER, amount=20.00)
    conflict = send_transfer(different_transfer, SEQUENTIAL_KEY)
    print_result("Same key, different amount (expect 409)", conflict)

    print("\n========== TEST 3: CONCURRENT RETRIES ==========")
    concurrent_results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(send_transfer, TRANSFER, CONCURRENT_KEY)
            for _ in range(5)
        ]
        for future in as_completed(futures):
            result = future.result()
            concurrent_results.append(result)
            print_result("Concurrent request", result)

    print("\n========== WAIT FOR TRANSACTIONS ==========")
    transaction_statuses = {}

    for key in [SEQUENTIAL_KEY, CONCURRENT_KEY]:
        # Find transaction ID using the Coordinator database.
        row = kubectl_psql(
            "coordinator-db-0", "coordinator",
            f"SELECT transaction_id FROM distributed_transactions WHERE idempotency_key = '{key}';"
        )
        transaction_id = row.strip() or None
        print(f"\n{key}: transaction_id={transaction_id}")

        if transaction_id:
            status = wait_for_completion(transaction_id)
            transaction_statuses[key] = status
            print("Final status:", json.dumps(status, indent=2))

    print("\n========== DATABASE VERIFICATION ==========")
    database_found = verify_database([SEQUENTIAL_KEY, CONCURRENT_KEY])

    print("\n========== FINAL BALANCES ==========")
    final_from = get_balance(FROM_ACCOUNT)
    final_to = get_balance(TO_ACCOUNT)
    print(f"{FROM_ACCOUNT}: ${final_from}")
    print(f"{TO_ACCOUNT}: ${final_to}")
    print(f"Combined: ${final_from + final_to}")

    expected_from = initial_from - (2 * AMOUNT)
    expected_to = initial_to + (2 * AMOUNT)

    print("\n========== EXPECTED VS ACTUAL ==========")
    print(f"Expected {FROM_ACCOUNT}: ${expected_from}")
    print(f"Actual   {FROM_ACCOUNT}: ${final_from}")
    print(f"Expected {TO_ACCOUNT}: ${expected_to}")
    print(f"Actual   {TO_ACCOUNT}: ${final_to}")
    print(f"Combined balance conserved: {initial_from + initial_to == final_from + final_to}")

    print("\n========== BASIC TEST ASSERTIONS ==========")
    print("Sequential first request accepted:", first[0] in (200, 202))
    print("Sequential retry accepted:", second[0] in (200, 202))
    print("Different payload rejected with 409:", conflict[0] == 409)
    print("All concurrent requests received a response:", len(concurrent_results) == 5)
    print("Two final balances match expected values:",
          final_from == expected_from and final_to == expected_to)
    print("Database records found:", database_found)

    print("\n========== EXPERIMENT FINISHED ==========")
    print("Save these keys for your experiment report:")
    print("Sequential:", SEQUENTIAL_KEY)
    print("Concurrent:", CONCURRENT_KEY)
    print("Note: inspect the database output and transaction statuses before declaring PASS.")


if __name__ == "__main__":
    main()
