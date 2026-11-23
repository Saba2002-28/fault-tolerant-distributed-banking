
import json
import subprocess
import time
import threading
from datetime import datetime
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


# ==========================================
# CONFIGURATION
# ==========================================

GATEWAY = "http://localhost:5000"

ACCOUNT_ID = "A0001"
AMOUNT = Decimal("1.00")

NUMBER_OF_EACH_OPERATION = 50
WORKERS = 10

NAMESPACE = "banking-assignment3"

PRIMARY_POD = "shard-a-db-0"
STANDBY_POD = "shard-a-standby-0"

REQUEST_TIMEOUT = 30
REPLICATION_TIMEOUT = 60

RESULT_FILE = (
    f"experiment3_results_"
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
)

# Concurrency measurement
active_requests = 0
max_active_requests = 0
concurrency_lock = threading.Lock()


# ==========================================
# BANKING OPERATIONS
# ==========================================

def get_balance():

    url = f"{GATEWAY}/accounts/{ACCOUNT_ID}/balance"

    with urlopen(url, timeout=REQUEST_TIMEOUT) as response:

        result = json.loads(
            response.read().decode("utf-8")
        )

    return Decimal(str(result["balance"])), result



def make_operation(operation, number, barrier):

    global active_requests, max_active_requests

    # Synchronize workers so the first group
    # reaches the request stage together.
    barrier.wait()

    url = (
        f"{GATEWAY}/accounts/"
        f"{ACCOUNT_ID}/{operation}"
    )

    data = json.dumps({
        "amount": float(AMOUNT)
    }).encode("utf-8")

    request = Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    start_time = time.perf_counter()
    start_timestamp = datetime.now().isoformat(
        timespec="milliseconds"
    )

    thread_name = threading.current_thread().name

    with concurrency_lock:
        active_requests += 1
        max_active_requests = max(
            max_active_requests,
            active_requests
        )
        current_active = active_requests

    print(
        f"START | {start_timestamp} | "
        f"Request {number:03d} | "
        f"{operation.upper():8s} | "
        f"Thread: {thread_name} | "
        f"Active: {current_active}"
    )

    # Initialize the result before the request.
    result = {
        "number": number,
        "operation": operation,
        "status": None,
        "outcome": "uncertain"
    }

    try:

        with urlopen(
            request,
            timeout=REQUEST_TIMEOUT
        ) as response:

            response_body = json.loads(
                response.read().decode("utf-8")
            )

            result["status"] = response.status
            result["outcome"] = "success"
            result["result"] = response_body

    except HTTPError as error:

        body = error.read().decode(
            "utf-8",
            errors="replace"
        )

        try:
            response_body = json.loads(body)
        except json.JSONDecodeError:
            response_body = body

        result["status"] = error.code
        result["outcome"] = (
            "rejected" if 400 <= error.code < 500
            else "uncertain"
        )
        result["result"] = response_body

    except (URLError, TimeoutError, OSError) as error:

        result["status"] = None
        result["outcome"] = "uncertain"
        result["error"] = str(error)

    finally:

        end_time = time.perf_counter()
        end_timestamp = datetime.now().isoformat(
            timespec="milliseconds"
        )

        duration = end_time - start_time

        with concurrency_lock:
            active_requests -= 1
            current_active = active_requests

        # Attach timing information to the SAME dictionary.
        result["start_timestamp"] = start_timestamp
        result["end_timestamp"] = end_timestamp
        result["duration_seconds"] = round(duration, 6)
        result["thread_name"] = thread_name

        print(
            f"END   | {end_timestamp} | "
            f"Request {number:03d} | "
            f"{operation.upper():8s} | "
            f"Duration: {duration:.3f}s | "
            f"Active: {current_active}"
        )

    return result



# ==========================================
# DATABASE VERIFICATION
# ==========================================

def get_database_state(pod):

    query = f"""
        SELECT
            balance,
            (
                SELECT COALESCE(SUM(balance), 0)
                FROM accounts
            )
        FROM accounts
        WHERE account_id = '{ACCOUNT_ID}';
    """

    command = [
        "kubectl",
        "exec",
        "-n", NAMESPACE,
        pod,
        "--",
        "psql",
        "-U", "bankuser",
        "-d", "shard_a",
        "-Atc", query
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
        timeout=20
    )

    values = result.stdout.strip().split("|")

    if len(values) != 2:
        raise RuntimeError(
            f"Unexpected database output from {pod}: "
            f"{result.stdout}"
        )

    return {
        "account_balance": Decimal(values[0]),
        "total_balance": Decimal(values[1])
    }


# ==========================================
# STANDBY REPLICATION VERIFICATION
# ==========================================

def wait_for_replication(expected_balance, expected_total):

    print("\nWaiting for standby replication...")

    deadline = time.monotonic() + REPLICATION_TIMEOUT

    while time.monotonic() < deadline:

        try:

            standby = get_database_state(STANDBY_POD)

            if (
                standby["account_balance"] == expected_balance
                and standby["total_balance"] == expected_total
            ):

                print("Standby has caught up with the primary.")

                return standby

        except Exception as error:

            print("Replication check:", error)

        time.sleep(2)

    raise TimeoutError(
        "Standby did not reach the expected database state "
        "within the replication timeout."
    )


# ==========================================
# SAVE RESULTS
# ==========================================

def save_results(results):

    with open(RESULT_FILE, "w", encoding="utf-8") as file:

        json.dump(
            results,
            file,
            indent=4,
            default=str
        )

    print("\nResults saved to:", RESULT_FILE)


# ==========================================
# MAIN EXPERIMENT
# ==========================================

print("\n==========================================")
print(" EXPERIMENT 3 - CONCURRENT TRANSACTIONS")
print("==========================================")


# ------------------------------------------
# 1. Initial state
# ------------------------------------------

print("\n[1] Reading initial account balance...")

initial_balance, initial_gateway = get_balance()

print("Gateway response:", initial_gateway)
print("Initial balance:", initial_balance)


print("\nReading initial primary database state...")

initial_primary = get_database_state(PRIMARY_POD)

print(initial_primary)


print("\nReading initial standby database state...")

initial_standby = get_database_state(STANDBY_POD)

print(initial_standby)


if initial_balance != initial_primary["account_balance"]:
    raise RuntimeError(
        "Initial Gateway balance does not match primary database."
    )

if initial_primary != initial_standby:
    raise RuntimeError(
        "Initial primary and standby database states do not match."
    )


minimum_required = (
    NUMBER_OF_EACH_OPERATION * AMOUNT
)

if initial_balance < minimum_required:

    raise RuntimeError(
        "Initial account balance is insufficient for "
        "the planned withdrawals."
    )


# ------------------------------------------
# 2. Prepare operations
# ------------------------------------------

operations = (
    ["deposit"] * NUMBER_OF_EACH_OPERATION
    + ["withdraw"] * NUMBER_OF_EACH_OPERATION
)

print("\n[2] Preparing concurrent operations...")

print("Account:", ACCOUNT_ID)
print("Deposit requests:", NUMBER_OF_EACH_OPERATION)
print("Withdrawal requests:", NUMBER_OF_EACH_OPERATION)
print("Amount per operation:", AMOUNT)
print("Total requests:", len(operations))
print("Concurrent workers:", WORKERS)


# ------------------------------------------
# 3. Execute concurrent operations
# ------------------------------------------

print("\n[3] Starting concurrent transactions...")

successful = {
    "deposit": 0,
    "withdraw": 0
}

rejected = {
    "deposit": 0,
    "withdraw": 0
}

uncertain = 0

operation_results = []

barrier = threading.Barrier(WORKERS)

with ThreadPoolExecutor(max_workers=WORKERS) as executor:

    futures = []

    for number, operation in enumerate(operations, start=1):

        future = executor.submit(
            make_operation,
            operation,
            number,
            barrier
        )

        futures.append(future)

    for future in as_completed(futures):

        result = future.result()

        operation_results.append(result)

        operation = result["operation"]
        outcome = result["outcome"]

        if outcome == "success":

            successful[operation] += 1

        elif outcome == "rejected":

            rejected[operation] += 1

        else:

            uncertain += 1

        print(
            f"Request {result['number']:03d}: "
            f"{operation.upper():8s} | "
            f"{outcome.upper()}"
        )


# ------------------------------------------
# 4. Calculate expected state
# ------------------------------------------

print("\n[4] Calculating expected database state...")

successful_deposits = successful["deposit"]
successful_withdrawals = successful["withdraw"]

net_change = (
    successful_deposits - successful_withdrawals
) * AMOUNT

expected_balance = initial_balance + net_change

expected_total = (
    initial_primary["total_balance"] + net_change
)

print("Successful deposits:", successful_deposits)
print("Successful withdrawals:", successful_withdrawals)
print("Rejected requests:", sum(rejected.values()))
print("Uncertain requests:", uncertain)

print("Expected account balance:", expected_balance)
print("Expected total balance:", expected_total)


# ------------------------------------------
# 5. Verify final Gateway balance
# ------------------------------------------

print("\n[5] Checking final Gateway balance...")

final_balance, final_gateway = get_balance()

print("Gateway response:", final_gateway)
print("Final Gateway balance:", final_balance)


# ------------------------------------------
# 6. Verify primary database
# ------------------------------------------

print("\n[6] Checking primary database...")

final_primary = get_database_state(PRIMARY_POD)

print("Primary account balance:",
      final_primary["account_balance"])

print("Primary total balance:",
      final_primary["total_balance"])


# ------------------------------------------
# 7. Verify standby database
# ------------------------------------------

print("\n[7] Checking standby database...")

final_standby = wait_for_replication(
    expected_balance,
    expected_total
)

print("Standby account balance:",
      final_standby["account_balance"])

print("Standby total balance:",
      final_standby["total_balance"])


# ------------------------------------------
# 8. Correctness checks
# ------------------------------------------

print("\n[8] Performing correctness checks...")

all_requests_succeeded = (
    successful["deposit"] == NUMBER_OF_EACH_OPERATION
    and successful["withdraw"] == NUMBER_OF_EACH_OPERATION
    and sum(rejected.values()) == 0
    and uncertain == 0
)

gateway_correct = (
    final_balance == expected_balance
)

primary_correct = (
    final_primary["account_balance"] == expected_balance
    and final_primary["total_balance"] == expected_total
)

standby_correct = (
    final_standby["account_balance"] == expected_balance
    and final_standby["total_balance"] == expected_total
)

replication_correct = (
    final_primary == final_standby
)

money_conserved = (
    final_primary["total_balance"]
    == initial_primary["total_balance"]
)

checks = {
    "all_requests_succeeded": all_requests_succeeded,
    "gateway_balance_correct": gateway_correct,
    "primary_database_correct": primary_correct,
    "standby_database_correct": standby_correct,
    "replication_consistent": replication_correct,
    "total_money_conserved": money_conserved
}


# ------------------------------------------
# 9. Final report
# ------------------------------------------

print("\n==========================================")
print("          EXPERIMENT 3 RESULTS")
print("==========================================")

print("\nTRANSACTION RESULTS")

print("Successful deposits:", successful["deposit"])
print("Successful withdrawals:", successful["withdraw"])
print("Rejected deposits:", rejected["deposit"])
print("Rejected withdrawals:", rejected["withdraw"])
print("Uncertain outcomes:", uncertain)

print("\nBALANCE VERIFICATION")

print("Initial account balance:", initial_balance)
print("Expected account balance:", expected_balance)
print("Final Gateway balance:", final_balance)
print("Final primary balance:", final_primary["account_balance"])
print("Final standby balance:", final_standby["account_balance"])

print("\nMONEY CONSERVATION")

print("Initial total money:", initial_primary["total_balance"])
print("Expected total money:", expected_total)
print("Final primary total money:", final_primary["total_balance"])
print("Final standby total money:", final_standby["total_balance"])

print("\nCONCURRENCY EVIDENCE")

print("Configured worker threads:", WORKERS)
print("Maximum simultaneous HTTP requests:", max_active_requests)

print("\nCORRECTNESS CHECKS")

for name, passed in checks.items():

    print(f"{name}: {'PASS' if passed else 'FAIL'}")


experiment_passed = all(checks.values())

print("\n==========================================")

if experiment_passed:

    print(" SUCCESS: EXPERIMENT 3 PASSED")

else:

    print(" EXPERIMENT 3 FAILED OR INCONCLUSIVE")

print("==========================================")


# ------------------------------------------
# 10. Save evidence
# ------------------------------------------

saved_results = {
    "experiment": "Experiment 3 - Concurrent Transactions",
    "timestamp": datetime.now().isoformat(),
    "account_id": ACCOUNT_ID,
    "amount_per_operation": str(AMOUNT),
    "workers": WORKERS,
    "max_simultaneous_http_requests": max_active_requests,
    "successful": successful,
    "rejected": rejected,
    "uncertain": uncertain,
    "initial_balance": str(initial_balance),
    "expected_balance": str(expected_balance),
    "final_gateway_balance": str(final_balance),
    "initial_total_money": str(initial_primary["total_balance"]),
    "expected_total_money": str(expected_total),
    "final_primary": {
        key: str(value)
        for key, value in final_primary.items()
    },
    "final_standby": {
        key: str(value)
        for key, value in final_standby.items()
    },
    "checks": checks,
    "passed": experiment_passed,
    "operation_results": operation_results
}

save_results(saved_results)
