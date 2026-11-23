import json
import os
import sys
import time
import subprocess
import urllib.request

from decimal import Decimal
from concurrent.futures import (
    ThreadPoolExecutor,
    wait,
    FIRST_COMPLETED
)


# ==========================================
# CONFIGURATION
# ==========================================

GATEWAY = "http://localhost:5000"

ACCOUNT_ID = "A0001"
AMOUNT = Decimal("1.00")
NUMBER_OF_EACH_OPERATION = 100
WORKERS = 10

NAMESPACE = "banking-assignment3"

LOAD_TEST_SCRIPT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "day5_autoscaling_load_test.py"
)


# ==========================================
# BANKING OPERATIONS
# ==========================================

def get_balance():

    url = f"{GATEWAY}/accounts/{ACCOUNT_ID}/balance"

    with urllib.request.urlopen(url, timeout=15) as response:
        result = json.loads(response.read().decode("utf-8"))

    return result


def make_operation(operation, number):

    url = f"{GATEWAY}/accounts/{ACCOUNT_ID}/{operation}"

    data = json.dumps({
        "amount": float(AMOUNT)
    }).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.loads(response.read().decode("utf-8"))

    return operation, result


# ==========================================
# DATABASE VERIFICATION
# ==========================================

def get_database_balance(pod):

    query = (
        f"SELECT balance FROM accounts "
        f"WHERE account_id = '{ACCOUNT_ID}';"
    )

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
        check=True
    )

    return Decimal(result.stdout.strip())


# ==========================================
# HPA MONITORING
# ==========================================

hpa_samples = []


def record_hpa():

    command = [
        "kubectl",
        "get",
        "hpa",
        "bank-a1",
        "-n", NAMESPACE,
        "-o", "json"
    ]

    try:

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            timeout=10
        )

        hpa = json.loads(result.stdout)
        status = hpa.get("status", {})

        current_replicas = status.get("currentReplicas", 0)
        desired_replicas = status.get("desiredReplicas", 0)

        cpu = "N/A"

        for metric in status.get("currentMetrics", []):

            resource = metric.get("resource", {})

            if resource.get("name") == "cpu":

                cpu = resource.get(
                    "current", {}
                ).get("averageUtilization", "N/A")

        sample = {
            "time": time.strftime("%H:%M:%S"),
            "current_replicas": current_replicas,
            "desired_replicas": desired_replicas,
            "cpu": cpu
        }

        hpa_samples.append(sample)

        print(
            f"[HPA {sample['time']}] "
            f"CPU: {cpu}% | "
            f"Current replicas: {current_replicas} | "
            f"Desired replicas: {desired_replicas}"
        )

        return sample

    except Exception as error:

        print("HPA monitoring error:", error)
        return None



# ==========================================
# MAIN TEST
# ==========================================

print("\n==========================================")
print(" ASSIGNMENT 3 - REPLICA SCALING CORRECTNESS")
print("==========================================")

# Check that the existing load generator exists.

if not os.path.isfile(LOAD_TEST_SCRIPT):

    print("ERROR: Load generator script not found:")
    print(LOAD_TEST_SCRIPT)
    sys.exit(1)


# ------------------------------------------
# 1. Initial balance verification
# ------------------------------------------

print("\n[1] Checking initial balance...")

initial_result = get_balance()

initial_balance = Decimal(initial_result["balance"])

print("Gateway response:", initial_result)
print("Initial balance:", initial_balance)


print("\nChecking initial database balances...")

initial_primary = get_database_balance("shard-a-db-0")
initial_standby = get_database_balance("shard-a-standby-0")

print("Primary database:", initial_primary)
print("Standby database:", initial_standby)


if not (
    initial_balance == initial_primary == initial_standby
):

    print("WARNING: Initial balances do not match.")
    sys.exit(1)


# ------------------------------------------
# 2. Prepare concurrent operations
# ------------------------------------------

operations = (
    ["deposit"] * NUMBER_OF_EACH_OPERATION
    + ["withdraw"] * NUMBER_OF_EACH_OPERATION
)

successful = {
    "deposit": 0,
    "withdraw": 0
}

failed = 0
uncertain = 0


print("\n[2] Starting concurrent banking operations...")

print("Deposits:", NUMBER_OF_EACH_OPERATION)
print("Withdrawals:", NUMBER_OF_EACH_OPERATION)
print("Total requests:", len(operations))
print("Workers:", WORKERS)


print("\nRecording initial HPA state...")

initial_hpa = record_hpa()

if initial_hpa is None:
    print("ERROR: Could not read initial HPA state.")
    sys.exit(1)

initial_replicas = initial_hpa["current_replicas"]

print("Initial replica count:", initial_replicas)

# ------------------------------------------
# 3. Start read-only load generator
# ------------------------------------------

print("\n[3] Starting HPA load generator...")

load_process = subprocess.Popen([
    sys.executable,
    LOAD_TEST_SCRIPT,
    "--duration", "120",
    "--workers", "10"
])

print("Load generator started.")
print("Duration: 120 seconds")
print("Workers: 10")


# ------------------------------------------
# 4. Run banking operations and monitor HPA
# ------------------------------------------

print("\n[4] Running banking operations and monitoring HPA...")

# Wait until HPA requests additional replicas.


# Wait until the additional replica is actually active.

print("\nWaiting for HPA to activate an additional replica...")

scaling_started = False
scaling_deadline = time.monotonic() + 90

while (
    time.monotonic() < scaling_deadline
    and load_process.poll() is None
):

    sample = record_hpa()

    if (
        sample is not None
        and sample["current_replicas"] > initial_replicas
        and sample["desired_replicas"] > initial_replicas
    ):

        scaling_started = True

        print("\nAdditional replica is now active!")
        print("Initial replicas:", initial_replicas)
        print("Current replicas:", sample["current_replicas"])
        print("Starting banking operations now.")

        break

    time.sleep(5)


if not scaling_started:

    print("\nERROR: No additional active replica detected within 90 seconds.")
    print("Stopping the load generator.")
    
    load_process.terminate()
    load_process.wait()

    sys.exit(1)


with ThreadPoolExecutor(max_workers=WORKERS) as executor:

    futures = {
        executor.submit(make_operation, operation, number)
        for number, operation in enumerate(operations)
    }

    pending = set(futures)

    while pending or load_process.poll() is None:

        if pending:

            completed, pending = wait(
                pending,
                timeout=5,
                return_when=FIRST_COMPLETED
            )

        else:

            time.sleep(5)
            completed = set()

        for future in completed:

            try:

                operation, result = future.result()

                successful[operation] += 1

                print(
                    f"{operation} succeeded through "
                    f"{result.get('replica', 'unknown replica')}: "
                    f"new balance = "
                    f"{result.get('new_balance', 'not returned')}"
                )

            except Exception as error:

                failed += 1
                uncertain += 1

                print("Request outcome uncertain:", error)

        record_hpa()


load_exit_code = load_process.wait()

print("\nLoad generator finished.")
print("Load generator exit code:", load_exit_code)


# ------------------------------------------
# 5. Final balance verification
# ------------------------------------------

print("\n[5] Checking final balance through Gateway...")

final_result = get_balance()

final_balance = Decimal(final_result["balance"])

print("Gateway response:", final_result)


# ------------------------------------------
# 6. Verify primary and standby databases
# ------------------------------------------

print("\n[6] Checking primary database...")

final_primary = get_database_balance("shard-a-db-0")

print("Primary database balance:", final_primary)


print("\nChecking standby database...")

final_standby = get_database_balance("shard-a-standby-0")

print("Standby database balance:", final_standby)


# ------------------------------------------
# 7. Calculate expected balance
# ------------------------------------------

expected_balance = (
    initial_balance
    + (
        successful["deposit"]
        - successful["withdraw"]
    ) * AMOUNT
)


# ------------------------------------------
# 8. Final results
# ------------------------------------------

print("\n==========================================")
print("              TEST RESULTS")
print("==========================================")

print("\nBANKING OPERATIONS")

print("Successful deposits:", successful["deposit"])
print("Successful withdrawals:", successful["withdraw"])
print("Failed requests:", failed)
print("Uncertain outcomes:", uncertain)

print("\nBALANCE VERIFICATION")

print("Initial balance:", initial_balance)
print("Expected balance:", expected_balance)
print("Gateway final balance:", final_balance)
print("Primary final balance:", final_primary)
print("Standby final balance:", final_standby)

if uncertain == 0:

    print(
        "Gateway matches expected:",
        final_balance == expected_balance
    )

    print(
        "Primary matches expected:",
        final_primary == expected_balance
    )

    print(
        "Standby matches expected:",
        final_standby == expected_balance
    )

else:

    print(
        "Expected balance comparison is inconclusive "
        "because some request outcomes are uncertain."
    )


print("\nHPA SCALING")

if hpa_samples:


    maximum_replicas = max(
        sample["current_replicas"]
        for sample in hpa_samples
    )

    print("Initial replicas:", initial_replicas)
    print("Maximum observed replicas:", maximum_replicas)

    print(
        "Scale-up observed:",
        maximum_replicas > initial_replicas
    )

else:

    print("No HPA samples were collected.")


print("\nLoad generator exit code:", load_exit_code)

print("\nFINAL EXPERIMENT VERIFICATION")

operations_correct = (
    successful["deposit"] == NUMBER_OF_EACH_OPERATION
    and successful["withdraw"] == NUMBER_OF_EACH_OPERATION
    and failed == 0
    and uncertain == 0
)

balances_correct = (
    final_balance == expected_balance
    and final_primary == expected_balance
    and final_standby == expected_balance
)

scaling_correct = (
    max(
        sample["current_replicas"]
        for sample in hpa_samples
    ) > initial_replicas
)

load_correct = load_exit_code == 0

checks = {
    "All banking operations succeeded": operations_correct,
    "All three balances are correct": balances_correct,
    "HPA scaled up": scaling_correct,
    "Load generator exited successfully": load_correct
}

for description, passed in checks.items():
    print(f"{description}: {'PASS' if passed else 'FAIL'}")

print(
    "\nOVERALL RESULT:",
    "PASS" if all(checks.values()) else "FAIL"
)

print("\n==========================================")
print("              TEST FINISHED")
print("==========================================")