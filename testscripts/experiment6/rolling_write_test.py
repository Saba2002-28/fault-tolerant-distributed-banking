import json
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

URL = "http://localhost:5000/accounts/A0001/deposit"
TOTAL_REQUESTS = 180
AMOUNT = 0.01
DELAY = 0.5
INITIAL_BALANCE = 465.10

success = 0
failed = 0

print("Starting rolling-update write workload...", flush=True)

for i in range(1, TOTAL_REQUESTS + 1):
    request = Request(
        URL,
        data=json.dumps({"amount": AMOUNT}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urlopen(request, timeout=35) as response:
            result = json.loads(response.read().decode())
            success += 1
            print(f"{i}/{TOTAL_REQUESTS}: HTTP {response.status} - "
                  f"{result}", flush=True)

    except HTTPError as error:
        failed += 1
        print(f"{i}/{TOTAL_REQUESTS}: HTTP {error.code} - "
              f"{error.read().decode()}", flush=True)

    except (URLError, TimeoutError, OSError) as error:
        failed += 1
        print(f"{i}/{TOTAL_REQUESTS}: CONNECTION ERROR - "
              f"{error}", flush=True)

    if i < TOTAL_REQUESTS:
        time.sleep(DELAY)

print("\n========== RESULTS ==========")
print(f"Total requests: {TOTAL_REQUESTS}")
print(f"Successful: {success}")
print(f"Failed: {failed}")

print(f"Expected balance: {INITIAL_BALANCE + success * AMOUNT:.2f}")
