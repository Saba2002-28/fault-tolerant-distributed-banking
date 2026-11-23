
import json
import time
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

GATEWAY_URL = "http://localhost:5000"
ACCOUNT_ID = "A0001"

TOTAL_REQUESTS = 600
INTERVAL_SECONDS = 0.1


def check_balance(request_number):
    url = f"{GATEWAY_URL}/accounts/{ACCOUNT_ID}/balance"

    request = Request(url, method="GET")
    timestamp = datetime.now().strftime("%H:%M:%S")

    try:
        start = time.time()

        with urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8")
            elapsed = time.time() - start

            print(
                f"[{timestamp}] Request {request_number:02d} | "
                f"HTTP {response.status} | "
                f"{elapsed:.3f}s"
            )
            print(f"Response: {body}")

    except HTTPError as error:
        body = error.read().decode("utf-8")
        print(
            f"[{timestamp}] Request {request_number:02d} | "
            f"HTTP {error.code} | Response: {body}"
        )

    except (URLError, TimeoutError, OSError) as error:
        print(
            f"[{timestamp}] Request {request_number:02d} | "
            f"CONNECTION ERROR | {error}"
        )


def main():
    print("=" * 60)
    print("EXPERIMENT 1: APPLICATION POD FAILURE")
    print("=" * 60)
    print(f"Account: {ACCOUNT_ID}")
    print(f"Gateway: {GATEWAY_URL}")
    print(f"Total requests: {TOTAL_REQUESTS}")
    print("Starting repeated balance requests...\n")

    successful = 0
    failed = 0

    for i in range(1, TOTAL_REQUESTS + 1):
        check_balance(i)
        time.sleep(INTERVAL_SECONDS)

    print("\n" + "=" * 60)
    print("TEST FINISHED")
    print("=" * 60)


if __name__ == "__main__":
    main()
