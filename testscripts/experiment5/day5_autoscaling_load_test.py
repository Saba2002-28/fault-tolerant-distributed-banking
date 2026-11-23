import argparse
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:5000"
ACCOUNT_ID = "A0001"


def worker(deadline):
    count = 0
    successful = 0
    failed = 0
    total_latency = 0
    max_latency = 0
    errors = []

    url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/balance"

    while time.monotonic() < deadline:
        start = time.perf_counter()

        try:
            request = urllib.request.Request(
                url,
                method="GET",
                headers={"Accept": "application/json"}
            )

            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
                status = response.status

            if status == 200:
                successful += 1
            else:
                failed += 1

        except urllib.error.HTTPError as error:
            failed += 1
            if len(errors) < 5:
                errors.append(str(error))

        except Exception as error:
            failed += 1
            if len(errors) < 5:
                errors.append(str(error))

        elapsed = time.perf_counter() - start
        total_latency += elapsed
        max_latency = max(max_latency, elapsed)
        count += 1

    return count, successful, failed, total_latency, max_latency, errors


def main():
    parser = argparse.ArgumentParser(
        description="Continuous read-only load test for bank-a1"
    )
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    if args.duration < 1 or args.workers < 1:
        parser.error("Duration and workers must be positive.")

    print("Starting continuous read-only load test")
    print(f"Target: {BASE_URL}/accounts/{ACCOUNT_ID}/balance")
    print(f"Duration: {args.duration} seconds")
    print(f"Concurrent workers: {args.workers}")
    print("Operation: GET balance (no account changes)")
    print("-" * 50)

    deadline = time.monotonic() + args.duration
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(worker, deadline)
            for _ in range(args.workers)
        ]

        results = [future.result() for future in futures]

    total_time = time.perf_counter() - start

    total = sum(r[0] for r in results)
    successful = sum(r[1] for r in results)
    failed = sum(r[2] for r in results)
    total_latency = sum(r[3] for r in results)
    max_latency = max(r[4] for r in results)
    errors = [e for r in results for e in r[5]]

    print("\nLoad test completed")
    print("-" * 50)
    print(f"Total requests:        {total}")
    print(f"Successful (HTTP 200): {successful}")
    print(f"Failed:                {failed}")
    print(f"Actual duration:       {total_time:.2f} seconds")
    print(f"Average response:      {total_latency / total * 1000:.2f} ms")
    print(f"Maximum response:      {max_latency * 1000:.2f} ms")
    print(f"Requests per second:   {total / total_time:.2f}")

    if errors:
        print("\nFirst few errors:")
        for error in errors[:5]:
            print(f"- {error}")


if __name__ == "__main__":
    main()
