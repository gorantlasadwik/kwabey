"""
Live speed benchmark — measures actual req/s against kwabey.com endpoint
Tests: 1 worker, 5 workers, 10 workers, 20 workers
"""
import time
import requests
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

BASE_URL  = "https://kwabey.com/via_gt_ajax/try_to_login/"
TEST_START = 9618595430   # already scanned range — just timing
TEST_COUNT = 100           # requests per worker-count test

def make_session(pool_size):
    s = requests.Session()
    s.headers.update({"User-Agent": "Kwabey-Benchmark/1.0"})
    a = requests.adapters.HTTPAdapter(
        pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0
    )
    s.mount("https://", a)
    return s

def check_one(phone, session):
    url = f"{BASE_URL}?phone_number={phone}"
    try:
        r = session.get(url, timeout=10)
        return r.status_code
    except Exception:
        return None

def run_test(workers, start, count):
    session = make_session(workers)
    phones  = [str(start + i) for i in range(count)]
    t0      = time.time()
    done_count = 0

    pending = {}
    phone_iter = iter(phones)
    exhausted  = False

    def submit_next():
        nonlocal exhausted
        if exhausted: return
        try:
            p = next(phone_iter)
            f = executor.submit(check_one, p, session)
            pending[f] = p
        except StopIteration:
            exhausted = True

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for _ in range(workers * 2):
            submit_next()

        while pending:
            done, _ = wait(pending, timeout=10, return_when="FIRST_COMPLETED")
            for f in done:
                pending.pop(f)
                done_count += 1
                submit_next()

    elapsed = time.time() - t0
    rps     = round(done_count / elapsed, 2)
    session.close()
    return elapsed, rps

print("=" * 55)
print("  Kwabey Scraper — Live Speed Benchmark")
print(f"  Target: {BASE_URL}")
print(f"  {TEST_COUNT} requests per test")
print("=" * 55)

results = []
for w in [1, 5, 10, 20]:
    print(f"\n  Testing {w:>2} worker(s)...", end=" ", flush=True)
    elapsed, rps = run_test(w, TEST_START, TEST_COUNT)
    print(f"{rps:>7.1f} req/s  ({elapsed:.1f}s total)")
    results.append((w, rps, elapsed))

print("\n" + "=" * 55)
print("  RESULTS SUMMARY")
print("=" * 55)
print(f"  {'Workers':>8}  {'req/s':>10}  {'vs 1-worker':>12}")
print("  " + "-" * 35)
base_rps = results[0][1]
for w, rps, _ in results:
    speedup = f"{rps/base_rps:.1f}x" if base_rps > 0 else "n/a"
    print(f"  {w:>8}  {rps:>10.1f}  {speedup:>12}")
print("=" * 55)

# Estimate time to scan full 4B range at 20 workers
best_rps = results[-1][1]
total_numbers = 4_000_000_000
est_hours = total_numbers / best_rps / 3600
print(f"\n  Full 4B range @ {best_rps} req/s = {est_hours:,.0f} hours")
print(f"  (~{est_hours/24:,.0f} days)")
