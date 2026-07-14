"""BatchCollector.add() latency under high concurrent fan-in (§17.1).

Measures whether the single ``asyncio.Lock`` shared by every producer becomes a bottleneck
before ``max_size``/``max_delay_ms`` ever matters — a question the production-readiness review
flagged as unconfirmed. No database needed: the flush callback is a cheap in-memory no-op, so
this isolates the collector/lock overhead itself from write throughput (already covered by
``bench_batch.py``/``bench_crud.py``). Run directly: ``python -m benchmarks.bench_batch_collector``.
"""

from __future__ import annotations

import asyncio
import time

from dbkit.integrations import BatchCollector

FAN_IN_LEVELS = (10, 100, 500, 1000, 2000)
ITEMS_PER_PRODUCER = 50


async def _flush(items: object) -> None:
    pass  # no-op: isolate collector/lock overhead from write cost


async def _producer(bc: BatchCollector[int], n: int) -> list[float]:
    latencies = []
    for i in range(n):
        start = time.monotonic()
        await bc.add(i)
        latencies.append(time.monotonic() - start)
    return latencies


async def _run_fan_in(fan_in: int) -> dict[str, float]:
    bc: BatchCollector[int] = BatchCollector(_flush, max_size=500, max_delay_ms=50)
    start = time.monotonic()
    results = await asyncio.gather(*[_producer(bc, ITEMS_PER_PRODUCER) for _ in range(fan_in)])
    await bc.close()
    total_elapsed = time.monotonic() - start
    all_latencies = sorted(x for r in results for x in r)
    total_items = fan_in * ITEMS_PER_PRODUCER
    return {
        "fan_in": fan_in,
        "items": total_items,
        "throughput_items_s": total_items / total_elapsed,
        "p50_ms": all_latencies[len(all_latencies) // 2] * 1000,
        "p99_ms": all_latencies[int(len(all_latencies) * 0.99)] * 1000,
        "max_ms": all_latencies[-1] * 1000,
    }


async def _main() -> None:
    print(
        f"{'fan_in':>8} {'items':>8} {'throughput/s':>14} {'p50 ms':>8} {'p99 ms':>8} {'max ms':>8}"
    )
    for fan_in in FAN_IN_LEVELS:
        r = await _run_fan_in(fan_in)
        print(
            f"{r['fan_in']:>8.0f} {r['items']:>8.0f} {r['throughput_items_s']:>14.0f} "
            f"{r['p50_ms']:>8.3f} {r['p99_ms']:>8.3f} {r['max_ms']:>8.3f}"
        )


if __name__ == "__main__":
    asyncio.run(_main())
