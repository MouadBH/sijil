"""`wait_extraction_idle`: polls a probe until the extraction worker is idle, or fails clearly."""

import pytest

from tests.support.extraction_idle import ExtractionBusy, wait_extraction_idle


async def test_returns_once_the_queue_is_empty_and_nothing_runs() -> None:
    answers = iter([(3, 1), (0, 1), (0, 0)])
    calls: list[int] = []

    async def probe() -> tuple[int, int]:
        calls.append(1)
        return next(answers)

    waited = await wait_extraction_idle(budget_s=10, poll_s=0, probe=probe)
    assert len(calls) == 3
    assert waited >= 0


async def test_fails_with_a_clear_message_when_still_busy() -> None:
    async def busy() -> tuple[int, int]:
        return (2, 1)

    with pytest.raises(ExtractionBusy, match="2 queued, 1 running"):
        await wait_extraction_idle(budget_s=0, poll_s=0, probe=busy)
