"""`closing_at_exit`: every workspace a test creates ends `closing`, and its tenant stopped."""

from collections.abc import Iterable
from uuid import UUID, uuid4

import pytest

from sijil_core.tenancy import tenant_ref
from tests.support.workspaces import closing_at_exit


class _Recorder:
    def __init__(self, failing: str | None = None) -> None:
        self.calls: list[tuple[str, object]] = []
        self.failing = failing

    async def mark(self, ids: Iterable[UUID]) -> None:
        self.calls.append(("closing", list(ids)))

    async def stop(self, ref: str) -> None:
        self.calls.append(("stop", ref))
        if ref == self.failing:
            raise RuntimeError("docker said no")


async def test_marks_closing_then_stops_each_tenant() -> None:
    rec = _Recorder()
    a, b = uuid4(), uuid4()
    async with closing_at_exit(rec, mark=rec.mark) as created:
        created += [a, b]
    assert rec.calls == [("closing", [a, b]), ("stop", tenant_ref(a)), ("stop", tenant_ref(b))]


async def test_stops_every_tenant_when_the_body_and_one_stop_fail() -> None:
    a, b = uuid4(), uuid4()
    rec = _Recorder(failing=tenant_ref(a))
    with pytest.raises(ValueError, match="the test failed"):
        async with closing_at_exit(rec, mark=rec.mark) as created:
            created += [a, b]
            raise ValueError("the test failed")
    assert rec.calls == [("closing", [a, b]), ("stop", tenant_ref(a)), ("stop", tenant_ref(b))]


async def test_a_failed_stop_fails_the_teardown_after_the_others() -> None:
    a, b = uuid4(), uuid4()
    rec = _Recorder(failing=tenant_ref(a))
    with pytest.raises(RuntimeError, match="docker said no"):
        async with closing_at_exit(rec, mark=rec.mark) as created:
            created += [a, b]
    assert ("stop", tenant_ref(b)) in rec.calls


async def test_without_a_runtime_only_marks_closing() -> None:
    rec = _Recorder()
    async with closing_at_exit(mark=rec.mark) as created:
        created.append(uuid4())
    assert [c[0] for c in rec.calls] == ["closing"]


async def test_a_failed_body_wins_over_a_failed_mark() -> None:
    rec = _Recorder()

    async def failing_mark(ids: Iterable[UUID]) -> None:
        raise RuntimeError("database gone")

    a = uuid4()
    with pytest.raises(ValueError, match="the test failed"):
        async with closing_at_exit(rec, mark=failing_mark) as created:
            created.append(a)
            raise ValueError("the test failed")
    assert rec.calls == [("stop", tenant_ref(a))]
