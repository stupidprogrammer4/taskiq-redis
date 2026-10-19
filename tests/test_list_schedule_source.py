import datetime
import uuid

import pytest
from freezegun import freeze_time
from redis.asyncio import Redis
from taskiq import ScheduledTask

from taskiq_redis.list_schedule_source import ListRedisScheduleSource
from taskiq_redis.schedule_source import RedisScheduleSource


@pytest.mark.anyio
@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "schedule",
        "auryx:schedules",
        ":leading",
        "trailing:",
        "tenant::account:retries",
        "tenant:time:queue",
        "زمان‌بندی:طلاملا",
        "emoji:🦋",
    ],
)
@pytest.mark.parametrize(
    "offset",
    [datetime.timedelta(), datetime.timedelta(hours=3, minutes=30)],
)
@freeze_time("2025-01-01 00:00:00")
async def test_past_schedule_preserves_arbitrary_prefix_and_payload(
    redis_url: str,
    prefix: str,
    offset: datetime.timedelta,
) -> None:
    """Recover existing past schedules without changing their stored payload."""
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=4)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={"queue_name": "test"},
        args=[7],
        kwargs={"note": "آزمون بازیابی"},
        time=at.astimezone(datetime.timezone(offset)),
    )
    try:
        await source.add_schedule(schedule)
        async with Redis.from_url(redis_url) as redis:
            key = f"{prefix}:data:{schedule.schedule_id}"
            before = await redis.get(key)
            assert before is not None
            assert await source.get_schedules() == [schedule]
            assert await redis.get(key) == before
        await source.post_send(schedule)
        assert await source.get_schedules() == []
    finally:
        await source.delete_schedule(schedule.schedule_id)
        await source.shutdown()


@pytest.mark.parametrize(
    "timestamp",
    ["not-a-date", "2025-13-01T00:00", "2025-01-01T25:00"],
)
def test_invalid_time_key_remains_ignored(timestamp: str) -> None:
    source = ListRedisScheduleSource("redis://localhost:1", prefix="tenant:time:jobs")
    assert source._parse_time_key(f"tenant:time:jobs:time:{timestamp}") is None


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_cron(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_interval(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=datetime.timedelta(seconds=5),
    )
    await source.add_schedule(schedule)
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_from_past(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=4),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    for schedule in schedules:
        await source.post_send(schedule)
    # After getting the schedules for the second time,
    # all the schedules in the past are ignored.
    schedules = await source.get_schedules()
    assert schedules == []


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_removal_time(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=4),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == []
    # Assert that we will get the schedule after the time has passed.
    with freeze_time("2025-01-01 00:04:00"):
        scehdules = await source.get_schedules()
        assert scehdules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_removal_cron(redis_url: str) -> None:
    """Test removing cron schedules."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    scehdules = await source.get_schedules()
    assert scehdules == []


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_removal_interval(redis_url: str) -> None:
    """Test removing cron schedules."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=datetime.timedelta(seconds=30),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    scehdules = await source.get_schedules()
    assert scehdules == []


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_migration(redis_url: str) -> None:
    """Test adding a cron schedule."""
    new_prefix = uuid.uuid4().hex
    old_prefix = uuid.uuid4().hex
    old_source = RedisScheduleSource(redis_url, prefix=old_prefix)

    for i in range(30):
        schedule = ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            time=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=i),
        )
        await old_source.add_schedule(schedule)

    old_schedules = await old_source.get_schedules()

    source = ListRedisScheduleSource(
        redis_url,
        prefix=new_prefix,
        skip_past_schedules=True,
    ).with_migrate_from(
        old_source,
        delete_schedules=True,
    )

    await source.startup()
    assert await old_source.get_schedules() == []

    for old_schedule in old_schedules:
        with freeze_time(old_schedule.time):
            assert await source.get_schedules() == [old_schedule]
