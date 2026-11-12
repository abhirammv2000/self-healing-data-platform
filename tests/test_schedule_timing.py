"""When a schedule is due. These are the rules of worker/app/scheduler.py that need no database: which fire time
counts, in which time zone, and what the idempotency key looks like. Starting runs is tested in
tests/integration/test_scheduler.py."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from worker.app.scheduler import idempotency_key, latest_fire_time

UTC = timezone.utc


def at(hour, minute=0, second=0, day=1, month=3, year=2026):
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def test_nothing_is_due_before_the_first_fire_time_after_the_mark():
    assert latest_fire_time("0 * * * *", "UTC", handled_until=at(10, 5), now=at(10, 59, 59)) is None


def test_a_fire_time_exactly_now_is_due():
    assert latest_fire_time("0 * * * *", "UTC", handled_until=at(10, 5), now=at(11, 0, 0)) == at(11)


def test_a_fire_time_that_was_already_handled_is_not_due_again():
    assert latest_fire_time("0 * * * *", "UTC", handled_until=at(11), now=at(11, 30)) is None


def test_after_a_long_stop_only_the_newest_missed_fire_time_is_due():
    # every minute, nothing ran for three hours: one run, for the newest minute, not 180
    assert latest_fire_time("* * * * *", "UTC", handled_until=at(8), now=at(11, 0, 30)) == at(11)


def test_the_fire_time_is_worked_out_in_the_schedules_time_zone():
    # 09:00 every day in New York. In March, before daylight saving starts, that is 14:00 UTC
    fire = latest_fire_time("0 9 * * *", "America/New_York", handled_until=at(0, day=1), now=at(15, day=1))
    assert fire == datetime(2026, 3, 1, 9, 0, tzinfo=ZoneInfo("America/New_York"))
    assert fire.astimezone(UTC) == at(14, day=1)
    assert latest_fire_time("0 9 * * *", "America/New_York", handled_until=at(0, day=1), now=at(13, 59, day=1)) is None


def test_the_clock_change_moves_the_fire_time_in_utc_but_not_in_local_time():
    # daylight saving started on 8 March 2026 in New York, so 09:00 local is 13:00 UTC afterwards
    fire = latest_fire_time("0 9 * * *", "America/New_York", handled_until=at(0, day=9), now=at(14, day=9))
    assert fire.astimezone(UTC) == at(13, day=9)


def test_the_key_names_the_schedule_and_the_fire_time_in_utc():
    local = datetime(2026, 3, 1, 9, 0, tzinfo=ZoneInfo("America/New_York"))
    assert idempotency_key(4, local) == "schedule:4:2026-03-01T14:00:00+00:00"


def test_the_same_fire_time_in_two_time_zones_gives_the_same_key():
    local = datetime(2026, 3, 1, 9, 0, tzinfo=ZoneInfo("America/New_York"))
    assert idempotency_key(4, local) == idempotency_key(4, local.astimezone(UTC))


def test_a_key_fits_the_idempotency_column():
    assert len(idempotency_key(10**9, at(10) + timedelta(days=36500))) <= 255
