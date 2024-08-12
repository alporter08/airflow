from pendulum import timezone, Date, DateTime, Time
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction
from typing import TYPE_CHECKING
import logging
from datetime import timedelta

if TYPE_CHECKING:
    from airflow.timetables.base import TimeRestriction

log = logging.getLogger(__name__)

try:
    from pandas.tseries.holiday import USFederalHolidayCalendar

    holiday_calendar = USFederalHolidayCalendar()
except ImportError:
    log.warning("Could not import pandas. Holidays will not be considered.")
    holiday_calendar = None  # type: ignore[assignment]

EASTERN = timezone("America/New_York")


class AfterWorkdayTimetable(Timetable):
    def get_next_workday(self, d: DateTime, incr=1) -> DateTime:
        next_start = d
        while True:
            if next_start.weekday() not in (5, 6):  # not on weekend
                if holiday_calendar is None:
                    holidays = set()
                else:
                    holidays = holiday_calendar.holidays(
                        start=next_start.in_timezone("UTC"),
                        end=next_start.in_timezone("UTC"),
                    ).tz_localize(None)
                if next_start.in_timezone("UTC").replace(tzinfo=None) not in holidays:
                    break
            next_start = next_start.add(days=incr)
        return next_start

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        start = DateTime.combine(
            (run_after - timedelta(days=1)).date(), Time.min
        ).replace(tzinfo=EASTERN)
        # Skip backwards over weekends and holidays to find last run
        start = self.get_next_workday(start, incr=-1)
        return DataInterval(start=start, end=(start + timedelta(days=1)))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if (
            last_automated_data_interval is not None
        ):  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            next_start = DateTime.combine(
                (last_start + timedelta(days=1)).date(), Time.min
            )
        # Otherwise this is the first ever run on the regular schedule...
        elif (earliest := restriction.earliest) is None:
            return None  # No start_date. Don't schedule.
        elif not restriction.catchup:
            # If the DAG has catchup=False, today is the earliest to consider.
            next_start = max(earliest, DateTime.combine(Date.today(), Time.min))
        elif earliest.time() != Time.min:
            # If earliest does not fall on midnight, skip to the next day.
            next_start = DateTime.combine(earliest.date() + timedelta(days=1), Time.min)
        else:
            next_start = earliest

        # Ensure next_start is in Eastern Time
        next_start = next_start.replace(tzinfo=EASTERN)

        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.

        return DagRunInfo.interval(
            start=next_start, end=(next_start + timedelta(days=1))
        )


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [AfterWorkdayTimetable]
