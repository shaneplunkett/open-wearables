"""Service for cardiac/POTS daily summaries.

Separate from summaries_service.py to keep that 730-line file focused.
Follows the same pattern: repository queries → Python aggregation → schema output.
"""

from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import asc

from app.database import DbSession
from app.models import DataPointSeries, DataSource
from app.repositories.data_point_series_repository import DataPointSeriesRepository
from app.schemas.cardiac import (
    CardiacDailySummary,
    CardiacHRV,
    CardiacSampleCounts,
    CardiacSpO2,
    CardiacTimeBlock,
)
from app.schemas.common_types import PaginatedResponse, Pagination, TimeseriesMetadata
from app.schemas.series_types import SeriesType, get_series_type_id
from app.utils.exceptions import handle_exceptions

# Series types needed for cardiac summary
CARDIAC_SERIES_TYPES = [
    SeriesType.heart_rate,
    SeriesType.resting_heart_rate,
    SeriesType.walking_heart_rate_average,
    SeriesType.heart_rate_variability_sdnn,
    SeriesType.oxygen_saturation,
    SeriesType.respiratory_rate,
]

# Time block boundaries (local timezone hours, half-open intervals)
TIME_BLOCKS = [
    ("overnight", 0, 6),
    ("morning", 6, 12),
    ("afternoon", 12, 18),
    ("evening", 18, 24),
]

# Tachycardia threshold — HR above this at rest is clinically significant for POTS
TACHYCARDIA_HR_THRESHOLD = 100


class CardiacService:
    """Service for cardiac/POTS daily summaries."""

    def __init__(self, log: Logger):
        self.logger = log
        self.data_point_repo = DataPointSeriesRepository(DataPointSeries)

    @handle_exceptions
    async def get_cardiac_summaries(
        self,
        db_session: DbSession,
        user_id: UUID,
        start_date: datetime,
        end_date: datetime,
        tz_name: str = "Australia/Melbourne",
    ) -> PaginatedResponse[CardiacDailySummary]:
        """Get cardiac daily summaries with POTS-relevant metrics.

        Queries raw data points, converts to local timezone for date grouping
        and time block computation, then aggregates per day.
        """
        self.logger.debug(
            f"Fetching cardiac summaries for user {user_id} from {start_date} to {end_date} (tz={tz_name})"
        )

        local_tz = ZoneInfo(tz_name)

        # Get all relevant series type IDs
        type_ids = [get_series_type_id(t) for t in CARDIAC_SERIES_TYPES]
        type_id_lookup = {get_series_type_id(t): t for t in CARDIAC_SERIES_TYPES}

        # Convert local date boundaries to UTC for the DB query.
        # Without this, querying 'March 5 Melbourne' misses data from
        # midnight-11am AEDT (which is still March 4 in UTC).
        local_start = datetime.combine(start_date.date(), datetime.min.time(), tzinfo=local_tz)
        local_end = datetime.combine((end_date + timedelta(days=1)).date(), datetime.min.time(), tzinfo=local_tz)
        utc_start = local_start.astimezone(timezone.utc)
        utc_end = local_end.astimezone(timezone.utc)

        # Query raw data points for the entire range
        results = (
            db_session.query(
                DataPointSeries.recorded_at,
                DataPointSeries.value,
                DataPointSeries.series_type_definition_id,
            )
            .join(DataSource, DataPointSeries.data_source_id == DataSource.id)
            .filter(
                DataSource.user_id == user_id,
                DataPointSeries.recorded_at >= utc_start,
                DataPointSeries.recorded_at < utc_end,
                DataPointSeries.series_type_definition_id.in_(type_ids),
            )
            .order_by(asc(DataPointSeries.recorded_at))
            .all()
        )

        # Group data by local date
        # Structure: {date_str: {SeriesType: [(local_datetime, value)]}}
        by_date: dict[str, dict[SeriesType, list[tuple[datetime, float]]]] = defaultdict(lambda: defaultdict(list))

        for recorded_at, value, type_def_id in results:
            series_type = type_id_lookup.get(type_def_id)
            if series_type is None:
                continue

            # Convert UTC to local timezone for correct date assignment
            if recorded_at.tzinfo is None:
                recorded_at = recorded_at.replace(tzinfo=timezone.utc)
            local_dt = recorded_at.astimezone(local_tz)
            local_date = local_dt.date().isoformat()

            by_date[local_date][series_type].append((local_dt, float(value)))

        # Build daily summaries
        data = []
        for date_str in sorted(by_date.keys()):
            day_data = by_date[date_str]
            summary = self._build_daily_summary(date_str, day_data)
            data.append(summary)

        return PaginatedResponse(
            data=data,
            pagination=Pagination(has_more=False, next_cursor=None, previous_cursor=None),
            metadata=TimeseriesMetadata(
                sample_count=len(data),
                start_time=start_date,
                end_time=end_date,
            ),
        )

    def _build_daily_summary(
        self,
        date_str: str,
        day_data: dict[SeriesType, list[tuple[datetime, float]]],
    ) -> CardiacDailySummary:
        """Build a single day's cardiac summary from raw data points."""
        hr_samples = day_data.get(SeriesType.heart_rate, [])
        resting_hr_samples = day_data.get(SeriesType.resting_heart_rate, [])
        walking_hr_samples = day_data.get(SeriesType.walking_heart_rate_average, [])
        hrv_samples = day_data.get(SeriesType.heart_rate_variability_sdnn, [])
        spo2_samples = day_data.get(SeriesType.oxygen_saturation, [])
        resp_samples = day_data.get(SeriesType.respiratory_rate, [])

        hr_values = [v for _, v in hr_samples]
        resting_hr_values = [v for _, v in resting_hr_samples]
        walking_hr_values = [v for _, v in walking_hr_samples]
        hrv_values = [v for _, v in hrv_samples]
        # Normalise SpO2: Apple Health stores as 0-1 fraction, we need 0-100 percent
        spo2_values = [v * 100 if v <= 1.0 else v for _, v in spo2_samples]
        spo2_samples_normalised = [(dt, v * 100 if v <= 1.0 else v) for dt, v in spo2_samples]
        resp_values = [v for _, v in resp_samples]

        # Resting HR — Apple Health provides a daily summary value
        resting_hr = int(round(sum(resting_hr_values) / len(resting_hr_values))) if resting_hr_values else None

        # Walking HR average
        walking_hr = int(round(sum(walking_hr_values) / len(walking_hr_values))) if walking_hr_values else None

        # Orthostatic delta — best proxy from passive wearable data
        orthostatic_delta = None
        if walking_hr is not None and resting_hr is not None:
            orthostatic_delta = walking_hr - resting_hr

        # HR aggregates
        avg_hr = int(round(sum(hr_values) / len(hr_values))) if hr_values else None
        min_hr = int(min(hr_values)) if hr_values else None
        max_hr = int(max(hr_values)) if hr_values else None

        # HRV
        hrv = None
        if hrv_values:
            hrv = CardiacHRV(
                avg_sdnn_ms=round(sum(hrv_values) / len(hrv_values), 1),
                min_sdnn_ms=round(min(hrv_values), 1),
                max_sdnn_ms=round(max(hrv_values), 1),
                readings_count=len(hrv_values),
            )

        # SpO2
        spo2 = None
        if spo2_values:
            spo2 = CardiacSpO2(
                avg_percent=round(sum(spo2_values) / len(spo2_values), 1),
                min_percent=round(min(spo2_values), 1),
                readings_below_90_count=sum(1 for v in spo2_values if v < 90),
                readings_count=len(spo2_values),
            )

        # Respiratory rate
        resp_avg = round(sum(resp_values) / len(resp_values), 1) if resp_values else None

        # Tachycardia minutes
        tachycardia_mins = self._compute_tachycardia_minutes(hr_samples)

        # Time blocks
        time_blocks = self._compute_time_blocks(hr_samples, hrv_samples, spo2_samples_normalised)

        # Sample counts
        sample_counts = CardiacSampleCounts(
            heart_rate=len(hr_values),
            hrv_sdnn=len(hrv_values),
            spo2=len(spo2_values),
        )

        return CardiacDailySummary(
            date=date_type.fromisoformat(date_str),
            resting_heart_rate_bpm=resting_hr,
            walking_heart_rate_avg_bpm=walking_hr,
            orthostatic_hr_delta_bpm=orthostatic_delta,
            avg_heart_rate_bpm=avg_hr,
            min_heart_rate_bpm=min_hr,
            max_heart_rate_bpm=max_hr,
            hrv=hrv,
            spo2=spo2,
            respiratory_rate_avg_brpm=resp_avg,
            tachycardia_minutes=tachycardia_mins,
            time_blocks=time_blocks,
            sample_counts=sample_counts,
            source="apple_health",
        )

    def _compute_tachycardia_minutes(self, hr_samples: list[tuple[datetime, float]]) -> int | None:
        """Count minutes where average HR exceeds tachycardia threshold.

        Buckets HR samples by minute, then counts minutes where
        the average HR in that minute was > 100bpm.
        """
        if not hr_samples:
            return None

        by_minute: dict[str, list[float]] = defaultdict(list)
        for dt, val in hr_samples:
            minute_key = dt.strftime("%Y-%m-%d %H:%M")
            by_minute[minute_key].append(val)

        tachy_count = 0
        for values in by_minute.values():
            avg = sum(values) / len(values)
            if avg > TACHYCARDIA_HR_THRESHOLD:
                tachy_count += 1

        return tachy_count

    def _compute_time_blocks(
        self,
        hr_samples: list[tuple[datetime, float]],
        hrv_samples: list[tuple[datetime, float]],
        spo2_samples: list[tuple[datetime, float]],
    ) -> list[CardiacTimeBlock]:
        """Compute per-block aggregations using local timezone hours."""
        blocks = []
        for block_name, start_hour, end_hour in TIME_BLOCKS:
            block_hr = [v for dt, v in hr_samples if start_hour <= dt.hour < end_hour]
            block_hrv = [v for dt, v in hrv_samples if start_hour <= dt.hour < end_hour]
            block_spo2 = [v for dt, v in spo2_samples if start_hour <= dt.hour < end_hour]

            blocks.append(
                CardiacTimeBlock(
                    block=block_name,
                    avg_hr_bpm=int(round(sum(block_hr) / len(block_hr))) if block_hr else None,
                    min_hr_bpm=int(min(block_hr)) if block_hr else None,
                    max_hr_bpm=int(max(block_hr)) if block_hr else None,
                    avg_hrv_sdnn_ms=round(sum(block_hrv) / len(block_hrv), 1) if block_hrv else None,
                    avg_spo2_percent=round(sum(block_spo2) / len(block_spo2), 1) if block_spo2 else None,
                    hr_samples=len(block_hr),
                )
            )

        return blocks


cardiac_service = CardiacService(log=getLogger(__name__))
