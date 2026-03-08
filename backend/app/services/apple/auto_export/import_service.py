import json
from datetime import datetime
from decimal import Decimal
from logging import Logger, getLogger
from typing import Any, Iterable
from uuid import UUID, uuid4

from app.database import DbSession
from app.schemas import (
    AEWorkoutJSON,
    EventRecordCreate,
    EventRecordDetailCreate,
    EventRecordMetrics,
    HeartRateSampleCreate,
    RootJSON,
    TimeSeriesSampleCreate,
    UploadDataResponse,
)
from app.schemas.series_types import SeriesType
from app.services.event_record_service import event_record_service
from app.services.timeseries_service import timeseries_service
from app.utils.exceptions import handle_exceptions
from app.utils.structured_logging import log_structured

APPLE_DT_FORMAT = "%Y-%m-%d %H:%M:%S %z"

# Health Auto Export metric name → SeriesType mapping
AUTO_EXPORT_METRIC_MAP: dict[str, SeriesType] = {
    "heart_rate": SeriesType.heart_rate,
    "resting_heart_rate": SeriesType.resting_heart_rate,
    "heart_rate_variability": SeriesType.heart_rate_variability_sdnn,
    "walking_heart_rate_average": SeriesType.walking_heart_rate_average,
    "step_count": SeriesType.steps,
    "active_energy": SeriesType.energy,
    "basal_energy_burned": SeriesType.basal_energy,
    "oxygen_saturation": SeriesType.oxygen_saturation,
    "respiratory_rate": SeriesType.respiratory_rate,
    "blood_glucose": SeriesType.blood_glucose,
    "body_mass": SeriesType.weight,
    "weight": SeriesType.weight,
    "body_fat_percentage": SeriesType.body_fat_percentage,
    "body_temperature": SeriesType.body_temperature,
    "flights_climbed": SeriesType.flights_climbed,
    "distance_walking_running": SeriesType.distance_walking_running,
    "vo2_max": SeriesType.vo2_max,
    "walking_speed": SeriesType.walking_speed,
    "walking_step_length": SeriesType.walking_step_length,
    "walking_double_support_percentage": SeriesType.walking_double_support_percentage,
    "walking_asymmetry_percentage": SeriesType.walking_asymmetry_percentage,
    "exercise_time": SeriesType.exercise_time,
    "stand_time": SeriesType.stand_time,
    "environmental_audio_exposure": SeriesType.environmental_audio_exposure,
    "headphone_audio_exposure": SeriesType.headphone_audio_exposure,
    "height": SeriesType.height,
    "lean_body_mass": SeriesType.lean_body_mass,
    "body_mass_index": SeriesType.body_mass_index,
    "distance_cycling": SeriesType.distance_cycling,
    "distance_swimming": SeriesType.distance_swimming,
    "apple_stand_time": SeriesType.stand_time,
    "walking_running_distance": SeriesType.distance_walking_running,
    "physical_effort": SeriesType.physical_effort,
    "blood_oxygen_saturation": SeriesType.oxygen_saturation,
    "apple_sleeping_wrist_temperature": SeriesType.skin_temperature,
    "breathing_disturbances": SeriesType.sleeping_breathing_disturbances,
    "apple_exercise_time": SeriesType.exercise_time,
    "time_in_daylight": SeriesType.time_in_daylight,
    "stair_speed_up": SeriesType.stair_ascent_speed,
    "stair_speed_down": SeriesType.stair_descent_speed,
    "six_minute_walking_test_distance": SeriesType.six_minute_walk_test_distance,
}

BLOOD_PRESSURE_METRIC = "blood_pressure"
SLEEP_METRIC = "sleep_analysis"


class ImportService:
    def __init__(self, log: Logger):
        self.log = log
        self.event_record_service = event_record_service
        self.timeseries_service = timeseries_service

    def _dt(self, s: str) -> datetime:
        s = s.replace(" +", "+").replace(" ", "T", 1)
        if len(s) >= 5 and (s[-5] in {"+", "-"} and s[-3] != ":"):
            s = f"{s[:-2]}:{s[-2:]}"
        return datetime.fromisoformat(s)

    def _dec(self, x: float | int | None) -> Decimal | None:
        return None if x is None else Decimal(str(x))

    def _compute_metrics(self, workout: AEWorkoutJSON) -> EventRecordMetrics:
        hr_entries = workout.heartRateData or []

        hr_min_candidates = [self._dec(entry.min) for entry in hr_entries if entry.min is not None]
        hr_max_candidates = [self._dec(entry.max) for entry in hr_entries if entry.max is not None]
        hr_avg_candidates = [self._dec(entry.avg) for entry in hr_entries if entry.avg is not None]

        heart_rate_min = min(hr_min_candidates) if hr_min_candidates else None
        heart_rate_max = max(hr_max_candidates) if hr_max_candidates else None
        heart_rate_avg = (
            sum(hr_avg_candidates, Decimal("0")) / Decimal(len(hr_avg_candidates)) if hr_avg_candidates else None
        )

        return {
            "heart_rate_min": int(heart_rate_min) if heart_rate_min is not None else None,
            "heart_rate_max": int(heart_rate_max) if heart_rate_max is not None else None,
            "heart_rate_avg": heart_rate_avg,
            "steps_count": None,
        }

    def _get_records(
        self,
        workout: AEWorkoutJSON,
        user_id: UUID,
    ) -> list[HeartRateSampleCreate]:
        samples: list[HeartRateSampleCreate] = []

        heart_rate_fields = ("heartRate", "heartRateRecovery")
        for field in heart_rate_fields:
            entries = getattr(workout, field, None)
            if not entries:
                continue

            for entry in entries:
                value = entry.avg or entry.max or entry.min or 0
                source_name = getattr(entry, "source", None) or "Auto Export"
                samples.append(
                    HeartRateSampleCreate(
                        id=uuid4(),
                        external_id=None,
                        user_id=user_id,
                        source="apple_health_auto_export",
                        device_model=source_name,
                        recorded_at=self._dt(entry.date),
                        value=self._dec(value) or 0,
                    ),
                )

        return samples

    def _extract_value(self, entry: dict[str, Any], metric_name: str) -> Decimal | None:
        """Extract the numeric value from a metric data entry.

        Heart rate-style entries use Avg/Min/Max.
        Generic entries use qty.
        """
        if "Avg" in entry:
            return self._dec(entry["Avg"])
        if "avg" in entry:
            return self._dec(entry["avg"])
        if "qty" in entry:
            return self._dec(entry["qty"])
        if "Min" in entry:
            return self._dec(entry["Min"])
        return None

    def _process_metrics(
        self,
        raw: dict,
        user_id: str,
    ) -> tuple[list[TimeSeriesSampleCreate], list[tuple[EventRecordCreate, EventRecordDetailCreate]], int]:
        """Parse data.metrics[] and return time series samples and sleep records.

        Returns:
            (samples, sleep_records, metrics_skipped) tuple
        """
        root = RootJSON(**raw)
        metrics_raw: list[dict[str, Any]] = root.data.get("metrics", [])
        user_uuid = UUID(user_id)

        samples: list[TimeSeriesSampleCreate] = []
        sleep_records: list[tuple[EventRecordCreate, EventRecordDetailCreate]] = []
        metrics_skipped = 0

        for metric in metrics_raw:
            name = metric.get("name", "")
            data_entries: list[dict[str, Any]] = metric.get("data", [])

            if not data_entries:
                continue

            # Sleep gets special handling — EventRecord, not time series
            if name == SLEEP_METRIC:
                log_structured(
                    self.log,
                    "info",
                    "Sleep metric data sample",
                    provider="apple",
                    action="apple_ae_sleep_debug",
                    entry_count=len(data_entries),
                    first_entry=data_entries[0] if data_entries else None,
                    entry_keys=list(data_entries[0].keys()) if data_entries else [],
                )
                sleep_records.extend(self._process_sleep_metric(data_entries, user_uuid))
                continue

            # Blood pressure creates two series per entry
            if name == BLOOD_PRESSURE_METRIC:
                for entry in data_entries:
                    date_str = entry.get("date")
                    if not date_str:
                        continue
                    recorded_at = self._dt(date_str)
                    systolic = self._dec(entry.get("systolic"))
                    diastolic = self._dec(entry.get("diastolic"))
                    source_name = entry.get("source", "Auto Export")
                    if systolic is not None:
                        samples.append(
                            TimeSeriesSampleCreate(
                                id=uuid4(),
                                user_id=user_uuid,
                                source="apple_health_auto_export",
                                device_model=source_name,
                                recorded_at=recorded_at,
                                value=systolic,
                                series_type=SeriesType.blood_pressure_systolic,
                            ),
                        )
                    if diastolic is not None:
                        samples.append(
                            TimeSeriesSampleCreate(
                                id=uuid4(),
                                user_id=user_uuid,
                                source="apple_health_auto_export",
                                device_model=source_name,
                                recorded_at=recorded_at,
                                value=diastolic,
                                series_type=SeriesType.blood_pressure_diastolic,
                            ),
                        )
                continue

            # Standard metric lookup
            series_type = AUTO_EXPORT_METRIC_MAP.get(name)
            if series_type is None:
                metrics_skipped += len(data_entries)
                log_structured(
                    self.log,
                    "debug",
                    "Skipping unknown auto export metric: %s (%d entries)",
                    provider="apple",
                    action="apple_ae_metric_skip",
                    metric_name=name,
                    entry_count=len(data_entries),
                )
                continue

            for entry in data_entries:
                date_str = entry.get("date")
                if not date_str:
                    continue
                value = self._extract_value(entry, name)
                if value is None:
                    continue
                source_name = entry.get("source", "Auto Export")
                samples.append(
                    TimeSeriesSampleCreate(
                        id=uuid4(),
                        user_id=user_uuid,
                        source="apple_health_auto_export",
                        device_model=source_name,
                        recorded_at=self._dt(date_str),
                        value=value,
                        series_type=series_type,
                    ),
                )

        return samples, sleep_records, metrics_skipped

    def _process_sleep_metric(
        self,
        data_entries: list[dict[str, Any]],
        user_uuid: UUID,
    ) -> list[tuple[EventRecordCreate, EventRecordDetailCreate]]:
        """Build sleep EventRecords from auto export sleep_analysis entries."""
        results: list[tuple[EventRecordCreate, EventRecordDetailCreate]] = []

        for entry in data_entries:
            total_sleep = entry.get("totalSleep")
            if total_sleep is None or total_sleep <= 0:
                continue

            sleep_start_str = entry.get("sleepStart") or entry.get("date")
            sleep_end_str = entry.get("sleepEnd")
            if not sleep_start_str or not sleep_end_str:
                continue

            sleep_start = self._dt(sleep_start_str)
            sleep_end = self._dt(sleep_end_str)

            asleep_hrs = entry.get("asleep", 0) or 0
            deep_hrs = entry.get("deep", 0) or 0
            rem_hrs = entry.get("rem", 0) or 0
            core_hrs = entry.get("core", 0) or 0
            awake_hrs = max(total_sleep - asleep_hrs, 0) if asleep_hrs else 0

            duration_seconds = int((sleep_end - sleep_start).total_seconds())
            efficiency = round(asleep_hrs / total_sleep * 100, 1) if total_sleep > 0 and asleep_hrs else None

            record_id = uuid4()
            record = EventRecordCreate(
                category="sleep",
                type="sleep_session",
                source_name="Auto Export",
                device_model=None,
                duration_seconds=duration_seconds,
                start_datetime=sleep_start,
                end_datetime=sleep_end,
                id=record_id,
                external_id=None,
                source="apple_health_auto_export",
                user_id=user_uuid,
            )

            detail = EventRecordDetailCreate(
                record_id=record_id,
                sleep_total_duration_minutes=round(asleep_hrs * 60, 1),
                sleep_time_in_bed_minutes=round(total_sleep * 60, 1),
                sleep_deep_minutes=round(deep_hrs * 60, 1),
                sleep_rem_minutes=round(rem_hrs * 60, 1),
                sleep_light_minutes=round(core_hrs * 60, 1),
                sleep_awake_minutes=round(awake_hrs * 60, 1),
                sleep_efficiency_score=efficiency,
                is_nap=False,
            )

            results.append((record, detail))

        return results

    def _build_import_bundles(
        self,
        raw: dict,
        user_id: str,
    ) -> Iterable[tuple[EventRecordCreate, EventRecordDetailCreate, list[HeartRateSampleCreate]]]:
        """
        Given the parsed JSON dict from HealthAutoExport, yield ImportBundles
        ready to insert the database.
        """
        root = RootJSON(**raw)
        workouts_raw = root.data.get("workouts", [])

        user_uuid = UUID(user_id)
        for w in workouts_raw:
            wjson = AEWorkoutJSON(**w)

            workout_id = uuid4()

            start_date = self._dt(wjson.start)
            end_date = self._dt(wjson.end)
            duration_seconds = int((end_date - start_date).total_seconds())

            metrics = self._compute_metrics(wjson)
            hr_samples = self._get_records(wjson, user_uuid)

            workout_type = wjson.name or "Unknown Workout"

            record = EventRecordCreate(
                category="workout",
                type=workout_type,
                source_name="Auto Export",
                device_model=None,
                duration_seconds=duration_seconds,
                start_datetime=start_date,
                end_datetime=end_date,
                id=workout_id,
                external_id=wjson.id,
                source="apple_health_auto_export",
                user_id=user_uuid,
            )

            detail = EventRecordDetailCreate(
                record_id=workout_id,
                **metrics,
            )

            yield record, detail, hr_samples

    def load_data(
        self,
        db_session: DbSession,
        raw: dict,
        user_id: str,
        batch_id: str | None = None,
    ) -> dict[str, int]:
        """
        Load data into database and return counts of saved items.

        Returns:
            dict with counts of workouts, records (HR), metrics, and sleep saved.
        """
        workouts_saved = 0
        records_saved = 0
        # Collect all HR samples from all workouts for a single batch insert
        all_hr_samples: list[HeartRateSampleCreate] = []

        for record, detail, hr_samples in self._build_import_bundles(raw, user_id):
            created_record = self.event_record_service.create(db_session, record)
            detail_for_record = detail.model_copy(update={"record_id": created_record.id})
            self.event_record_service.create_detail(db_session, detail_for_record)
            workouts_saved += 1

            if hr_samples:
                all_hr_samples.extend(hr_samples)

        # Single batch insert for all HR samples
        if all_hr_samples:
            self.timeseries_service.bulk_create_samples(db_session, all_hr_samples)
            records_saved = len(all_hr_samples)

        # Process metrics (heart rate, steps, sleep, blood pressure, etc.)
        metric_samples, sleep_records, metrics_skipped = self._process_metrics(raw, user_id)

        if metric_samples:
            self.timeseries_service.bulk_create_samples(db_session, metric_samples)

        # Persist sleep records
        for sleep_record, sleep_detail in sleep_records:
            created = self.event_record_service.create(db_session, sleep_record)
            detail_for_sleep = sleep_detail.model_copy(update={"record_id": created.id})
            self.event_record_service.create_detail(db_session, detail_for_sleep, detail_type="sleep")

        # Commit all changes in one transaction
        db_session.commit()

        return {
            "workouts_saved": workouts_saved,
            "records_saved": records_saved,
            "metrics_saved": len(metric_samples),
            "metrics_skipped": metrics_skipped,
            "sleep_saved": len(sleep_records),
        }

    @handle_exceptions
    def import_data_from_request(
        self,
        db_session: DbSession,
        request_content: str,
        content_type: str,
        user_id: str,
        batch_id: str | None = None,
    ) -> UploadDataResponse:
        try:
            # Parse content based on type
            if "multipart/form-data" in content_type:
                data = self._parse_multipart_content(request_content)
            else:
                data = self._parse_json_content(request_content)

            if not data:
                log_structured(
                    self.log,
                    "warning",
                    "No valid data found in request",
                    provider="apple",
                    action="apple_ae_validate_data",
                    batch_id=batch_id,
                    user_id=user_id,
                )
                return UploadDataResponse(status_code=400, response="No valid data found", user_id=user_id)

            # Extract incoming counts for logging
            data_section = data.get("data", {})
            incoming_workouts = len(data_section.get("workouts", []))
            incoming_metrics = len(data_section.get("metrics", []))

            # Debug: log all top-level keys and metric names
            log_structured(
                self.log,
                "info",
                "Auto Export payload structure",
                provider="apple",
                action="apple_ae_payload_debug",
                data_keys=list(data_section.keys()),
                metric_names=[m.get("name") for m in data_section.get("metrics", [])],
                user_id=user_id,
            )

            # Load data and get saved counts
            saved_counts = self.load_data(db_session, data, user_id=user_id, batch_id=batch_id)

            # Log detailed processing results
            log_structured(
                self.log,
                "info",
                "Apple Auto Export data import completed",
                provider="apple",
                action="apple_ae_import_complete",
                batch_id=batch_id,
                user_id=user_id,
                incoming_workouts=incoming_workouts,
                incoming_metrics=incoming_metrics,
                workouts_saved=saved_counts["workouts_saved"],
                records_saved=saved_counts["records_saved"],
                metrics_saved=saved_counts["metrics_saved"],
                metrics_skipped=saved_counts["metrics_skipped"],
                sleep_saved=saved_counts["sleep_saved"],
            )

        except Exception as e:
            log_structured(
                self.log,
                "error",
                f"Import failed for user {user_id}: {e}",
                provider="apple",
                action="apple_ae_import_failed",
                batch_id=batch_id,
                user_id=user_id,
                error_type=type(e).__name__,
            )
            return UploadDataResponse(status_code=400, response=f"Import failed: {str(e)}", user_id=user_id)

        return UploadDataResponse(status_code=200, response="Import successful", user_id=user_id)

    def _parse_multipart_content(self, content: str) -> dict | None:
        """Parse multipart form data to extract JSON."""
        json_start = content.find('{\n  "data"')
        if json_start == -1:
            json_start = content.find('{"data"')
        if json_start == -1:
            return None

        brace_count = 0
        json_end = json_start
        for i, char in enumerate(content[json_start:], json_start):
            if char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    json_end = i
                    break

        if brace_count != 0:
            return None

        json_str = content[json_start : json_end + 1]
        return json.loads(json_str)

    def _parse_json_content(self, content: str) -> dict | None:
        """Parse JSON content directly."""
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return None


import_service = ImportService(log=getLogger(__name__))
