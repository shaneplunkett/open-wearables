import contextlib
from datetime import datetime
from uuid import UUID

from sqlalchemy import Date, and_, asc, cast, desc, func, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query, selectinload

from app.database import DbSession
from app.models import DataSource, EventRecord, SleepDetails
from app.models.workout_details import WorkoutDetails
from app.repositories.data_source_repository import DataSourceRepository
from app.repositories.repositories import CrudRepository
from app.schemas import EventRecordCreate, EventRecordQueryParams, EventRecordUpdate, ProviderName
from app.utils.exceptions import handle_exceptions
from app.utils.pagination import decode_cursor

# Identity tuple: (user_id, device_model, source)
DataSourceIdentity = tuple[UUID, str | None, str | None]


class EventRecordRepository(
    CrudRepository[EventRecord, EventRecordCreate, EventRecordUpdate],
):
    def __init__(self, model: type[EventRecord]):
        super().__init__(model)
        self.data_source_repo = DataSourceRepository()

    @handle_exceptions
    def create(self, db_session: DbSession, creator: EventRecordCreate) -> EventRecord:
        if creator.data_source_id:
            data_source_id = creator.data_source_id
        else:
            provider = self.data_source_repo.infer_provider_from_source(creator.source)
            if creator.provider:
                with contextlib.suppress(ValueError):
                    provider = ProviderName(creator.provider)
            data_source = self.data_source_repo.ensure_data_source(
                db_session,
                user_id=creator.user_id,
                provider=provider,
                user_connection_id=creator.user_connection_id,
                device_model=creator.device_model,
                source=creator.source,
                software_version=creator.software_version,
            )
            data_source_id = data_source.id

        creation_data = creator.model_dump()
        creation_data["data_source_id"] = data_source_id
        for redundant_key in (
            "user_id",
            "source",
            "device_model",
            "provider",
            "user_connection_id",
            "software_version",
        ):
            creation_data.pop(redundant_key, None)

        creation = self.model(**creation_data)

        try:
            db_session.add(creation)
            db_session.commit()
            db_session.refresh(creation)
            return creation
        except IntegrityError:
            db_session.rollback()
            existing = (
                db_session.query(self.model)
                .filter(
                    self.model.data_source_id == data_source_id,
                    self.model.start_datetime == creation.start_datetime,
                    self.model.end_datetime == creation.end_datetime,
                )
                .one_or_none()
            )
            if existing:
                return existing
            raise

    @handle_exceptions
    def bulk_create(
        self,
        db_session: DbSession,
        creators: list[EventRecordCreate],
    ) -> list[UUID]:
        if not creators:
            return []

        # Group by provider for batch processing
        by_provider: dict[ProviderName, list[EventRecordCreate]] = {}
        for c in creators:
            provider = self.data_source_repo.infer_provider_from_source(c.source)
            if c.provider:
                with contextlib.suppress(ValueError):
                    provider = ProviderName(c.provider)
            by_provider.setdefault(provider, []).append(c)

        identity_to_source_id: dict[DataSourceIdentity, UUID] = {}

        for provider, provider_creators in by_provider.items():
            unique_identities: set[DataSourceIdentity] = set()
            user_connection_id = provider_creators[0].user_connection_id if provider_creators else None
            for c in provider_creators:
                unique_identities.add((c.user_id, c.device_model, c.source))

            batch_result = self.data_source_repo.batch_ensure_data_sources(
                db_session, provider, user_connection_id, unique_identities
            )
            identity_to_source_id.update(batch_result)

        values_list = []
        for creator in creators:
            identity: DataSourceIdentity = (creator.user_id, creator.device_model, creator.source)
            source_id = identity_to_source_id.get(identity)

            if not source_id:
                continue

            values_list.append(
                {
                    "id": creator.id,
                    "external_id": creator.external_id,
                    "data_source_id": source_id,
                    "category": creator.category,
                    "type": creator.type,
                    "source_name": creator.source_name,
                    "duration_seconds": creator.duration_seconds,
                    "start_datetime": creator.start_datetime,
                    "end_datetime": creator.end_datetime,
                }
            )

        if not values_list:
            return []

        # 3. Batch insert with ON CONFLICT DO NOTHING
        # Chunk to stay under PostgreSQL's 65535 parameter limit (9 params/row → max ~7281 rows)
        chunk_size = 7_000
        inserted_ids: set[UUID] = set()
        for i in range(0, len(values_list), chunk_size):
            chunk = values_list[i : i + chunk_size]
            stmt = insert(self.model).values(chunk).on_conflict_do_nothing(constraint="uq_event_record_datetime")
            result = db_session.execute(stmt.returning(self.model.id))
            inserted_ids.update(row[0] for row in result.fetchall())
        # NOTE: Caller should commit - allows batching multiple operations

        return list(inserted_ids)

    def get_record_with_details(
        self,
        db_session: DbSession,
        record_id: UUID,
        category: str,
    ) -> EventRecord | None:
        return (
            db_session.query(EventRecord)
            .options(selectinload(EventRecord.detail))
            .filter(EventRecord.id == record_id, EventRecord.category == category)
            .first()
        )

    def get_records_with_filters(
        self,
        db_session: DbSession,
        query_params: EventRecordQueryParams,
        user_id: str,
    ) -> tuple[list[tuple[EventRecord, DataSource]], int]:
        query: Query = (
            db_session.query(EventRecord, DataSource)
            .join(
                DataSource,
                EventRecord.data_source_id == DataSource.id,
            )
            .options(selectinload(EventRecord.detail))
        )

        filters = [DataSource.user_id == UUID(user_id)]

        if query_params.category:
            filters.append(EventRecord.category == query_params.category)

        if query_params.record_type:
            filters.append(EventRecord.type.ilike(f"%{query_params.record_type}%"))

        if query_params.source_name:
            filters.append(EventRecord.source_name.ilike(f"%{query_params.source_name}%"))

        if query_params.device_model:
            filters.append(DataSource.device_model == query_params.device_model)

        if getattr(query_params, "source", None):
            filters.append(DataSource.source == query_params.source)

        if getattr(query_params, "data_source_id", None):
            filters.append(EventRecord.data_source_id == query_params.data_source_id)

        if query_params.start_datetime:
            filters.append(EventRecord.start_datetime >= query_params.start_datetime)

        if query_params.end_datetime:
            filters.append(EventRecord.end_datetime < query_params.end_datetime)

        if query_params.min_duration is not None:
            filters.append(EventRecord.duration_seconds >= query_params.min_duration)

        if query_params.max_duration is not None:
            filters.append(EventRecord.duration_seconds <= query_params.max_duration)

        if filters:
            query = query.filter(and_(*filters))

        # Determine sort column and direction
        sort_by = query_params.sort_by or "start_datetime"
        sort_column = getattr(EventRecord, sort_by)
        is_asc = query_params.sort_order == "asc"

        # Calculate total count BEFORE applying cursor filters
        # This gives us the total matching records (after all other filters)
        total_count = query.count()

        # Cursor pagination (keyset)
        if query_params.cursor:
            cursor_ts, cursor_id, direction = decode_cursor(query_params.cursor)

            if direction == "prev":
                # Backward pagination: get items BEFORE cursor
                if sort_by == "start_datetime":
                    comparison = (
                        tuple_(EventRecord.start_datetime, EventRecord.id) < (cursor_ts, cursor_id)
                        if is_asc
                        else tuple_(EventRecord.start_datetime, EventRecord.id) > (cursor_ts, cursor_id)
                    )
                    query = query.filter(comparison)
                else:
                    query = query.filter(EventRecord.id < cursor_id if is_asc else EventRecord.id > cursor_id)

                # Reverse sort order for backward pagination
                sort_order = desc if is_asc else asc
                query = query.order_by(sort_order(sort_column), sort_order(EventRecord.id))

                # Limit + 1 to check for previous page
                limit = query_params.limit or 20
                results = query.limit(limit + 1).all()
                # Reverse to get correct order
                return list(reversed(results)), total_count

            # Forward pagination: get items AFTER cursor
            if sort_by == "start_datetime":
                comparison = (
                    tuple_(EventRecord.start_datetime, EventRecord.id) > (cursor_ts, cursor_id)
                    if is_asc
                    else tuple_(EventRecord.start_datetime, EventRecord.id) < (cursor_ts, cursor_id)
                )
                query = query.filter(comparison)
            else:
                query = query.filter(EventRecord.id > cursor_id if is_asc else EventRecord.id < cursor_id)

        # Apply ordering (ID as secondary sort for deterministic pagination)
        sort_order = asc if is_asc else desc
        query = query.order_by(sort_order(sort_column), sort_order(EventRecord.id))

        # Limit + 1 to check for next page (cursor pagination)
        limit = query_params.limit or 20

        # When using cursor, we don't use offset (keyset pagination)
        if not query_params.cursor and query_params.offset:
            query = query.offset(query_params.offset)

        return query.limit(limit + 1).all(), total_count

    def get_count_by_workout_type(self, db_session: DbSession) -> list[tuple[str | None, int]]:
        """Get count of workouts grouped by workout type.

        Returns list of (workout_type, count) tuples ordered by count descending.
        Only includes records with category='workout'.
        """

        results = (
            db_session.query(self.model.type, func.count(self.model.id).label("count"))
            .filter(self.model.category == "workout")
            .group_by(self.model.type)
            .order_by(func.count(self.model.id).desc())
            .all()
        )
        return [(workout_type, count) for workout_type, count in results]

    def get_sleep_summaries(
        self,
        db_session: DbSession,
        user_id: UUID,
        start_date: datetime,
        end_date: datetime,
        cursor: str | None,
        limit: int,
    ) -> list[dict]:
        """Get individual sleep session records with stage details.

        Each EventRecord with category='sleep' is already a complete session
        (assembled from fragments at import time). Returns one row per session.

        Returns list of dicts with keys:
        - sleep_date, min_start_time, max_end_time, total_duration_minutes
        - source, device_model, record_id
        - time_in_bed_minutes, efficiency_percent
        - deep_minutes, light_minutes, rem_minutes, awake_minutes
        - is_nap
        """
        query = (
            db_session.query(
                cast(EventRecord.end_datetime, Date).label("sleep_date"),
                EventRecord.start_datetime.label("min_start_time"),
                EventRecord.end_datetime.label("max_end_time"),
                EventRecord.duration_seconds.label("total_duration"),
                DataSource.source,
                DataSource.device_model,
                EventRecord.id.label("record_id"),
                SleepDetails.sleep_time_in_bed_minutes.label("time_in_bed_minutes"),
                SleepDetails.sleep_deep_minutes.label("deep_minutes"),
                SleepDetails.sleep_light_minutes.label("light_minutes"),
                SleepDetails.sleep_rem_minutes.label("rem_minutes"),
                SleepDetails.sleep_awake_minutes.label("awake_minutes"),
                SleepDetails.sleep_efficiency_score.label("efficiency_percent"),
                func.coalesce(SleepDetails.is_nap, False).label("is_nap"),
            )
            .join(DataSource, EventRecord.data_source_id == DataSource.id)
            .outerjoin(SleepDetails, SleepDetails.record_id == EventRecord.id)
            .filter(
                DataSource.user_id == user_id,
                EventRecord.category == "sleep",
                EventRecord.end_datetime >= start_date,
                cast(EventRecord.end_datetime, Date) < cast(end_date, Date),
            )
        )

        # Handle cursor pagination
        if cursor:
            cursor_ts, cursor_id, direction = decode_cursor(cursor)
            cursor_date = cursor_ts.date()

            if direction == "prev":
                query = query.filter(
                    tuple_(cast(EventRecord.end_datetime, Date), EventRecord.id) < (cursor_date, cursor_id)
                )
                query = query.order_by(desc(cast(EventRecord.end_datetime, Date)), desc(EventRecord.id))
            else:
                query = query.filter(
                    tuple_(cast(EventRecord.end_datetime, Date), EventRecord.id) > (cursor_date, cursor_id)
                )
                query = query.order_by(asc(cast(EventRecord.end_datetime, Date)), asc(EventRecord.id))
        else:
            query = query.order_by(asc(cast(EventRecord.end_datetime, Date)), asc(EventRecord.id))

        results = query.limit(limit + 1).all()

        summaries = []
        for row in results:
            summaries.append(
                {
                    "sleep_date": row.sleep_date,
                    "min_start_time": row.min_start_time,
                    "max_end_time": row.max_end_time,
                    "total_duration_minutes": int(row.total_duration or 0) // 60,
                    "source": row.source,
                    "device_model": row.device_model,
                    "record_id": row.record_id,
                    "time_in_bed_minutes": int(row.time_in_bed_minutes)
                    if row.time_in_bed_minutes is not None
                    else None,
                    "deep_minutes": int(row.deep_minutes) if row.deep_minutes is not None else None,
                    "light_minutes": int(row.light_minutes) if row.light_minutes is not None else None,
                    "rem_minutes": int(row.rem_minutes) if row.rem_minutes is not None else None,
                    "awake_minutes": int(row.awake_minutes) if row.awake_minutes is not None else None,
                    "efficiency_percent": row.efficiency_percent,
                    "nap_count": 1 if row.is_nap else 0,
                    "nap_duration_minutes": int(row.total_duration or 0) // 60 if row.is_nap else 0,
                }
            )
        return summaries

    def get_daily_workout_aggregates(
        self,
        db_session: DbSession,
        user_id: UUID,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get daily workout aggregates including elevation, distance, and energy.

        Aggregates WorkoutDetails data by date for activity summaries.

        Returns list of dicts with keys:
        - workout_date, source, device_model
        - elevation_meters, distance_meters, energy_burned_kcal
        """
        results = (
            db_session.query(
                cast(self.model.end_datetime, Date).label("workout_date"),
                DataSource.source,
                DataSource.device_model,
                # Sum elevation gain for all workouts on that day
                func.sum(WorkoutDetails.total_elevation_gain).label("elevation_sum"),
                # Sum distance for all workouts
                func.sum(WorkoutDetails.distance).label("distance_sum"),
                # Sum energy burned
                func.sum(WorkoutDetails.energy_burned).label("energy_sum"),
            )
            .join(DataSource, self.model.data_source_id == DataSource.id)
            # Use outerjoin since WorkoutDetails is optional - some workouts may not have details
            .outerjoin(WorkoutDetails, self.model.id == WorkoutDetails.record_id)
            .filter(
                DataSource.user_id == user_id,
                self.model.category == "workout",
                self.model.end_datetime >= start_date,
                cast(self.model.end_datetime, Date) < cast(end_date, Date),
            )
            .group_by(
                cast(self.model.end_datetime, Date),
                DataSource.source,
                DataSource.device_model,
            )
            .order_by(asc(cast(self.model.end_datetime, Date)))
            .all()
        )

        aggregates = []
        for row in results:
            aggregates.append(
                {
                    "workout_date": row.workout_date,
                    "source": row.source,
                    "device_model": row.device_model,
                    "elevation_meters": float(row.elevation_sum) if row.elevation_sum is not None else None,
                    "distance_meters": float(row.distance_sum) if row.distance_sum is not None else None,
                    "energy_burned_kcal": float(row.energy_sum) if row.energy_sum is not None else None,
                }
            )
        return aggregates
