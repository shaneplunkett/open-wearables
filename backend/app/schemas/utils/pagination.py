from pydantic import BaseModel, Field

from app.schemas.utils import TimeseriesMetadata


class Pagination(BaseModel):
    next_cursor: str | None = Field(
        None,
        description="Cursor to fetch next page, null if no more data",
        example="eyJpZCI6IjEyMzQ1Njc4OTAiLCJ0cyI6MTcwNDA2NzIwMH0",
    )
    previous_cursor: str | None = Field(None, description="Cursor to fetch previous page")
    has_more: bool = Field(..., description="Whether more data is available")
    total_count: int | None = Field(
        None,
        description="Total number of records matching the query",
        example=150,
    )


class PaginatedResponse[DataT](BaseModel):
    """Generic response model for paginated data with metadata.

    Can be used with any data type by specifying the type parameter:
    - PaginatedResponse[HeartRateSample]
    - PaginatedResponse[HeartRateSample | HrvSample | Spo2Sample]
    - PaginatedResponse[Workout]  # for other endpoints
    """

    data: list[DataT]
    pagination: Pagination
    metadata: TimeseriesMetadata
