"""Cardiac/POTS daily summary schemas."""

from datetime import date

from pydantic import BaseModel, Field


class CardiacTimeBlock(BaseModel):
    """Cardiac metrics for a time block within a day.

    Blocks are in user's local timezone:
    - overnight: 00:00-06:00
    - morning: 06:00-12:00
    - afternoon: 12:00-18:00
    - evening: 18:00-00:00
    """

    block: str = Field(..., description="Time block name")
    avg_hr_bpm: int | None = None
    min_hr_bpm: int | None = None
    max_hr_bpm: int | None = None
    avg_hrv_sdnn_ms: float | None = None
    avg_spo2_percent: float | None = None
    hr_samples: int = 0


class CardiacHRV(BaseModel):
    """HRV (SDNN) sub-object."""

    avg_sdnn_ms: float | None = None
    min_sdnn_ms: float | None = None
    max_sdnn_ms: float | None = None
    readings_count: int = 0


class CardiacSpO2(BaseModel):
    """Blood oxygen sub-object."""

    avg_percent: float | None = None
    min_percent: float | None = None
    readings_below_90_count: int = Field(0, description="Desaturation events (SpO2 < 90%)")
    readings_count: int = 0


class CardiacSampleCounts(BaseModel):
    """Data quality indicator — how many raw samples backed each metric."""

    heart_rate: int = 0
    hrv_sdnn: int = 0
    spo2: int = 0


class CardiacDailySummary(BaseModel):
    """Full cardiac daily summary with POTS-relevant metrics.

    Designed for autonomic monitoring:
    - orthostatic_hr_delta_bpm: walking HR minus resting HR (POTS threshold >= 30bpm)
    - tachycardia_minutes: time spent with HR > 100bpm at rest
    - time_blocks: intra-day shape without raw sample firehose
    """

    date: date
    resting_heart_rate_bpm: int | None = Field(None, description="Daily resting HR")
    walking_heart_rate_avg_bpm: int | None = Field(None, description="Average walking HR")
    orthostatic_hr_delta_bpm: int | None = Field(
        None,
        description="walking_hr - resting_hr. POTS diagnostic threshold is +30bpm",
    )
    avg_heart_rate_bpm: int | None = None
    min_heart_rate_bpm: int | None = None
    max_heart_rate_bpm: int | None = None
    hrv: CardiacHRV | None = None
    spo2: CardiacSpO2 | None = None
    respiratory_rate_avg_brpm: float | None = None
    tachycardia_minutes: int | None = Field(
        None,
        description="Minutes with HR > 100bpm (non-exercise proxy)",
    )
    time_blocks: list[CardiacTimeBlock] = Field(default_factory=list)
    sample_counts: CardiacSampleCounts = Field(default_factory=CardiacSampleCounts)
    source: str = "apple_health"
