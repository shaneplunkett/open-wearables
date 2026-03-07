from enum import StrEnum


class SleepPhase(StrEnum):
    IN_BED = "in_bed"
    SLEEPING = "sleeping"
    AWAKE = "awake"
    ASLEEP_LIGHT = "light"
    ASLEEP_DEEP = "deep"
    ASLEEP_REM = "rem"
    UNKNOWN = "unknown"


APPLE_XML_SLEEP_VALUES: dict[str, SleepPhase] = {
    "HKCategoryValueSleepAnalysisInBed": SleepPhase.IN_BED,
    "HKCategoryValueSleepAnalysisAwake": SleepPhase.AWAKE,
    "HKCategoryValueSleepAnalysisAsleep": SleepPhase.SLEEPING,
    "HKCategoryValueSleepAnalysisAsleepUnspecified": SleepPhase.SLEEPING,
    "HKCategoryValueSleepAnalysisAsleepCore": SleepPhase.ASLEEP_LIGHT,
    "HKCategoryValueSleepAnalysisAsleepDeep": SleepPhase.ASLEEP_DEEP,
    "HKCategoryValueSleepAnalysisAsleepREM": SleepPhase.ASLEEP_REM,
}


def get_apple_sleep_phase(apple_sleep_phase: str) -> SleepPhase | None:
    try:
        return SleepPhase(apple_sleep_phase)
    except ValueError:
        return None


def get_sleep_phase_from_xml_value(xml_value: str) -> SleepPhase | None:
    """Map an Apple Health XML category value string to a SleepPhase."""
    return APPLE_XML_SLEEP_VALUES.get(xml_value)
