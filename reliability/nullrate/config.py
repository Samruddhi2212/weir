"""Null-rate detector config - one detector_name per monitored column
(detector_name = f"null_rate_{column_name}"), reusing VolumeDetectorConfig's
shape/scoring math as-is (no schema change needed - weir_incidents'
tables are already keyed by detector_name, not by any volume-specific
concept)."""
import dataclasses

from reliability.volume.config import VolumeDetectorConfig


def config_for_column(column_name):
    return dataclasses.replace(VolumeDetectorConfig(), detector_name=f"null_rate_{column_name}")
