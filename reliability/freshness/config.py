"""Freshness detector config - reuses VolumeDetectorConfig's shape and
scoring math as-is (detector.py/adapter.py have no volume-specific
logic; only the detector_name and which window_metrics column feeds
observed_value differ per detector)."""
import dataclasses

from reliability.volume.config import VolumeDetectorConfig

DEFAULT_CONFIG = dataclasses.replace(VolumeDetectorConfig(), detector_name="freshness")
