"""Part 5.1: failure-scenario base for the benchmark catalog.

Scenarios transform the event stream `replay_producer.py` sends to
Kafka, so an injected failure flows through the real pipeline
(Kafka -> Flink -> weir_metrics -> detector). That is what makes
detection latency a real measurement instead of a property of a direct
Postgres write - and it's why this catalog is strictly separate from
`incidents/dev/`, whose triggers deliberately bypass the pipeline for a
fast dev loop. Nothing here may import from `incidents/dev/` (hard
rule 2, enforced by tests/unit/test_benchmark_isolation.py).

Contract: `inject(events) -> list`. A scenario may drop, duplicate,
reorder, or mutate events. It must NOT mutate the caller's dicts in
place - composition and re-use both depend on that, and a test asserts
it. The returned list's order IS the send order; scenarios that aren't
modelling arrival delay preserve the incoming order.

Events are the dicts `replay_producer.load_sorted_trips` produces:
`PICKUP_COLUMN` carries event time, `_row_index` is the stable
trip-key source.
"""
import abc
import enum
import hashlib

PICKUP_COLUMN = "tpep_pickup_datetime"
TRIP_KEY_FIELD = "_row_index"


class Severity(enum.Enum):
    """Impact on the data itself, not on how hard it is to detect.

    CRITICAL - the dataset is unusable for the affected span.
    HIGH     - sustained silent loss or corruption; looks healthy.
    MEDIUM   - real distortion, but visible/recoverable once known.
    LOW      - waste or noise; no lasting corruption.
    """

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


def _digest_int(event, salt):
    """Stable 64-bit int from an event's trip key, salted per use.

    hashlib, not the builtin hash() - str hashing is salted per process
    (PYTHONHASHSEED), which would make every scenario unreproducible
    across runs, the one property a benchmark cannot give up. The salt
    keeps independent uses (partition choice vs drop choice vs lateness
    draw) from correlating with each other.
    """
    key = f"{salt}:{event[TRIP_KEY_FIELD]}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(key).digest()[:8], "big")


def stable_uniform(event, salt=""):
    """Deterministic float in [0, 1) for this event and use."""
    return _digest_int(event, salt) / 2**64


def simulated_partition(event, num_partitions):
    """Which Kafka partition this event's key would land on, simulated
    by a stable hash rather than reproducing Kafka's own murmur2
    partitioner.

    Shape-faithful, not partitioner-identical, on purpose: what a
    benchmark needs from "one partition degrades" is that a fixed,
    key-determined ~1/N subset degrades while the rest stays healthy.
    Reproducing that shape doesn't require matching the broker's hash,
    and matching it would couple this catalog to a client library it
    otherwise doesn't need (hard rule 6).
    """
    return _digest_int(event, "partition") % num_partitions


class Scenario(abc.ABC):
    """One injectable failure, with the ground truth a benchmark needs
    to score detection against it.

    `expected_detector = None` declares an expected MISS: no detector
    targets this failure mode. That's a declaration of design intent
    for the results table, never a license to tune a detector until it
    matches (hard rule 3).
    """

    name = None
    affected_dataset = "nyc_tlc_yellow_trips"
    affected_column = None
    observable_in = None
    severity = None
    expected_detector = None

    def __init__(self, starts_at, ends_at=None):
        """starts_at/ends_at are ground truth in the event-time domain
        the replayed data uses (naive America/New_York civil time, the
        same domain window_metrics stores - DEFENSE.md #46). No
        defaults: a benchmark's ground-truth timestamps are chosen
        against the slice being replayed, never guessed at here.
        """
        if ends_at is not None and ends_at <= starts_at:
            raise ValueError(
                f"{type(self).__name__}: ends_at {ends_at!r} must be after starts_at {starts_at!r}"
            )
        self.starts_at = starts_at
        self.ends_at = ends_at

    @abc.abstractmethod
    def inject(self, events):
        """Return a new event list with this failure applied."""

    def covers(self, event_time):
        """Whether an event falls in this scenario's active span,
        [starts_at, ends_at) - open-ended when ends_at is None."""
        if event_time < self.starts_at:
            return False
        return self.ends_at is None or event_time < self.ends_at

    def progress(self, event_time):
        """0.0 at starts_at rising to 1.0 at ends_at - for the ramped
        scenarios (gradual delay, creeping null rate). Requires a
        bounded span."""
        if self.ends_at is None:
            raise ValueError(f"{type(self).__name__}.progress needs a bounded ends_at")
        span_seconds = (self.ends_at - self.starts_at).total_seconds()
        elapsed = (event_time - self.starts_at).total_seconds()
        return min(max(elapsed / span_seconds, 0.0), 1.0)

    def ground_truth(self):
        """What the runner records alongside a measured outcome."""
        return {
            "name": self.name,
            "starts_at": self.starts_at,
            "ends_at": self.ends_at,
            "affected_dataset": self.affected_dataset,
            "affected_column": self.affected_column,
            "observable_in": self.observable_in,
            "severity": self.severity.value if self.severity else None,
            "expected_detector": self.expected_detector,
        }

    def __repr__(self):
        return f"<{type(self).__name__} {self.name} starts_at={self.starts_at!r} ends_at={self.ends_at!r}>"


def compose(scenarios, events):
    """Apply scenarios in list order so several can run in one replay.

    Order matters wherever two scenarios overlap in time, and is not an
    implementation detail to be papered over: a duplicate storm composed
    BEFORE a gradual delay has its duplicates delayed too; composed
    after, it doesn't. The runner records the order it used.
    """
    for scenario in scenarios:
        events = scenario.inject(events)
    return events
