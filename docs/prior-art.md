# Prior art

What already exists in this space, what Weir takes from it, and where the
difference actually is. Written so the comparison survives being
questioned: every claim here is about *published material I could find*,
not about performance I measured in another tool. I have not benchmarked
any of these against Weir's failure catalog, and saying otherwise would
be inventing a number.

## The honest version of the differentiator

The README says Weir publishes measured detection rate, false-positive
rate and time-to-flag against a reproducible injected-failure catalog,
and that no comparable OSS project does. That claim is scoped to this:
**I could not find, in these projects' documentation or repositories,
published figures for detection rate / false-positive rate / detection
latency measured against a reproducible catalog of injected failures,
with expected misses included in the denominator.**

That is not the same as claiming they perform worse. Several of them are
far more mature, more general, and better tested than Weir. The gap I am
pointing at is in *published evidence of detection quality*, which is a
narrower and more defensible claim than "better".

## Batch / declarative data quality

**Great Expectations** — you declare expectations ("this column is never
null", "this value is between X and Y") and they are validated against a
batch. Mature, large ecosystem. The detection question is pushed to the
user: an expectation catches exactly what you thought to declare, so
there is no detection *rate* to publish, because there is no notion of a
failure you didn't anticipate.

**Soda (soda-core)** — same essential shape via SodaCL, a checks
language. Same consequence for measurement.

**dbt tests / Elementary** — tests attached to dbt models, with
Elementary adding anomaly detection over model run history. Runs on the
dbt schedule, so its natural cadence is per-model-run rather than
per-window. Weir's detectors read a streaming windowed aggregate instead,
which is why its time-to-flag is even a meaningful quantity.

**Apache Griffin** — data quality on Spark, batch and streaming
profiles. Closest in architecture of the batch tools; the measurement
difference stands.

## Anomaly detection on data quality metrics

**AWS Deequ** (specifically its anomaly detection module) — the closest
prior art to Weir's detector design, and the one I read directly. It
computes metrics over data and applies anomaly-detection strategies to a
metric's history. Weir's baseline strategy is a deliberate divergence
from it rather than a port: see DEFENSE.md #44 for the chosen strategy
and the alternatives rejected, and PROVENANCE.md for what was read versus
written. Deequ is Spark-based and batch-oriented; Weir's equivalent runs
per-window off a Flink aggregate with O(1) state per bucket.

**Commercial data observability** (Monte Carlo, Bigeye, Anomalo,
Metaplane) — ML-based anomaly detection over warehouse tables, generally
closed-source. They publish customer outcomes and methodology posts;
detection-rate/FP-rate figures against a reproducible public failure
catalog are not something I could find, and their catalogs are not
runnable by a third party in any case.

## Lineage

**OpenLineage / Marquez** — event spec and lineage store. Weir models its
lineage event shape on the OpenLineage spec rather than adapting its code
(PROVENANCE.md). Weir v1 ships *declared* lineage, not runtime-emitted —
stated in the README because CLAUDE.md rule 7 requires the fallback be
disclosed there, not just in internal docs.

## Where Weir is genuinely weaker

Worth saying plainly, because an evaluator will find these anyway:

- **Single dataset.** Everything is measured on NYC TLC yellow-taxi
  trips. Nothing here demonstrates generality across schemas or domains.
- **Three detectors.** Volume, freshness, null-rate. The tools above
  cover far broader check surfaces.
- **One benchmark run**, so repeated-run variance is unmeasured, and one
  of the three targeted scenarios could not be validly measured at all
  under the benchmark's sampling (README explains why).
- **Not production-hardened.** Docker Compose, no auth story beyond a
  local dev identity, no multi-tenancy, no deployment story.
- **The failure catalog is Weir's own.** A catalog authored alongside the
  detectors is not an independent benchmark, however carefully the
  expected misses are declared. The mitigation is that the scenarios are
  deliberately variants the detectors were *not* designed against, and
  that expected misses are published rather than omitted — but it is not
  the same as being scored by someone else's suite.

## What is actually transferable

The part of this worth reusing elsewhere is not the detectors. It is the
measurement discipline: injected failures with declared ground truth,
expected misses carried in the denominator, independently-computed
ground truth in every verification script, and results that are generated
into the README from a result file rather than typed by hand
(DEFENSE.md #9).
