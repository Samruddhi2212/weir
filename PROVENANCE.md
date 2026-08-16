# Provenance

## Adapted from streaming-lakehouse-lab (MIT, 770189b05beca6f8a14e5fb6e45940ddc321847f, 2026-08-15)

- config/flink/flink-conf.yaml — checkpointing and state backend block,
  adapted from infra/flink/conf/flink-conf.yaml with modified checkpoint
  interval (see DEFENSE.md #1)

## Read as reference, written independently

- StatefulDedupJob.java — stateful pattern reference
- ewma_ad.py — EWMA algorithm reference; Weir's implementation written from
  the statistical definition, not adapted

## Design references (read, not copied)

- OpenLineage event spec — Weir's lineage event shape is modeled on it, not
  adapted from its code
- AWS Deequ anomaly detection module — read as a baseline strategy
  comparison (see DEFENSE.md #4)

## Written from scratch

- Everything else

## Note

- Upstream README states Apache-2.0; upstream LICENSE file is MIT. Relying on
  the LICENSE file. See THIRD_PARTY_NOTICES.md for the reproduced MIT notice
  covering the adapted file above.
