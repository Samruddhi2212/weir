# Provenance

## Adapted from streaming-lakehouse-lab (MIT, 770189b05beca6f8a14e5fb6e45940ddc321847f)

- infra/flink/conf/flink-conf.yaml — checkpointing and state backend config,
  adapted with modified checkpoint interval (see DEFENSE.md #1)

## Read as reference, written independently

- StatefulDedupJob.java — stateful pattern reference
- ewma_ad.py — EWMA algorithm reference; Weir's implementation written from
  the statistical definition, not adapted

## Written from scratch

- Everything else

## Note

- Upstream README states Apache-2.0; upstream LICENSE file is MIT. Relying on
  the LICENSE file.
