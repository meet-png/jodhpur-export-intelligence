"""Data cleaning and validation layer.

- ``clean.py``    : produces ``data/processed/exports_clean.parquet`` and
                    ``rig_count_clean.parquet`` from the raw files written
                    by ``src.ingest``.
- ``validate.py`` : Great Expectations checkpoint over the cleaned output.
                    The pipeline halts on any expectation failure (FR-2).
"""
