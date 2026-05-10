"""Database load layer.

- ``schema.sql``  : DDL for the JEIS star schema (1 fact + 4 dimensions).
                    Idempotent: safe to run multiple times.
- ``load_db.py``  : SQLAlchemy-driven upsert of cleaned data into the
                    Supabase Postgres instance referenced by DATABASE_URL.
"""
