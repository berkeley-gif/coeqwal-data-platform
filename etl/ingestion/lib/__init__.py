"""Private library tier for `etl.ingestion`.

These modules are imported by `gdrive_bulk_download.py` and by the
auxiliary scripts under `etl/ingestion/tools/`. They are not meant to be
run directly. Intra-package imports use relative syntax (`from .config
import ...`) so the folder name does not leak into call sites.
"""
