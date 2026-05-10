"""Source-specific ingest modules.

Each module is responsible for one external data source. Modules write
date-stamped raw files into ``data/raw/`` and never modify previously written
files (PRD requirement FR-1 — historical pulls must be preserved).

Modules
-------
- ``comtrade_api``    : UN Comtrade Plus REST API → JSON
- ``zauba_scraper``   : Zauba.com manual CSV / scraping fallback
- ``volza_extract``   : Volza free-trial XLSX export normaliser
- ``baker_hughes``    : Baker Hughes North America rotary rig count
- ``monsoon_extract`` : IMD / data.gov.in Rajasthan rainfall
"""
