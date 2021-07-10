#!/bin/sh

python3 insert.py
python3 upsert_secondary_basic.py
python3 upsert_secondary_index.py
python3 upsert_secondary_merge.py
python3 repair.py
python3 query_breakdown.py
python3 query_misc.py
python3 query_index.py
python3 query_filter.py
python3 bitmap.py
