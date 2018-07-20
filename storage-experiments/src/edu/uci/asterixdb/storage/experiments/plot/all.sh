#!/bin/sh

python insert.py
python upsert_secondary_basic.py
python upsert_secondary_index.py
python upsert_secondary_merge.py
python repair.py
python query_breakdown.py
python query_misc.py
python query_index.py
python query_filter.py
python bitmap.py
