#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка статуса задач в БД"""

import sys
sys.path.insert(0, "html/ops")
from db import *

db = BoincDB()
cursor = db.cursor()

# Проверяем workunits
cursor.execute("SELECT COUNT(*) FROM workunit WHERE appid IN (SELECT id FROM app WHERE name IN ('fast_task', 'medium_task', 'long_task', 'random_task'))")
wu_count = cursor.fetchone()[0]
print(f"Workunits: {wu_count}")

# Проверяем results
cursor.execute("SELECT COUNT(*) FROM result WHERE workunitid IN (SELECT id FROM workunit WHERE appid IN (SELECT id FROM app WHERE name IN ('fast_task', 'medium_task', 'long_task', 'random_task')))")
result_count = cursor.fetchone()[0]
print(f"Results: {result_count}")

# Проверяем results по состояниям
cursor.execute("SELECT server_state, COUNT(*) FROM result WHERE workunitid IN (SELECT id FROM workunit WHERE appid IN (SELECT id FROM app WHERE name IN ('fast_task', 'medium_task', 'long_task', 'random_task'))) GROUP BY server_state")
states = cursor.fetchall()
print("\nResults by state:")
for state, count in states:
    state_names = {2: "ready to send", 4: "in progress", 5: "over", 6: "committed"}
    print(f"  State {state} ({state_names.get(state, 'unknown')}): {count}")

# Проверяем app versions
cursor.execute("SELECT id, name, version_num FROM app_version WHERE appid IN (SELECT id FROM app WHERE name IN ('fast_task', 'medium_task', 'long_task', 'random_task'))")
versions = cursor.fetchall()
print(f"\nApp versions: {len(versions)}")
for v in versions:
    print(f"  ID={v[0]}, name={v[1]}, version={v[2]}")

