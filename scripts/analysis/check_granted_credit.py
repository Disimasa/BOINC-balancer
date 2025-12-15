#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка granted_credit."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from lib.utils import run_command, PROJECT_HOME

query = """
SELECT 
    a.name as app_name,
    COUNT(*) as total_results,
    COUNT(CASE WHEN r.granted_credit = 0 THEN 1 END) as zero_credit,
    COUNT(CASE WHEN r.granted_credit > 0 THEN 1 END) as non_zero_credit,
    ROUND(COUNT(CASE WHEN r.granted_credit = 0 THEN 1 END) * 100.0 / COUNT(*), 2) as zero_credit_pct
FROM result r
JOIN workunit w ON r.workunitid = w.id
JOIN app a ON w.appid = a.id
WHERE r.server_state = 5 
    AND r.outcome = 1
    AND a.name IN ('fast_task', 'medium_task', 'long_task', 'random_task')
    AND a.deprecated = 0
GROUP BY a.id, a.name
ORDER BY a.name;
"""

cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -e \"{query}\""
stdout, success = run_command(cmd, check=False, capture_output=True)

if success and stdout:
    print("=" * 80)
    print("СТАТИСТИКА ПО granted_credit")
    print("=" * 80)
    print(stdout)
else:
    print("Ошибка при получении данных")


