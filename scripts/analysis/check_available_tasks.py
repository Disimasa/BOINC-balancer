#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка доступных задач для отправки."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from lib.utils import run_command, PROJECT_HOME

query = """
SELECT 
    a.name as app_name,
    COUNT(DISTINCT CASE WHEN r.server_state = 2 THEN r.id END) as unsent,
    COUNT(DISTINCT CASE WHEN r.server_state = 4 THEN r.id END) as in_progress,
    COUNT(DISTINCT CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.id END) as completed
FROM app a
LEFT JOIN workunit w ON a.id = w.appid
LEFT JOIN result r ON w.id = r.workunitid
WHERE a.name IN ('fast_task', 'medium_task', 'long_task', 'random_task') 
    AND a.deprecated = 0
GROUP BY a.id, a.name
ORDER BY a.name;
"""

cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -e \"{query}\""
stdout, success = run_command(cmd, check=False, capture_output=True)

if success and stdout:
    print("=" * 80)
    print("СТАТУС ЗАДАЧ ПО ПРИЛОЖЕНИЯМ")
    print("=" * 80)
    print(stdout)
else:
    print("Ошибка при получении данных")

