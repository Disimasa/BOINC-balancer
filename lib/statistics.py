#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Функции для сбора статистики по выполненным задачам BOINC.
"""
import time
from .utils import run_command, PROJECT_HOME


def parse_sql_output(output):
    """Парсить вывод SQL запроса в список словарей."""
    if not output:
        return []
    
    stats = []
    lines = output.strip().split('\n')
    if len(lines) < 2:
        return stats
    
    # Парсим заголовки
    headers = lines[0].split('\t')
    
    # Парсим данные
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) < len(headers):
            continue
        
        stat = {}
        for i, header in enumerate(headers):
            value = parts[i] if i < len(parts) else None
            # Преобразуем числовые значения
            try:
                if '.' in str(value):
                    stat[header] = float(value) if value else 0.0
                else:
                    stat[header] = int(value) if value else 0
            except (ValueError, TypeError):
                stat[header] = value
        
        stats.append(stat)
    
    return stats


def get_completed_task_statistics():
    """
    Собрать статистику ТОЛЬКО по завершенным таскам (приложениям).
    
    Возвращает статистику только для задач со статусом:
    - server_state = 5 (RESULT_SERVER_STATE_OVER)
    - outcome = 1 (RESULT_OUTCOME_SUCCESS)
    """
    query = """
    SELECT 
        a.name as app_name,
        a.weight as app_weight,
        COUNT(DISTINCT CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN w.id END) as completed_workunits,
        COUNT(DISTINCT CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.id END) as completed_results,
        -- Средние метрики для завершенных задач (только с валидным elapsed_time > 0)
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.elapsed_time > 0 THEN r.elapsed_time END), 0) as avg_elapsed_time,
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.cpu_time > 0 THEN r.cpu_time END), 0) as avg_cpu_time,
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.granted_credit > 0 THEN r.granted_credit END), 0) as avg_credit,
        COALESCE(SUM(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.granted_credit > 0 THEN r.granted_credit END), 0) as total_credit,
        -- Время в очереди (от создания workunit до отправки клиенту) - только для завершенных
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.sent_time > 0 AND w.create_time > 0 THEN r.sent_time - w.create_time END), 0) as avg_queue_time,
        -- Время выполнения (от отправки до получения) - только для завершенных
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.received_time > 0 AND r.sent_time > 0 THEN r.received_time - r.sent_time END), 0) as avg_execution_time,
        -- Минимальные и максимальные значения (только с валидным elapsed_time > 0)
        COALESCE(MIN(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.elapsed_time > 0 THEN r.elapsed_time END), 0) as min_elapsed_time,
        COALESCE(MAX(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.elapsed_time > 0 THEN r.elapsed_time END), 0) as max_elapsed_time,
        -- Количество задач в процессе выполнения (server_state = 4)
        COUNT(DISTINCT CASE WHEN r.server_state = 4 THEN r.id END) as in_progress_count
    FROM app a
    LEFT JOIN workunit w ON a.id = w.appid
    LEFT JOIN result r ON w.id = r.workunitid
    WHERE a.name IN ('fast_task', 'medium_task', 'long_task', 'random_task')
    GROUP BY a.id, a.name, a.weight
    HAVING completed_results > 0
    ORDER BY a.name;
    """
    
    cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -e \"{query}\""
    output, success = run_command(cmd, check=False, capture_output=True)
    
    if not success or not output:
        return None
    
    return parse_sql_output(output)


def get_completed_client_statistics():
    """
    Собрать статистику по клиентам ТОЛЬКО для завершенных задач.
    
    Возвращает статистику только для задач со статусом:
    - server_state = 5 (RESULT_SERVER_STATE_OVER)
    - outcome = 1 (RESULT_OUTCOME_SUCCESS)
    """
    query = """
    SELECT 
        h.id as host_id,
        h.domain_name as host_name,
        h.p_fpops as host_fpops,
        COUNT(DISTINCT CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.id END) as completed_results,
        -- Статистика по приложениям (только завершенные)
        COUNT(DISTINCT CASE WHEN a.name = 'fast_task' AND r.server_state = 5 AND r.outcome = 1 THEN r.id END) as fast_task_completed,
        COUNT(DISTINCT CASE WHEN a.name = 'medium_task' AND r.server_state = 5 AND r.outcome = 1 THEN r.id END) as medium_task_completed,
        COUNT(DISTINCT CASE WHEN a.name = 'long_task' AND r.server_state = 5 AND r.outcome = 1 THEN r.id END) as long_task_completed,
        COUNT(DISTINCT CASE WHEN a.name = 'random_task' AND r.server_state = 5 AND r.outcome = 1 THEN r.id END) as random_task_completed,
        -- Среднее время выполнения (только с валидным elapsed_time > 0)
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.elapsed_time > 0 THEN r.elapsed_time END), 0) as avg_elapsed_time,
        -- Общий кредит (только с валидным granted_credit > 0)
        COALESCE(SUM(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.granted_credit > 0 THEN r.granted_credit END), 0) as total_credit,
        -- Время простоя (оценка: время между завершением последней задачи и текущим временем)
        COALESCE(MAX(CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.received_time END), 0) as last_completion_time,
        -- Время последнего RPC запроса от клиента
        h.rpc_time as last_rpc_time,
        -- Время создания хоста (когда клиент подключился)
        h.create_time as host_create_time,
        -- Время отправки первой завершенной задачи
        COALESCE(MIN(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.sent_time > 0 THEN r.sent_time END), 0) as first_task_sent_time
    FROM host h
    LEFT JOIN result r ON h.id = r.hostid AND r.server_state = 5 AND r.outcome = 1
    LEFT JOIN workunit w ON r.workunitid = w.id
    LEFT JOIN app a ON w.appid = a.id
    WHERE h.id > 0
    GROUP BY h.id, h.domain_name, h.p_fpops, h.rpc_time, h.create_time
    HAVING completed_results > 0
    ORDER BY h.id;
    """
    
    cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -e \"{query}\""
    output, success = run_command(cmd, check=False, capture_output=True)
    
    if not success or not output:
        return None
    
    stats = parse_sql_output(output)
    
    # Текущее время для расчета простоя
    current_time = int(time.time())
    
    # Добавляем вычисляемые поля
    for stat in stats:
        # Рассчитываем время простоя
        if stat.get('last_completion_time', 0) > 0:
            idle_time = current_time - stat['last_completion_time']
            stat['idle_time_seconds'] = idle_time
        else:
            stat['idle_time_seconds'] = 0
        
        # Рассчитываем время с последнего RPC запроса
        if stat.get('last_rpc_time', 0) > 0:
            time_since_rpc = current_time - stat['last_rpc_time']
            stat['time_since_last_rpc_seconds'] = time_since_rpc
        else:
            stat['time_since_last_rpc_seconds'] = 0
        
        # Рассчитываем время с момента подключения
        if stat.get('host_create_time', 0) > 0:
            time_since_connect = current_time - stat['host_create_time']
            stat['time_since_connect_seconds'] = time_since_connect
        else:
            stat['time_since_connect_seconds'] = 0
    
    return stats


def get_credit_statistics():
    """
    Получить статистику по кредитам для каждого приложения.
    
    Возвращает сырые данные без расчетов:
    - Завершенные кредиты и количество завершенных задач
    - Средний кредит завершенных задач
    - Количество задач в процессе выполнения
    - Количество задач в очереди на отправку
    """
    query = """
    SELECT 
        a.name as app_name,
        COALESCE(SUM(CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.granted_credit ELSE 0 END), 0) as completed_credit,
        COUNT(DISTINCT CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.id END) as completed_count,
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.granted_credit > 0 THEN r.granted_credit END), 0) as avg_credit,
        COUNT(DISTINCT CASE WHEN r.server_state = 4 THEN r.id END) as in_progress_count,
        COUNT(DISTINCT CASE WHEN r.server_state = 2 THEN r.id END) as unsent_count
    FROM app a
    LEFT JOIN workunit w ON a.id = w.appid
    LEFT JOIN result r ON w.id = r.workunitid
    WHERE a.name IN ('fast_task', 'medium_task', 'long_task', 'random_task') 
        AND a.deprecated = 0
    GROUP BY a.id, a.name
    ORDER BY a.name;
    """
    
    cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -N -e \"{query}\""
    output, success = run_command(cmd, check=False, capture_output=True)
    
    if not success or not output:
        return {}
    
    stats = {}
    for line in output.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 6:
            app_name = parts[0].strip()
            try:
                completed_credit = float(parts[1].strip())
                completed_count = int(parts[2].strip())
                avg_credit = float(parts[3].strip())
                in_progress_count = int(parts[4].strip())
                unsent_count = int(parts[5].strip())
                
                if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
                    avg_credit = completed_credit / completed_count
                
                stats[app_name] = {
                    'completed_credit': completed_credit,
                    'completed_count': completed_count,
                    'avg_credit': avg_credit,
                    'in_progress_count': in_progress_count,
                    'unsent_count': unsent_count
                }
            except (ValueError, IndexError):
                continue
    
    return stats

