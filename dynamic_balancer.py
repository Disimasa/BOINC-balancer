#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Динамический балансировщик нагрузки для BOINC.

Балансирует нагрузку между приложениями через изменение app.weight,
стремясь к равномерному распределению суммы granted_credit по приложениям.
"""
from __future__ import print_function
import subprocess
import sys
import time
import math
import logging
from datetime import datetime

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

# Минимальный и максимальный вес приложения
MIN_WEIGHT = 0.00001
MAX_WEIGHT = 100000.0

# Коэффициент сглаживания (0.0 - резкие изменения, 1.0 - без изменений)
# Рекомендуется 0.3-0.5 для плавных изменений
DEFAULT_SMOOTHING = 0


def run_command(cmd, check=True, capture_output=False, silent=False):
    """Выполнить команду в контейнере apache и вывести stdout/stderr."""
    env_cmd = "export BOINC_PROJECT_DIR={proj} && cd {proj} && {cmd}".format(
        proj=PROJECT_HOME, cmd=cmd
    )
    full_cmd = [
        "wsl.exe", "-e", "docker", "exec", CONTAINER_NAME,
        "bash", "-lc", env_cmd
    ]
    if not capture_output and not silent:
        print("Выполняю: {}".format(cmd))
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout_b, stderr_b = proc.communicate()
        
        # Декодируем вывод с обработкой ошибок
        stdout = stdout_b.decode('utf-8', errors='replace') if stdout_b else ""
        stderr = stderr_b.decode('utf-8', errors='replace') if stderr_b else ""

        if capture_output:
            if check and proc.returncode != 0:
                return ""
            return stdout.strip() if stdout else ""
        else:
            if stdout:
                print(stdout, end="")
            if stderr:
                print(stderr, file=sys.stderr, end="")
            if check and proc.returncode != 0:
                print("Ошибка: команда завершилась с кодом {}".format(proc.returncode), file=sys.stderr)
                return False
            return proc.returncode == 0
    except Exception as e:
        print("Исключение при выполнении команды: {}".format(e), file=sys.stderr)
        if capture_output:
            return ""
        return False


def get_current_weights():
    """Получить текущие веса приложений из БД."""
    query = """
    SELECT name, weight 
    FROM app 
    WHERE name IN ('fast_task', 'medium_task', 'long_task', 'random_task') 
        AND deprecated = 0
    ORDER BY name;
    """
    
    cmd = "mysql -u root -ppassword boincserver -N -e \"{}\"".format(query)
    output = run_command(cmd, capture_output=True)
    
    if not output:
        return {}
    
    weights = {}
    for line in output.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 2:
            app_name = parts[0].strip()
            try:
                weight = float(parts[1].strip())
                weights[app_name] = weight
            except (ValueError, IndexError):
                continue
    
    return weights


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
        -- Завершенные кредиты
        COALESCE(SUM(CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.granted_credit ELSE 0 END), 0) as completed_credit,
        COUNT(DISTINCT CASE WHEN r.server_state = 5 AND r.outcome = 1 THEN r.id END) as completed_count,
        -- Средний кредит завершенных задач (для оценки ожидаемых кредитов)
        COALESCE(AVG(CASE WHEN r.server_state = 5 AND r.outcome = 1 AND r.granted_credit > 0 THEN r.granted_credit END), 0) as avg_credit,
        -- Количество отправленных задач (в процессе выполнения)
        COUNT(DISTINCT CASE WHEN r.server_state = 4 THEN r.id END) as in_progress_count,
        -- Количество задач в очереди на отправку
        COUNT(DISTINCT CASE WHEN r.server_state = 2 THEN r.id END) as unsent_count
    FROM app a
    LEFT JOIN workunit w ON a.id = w.appid
    LEFT JOIN result r ON w.id = r.workunitid
    WHERE a.name IN ('fast_task', 'medium_task', 'long_task', 'random_task') 
        AND a.deprecated = 0
    GROUP BY a.id, a.name
    ORDER BY a.name;
    """
    
    cmd = "mysql -u root -ppassword boincserver -N -e \"{}\"".format(query)
    output = run_command(cmd, capture_output=True)
    
    if not output:
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
                
                # Если нет среднего кредита, но есть завершенные задачи, вычисляем среднее
                if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
                    avg_credit = completed_credit / completed_count
                
                stats[app_name] = {
                    'completed_credit': completed_credit,
                    'completed_count': completed_count,
                    'avg_credit': avg_credit,
                    'in_progress_count': in_progress_count,
                    'unsent_count': unsent_count
                }
            except (ValueError, IndexError) as e:
                continue
    
    return stats


def calculate_total_credits(credit_stats):
    """
    Вычислить итоговые кредиты для каждого приложения (завершенные + ожидаемые).
    
    Args:
        credit_stats: словарь {app_name: {
            'completed_credit': float,
            'completed_count': int,
            'avg_credit': float,
            'in_progress_count': int,
            'unsent_count': int
        }}
    
    Returns:
        словарь {app_name: float} - итоговые кредиты для каждого приложения
    """
    # Вычисляем глобальный средний кредит (fallback для приложений без завершенных задач)
    total_completed_credit = sum(stats.get('completed_credit', 0) for stats in credit_stats.values())
    total_completed_count = sum(stats.get('completed_count', 0) for stats in credit_stats.values())
    global_avg_credit = total_completed_credit / total_completed_count if total_completed_count > 0 else 0
    
    app_total_credits = {}
    for app_name, app_stats in credit_stats.items():
        completed_credit = app_stats.get('completed_credit', 0)
        completed_count = app_stats.get('completed_count', 0)
        avg_credit = app_stats.get('avg_credit', 0)
        in_progress_count = app_stats.get('in_progress_count', 0)
        unsent_count = app_stats.get('unsent_count', 0)
        
        # Если нет среднего кредита, но есть завершенные задачи, вычисляем среднее
        if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
            avg_credit = completed_credit / completed_count
        # Если все еще нет среднего кредита, используем глобальный средний
        elif avg_credit == 0:
            avg_credit = global_avg_credit
        
        # Ожидаемые кредиты от отправленных задач (в процессе выполнения)
        expected_credit_from_in_progress = avg_credit * in_progress_count
        
        # Ожидаемые кредиты от задач в очереди (готовы к отправке)
        # Используем меньший вес, так как они еще не отправлены
        expected_credit_from_unsent = avg_credit * unsent_count * 0.5
        
        # Общий ожидаемый кредит
        expected_credit = expected_credit_from_in_progress + expected_credit_from_unsent
        
        # Итоговый кредит = завершенный + ожидаемый
        total_credit = completed_credit + expected_credit
        app_total_credits[app_name] = total_credit
    
    return app_total_credits


def calculate_target_weights(credit_stats, current_weights, smoothing=0.3):
    """
    Вычислить целевые веса приложений на основе кредитов.
    
    Цель: сумма кредитов по всем приложениям должна быть равна.
    Учитывает не только завершенные кредиты, но и ожидаемые от отправленных задач.
    
    Args:
        credit_stats: словарь {app_name: {
            'completed_credit': float,
            'completed_count': int,
            'avg_credit': float,
            'in_progress_count': int,
            'unsent_count': int
        }}
        current_weights: словарь {app_name: float} - текущие веса
        smoothing: коэффициент сглаживания (0.0 - резкие изменения, 1.0 - без изменений)
    
    Returns:
        словарь {app_name: float} - новые веса
    """
    # Получаем список всех приложений
    all_apps = set(credit_stats.keys()) | set(current_weights.keys())
    if not all_apps:
        return current_weights
    
    # Вычисляем итоговые кредиты для каждого приложения (завершенные + ожидаемые)
    app_total_credits = calculate_total_credits(credit_stats)
    
    # Добавляем приложения, которых нет в credit_stats (с нулевыми кредитами)
    for app_name in all_apps:
        if app_name not in app_total_credits:
            app_total_credits[app_name] = 0
    
    # Вычисляем общую сумму кредитов
    total_credit = sum(app_total_credits.values())
    
    # Если нет кредитов, возвращаем текущие веса
    if total_credit == 0:
        logger = logging.getLogger()
        logger.warning("  ⚠ Нет данных о кредитах, веса не изменяются")
        return current_weights
    
    # Целевая доля для каждого приложения (равномерное распределение)
    target_share = 1.0 / len(all_apps)
    
    # Вычисляем текущие доли и целевые веса
    target_weights = {}
    
    for app_name in all_apps:
        current_weight = current_weights.get(app_name, 1.0)
        current_credit = app_total_credits.get(app_name, 0)
        
        # Текущая доля приложения
        current_share = current_credit / total_credit if total_credit > 0 else 0
        
        # Если текущая доля = 0 (нет кредитов), используем небольшой вес для начала
        if current_share == 0:
            # Если у приложения нет кредитов, но есть завершенные задачи, даем минимальный вес
            if app_stats['completed_count'] > 0:
                new_weight = MIN_WEIGHT
            else:
                # Если вообще нет данных, оставляем текущий вес
                new_weight = current_weight
        else:
            # Вычисляем новый вес: weight_new = weight_old * (target_share / current_share)
            new_weight = current_weight * (target_share / current_share)
        
        # Ограничиваем вес минимальным и максимальным значениями
        new_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, new_weight))
        
        # Применяем сглаживание: weight_final = smoothing * weight_old + (1 - smoothing) * weight_new
        smoothed_weight = smoothing * current_weight + (1 - smoothing) * new_weight
        smoothed_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, smoothed_weight))
        
        target_weights[app_name] = smoothed_weight
    
    return target_weights


def update_weights(new_weights):
    """Обновить веса приложений в БД."""
    if not new_weights:
        return False
    
    # Формируем SQL для обновления весов
    update_statements = []
    for app_name, weight in new_weights.items():
        update_statements.append(
            "UPDATE app SET weight = {} WHERE name = '{}';".format(weight, app_name)
        )
    
    # Выполняем все обновления одной транзакцией
    sql = "START TRANSACTION;\n" + "\n".join(update_statements) + "\nCOMMIT;"
    
    # Выполняем SQL через heredoc (silent=True, чтобы не логировать команду)
    mysql_cmd = """bash -c "mysql -u root -ppassword boincserver << 'EOFSQL'
{}
EOFSQL" """.format(sql)
    
    result = run_command(mysql_cmd, check=False, silent=True)
    return result


def trigger_feeder_update():
    """Обновить feeder, чтобы он пересобрал массив задач с новыми весами."""
    run_command("touch {}/reread_db".format(PROJECT_HOME), check=False, silent=True)
    time.sleep(1)  # Даем время feeder обработать триггер


def balance_once(smoothing=DEFAULT_SMOOTHING, verbose=True, min_change_threshold=0.01):
    """
    Выполнить одну итерацию балансировки.
    
    Args:
        smoothing: коэффициент сглаживания
        verbose: подробный вывод
        min_change_threshold: минимальное изменение веса для обновления (по умолчанию 0.01)
    
    Returns:
        tuple: (success: bool, old_weights: dict, new_weights: dict, stats: dict)
    """
    logger = logging.getLogger()
    
    # Получаем текущие веса
    current_weights = get_current_weights()
    if not current_weights:
        logger.error("  ✗ Не удалось получить текущие веса")
        return False, {}, {}, {}
    
    # Получаем статистику по кредитам
    credit_stats = get_credit_statistics()
    if not credit_stats:
        logger.warning("  ⚠ Нет статистики по кредитам")
        return False, current_weights, current_weights, {}
    
    if verbose:
        logger.info("\nСтатистика по кредитам:")
        # Вычисляем итоговые кредиты для отображения
        app_total_credits = calculate_total_credits(credit_stats)
        total_credit = sum(app_total_credits.values())
        
        for app_name in sorted(credit_stats.keys()):
            stats = credit_stats[app_name]
            total_credit_app = app_total_credits.get(app_name, 0)
            share = (total_credit_app / total_credit * 100) if total_credit > 0 else 0
            
            completed_credit = stats.get('completed_credit', 0)
            completed_count = stats.get('completed_count', 0)
            in_progress_count = stats.get('in_progress_count', 0)
            unsent_count = stats.get('unsent_count', 0)
            
            # Вычисляем ожидаемый кредит для отображения
            avg_credit = stats.get('avg_credit', 0)
            if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
                avg_credit = completed_credit / completed_count
            elif avg_credit == 0:
                # Используем глобальный средний
                total_completed_credit = sum(s.get('completed_credit', 0) for s in credit_stats.values())
                total_completed_count = sum(s.get('completed_count', 0) for s in credit_stats.values())
                avg_credit = total_completed_credit / total_completed_count if total_completed_count > 0 else 0
            
            expected_credit = avg_credit * in_progress_count + avg_credit * unsent_count * 0.5
            
            logger.info(f"  {app_name}:")
            logger.info(f"    Итого кредит: {total_credit_app:.2f} ({share:.1f}%)")
            logger.info(f"      - Завершено: {completed_credit:.2f} ({completed_count} задач)")
            logger.info(f"      - Ожидается: {expected_credit:.2f} "
                  f"({in_progress_count} в работе, {unsent_count} в очереди)")
            logger.info(f"      - Средний кредит: {avg_credit:.4f}")
    
    # Вычисляем целевые веса
    target_weights = calculate_target_weights(credit_stats, current_weights, smoothing)
    
    if verbose:
        logger.info("\nНовые веса (после балансировки):")
        for app_name in sorted(target_weights.keys()):
            old_w = current_weights.get(app_name, 1.0)
            new_w = target_weights[app_name]
            change = ((new_w - old_w) / old_w * 100) if old_w > 0 else 0
            logger.info(f"  {app_name}: {new_w:.4f} (было {old_w:.4f}, изменение {change:+.1f}%)")
    
    # Проверяем, нужно ли обновлять веса
    weights_changed = False
    changes_detail = []
    for app_name in target_weights:
        old_w = current_weights.get(app_name, 1.0)
        new_w = target_weights[app_name]
        change = abs(new_w - old_w)
        change_pct = ((new_w - old_w) / old_w * 100) if old_w > 0 else 0
        changes_detail.append((app_name, old_w, new_w, change, change_pct))
        if change > min_change_threshold:
            weights_changed = True
    
    if not weights_changed:
        if verbose:
            logger.info(f"\n  ⚠ Веса не обновляются: все изменения ≤ {min_change_threshold}")
            logger.info("  Детали изменений:")
            for app_name, old_w, new_w, change, change_pct in changes_detail:
                logger.info(f"    {app_name}: изменение = {change:.6f} ({change_pct:+.4f}%)")
            logger.info(f"  (Порог обновления: {min_change_threshold})")
        return True, current_weights, target_weights, credit_stats
    
    # Обновляем веса в БД
    success = update_weights(target_weights)
    
    if not success:
        logger.error("  ✗ Ошибка при обновлении весов")
        return False, current_weights, target_weights, credit_stats
    
    # Обновляем feeder
    trigger_feeder_update()
    
    return True, current_weights, target_weights, credit_stats


def setup_logging(log_file=None):
    """Настроить логирование в файл и/или консоль"""
    handlers = []
    
    # Консольный вывод
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)
    
    # Файловый вывод (если указан)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    # Настраиваем root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = handlers
    
    return root_logger


def balance_loop(interval=60, smoothing=DEFAULT_SMOOTHING, max_iterations=None, log_file=None, min_change_threshold=0.01):
    """
    Запустить цикл балансировки с заданным интервалом.
    
    Args:
        interval: интервал между итерациями в секундах
        smoothing: коэффициент сглаживания
        max_iterations: максимальное количество итераций (None = бесконечно)
        log_file: путь к файлу для логирования (None = только консоль)
        min_change_threshold: минимальное изменение веса для обновления (по умолчанию 0.01)
    """
    logger = setup_logging(log_file)
    
    logger.info("="*80)
    logger.info("ЗАПУСК ЦИКЛА БАЛАНСИРОВКИ")
    logger.info("="*80)
    logger.info(f"Интервал: {interval} секунд")
    logger.info(f"Сглаживание: {smoothing}")
    if log_file:
        logger.info(f"Логи записываются в: {log_file}")
    if max_iterations:
        logger.info(f"Максимум итераций: {max_iterations}")
    else:
        logger.info("Бесконечный цикл (Ctrl+C для остановки)")
    logger.info("="*80)
    
    iteration = 0
    try:
        while True:
            iteration += 1
            logger.info(f"\n--- Итерация {iteration} ---")
            
            success, old_weights, new_weights, stats = balance_once(
                smoothing=smoothing, verbose=True, min_change_threshold=min_change_threshold
            )
            
            if max_iterations and iteration >= max_iterations:
                logger.info(f"\n✓ Достигнуто максимальное количество итераций ({max_iterations})")
                break
            
            if interval > 0:
                logger.info(f"\nОжидание {interval} секунд до следующей итерации...")
                time.sleep(interval)
    
    except KeyboardInterrupt:
        logger.info("\n\n✓ Цикл балансировки остановлен пользователем")
    except Exception as e:
        logger.error(f"\n✗ Ошибка в цикле балансировки: {e}")
        raise


def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Динамический балансировщик нагрузки для BOINC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Одна итерация балансировки
  python3 dynamic_balancer.py
  
  # Цикл балансировки каждые 60 секунд
  python3 dynamic_balancer.py --loop --interval 60
  
  # Цикл с настройками
  python3 dynamic_balancer.py --loop --interval 30 --smoothing 0.5 --max-iterations 10
        """
    )
    parser.add_argument("--loop", action="store_true", 
                       help="Запустить цикл балансировки")
    parser.add_argument("--interval", type=int, default=60,
                       help="Интервал между итерациями в секундах (по умолчанию 60)")
    parser.add_argument("--smoothing", type=float, default=DEFAULT_SMOOTHING,
                       help=f"Коэффициент сглаживания 0.0-1.0 (по умолчанию {DEFAULT_SMOOTHING})")
    parser.add_argument("--max-iterations", type=int, default=None,
                       help="Максимальное количество итераций (только для --loop)")
    parser.add_argument("--quiet", action="store_true",
                       help="Минимальный вывод")
    parser.add_argument("--log-file", type=str, default=None,
                       help="Путь к файлу для записи логов (по умолчанию: dynamic_balancer.log)")
    parser.add_argument("--min-change", type=float, default=0.01,
                       help="Минимальное изменение веса для обновления (по умолчанию: 0.01)")
    
    args = parser.parse_args()
    
    # Проверяем параметры
    if args.smoothing < 0 or args.smoothing > 1:
        print("✗ Ошибка: smoothing должен быть в диапазоне 0.0-1.0", file=sys.stderr)
        return 1
    
    # Определяем файл логов
    log_file = args.log_file
    if args.loop and log_file is None:
        # По умолчанию логируем в файл при запуске цикла
        from pathlib import Path
        script_dir = Path(__file__).parent
        log_file = str(script_dir / "dynamic_balancer.log")
    
    # Инициализируем логирование (если не в цикле, то только консоль)
    if args.loop:
        setup_logging(log_file)
    else:
        setup_logging(None)
    
    if args.loop:
        balance_loop(interval=args.interval, smoothing=args.smoothing, 
                    max_iterations=args.max_iterations, log_file=log_file,
                    min_change_threshold=args.min_change)
    else:
        success, old_weights, new_weights, stats = balance_once(
            smoothing=args.smoothing, verbose=not args.quiet,
            min_change_threshold=args.min_change
        )
        return 0 if success else 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

