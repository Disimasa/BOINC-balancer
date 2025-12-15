#!/usr/bin/env python3
from __future__ import print_function
import sys
import time
import logging
from lib.apps import get_current_weights, update_weights
from lib.statistics import get_credit_statistics
from lib.boinc_utils import trigger_feeder_update, restart_feeder, ensure_daemons_running

MIN_WEIGHT = 0.01
MAX_WEIGHT = 100.0

DEFAULT_SMOOTHING = 0.3

INITIAL_WEIGHT_MIN = 0.8
INITIAL_WEIGHT_MAX = 1.2


def calculate_total_credits(credit_stats):
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
        
        if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
            avg_credit = completed_credit / completed_count
        elif avg_credit == 0:
            avg_credit = global_avg_credit
        
        expected_credit_from_in_progress = avg_credit * in_progress_count
        
        total_credit = completed_credit + expected_credit_from_in_progress
        app_total_credits[app_name] = total_credit
    
    return app_total_credits


def calculate_target_weights(credit_stats, current_weights, smoothing=0.3):
    all_apps = set(credit_stats.keys()) | set(current_weights.keys())
    if not all_apps:
        return current_weights
    
    all_apps_have_completed = True
    any_app_has_completed = False
    for app_name in all_apps:
        app_stats = credit_stats.get(app_name, {})
        completed_count = app_stats.get('completed_count', 0)
        if completed_count > 0:
            any_app_has_completed = True
        else:
            all_apps_have_completed = False
    
    if not any_app_has_completed:
        logger = logging.getLogger()
        logger.warning("  ⚠ Нет завершенных задач ни у одного приложения, веса не изменяются")
        return current_weights
    
    app_total_credits = calculate_total_credits(credit_stats)
    
    for app_name in all_apps:
        if app_name not in app_total_credits:
            app_total_credits[app_name] = 0
    
    total_credit = sum(app_total_credits.values())
    
    if total_credit == 0:
        logger = logging.getLogger()
        logger.warning("  ⚠ Нет данных о кредитах, веса не изменяются")
        return current_weights
    
    use_limited_range = not all_apps_have_completed
    if use_limited_range:
        logger = logging.getLogger()
        logger.info(f"  ⚠ Не все приложения имеют завершенные задачи - веса ограничены диапазоном [{INITIAL_WEIGHT_MIN}, {INITIAL_WEIGHT_MAX}]")
    
    target_share = 1.0 / len(all_apps)
    
    target_weights = {}
    
    for app_name in all_apps:
        current_weight = current_weights.get(app_name, 1.0)
        current_credit = app_total_credits.get(app_name, 0)
        
        current_share = current_credit / total_credit if total_credit > 0 else 0
        
        if current_share > 0:
            new_weight = current_weight * (target_share / current_share)
        else:
            new_weight = current_weight
        
        new_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, new_weight))
        
        smoothed_weight = smoothing * current_weight + (1 - smoothing) * new_weight
        smoothed_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, smoothed_weight))
        
        if use_limited_range:
            smoothed_weight = max(INITIAL_WEIGHT_MIN, min(INITIAL_WEIGHT_MAX, smoothed_weight))
        
        target_weights[app_name] = smoothed_weight
    
    return target_weights


_last_feeder_restart_time = 0
_min_restart_interval = 30
_min_restart_change_threshold = 0.1

def balance_once(smoothing=DEFAULT_SMOOTHING, verbose=True, min_change_threshold=0.01):
    logger = logging.getLogger()
    
    current_weights = get_current_weights()
    if not current_weights:
        logger.error("  ✗ Не удалось получить текущие веса")
        return False, {}, {}, {}
    
    credit_stats = get_credit_statistics()
    if not credit_stats:
        logger.warning("  ⚠ Нет статистики по кредитам")
        return False, current_weights, current_weights, {}
    
    if verbose:
        logger.info("\nСтатистика по кредитам:")
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
            
            avg_credit = stats.get('avg_credit', 0)
            if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
                avg_credit = completed_credit / completed_count
            elif avg_credit == 0:
                total_completed_credit = sum(s.get('completed_credit', 0) for s in credit_stats.values())
                total_completed_count = sum(s.get('completed_count', 0) for s in credit_stats.values())
                avg_credit = total_completed_credit / total_completed_count if total_completed_count > 0 else 0
            
            expected_credit = avg_credit * in_progress_count
            
            logger.info(f"  {app_name}:")
            logger.info(f"    Итого кредит: {total_credit_app:.2f} ({share:.1f}%)")
            logger.info(f"      - Завершено: {completed_credit:.2f} ({completed_count} задач)")
            logger.info(f"      - Ожидается: {expected_credit:.2f} "
                  f"({in_progress_count} в работе)")
            if unsent_count > 0:
                logger.info(f"      - В очереди: {unsent_count} (не учитываются в расчете)")
            logger.info(f"      - Средний кредит: {avg_credit:.4f}")
    
    target_weights = calculate_target_weights(credit_stats, current_weights, smoothing)
    
    if verbose:
        logger.info("\nНовые веса (после балансировки):")
        for app_name in sorted(target_weights.keys()):
            old_w = current_weights.get(app_name, 1.0)
            new_w = target_weights[app_name]
            change = ((new_w - old_w) / old_w * 100) if old_w > 0 else 0
            logger.info(f"  {app_name}: {new_w:.4f} (было {old_w:.4f}, изменение {change:+.1f}%)")
    
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
    
    success = update_weights(target_weights)
    
    if not success:
        logger.error("  ✗ Ошибка при обновлении весов")
        return False, current_weights, target_weights, credit_stats
    
    global _last_feeder_restart_time
    current_time = time.time()
    time_since_last_restart = current_time - _last_feeder_restart_time
    
    max_change_pct = 0
    for app_name in target_weights:
        old_w = current_weights.get(app_name, 1.0)
        new_w = target_weights[app_name]
        if old_w > 0:
            change_pct = abs((new_w - old_w) / old_w)
            max_change_pct = max(max_change_pct, change_pct)
        elif new_w > 0:
            max_change_pct = max(max_change_pct, 1.0)
    
    should_restart = (max_change_pct >= _min_restart_change_threshold and 
                     time_since_last_restart >= _min_restart_interval)
    
    if should_restart:
        if verbose:
            logger.info(f"\nПерезапуск feeder для применения новых весов (изменение {max_change_pct*100:.1f}%)...")
        restart_feeder()
        _last_feeder_restart_time = current_time
        time.sleep(3)
        if verbose:
            logger.info("Проверка валидаторов и ассимиляторов...")
        ensure_daemons_running()
    else:
        trigger_feeder_update()
        if verbose and max_change_pct < _min_restart_change_threshold:
            logger.info(f"\nВеса обновлены через reread_db (изменение {max_change_pct*100:.1f}% < {_min_restart_change_threshold*100:.0f}%, перезапуск не требуется)")
        elif verbose:
            logger.info(f"\nВеса обновлены через reread_db (последний перезапуск был {time_since_last_restart:.0f} сек назад)")
    
    return True, current_weights, target_weights, credit_stats


def setup_logging(log_file=None):
    handlers = []
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)
    
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = handlers
    
    return root_logger


def balance_loop(interval=60, smoothing=DEFAULT_SMOOTHING, max_iterations=None, log_file=None, min_change_threshold=0.01):
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
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Динамический балансировщик нагрузки для BOINC",
        formatter_class=argparse.RawDescriptionHelpFormatter,)
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
    
    if args.smoothing < 0 or args.smoothing > 1:
        print("✗ Ошибка: smoothing должен быть в диапазоне 0.0-1.0", file=sys.stderr)
        return 1
    
    log_file = args.log_file
    if args.loop and log_file is None:
        from pathlib import Path
        script_dir = Path(__file__).parent.parent.parent.absolute()
        log_file = str(script_dir / "dynamic_balancer.log")
    
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

