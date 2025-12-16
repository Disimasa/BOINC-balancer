#!/usr/bin/env python3
from __future__ import print_function
import sys
import time
import json
import logging
from pathlib import Path
from datetime import datetime
from lib.apps import get_current_weights, update_weights
from lib.statistics import get_credit_statistics
from lib.boinc_utils import trigger_feeder_update, restart_feeder, ensure_daemons_running
from scripts.analysis.show_feeder_queue import get_queue_shares_from_shmem, get_queue_counts_from_shmem

MIN_WEIGHT = 0.001
MAX_WEIGHT = 1000.0

DEFAULT_KP = 1
DEFAULT_KI = 0.1
DEFAULT_KD = 0.3

MAX_STEP_CHANGE = 1
INTEGRAL_LIMIT = 1.0
QUEUE_SATURATION_THRESHOLD = 0.99

_last_feeder_restart_time = 0
_min_restart_interval = 30
_min_restart_change_threshold = 0.1


def init_snapshot_file(kp, ki, kd):
    base_dir = Path(__file__).parent.parent.parent.absolute()
    snapshots_dir = base_dir / "data" / "weights_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshots_dir / f"pid_weights_{ts}.json"
    header = {
        "created_at": datetime.now().isoformat(),
        "kp": kp,
        "ki": ki,
        "kd": kd,
        "max_step_change": MAX_STEP_CHANGE,
        "states": []
    }
    with snapshot_path.open("w", encoding="utf-8") as f:
        json.dump(header, f, ensure_ascii=False, indent=2)
    return snapshot_path


def append_snapshot(snapshot_path, state):
    try:
        if snapshot_path is None:
            return
        snapshot_path = Path(snapshot_path)
        if snapshot_path.exists():
            with snapshot_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {
                "created_at": datetime.now().isoformat(),
                "kp": state.get("kp"),
                "ki": state.get("ki"),
                "kd": state.get("kd"),
                "max_step_change": MAX_STEP_CHANGE,
                "states": []
            }
        data.setdefault("states", []).append(state)
        with snapshot_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger = logging.getLogger()
        logger.warning(f"  ⚠ Не удалось сохранить снимок весов: {e}")


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


def pid_calculate_weights(credit_stats, current_weights, dt, pid_state, kp, ki, kd):
    logger = logging.getLogger()

    all_apps = set(credit_stats.keys()) | set(current_weights.keys())
    if not all_apps:
        return current_weights, pid_state, {}

    all_apps_have_nonzero_credit = all(
        credit_stats.get(app, {}).get('completed_credit', 0) > 0 for app in all_apps
    )
    any_app_has_nonzero_credit = any(
        credit_stats.get(app, {}).get('completed_credit', 0) > 0 for app in all_apps
    )
    if not any_app_has_nonzero_credit:
        logger.warning("  ⚠ Нет завершенных задач с ненулевым кредитом ни у одного приложения, веса не изменяются")
        return current_weights, pid_state, {}
    if not all_apps_have_nonzero_credit:
        logger.warning("  ⚠ Не у всех приложений есть завершенные задачи с ненулевым кредитом, веса не изменяются")
        return current_weights, pid_state, {}

    app_total_credits = calculate_total_credits(credit_stats)

    total_completed_credit = sum(stats.get('completed_credit', 0) for stats in credit_stats.values())
    total_completed_count = sum(stats.get('completed_count', 0) for stats in credit_stats.values())
    global_avg_credit = total_completed_credit / total_completed_count if total_completed_count > 0 else 0

    per_app_scale = {}
    for app_name in all_apps:
        stats = credit_stats.get(app_name, {})
        completed_credit = stats.get('completed_credit', 0)
        completed_count = stats.get('completed_count', 0)
        avg_credit = stats.get('avg_credit', 0)
        if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
            avg_credit = completed_credit / completed_count
        elif avg_credit == 0:
            avg_credit = global_avg_credit
        if global_avg_credit > 0 and avg_credit > 0:
            scale = avg_credit / global_avg_credit
        else:
            scale = 1.0
        scale = max(0.5, min(4.0, scale))
        per_app_scale[app_name] = scale

    for app_name in all_apps:
        if app_name not in app_total_credits:
            app_total_credits[app_name] = 0

    total_credit = sum(app_total_credits.values())
    if total_credit == 0:
        logger.warning("  ⚠ Нет данных о кредитах, веса не изменяются")
        return current_weights, pid_state, {}

    shmem_queue_shares = get_queue_shares_from_shmem()
    shmem_queue_counts, total_slots = get_queue_counts_from_shmem()
    queue_shares = {}
    saturated_apps = set()
    if shmem_queue_shares:
        for app_name in all_apps:
            share = shmem_queue_shares.get(app_name, 0.0)
            queue_shares[app_name] = share
            if share >= QUEUE_SATURATION_THRESHOLD:
                saturated_apps.add(app_name)
    else:
        for app_name in all_apps:
            queue_shares[app_name] = 0.0
    any_saturated = bool(saturated_apps)

    target_share = 1.0 / len(all_apps)

    integral_error = pid_state.get("integral_error", {})
    prev_error = pid_state.get("prev_error", {})

    raw_weights = {}
    frozen_weights = {}
    freeze_flags = {}

    for app_name in all_apps:
        current_weight = current_weights.get(app_name, 1.0)
        current_credit = app_total_credits.get(app_name, 0)
        current_share = current_credit / total_credit if total_credit > 0 else 0
        queue_share = queue_shares.get(app_name, 0.0)
        queue_count = shmem_queue_counts.get(app_name, 0)

        error = target_share - current_share

        ie = integral_error.get(app_name, 0.0) + error * dt
        ie = max(-INTEGRAL_LIMIT, min(INTEGRAL_LIMIT, ie))
        integral_error[app_name] = ie

        prev_e = prev_error.get(app_name, 0.0)
        de = (error - prev_e) / dt if dt > 0 else 0.0
        prev_error[app_name] = error

        output = kp * error + ki * ie + kd * de
        scale = per_app_scale.get(app_name, 1.0)
        # Временно не используем масштабирование по среднему кредиту
        # if scale != 0:
        #     output = output / scale

        factor = 1.0 + output

        frozen = False

        if queue_share >= QUEUE_SATURATION_THRESHOLD and factor > 1.0:
            factor = 1.0
            frozen = True

        if any_saturated and app_name not in saturated_apps and factor < 1.0:
            factor = 1.0
            frozen = True

        if queue_count <= 1 and factor < 1.0:
            factor = 1.0
            frozen = True

        if total_slots > 0 and queue_count >= total_slots - 1 and factor > 1.0:
            factor = 1.0
            frozen = True

        factor = max(1.0 - MAX_STEP_CHANGE, min(1.0 + MAX_STEP_CHANGE, factor))

        if frozen:
            frozen_weights[app_name] = current_weight
        else:
            raw_w = current_weight * factor
            raw_w = max(MIN_WEIGHT, min(MAX_WEIGHT, raw_w))
            raw_weights[app_name] = raw_w

        freeze_flags[app_name] = frozen

    total_raw = sum(raw_weights.values())
    total_current = sum(current_weights.values()) if current_weights else 1.0
    frozen_sum = sum(frozen_weights.values())

    new_weights = {}

    if total_raw > 0 and total_current > frozen_sum:
        target_sum_for_raw = total_current - frozen_sum
        for app_name, rw in raw_weights.items():
            w = rw / total_raw * target_sum_for_raw
            new_weights[app_name] = max(MIN_WEIGHT, min(MAX_WEIGHT, w))
    else:
        for app_name, rw in raw_weights.items():
            new_weights[app_name] = rw

    for app_name, fw in frozen_weights.items():
        new_weights[app_name] = fw

    pid_state["integral_error"] = integral_error
    pid_state["prev_error"] = prev_error

    return new_weights, pid_state, freeze_flags


def balance_once(pid_state, kp=DEFAULT_KP, ki=DEFAULT_KI, kd=DEFAULT_KD, verbose=True,
                 min_change_threshold=0.001, dt=60):
    logger = logging.getLogger()

    current_weights = get_current_weights()
    if not current_weights:
        logger.error("  ✗ Не удалось получить текущие веса")
        return False, {}, {}, {}, pid_state

    credit_stats = get_credit_statistics()
    if not credit_stats:
        logger.warning("  ⚠ Нет статистики по кредитам")
        return False, current_weights, current_weights, {}, pid_state

    app_total_credits = calculate_total_credits(credit_stats)
    total_credit_sum = sum(app_total_credits.values())
    completed_credits_by_app = {name: stats.get("completed_credit", 0) for name, stats in credit_stats.items()}
    completed_credit_sum = sum(completed_credits_by_app.values())

    if verbose:
        logger.info("\nСтатистика по кредитам:")

        for app_name in sorted(credit_stats.keys()):
            stats = credit_stats[app_name]
            total_credit_app = app_total_credits.get(app_name, 0)
            share = (total_credit_app / total_credit_sum * 100) if total_credit_sum > 0 else 0

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
            logger.info(f"      - Ожидается: {expected_credit:.2f} ({in_progress_count} в работе)")
            if unsent_count > 0:
                logger.info(f"      - В очереди: {unsent_count} (не учитываются в расчете)")
            logger.info(f"      - Средний кредит: {avg_credit:.4f}")

    target_weights, pid_state, freeze_flags = pid_calculate_weights(
        credit_stats, current_weights, dt, pid_state, kp, ki, kd
    )

    if verbose:
        logger.info("\nНовые веса (после PID):")
        for app_name in sorted(target_weights.keys()):
            old_w = current_weights.get(app_name, 1.0)
            new_w = target_weights[app_name]
            change_pct = ((new_w - old_w) / old_w * 100) if old_w > 0 else 0
            freeze_suffix = " freeze" if freeze_flags.get(app_name) else ""
            logger.info(f"  {app_name}: {new_w:.4f} (было {old_w:.4f}, изменение {change_pct:+.1f}%){freeze_suffix}")

    weights_changed = False
    changes_detail = []
    for app_name in target_weights:
        old_w = current_weights.get(app_name, 1.0)
        new_w = target_weights[app_name]
        change_pct = ((new_w - old_w) / old_w) if old_w > 0 else 0.0
        changes_detail.append((app_name, old_w, new_w, change_pct))
        if abs(change_pct) > min_change_threshold:
            weights_changed = True

    if not weights_changed:
        if verbose:
            logger.info(f"\n  ⚠ Веса не обновляются: все относительные изменения ≤ {min_change_threshold*100:.1f}%")
            logger.info("  Детали изменений:")
            for app_name, old_w, new_w, change_pct in changes_detail:
                logger.info(f"    {app_name}: {old_w:.6f} → {new_w:.6f} (изменение {change_pct*100:+.4f}%)")
        return True, current_weights, target_weights, credit_stats, pid_state

    success = update_weights(target_weights)
    if not success:
        logger.error("  ✗ Ошибка при обновлении весов")
        return False, current_weights, target_weights, credit_stats, pid_state

    snapshot_state = {
        "timestamp": datetime.now().isoformat(),
        "kp": kp,
        "ki": ki,
        "kd": kd,
        "max_step_change": MAX_STEP_CHANGE,
        "min_restart_change_threshold": _min_restart_change_threshold,
        "max_change_pct": 0,
        "current_weights": current_weights,
        "new_weights": target_weights,
        "total_credits_by_app": app_total_credits,
        "total_credit_sum": total_credit_sum,
        "completed_credits_by_app": completed_credits_by_app,
        "completed_credit_sum": completed_credit_sum,
    }
    append_snapshot(pid_state.get("snapshot_path"), snapshot_state)

    if verbose:
        logger.info("\nПерезапуск feeder для применения новых весов (без проверки порога изменений)...")
    restart_feeder()
    time.sleep(3)
    if verbose:
        logger.info("Проверка валидаторов и ассимиляторов...")
    ensure_daemons_running()

    return True, current_weights, target_weights, credit_stats, pid_state


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


def balance_loop(interval=60, kp=DEFAULT_KP, ki=DEFAULT_KI, kd=DEFAULT_KD,
                 max_iterations=None, log_file=None, min_change_threshold=0.001):
    logger = setup_logging(log_file)

    logger.info("="*80)
    logger.info("ЗАПУСК PID-БАЛАНСИРОВКИ")
    logger.info("="*80)
    logger.info(f"Интервал: {interval} секунд")
    logger.info(f"Kp={kp}, Ki={ki}, Kd={kd}")
    if log_file:
        logger.info(f"Логи: {log_file}")
    if max_iterations:
        logger.info(f"Максимум итераций: {max_iterations}")
    else:
        logger.info("Бесконечный цикл (Ctrl+C для остановки)")
    logger.info("="*80)

    snapshot_path = init_snapshot_file(kp, ki, kd)
    pid_state = {"integral_error": {}, "prev_error": {}, "snapshot_path": str(snapshot_path)}
    iteration = 0
    try:
        while True:
            iteration += 1
            logger.info(f"\n--- Итерация {iteration} ---")

            dt = interval if interval > 0 else 1
            success, old_weights, new_weights, stats, pid_state = balance_once(
                pid_state=pid_state, kp=kp, ki=ki, kd=kd,
                verbose=True, min_change_threshold=min_change_threshold, dt=dt
            )

            if max_iterations and iteration >= max_iterations:
                logger.info(f"\n✓ Достигнуто максимальное количество итераций ({max_iterations})")
                break

            if interval > 0:
                logger.info(f"\nОжидание {interval} секунд до следующей итерации...")
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("\n\n✓ Цикл PID-балансировки остановлен пользователем")
    except Exception as e:
        logger.error(f"\n✗ Ошибка в цикле PID-балансировки: {e}")
        raise


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="PID-балансировщик нагрузки для BOINC",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--loop", action="store_true", help="Запустить цикл балансировки")
    parser.add_argument("--interval", type=int, default=60, help="Интервал между итерациями (сек)")
    parser.add_argument("--kp", type=float, default=DEFAULT_KP, help="Пропорциональный коэффициент")
    parser.add_argument("--ki", type=float, default=DEFAULT_KI, help="Интегральный коэффициент")
    parser.add_argument("--kd", type=float, default=DEFAULT_KD, help="Дифференциальный коэффициент")
    parser.add_argument("--max-iterations", type=int, default=None, help="Макс. итераций (для --loop)")
    parser.add_argument("--quiet", action="store_true", help="Минимальный вывод")
    parser.add_argument("--log-file", type=str, default=None, help="Файл логов (по умолчанию: dynamic_balancer_pid.log)")
    parser.add_argument("--min-change", type=float, default=0.001, help="Мин. относительное изменение веса (доля, по умолчанию 0.05 = 5%)")

    args = parser.parse_args()

    if args.interval < 0:
        print("✗ Ошибка: interval должен быть >= 0", file=sys.stderr)
        return 1

    if args.log_file is None and args.loop:
        from pathlib import Path
        script_dir = Path(__file__).parent.parent.parent.absolute()
        args.log_file = str(script_dir / "dynamic_balancer_pid.log")

    if args.loop:
        setup_logging(args.log_file)
        balance_loop(
            interval=args.interval,
            kp=args.kp,
            ki=args.ki,
            kd=args.kd,
            max_iterations=args.max_iterations,
            log_file=args.log_file,
            min_change_threshold=args.min_change,
        )
    else:
        setup_logging(None)
        dt = args.interval if args.interval > 0 else 1
        snapshot_path = init_snapshot_file(args.kp, args.ki, args.kd)
        pid_state = {"integral_error": {}, "prev_error": {}, "snapshot_path": str(snapshot_path)}
        success, _, _, _, _ = balance_once(
            pid_state=pid_state,
            kp=args.kp,
            ki=args.ki,
            kd=args.kd,
            verbose=not args.quiet,
            min_change_threshold=args.min_change,
            dt=dt,
        )
        return 0 if success else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

