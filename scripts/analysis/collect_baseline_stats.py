#!/usr/bin/env python3
import sys
import time
import json
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from statistics import mean, median
from tqdm import tqdm
from lib.statistics import (
    get_completed_task_statistics,
    get_completed_client_statistics,
    get_credit_statistics,
)
from lib.pipeline import run_full_pipeline

SCRIPT_DIR = Path(__file__).parent.absolute()
SERVER_DIR = SCRIPT_DIR.parent.parent


def init_baseline_snapshot():
    snapshots_dir = SERVER_DIR / "data" / "weights_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = snapshots_dir / f"baseline_weights_{ts}.json"
    header = {
        "created_at": datetime.now().isoformat(),
        "mode": "baseline_collect",
        "states": [],
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(header, f, ensure_ascii=False, indent=2)
    return path


def append_baseline_state(snapshot_path, credit_stats):
    if not credit_stats or snapshot_path is None:
        return
    if any(int(s.get("completed_count", 0)) == 0 for s in credit_stats.values()):
        return
    total_completed_credit = sum(s.get("completed_credit", 0.0) for s in credit_stats.values())
    total_completed_count = sum(s.get("completed_count", 0) for s in credit_stats.values())
    global_avg_credit = (total_completed_credit / total_completed_count) if total_completed_count > 0 else 0.0

    total_credits_by_app = {}
    completed_credits_by_app = {}
    total_credit_sum = 0.0
    completed_credit_sum = 0.0

    for app_name, s in credit_stats.items():
        completed = float(s.get("completed_credit", 0.0))
        count = int(s.get("completed_count", 0))
        avg = float(s.get("avg_credit", 0.0))
        in_progress = int(s.get("in_progress_count", 0))

        if avg == 0 and count > 0 and completed > 0:
            avg = completed / count
        elif avg == 0:
            avg = global_avg_credit

        total_app = completed + avg * in_progress
        total_credits_by_app[app_name] = total_app
        completed_credits_by_app[app_name] = completed
        total_credit_sum += total_app
        completed_credit_sum += completed

    state = {
        "timestamp": datetime.now().isoformat(),
        "total_credits_by_app": total_credits_by_app,
        "total_credit_sum": total_credit_sum,
        "completed_credits_by_app": completed_credits_by_app,
        "completed_credit_sum": completed_credit_sum,
    }

    try:
        path = Path(snapshot_path)
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {
                "created_at": datetime.now().isoformat(),
                "mode": "baseline_collect",
                "states": [],
            }
        data.setdefault("states", []).append(state)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠ Не удалось записать состояние в {snapshot_path}: {e}", file=sys.stderr)

def run_pipeline_setup(balance_hosts=False):
    print("\n" + "="*80)
    print("ШАГ 1: Запуск pipeline для чистого старта")
    print("="*80)
    
    success, account_key = run_full_pipeline(balance_hosts=balance_hosts, client_count=20, update_clients=True)
    return success


def step_wait():
    wait_seconds = 2400
    observe_window = 120

    print("\n" + "="*80)
    print(f"ШАГ 3: Ожидание {wait_seconds / 60} минут для выполнения задач")
    print("="*80)

    credit_shares_history = {}

    with tqdm(total=wait_seconds, desc="Ожидание выполнения задач", unit="сек") as pbar:
        for sec in range(1, wait_seconds + 1):
            time.sleep(1)
            pbar.update(1)

            if sec > wait_seconds - observe_window:
                stats = get_credit_statistics()
                if not stats:
                    continue
                total_credits_per_app = {}
                total_credit_all = 0.0

                total_credits_per_app = {}
                total_credit_all = 0.0
                total_completed_credit = sum(
                    s.get("completed_credit", 0.0) for s in stats.values()
                )
                total_completed_count = sum(
                    s.get("completed_count", 0) for s in stats.values()
                )
                global_avg_credit = (
                    total_completed_credit / total_completed_count
                    if total_completed_count > 0
                    else 0.0
                )

                for app_name, s in stats.items():
                    completed_credit = s.get("completed_credit", 0.0)
                    completed_count = s.get("completed_count", 0)
                    avg_credit = s.get("avg_credit", 0.0)
                    in_progress = s.get("in_progress_count", 0)

                    if avg_credit == 0 and completed_count > 0 and completed_credit > 0:
                        avg_credit = completed_credit / completed_count
                    elif avg_credit == 0:
                        avg_credit = global_avg_credit

                    total_app = completed_credit + avg_credit * in_progress
                    total_credits_per_app[app_name] = total_app
                    total_credit_all += total_app

                if total_credit_all <= 0:
                    continue

                for app_name, total_app in total_credits_per_app.items():
                    share = (total_app / total_credit_all) * 100.0
                    credit_shares_history.setdefault(app_name, []).append(share)

    # Считаем метрики по окну последних observe_window секунд
    window_metrics = {}
    for app_name, values in credit_shares_history.items():
        if not values:
            continue
        window_metrics[app_name] = {
            "share_min": min(values),
            "share_max": max(values),
            "share_mean": mean(values),
            "share_median": median(values),
        }

    return window_metrics


def print_statistics(task_stats, client_stats, window_metrics=None):
    if window_metrics is None:
        window_metrics = {}
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО ЗАВЕРШЕННЫМ ЗАДАЧАМ")
    print("="*80)
    
    if task_stats:
        print(
            f"\n{'Приложение':<20} {'Weight':<10} {'Results':<10} {'Running':<10} "
            f"{'Avg время (с)':<15} {'Min (с)':<12} {'Max (с)':<12} "
            f"{'Avg кредит':<15} {'Total кредит':<15} {'Total+run':<15} "
            f"{'2m_min%':<10} {'2m_max%':<10} {'2m_mean%':<10} {'2m_med%':<10}"
        )
        print("-" * 196)
        for stat in task_stats:
            app_name = stat.get('app_name', 'N/A')
            weight = stat.get('app_weight', 0)
            completed_results = stat.get('completed_results', 0)
            in_progress = stat.get('in_progress_count', 0)
            avg_time = stat.get('avg_elapsed_time', 0)
            min_time = stat.get('min_elapsed_time', 0)
            max_time = stat.get('max_elapsed_time', 0)
            avg_credit = stat.get('avg_credit', 0)
            total_credit = stat.get('total_credit', 0)
            total_with_running = total_credit + avg_credit * in_progress
            win = window_metrics.get(app_name, {})
            share_min = win.get("share_min", 0.0)
            share_max = win.get("share_max", 0.0)
            share_mean = win.get("share_mean", 0.0)
            share_median = win.get("share_median", 0.0)

            print(
                f"{app_name:<20} {weight:<10.2f} {completed_results:<10} {in_progress:<10} "
                f"{avg_time:<15.2f} {min_time:<12.2f} {max_time:<12.2f} "
                f"{avg_credit:<15.2f} {total_credit:<15.2f} {total_with_running:<15.2f} "
                f"{share_min:<10.2f} {share_max:<10.2f} {share_mean:<10.2f} {share_median:<10.2f}"
            )
    else:
        print("Нет данных по задачам")
    
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО КЛИЕНТАМ")
    print("="*80)
    
    if client_stats:
        print(f"\n{'Хост ID':<10} {'Имя хоста':<25} {'Завершено':<12} {'Fast':<8} {'Medium':<8} {'Long':<8} {'Random':<8} {'Avg время (с)':<15} {'Кредит':<15}")
        print("-" * 115)
        for stat in client_stats:
            host_id = stat.get('host_id', 'N/A')
            host_name = stat.get('host_name', 'N/A')
            completed = stat.get('completed_results', 0)
            fast = stat.get('fast_task_completed', 0)
            medium = stat.get('medium_task_completed', 0)
            long_task = stat.get('long_task_completed', 0)
            random = stat.get('random_task_completed', 0)
            avg_time = stat.get('avg_elapsed_time', 0)
            total_credit = stat.get('total_credit', 0)
            print(f"{host_id:<10} {host_name[:24]:<25} {completed:<12} {fast:<8} {medium:<8} {long_task:<8} {random:<8} {avg_time:<15.2f} {total_credit:<15.2f}")
    else:
        print("Нет данных по клиентам")


def save_statistics_to_file(task_stats, client_stats):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = "stats"
    filename = SERVER_DIR / "data" / "stats_results" / f"{prefix}_{timestamp}.json"
    
    data = {
        "timestamp": datetime.now().isoformat(),
        "task_statistics": task_stats,
        "client_statistics": client_stats
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return filename


def main():
    print("="*80)
    print(f"СБОР МЕТРИК")
    print("="*80)

    snapshot_path = init_baseline_snapshot()

    stop_event = threading.Event()

    def baseline_logger():
        counter = 0
        while not stop_event.is_set():
            time.sleep(1)
            counter += 1
            if counter % 30 == 0:
                stats = get_credit_statistics()
                if stats:
                    append_baseline_state(snapshot_path, stats)

    logger_thread = threading.Thread(target=baseline_logger, daemon=True)
    logger_thread.start()

    if not run_pipeline_setup():
        print("✗ Ошибка при запуске pipeline", file=sys.stderr)
        stop_event.set()
        logger_thread.join(timeout=5)
        return 1
    
    window_metrics = step_wait()
    
    task_stats = get_completed_task_statistics()
    client_stats = get_completed_client_statistics()
    
    if task_stats is not None and client_stats is not None:
        print_statistics(task_stats, client_stats, window_metrics=window_metrics)
        filename = save_statistics_to_file(task_stats, client_stats)
        print(f"\n✓ Статистика сохранена в файл: {filename}")
    else:
        print("✗ Не удалось собрать статистику", file=sys.stderr)
        stop_event.set()
        logger_thread.join(timeout=5)
        return 1

    stop_event.set()
    logger_thread.join(timeout=5)

    return 0


if __name__ == "__main__":
    sys.exit(main())
