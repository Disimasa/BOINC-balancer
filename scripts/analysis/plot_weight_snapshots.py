#!/usr/bin/env python3
import argparse
import json
import os
import math
from pathlib import Path

import matplotlib.pyplot as plt

SERVER_DIR = Path(__file__).resolve().parent.parent.parent
SNAPSHOT_DIR = SERVER_DIR / "data" / "weights_snapshots"


def load_snapshot(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def find_latest_snapshot() -> Path | None:
    if not SNAPSHOT_DIR.exists():
        return None
    files = sorted(
        [p for p in SNAPSHOT_DIR.glob("pid_weights_*.json") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
    )
    return files[-1] if files else None


def compute_credit_shares(states, use_completed=False):
    app_names = set()
    for st in states:
        if use_completed:
            tc = st.get("completed_credits_by_app") or {}
        else:
            tc = st.get("total_credits_by_app") or {}
        app_names.update(tc.keys())
    sorted_apps = sorted(app_names)

    shares_by_app = {name: [] for name in sorted_apps}

    for st in states:
        if use_completed:
            tc = st.get("completed_credits_by_app") or {}
            total_sum = st.get("completed_credit_sum")
        else:
            tc = st.get("total_credits_by_app") or {}
            total_sum = st.get("total_credit_sum")
        if not tc or not total_sum or total_sum == 0:
            continue

        for name in sorted_apps:
            val = float(tc.get(name, 0.0))
            share = (val / float(total_sum)) * 100.0 if total_sum > 0 else 0.0
            shares_by_app[name].append(share)

    return sorted_apps, shares_by_app


def calculate_error_metrics(shares_by_app, max_iter=20, target_share=25.0):
    """Вычисляет метрики ошибки для последних max_iter итераций."""
    if not shares_by_app:
        return None
    
    # Берем последние max_iter итераций для каждого приложения
    last_shares = {}
    for app_name, shares in shares_by_app.items():
        last_shares[app_name] = shares[-max_iter:] if len(shares) >= max_iter else shares
    
    if not last_shares:
        return None
    
    # Находим максимальную длину (на случай, если у разных приложений разное количество итераций)
    max_len = max(len(shares) for shares in last_shares.values())
    if max_len == 0:
        return None
    
    # Вычисляем метрики для каждой итерации
    errors_by_iteration = []
    for i in range(max_len):
        iteration_errors = []
        for app_name in sorted(last_shares.keys()):
            shares = last_shares[app_name]
            if i < len(shares):
                error = abs(shares[i] - target_share)
                iteration_errors.append(error)
        
        if iteration_errors:
            # RMSE для этой итерации
            rmse = math.sqrt(sum(e**2 for e in iteration_errors) / len(iteration_errors))
            # MAE для этой итерации
            mae = sum(iteration_errors) / len(iteration_errors)
            # Максимальная ошибка для этой итерации
            max_err = max(iteration_errors)
            errors_by_iteration.append({
                'rmse': rmse,
                'mae': mae,
                'max_err': max_err
            })
    
    if not errors_by_iteration:
        return None
    
    # Средние метрики по всем итерациям
    avg_rmse = sum(e['rmse'] for e in errors_by_iteration) / len(errors_by_iteration)
    avg_mae = sum(e['mae'] for e in errors_by_iteration) / len(errors_by_iteration)
    avg_max_err = sum(e['max_err'] for e in errors_by_iteration) / len(errors_by_iteration)
    
    return {
        'avg_rmse': avg_rmse,
        'avg_mae': avg_mae,
        'avg_max_err': avg_max_err,
        'by_iteration': errors_by_iteration
    }


def plot_shares(sorted_apps, shares_by_app, title_suffix=""):
    if not sorted_apps:
        print("Нет данных для построения графиков (нет total_credits_by_app в снапшотах).")
        return

    num_apps = len(sorted_apps)
    cols = 2
    rows = (num_apps + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 4 * rows), squeeze=False)
    fig.suptitle(f"Доля кредита по приложениям{(' ' + title_suffix) if title_suffix else ''}")

    for idx, app_name in enumerate(sorted_apps):
        r = idx // cols
        c = idx % cols
        ax = axes[r][c]
        series = shares_by_app.get(app_name, [])
        if not series:
            ax.text(0.5, 0.5, "Нет данных", ha="center", va="center")
            ax.set_title(app_name)
            ax.set_xlabel("Итерация (номер снапшота)")
            ax.set_ylabel("Доля кредита, %")
            ax.set_ylim(0, 60)
            ax.axhline(25, linestyle="--", color="gray", linewidth=1)
            continue
        x = list(range(1, len(series) + 1))
        ax.plot(x, series, marker="o")
        ax.set_title(app_name)
        ax.set_xlabel("Итерация (перезапуск feeder)")
        ax.set_ylabel("Доля кредита, %")
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 60)
        ax.axhline(25, linestyle="--", color="gray", linewidth=1)

    for idx in range(len(sorted_apps), rows * cols):
        r = idx // cols
        c = idx % cols
        fig.delaxes(axes[r][c])

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Построение графиков доли кредита по приложениям из pid_weights_*.json"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Путь к конкретному файлу pid_weights_*.json. "
             "Если не указан, берётся последний файл из server/data/weights_snapshots.",
    )
    parser.add_argument(
        "--completed",
        action="store_true",
        help="Строить графики по completed_credits_by_app (по завершённым задачам), "
             "а не по total_credits_by_app.",
    )
    args = parser.parse_args()

    if args.file:
        snapshot_path = Path(args.file)
    else:
        snapshot_path = find_latest_snapshot()

    if not snapshot_path or not snapshot_path.exists():
        print("Не найден файл снапшота. Убедитесь, что в server/data/weights_snapshots есть pid_weights_*.json")
        return 1

    print(f"Используется файл снапшота: {snapshot_path}")
    data = load_snapshot(snapshot_path)
    states = data.get("states", [])

    if not states:
        print("В файле снапшота нет поля 'states' или список пуст.")
        return 1

    apps, shares_by_app = compute_credit_shares(states, use_completed=args.completed)
    if args.completed:
        title_suffix = f"(completed, {snapshot_path.name})"
    else:
        title_suffix = f"(total, {snapshot_path.name})"
    
    # Расчет метрик ошибки для последних 20 итераций
    metrics = calculate_error_metrics(shares_by_app, max_iter=20, target_share=25.0)
    if metrics:
        print("\n" + "="*80)
        print("МЕТРИКИ ОШИБКИ (последние 20 итераций)")
        print("="*80)
        print(f"Средний RMSE: {metrics['avg_rmse']:.2f}")
        print(f"Средний MAE:   {metrics['avg_mae']:.2f}")
        print(f"Средняя максимальная ошибка: {metrics['avg_max_err']:.2f}")
        print("="*80 + "\n")
    
    plot_shares(apps, shares_by_app, title_suffix=title_suffix)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


