#!/usr/bin/env python
"""
查看处理进度
"""

from db.source_db import SourceDatabase
from db.target_db import TargetDatabase
from datetime import datetime

def main():
    source_db = SourceDatabase()
    source_db.connect()

    target_db = TargetDatabase()
    target_db.connect()

    # 统计
    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    total_needed = graduate + undergraduate

    completed_tasks = target_db.get_tasks_by_status('completed')
    failed_tasks = target_db.get_tasks_by_status('failed')
    pending_tasks = target_db.get_tasks_by_status('pending')
    crawling_tasks = target_db.get_tasks_by_status('crawling')

    completed = len(completed_tasks)
    failed = len(failed_tasks)
    pending = len(pending_tasks)
    crawling = len(crawling_tasks)

    # 计算预估时间 (假设每任务15分钟)
    remaining = total_needed - completed
    est_hours = remaining * 15 / 60
    est_days = est_hours / 24

    print()
    print("=" * 55)
    print(f"  处理进度报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)
    print()
    print(f"  {'总任务数:':<15} {total_needed:>8}")
    print(f"    {'- graduate:':<13} {graduate:>8}")
    print(f"    {'- undergraduate:':<13} {undergraduate:>8}")
    print()
    print(f"  {'已完成:':<15} {completed:>8}  ({completed/total_needed*100:.1f}%)")
    print(f"  {'已失败:':<15} {failed:>8}")
    print(f"  {'处理中:':<15} {crawling:>8}")
    print(f"  {'等待中:':<15} {pending:>8}")
    print(f"  {'剩余:':<15} {remaining:>8}")
    print()
    print("-" * 55)
    print(f"  预估剩余时间: {est_hours:.0f} 小时 ({est_days:.1f} 天)")
    print("-" * 55)

    # 显示最近完成的任务
    if completed_tasks:
        print()
        print("  最近完成的任务:")
        for task in completed_tasks[-5:]:
            school = task.school_name or "Unknown"
            print(f"    - task_id={task.id}, files={task.file_count}, {school[:20]}")

    # 显示失败的任务
    if failed_tasks:
        print()
        print("  失败的任务 (需要检查):")
        for task in failed_tasks[:5]:
            error = (task.error_message or "")[:40]
            print(f"    - task_id={task.id}: {error}...")

    print()
    print("=" * 55)

    source_db.close()
    target_db.close()


if __name__ == "__main__":
    main()
