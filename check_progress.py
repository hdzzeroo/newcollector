#!/usr/bin/env python
"""
进度查看脚本 - 可在 tmux 外运行
用法: python check_progress.py [--watch]
"""

import os
import sys
import time
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_progress():
    """获取爬虫进度"""
    from db.source_db import SourceDatabase
    from db.target_db import TargetDatabase

    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()

    # 源数据统计
    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    total = graduate + undergraduate

    # 目标数据统计
    completed = len(target_db.get_tasks_by_status('completed'))
    downloaded = len(target_db.get_tasks_by_status('downloaded'))
    crawling = len(target_db.get_tasks_by_status('crawling'))
    failed = len(target_db.get_tasks_by_status('failed'))
    pending_count = total - completed - downloaded - crawling - failed

    # 文件统计
    try:
        from sqlalchemy import text
        with target_db.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM crawl_files WHERE download_status = 'downloaded'"))
            files_downloaded = result.scalar() or 0
            result = conn.execute(text("SELECT COUNT(*) FROM crawl_files WHERE process_status = 'completed'"))
            files_renamed = result.scalar() or 0
    except:
        files_downloaded = 0
        files_renamed = 0

    source_db.close()
    target_db.close()

    return {
        'total': total,
        'graduate': graduate,
        'undergraduate': undergraduate,
        'completed': completed,
        'downloaded': downloaded,
        'crawling': crawling,
        'failed': failed,
        'pending': pending_count,
        'files_downloaded': files_downloaded,
        'files_renamed': files_renamed
    }


def check_tmux_session():
    """检查 tmux session 状态"""
    try:
        result = subprocess.run(
            ['tmux', 'has-session', '-t', 'crawler'],
            capture_output=True
        )
        return result.returncode == 0
    except:
        return False


def get_recent_logs(lines=5):
    """获取最近的日志"""
    log_file = "logs/crawler_latest.log"
    if not os.path.exists(log_file):
        return []

    try:
        result = subprocess.run(
            ['tail', '-n', str(lines), log_file],
            capture_output=True,
            text=True
        )
        return result.stdout.strip().split('\n') if result.stdout else []
    except:
        return []


def print_progress(progress, is_running):
    """打印进度"""
    # 清屏（仅 watch 模式）
    total = progress['total']
    done = progress['completed'] + progress['downloaded']
    percent = (done / total * 100) if total > 0 else 0

    # 进度条
    bar_width = 40
    filled = int(bar_width * done / total) if total > 0 else 0
    bar = '█' * filled + '░' * (bar_width - filled)

    status_icon = "🟢 运行中" if is_running else "⚪ 已停止"

    print(f"""
╔══════════════════════════════════════════════════════════╗
║              爬虫进度监控  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
╠══════════════════════════════════════════════════════════╣
║  状态: {status_icon}
╠══════════════════════════════════════════════════════════╣
║  总进度: [{bar}] {percent:5.1f}%
║
║  ┌─ 任务统计 ─────────────────────────────────────────┐
║  │  总任务:     {progress['total']:>6}  (研究生: {progress['graduate']}, 本科: {progress['undergraduate']})
║  │  待处理:     {progress['pending']:>6}
║  │  正在爬取:   {progress['crawling']:>6}
║  │  已下载:     {progress['downloaded']:>6}  (等待重命名)
║  │  已完成:     {progress['completed']:>6}
║  │  失败:       {progress['failed']:>6}
║  └────────────────────────────────────────────────────┘
║
║  ┌─ 文件统计 ─────────────────────────────────────────┐
║  │  已下载文件: {progress['files_downloaded']:>6}
║  │  已重命名:   {progress['files_renamed']:>6}
║  └────────────────────────────────────────────────────┘
╚══════════════════════════════════════════════════════════╝
""")

    # 最近日志
    logs = get_recent_logs(5)
    if logs:
        print("最近日志:")
        print("-" * 60)
        for log in logs:
            # 截断过长的日志
            if len(log) > 80:
                log = log[:77] + "..."
            print(f"  {log}")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='查看爬虫进度')
    parser.add_argument('--watch', '-w', action='store_true', help='持续监控（每10秒刷新）')
    parser.add_argument('--interval', '-i', type=int, default=10, help='刷新间隔（秒）')
    args = parser.parse_args()

    if args.watch:
        print("持续监控模式，按 Ctrl+C 退出...")
        try:
            while True:
                os.system('clear')
                progress = get_progress()
                is_running = check_tmux_session()
                print_progress(progress, is_running)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n退出监控")
    else:
        progress = get_progress()
        is_running = check_tmux_session()
        print_progress(progress, is_running)


if __name__ == "__main__":
    main()
