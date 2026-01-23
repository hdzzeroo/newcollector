#!/usr/bin/env python
"""
批量处理脚本 - 稳健运行模式
自动处理所有任务，支持中断恢复、错误重试、进度监控
"""

import os
import sys
import time
import signal
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# 配置日志
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 全局变量用于优雅退出
should_stop = False

def signal_handler(signum, frame):
    global should_stop
    logger.warning("收到中断信号，将在当前任务完成后安全退出...")
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_progress():
    """获取当前进度"""
    from db.source_db import SourceDatabase
    from db.target_db import TargetDatabase

    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()

    # 统计
    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    total_needed = graduate + undergraduate

    completed = len(target_db.get_tasks_by_status('completed'))
    failed = len(target_db.get_tasks_by_status('failed'))

    source_db.close()
    target_db.close()

    return {
        'total': total_needed,
        'completed': completed,
        'failed': failed,
        'remaining': total_needed - completed
    }


def run_single_batch(batch_size: int = 5, link_type: str = None, workers: int = 1):
    """
    运行单个批次

    Args:
        batch_size: 每批处理数量
        link_type: 筛选类型 (graduate/undergraduate)
        workers: LLM 并行处理线程数

    Returns:
        成功处理的数量
    """
    from main_v3 import OverViewV3

    controller = OverViewV3()
    controller.crawl_depth = 1
    controller.enable_download = True
    controller.enable_rename = True
    controller.llm_workers = workers

    try:
        controller.run(link_type=link_type, max_tasks=batch_size)
        return batch_size
    except Exception as e:
        logger.error(f"批次处理出错: {e}")
        return 0
    finally:
        controller.cleanup()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量处理脚本')
    parser.add_argument('--batch-size', '-b', type=int, default=5,
                        help='每批处理任务数 (默认5)')
    parser.add_argument('--type', '-t', choices=['graduate', 'undergraduate'],
                        help='只处理指定类型')
    parser.add_argument('--max-batches', '-m', type=int, default=0,
                        help='最大批次数 (0=无限制)')
    parser.add_argument('--rest-time', '-r', type=int, default=60,
                        help='批次间休息时间(秒)')
    parser.add_argument('--workers', '-w', type=int, default=1,
                        help='LLM 并行处理线程数 (默认1，建议3-5)')
    parser.add_argument('--status', '-s', action='store_true',
                        help='只显示进度状态')

    args = parser.parse_args()

    # 只显示状态
    if args.status:
        progress = get_progress()
        print(f"""
========== 处理进度 ==========
总任务数:   {progress['total']}
已完成:     {progress['completed']}
已失败:     {progress['failed']}
剩余:       {progress['remaining']}
完成率:     {progress['completed']/progress['total']*100:.1f}%
==============================
        """)
        return

    logger.info("=" * 60)
    logger.info("批量处理启动")
    logger.info(f"配置: batch_size={args.batch_size}, type={args.type}, rest_time={args.rest_time}s, workers={args.workers}")
    logger.info("=" * 60)

    batch_count = 0
    total_processed = 0
    start_time = time.time()

    while not should_stop:
        # 检查是否达到最大批次
        if args.max_batches > 0 and batch_count >= args.max_batches:
            logger.info(f"已达到最大批次数 {args.max_batches}，停止")
            break

        # 获取进度
        progress = get_progress()
        if progress['remaining'] <= 0:
            logger.info("所有任务已完成!")
            break

        # 运行批次
        batch_count += 1
        logger.info(f"")
        logger.info(f"===== 批次 {batch_count} 开始 =====")
        logger.info(f"剩余任务: {progress['remaining']}")

        try:
            processed = run_single_batch(args.batch_size, args.type, args.workers)
            total_processed += processed
            logger.info(f"批次 {batch_count} 完成，本批处理: {processed}")
        except Exception as e:
            logger.error(f"批次 {batch_count} 出错: {e}")
            logger.info("等待60秒后重试...")
            time.sleep(60)
            continue

        # 显示统计
        elapsed = (time.time() - start_time) / 3600
        avg_per_hour = total_processed / elapsed if elapsed > 0 else 0
        logger.info(f"累计处理: {total_processed}, 运行时间: {elapsed:.1f}小时, 平均: {avg_per_hour:.1f}个/小时")

        # 批次间休息
        if not should_stop and progress['remaining'] > args.batch_size:
            logger.info(f"休息 {args.rest_time} 秒...")
            time.sleep(args.rest_time)

    # 最终统计
    elapsed = (time.time() - start_time) / 3600
    logger.info("")
    logger.info("=" * 60)
    logger.info("批量处理结束")
    logger.info(f"总批次: {batch_count}")
    logger.info(f"总处理: {total_processed}")
    logger.info(f"总耗时: {elapsed:.2f} 小时")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
