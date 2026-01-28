#!/bin/bash
#
# 爬虫自动重启脚本 - 程序崩溃后自动重启
# 使用方法: ./run_crawler_forever.sh [chrome进程数] [批次大小]
#
# 功能:
#   1. 自动重启崩溃的爬虫
#   2. 重启前重置卡住的任务
#   3. 清理僵尸进程
#   4. 记录详细日志
#

cd "$(dirname "$0")"

# ============ 配置 ============
CHROME_WORKERS=${1:-16}      # Chrome 并行进程数
BATCH_SIZE=${2:-50}          # 每批任务数
CRAWL_DEPTH=1                # 爬取深度
CRASH_WAIT=60                # 崩溃后等待时间(秒)
MAX_RESTARTS=100             # 最大重启次数（0=无限）
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/crawler_forever_$(date +%Y%m%d_%H%M%S).log"

# 创建日志目录
mkdir -p "$LOG_DIR"

# ============ 辅助函数 ============

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

cleanup_processes() {
    log "清理残留进程..."
    pkill -9 -f chromedriver 2>/dev/null
    pkill -9 -f "chrome --" 2>/dev/null
    # 等待进程完全退出
    sleep 3
}

reset_stuck_tasks() {
    log "检查并重置卡住的任务..."
    python3 << 'PYTHON_SCRIPT' 2>&1 | tee -a "$LOG_FILE"
import os
import sys
from dotenv import load_dotenv
load_dotenv()

try:
    from db.target_db import TargetDatabase
    from sqlalchemy import text

    db = TargetDatabase()
    db.connect()

    with db.engine.connect() as conn:
        # 查询卡住的任务
        result = conn.execute(text("SELECT COUNT(*) FROM crawl_tasks WHERE status = 'crawling'"))
        stuck_count = result.scalar()

        if stuck_count > 0:
            # 重置卡住的任务
            result = conn.execute(text("""
                UPDATE crawl_tasks
                SET status = 'pending', started_at = NULL
                WHERE status = 'crawling'
            """))
            conn.commit()
            print(f"已重置 {stuck_count} 个卡住的任务")
        else:
            print("没有卡住的任务")

    db.close()
except Exception as e:
    print(f"重置任务时出错: {e}")
PYTHON_SCRIPT
}

get_progress() {
    python3 << 'PYTHON_SCRIPT' 2>/dev/null
import os
from dotenv import load_dotenv
load_dotenv()

try:
    from db.source_db import SourceDatabase
    from db.target_db import TargetDatabase

    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()

    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    total = graduate + undergraduate

    completed = len(target_db.get_tasks_by_status('completed'))
    downloaded = len(target_db.get_tasks_by_status('downloaded'))

    remaining = total - completed - downloaded
    percent = (completed + downloaded) / total * 100 if total > 0 else 0

    print(f"TOTAL={total}")
    print(f"DONE={completed + downloaded}")
    print(f"REMAINING={remaining}")
    print(f"PERCENT={percent:.1f}")

    source_db.close()
    target_db.close()
except Exception as e:
    print(f"TOTAL=0")
    print(f"DONE=0")
    print(f"REMAINING=0")
    print(f"PERCENT=0")
PYTHON_SCRIPT
}

# ============ 主程序 ============

log "========================================"
log "爬虫自动重启脚本启动"
log "配置:"
log "  Chrome 进程数: $CHROME_WORKERS"
log "  批次大小: $BATCH_SIZE"
log "  爬取深度: $CRAWL_DEPTH"
log "  崩溃等待: ${CRASH_WAIT}秒"
log "  最大重启: $MAX_RESTARTS (0=无限)"
log "  日志文件: $LOG_FILE"
log "========================================"
log ""
log "按 Ctrl+C 可停止脚本"
log ""

# 运行计数
RUN_COUNT=0
START_TIME=$(date +%s)

# 捕获 SIGINT 信号
trap 'log "收到停止信号，正在退出..."; cleanup_processes; exit 0' SIGINT SIGTERM

while true; do
    RUN_COUNT=$((RUN_COUNT + 1))

    # 检查最大重启次数
    if [ "$MAX_RESTARTS" -gt 0 ] && [ "$RUN_COUNT" -gt "$MAX_RESTARTS" ]; then
        log "已达到最大重启次数 ($MAX_RESTARTS)，退出"
        break
    fi

    log ""
    log "======== 第 $RUN_COUNT 次运行 ========"

    # 获取进度
    eval $(get_progress)
    log "当前进度: $DONE/$TOTAL ($PERCENT%) | 剩余: $REMAINING"

    # 检查是否完成
    if [ "$REMAINING" -le 0 ]; then
        log "所有任务已完成!"
        break
    fi

    # 清理残留进程
    cleanup_processes

    # 重置卡住的任务
    reset_stuck_tasks

    log "启动爬虫..."
    log ""

    # 运行爬虫
    python run_crawler.py \
        --chrome "$CHROME_WORKERS" \
        --batch "$BATCH_SIZE" \
        --depth "$CRAWL_DEPTH" \
        2>&1 | tee -a "$LOG_FILE"

    EXIT_CODE=$?

    # 计算运行时间
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    ELAPSED_MIN=$((ELAPSED / 60))

    if [ $EXIT_CODE -eq 0 ]; then
        log "爬虫正常退出"

        # 获取最新进度
        eval $(get_progress)
        log "当前进度: $DONE/$TOTAL ($PERCENT%)"

        if [ "$REMAINING" -le 0 ]; then
            log "所有任务已完成!"
            break
        fi

        log "还有 $REMAINING 个任务，继续下一轮..."
    else
        log "爬虫异常退出 (code=$EXIT_CODE)"
        log "等待 $CRASH_WAIT 秒后重启..."
        sleep "$CRASH_WAIT"
    fi

    log "总运行时间: ${ELAPSED_MIN} 分钟"
done

# 清理
cleanup_processes

# 最终统计
CURRENT_TIME=$(date +%s)
ELAPSED=$((CURRENT_TIME - START_TIME))
ELAPSED_HOUR=$((ELAPSED / 3600))
ELAPSED_MIN=$(((ELAPSED % 3600) / 60))

log ""
log "========================================"
log "运行结束: $(date)"
log "总运行次数: $RUN_COUNT"
log "总运行时间: ${ELAPSED_HOUR}小时 ${ELAPSED_MIN}分钟"
eval $(get_progress)
log "最终进度: $DONE/$TOTAL ($PERCENT%)"
log "========================================"
