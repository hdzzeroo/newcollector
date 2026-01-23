#!/bin/bash
#
# Pipeline 自动重启脚本 - 崩溃后自动重启
# 使用方法: ./run_pipeline_forever.sh
#
# 可通过环境变量自定义配置:
#   CHROME_WORKERS=4 DOCLING_WORKERS=3 ./run_pipeline_forever.sh
#

cd "$(dirname "$0")"

# ============================================================
# 配置（可通过环境变量覆盖）
# ============================================================
AUTO_MODE=${AUTO_MODE:-true}             # 是否使用自动资源检测模式
CHROME_WORKERS=${CHROME_WORKERS:-}       # Chrome 爬虫进程数（空=自动）
DOCLING_WORKERS=${DOCLING_WORKERS:-}     # Docling GPU 进程数（空=自动）
LLM_WORKERS=${LLM_WORKERS:-}             # LLM 并发线程数（空=自动）
BATCH_SIZE=${BATCH_SIZE:-}               # 每批任务数（空=自动）
CRAWL_DEPTH=${CRAWL_DEPTH:-1}            # 爬取深度
REST_TIME=${REST_TIME:-30}               # 批次间休息时间(秒)
CRASH_WAIT=${CRASH_WAIT:-120}            # 崩溃后等待时间(秒)

# Conda 环境名
CONDA_ENV=${CONDA_ENV:-overview}

# 日志文件
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pipeline_forever_$(date +%Y%m%d_%H%M%S).log"

# ============================================================
# 函数
# ============================================================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

get_remaining_tasks() {
    python -c "
from db.source_db import SourceDatabase
from db.target_db import TargetDatabase
try:
    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()
    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    completed = len(target_db.get_tasks_by_status('completed'))
    print(graduate + undergraduate - completed)
    source_db.close()
    target_db.close()
except Exception as e:
    print(-1)
" 2>/dev/null
}

show_progress() {
    python -c "
from db.source_db import SourceDatabase
from db.target_db import TargetDatabase
try:
    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()
    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    total = graduate + undergraduate
    completed = len(target_db.get_tasks_by_status('completed'))
    failed = len(target_db.get_tasks_by_status('failed'))
    processing = len(target_db.get_tasks_by_status('processing'))
    print(f'总任务: {total}, 已完成: {completed}, 处理中: {processing}, 失败: {failed}, 剩余: {total-completed}')
    source_db.close()
    target_db.close()
except Exception as e:
    print(f'获取进度失败: {e}')
" 2>/dev/null
}

# ============================================================
# 主程序
# ============================================================

echo "========================================================" | tee -a "$LOG_FILE"
echo "  Pipeline 自动运行脚本" | tee -a "$LOG_FILE"
echo "========================================================" | tee -a "$LOG_FILE"
log "启动时间: $(date)"
log "配置:"
log "  Auto Mode:       $AUTO_MODE"
log "  Chrome Workers:  ${CHROME_WORKERS:-自动检测}"
log "  Docling Workers: ${DOCLING_WORKERS:-自动检测}"
log "  LLM Workers:     ${LLM_WORKERS:-自动检测}"
log "  Batch Size:      ${BATCH_SIZE:-自动检测}"
log "  Crawl Depth:     $CRAWL_DEPTH"
log "  Rest Time:       ${REST_TIME}s"
log "  Crash Wait:      ${CRASH_WAIT}s"
log "  Conda Env:       $CONDA_ENV"
log "  Log File:        $LOG_FILE"
echo "========================================================" | tee -a "$LOG_FILE"
log "按 Ctrl+C 停止（会等待当前任务完成）"
echo "========================================================" | tee -a "$LOG_FILE"

# 激活 conda
if [ -f "/workspace/miniconda/bin/activate" ]; then
    source /workspace/miniconda/bin/activate "$CONDA_ENV"
elif command -v conda &> /dev/null; then
    eval "$(conda shell.bash hook)"
    conda activate "$CONDA_ENV"
fi

# 显示初始进度
log "当前进度: $(show_progress)"

# 运行计数
RUN_COUNT=0
TOTAL_START_TIME=$(date +%s)

while true; do
    RUN_COUNT=$((RUN_COUNT + 1))

    echo "" | tee -a "$LOG_FILE"
    log "===== 第 $RUN_COUNT 次运行开始 ====="

    # 构建运行命令
    CMD="python run_pipeline.py --depth $CRAWL_DEPTH --rest $REST_TIME"

    # 如果是自动模式且没有指定参数，使用 --auto
    if [ "$AUTO_MODE" = "true" ] && [ -z "$CHROME_WORKERS" ] && [ -z "$DOCLING_WORKERS" ]; then
        CMD="$CMD --auto"
        log "使用自动资源检测模式"
    else
        # 手动模式，添加指定的参数
        [ -n "$CHROME_WORKERS" ] && CMD="$CMD --chrome $CHROME_WORKERS"
        [ -n "$DOCLING_WORKERS" ] && CMD="$CMD --docling $DOCLING_WORKERS"
        [ -n "$LLM_WORKERS" ] && CMD="$CMD --llm $LLM_WORKERS"
        [ -n "$BATCH_SIZE" ] && CMD="$CMD --batch $BATCH_SIZE"
    fi

    log "执行命令: $CMD"

    # 运行 Pipeline
    eval "$CMD" 2>&1 | tee -a "$LOG_FILE"

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        log "Pipeline 正常退出"

        # 检查剩余任务
        REMAINING=$(get_remaining_tasks)

        if [ "$REMAINING" = "-1" ]; then
            log "无法获取剩余任务数，等待 ${CRASH_WAIT}s 后重试..."
            sleep "$CRASH_WAIT"
            continue
        fi

        if [ "$REMAINING" -le 0 ]; then
            log "所有任务已完成!"
            break
        fi

        log "还有 $REMAINING 个任务，继续处理..."
        log "当前进度: $(show_progress)"

    else
        log "Pipeline 异常退出 (exit_code=$EXIT_CODE)"
        log "等待 ${CRASH_WAIT}s 后自动重启..."
        sleep "$CRASH_WAIT"
    fi
done

# ============================================================
# 结束统计
# ============================================================

TOTAL_END_TIME=$(date +%s)
TOTAL_DURATION=$((TOTAL_END_TIME - TOTAL_START_TIME))
HOURS=$((TOTAL_DURATION / 3600))
MINUTES=$(((TOTAL_DURATION % 3600) / 60))
SECONDS=$((TOTAL_DURATION % 60))

echo "" | tee -a "$LOG_FILE"
echo "========================================================" | tee -a "$LOG_FILE"
log "运行结束!"
log "总运行次数: $RUN_COUNT"
log "总耗时: ${HOURS}小时 ${MINUTES}分钟 ${SECONDS}秒"
log "最终进度: $(show_progress)"
echo "========================================================" | tee -a "$LOG_FILE"
