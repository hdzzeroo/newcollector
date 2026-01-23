#!/bin/bash
#
# 自动重启脚本 - 程序崩溃后自动重启
# 使用方法: ./run_forever.sh
#

cd "$(dirname "$0")"

# 配置
BATCH_SIZE=5        # 每批处理数量
REST_TIME=60        # 批次间休息时间(秒)
CRASH_WAIT=120      # 崩溃后等待时间(秒)
WORKERS=5           # LLM 并行线程数
LOG_FILE="logs/forever_$(date +%Y%m%d_%H%M%S).log"

# 创建日志目录
mkdir -p logs

echo "========================================" | tee -a "$LOG_FILE"
echo "自动运行脚本启动: $(date)" | tee -a "$LOG_FILE"
echo "配置: BATCH_SIZE=$BATCH_SIZE, REST_TIME=$REST_TIME, WORKERS=$WORKERS" | tee -a "$LOG_FILE"
echo "日志文件: $LOG_FILE" | tee -a "$LOG_FILE"
echo "按 Ctrl+C 两次可完全停止" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# 运行计数
RUN_COUNT=0

while true; do
    RUN_COUNT=$((RUN_COUNT + 1))
    echo "" | tee -a "$LOG_FILE"
    echo "[$(date)] 第 $RUN_COUNT 次运行开始" | tee -a "$LOG_FILE"

    # 激活 conda 环境并运行
    eval "$(conda shell.bash hook)"
    conda activate takumi

    python run_batch.py --batch-size $BATCH_SIZE --rest-time $REST_TIME --workers $WORKERS 2>&1 | tee -a "$LOG_FILE"

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date)] 正常完成，检查是否还有剩余任务..." | tee -a "$LOG_FILE"

        # 检查是否还有剩余任务
        REMAINING=$(python -c "
from db.source_db import SourceDatabase
from db.target_db import TargetDatabase
source_db = SourceDatabase()
source_db.connect()
target_db = TargetDatabase()
target_db.connect()
graduate = source_db.get_count_by_type('graduate')
undergraduate = source_db.get_count_by_type('undergraduate')
completed = len(target_db.get_tasks_by_status('completed'))
print(graduate + undergraduate - completed)
" 2>/dev/null)

        if [ "$REMAINING" -le 0 ]; then
            echo "[$(date)] 所有任务已完成!" | tee -a "$LOG_FILE"
            break
        fi
        echo "[$(date)] 还有 $REMAINING 个任务，继续..." | tee -a "$LOG_FILE"
    else
        echo "[$(date)] 程序异常退出 (code=$EXIT_CODE)，等待 $CRASH_WAIT 秒后重启..." | tee -a "$LOG_FILE"
        sleep $CRASH_WAIT
    fi
done

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "运行结束: $(date)" | tee -a "$LOG_FILE"
echo "总运行次数: $RUN_COUNT" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
