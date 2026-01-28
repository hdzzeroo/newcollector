#!/bin/bash
# 启动爬虫（在 tmux 中运行，带自动重启）
# 用法: ./start_crawler.sh [chrome进程数] [批次大小]

CHROME_WORKERS=${1:-16}
BATCH_SIZE=${2:-50}
SESSION_NAME="crawler"

cd "$(dirname "$0")"

# 检查是否已有 session 在运行
if tmux has-session -t $SESSION_NAME 2>/dev/null; then
    echo "爬虫已在运行中!"
    echo ""
    echo "查看进度:  python check_progress.py"
    echo "实时监控:  python check_progress.py --watch"
    echo "查看日志:  tmux attach -t $SESSION_NAME"
    echo "停止爬虫:  tmux kill-session -t $SESSION_NAME"
    exit 1
fi

# 清理可能残留的进程
echo "清理残留进程..."
pkill -9 -f chromedriver 2>/dev/null
pkill -9 -f "chrome --" 2>/dev/null
sleep 2

echo "启动爬虫（带自动重启）..."
echo "  Chrome 进程数: $CHROME_WORKERS"
echo "  批次大小: $BATCH_SIZE"
echo ""

# 创建 tmux session 并启动自动重启脚本
tmux new-session -d -s $SESSION_NAME \
    "cd /workspace/newcollector && ./run_crawler_forever.sh $CHROME_WORKERS $BATCH_SIZE"

sleep 2

if tmux has-session -t $SESSION_NAME 2>/dev/null; then
    echo "爬虫已启动（带自动重启机制）!"
    echo ""
    echo "=========================================="
    echo "  查看进度:  python check_progress.py"
    echo "  实时监控:  python check_progress.py --watch"
    echo "  进入终端:  tmux attach -t $SESSION_NAME"
    echo "  退出终端:  按 Ctrl+B 然后按 D"
    echo "  停止爬虫:  tmux kill-session -t $SESSION_NAME"
    echo "=========================================="
else
    echo "启动失败，请检查错误"
    exit 1
fi
