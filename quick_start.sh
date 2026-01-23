#!/bin/bash
#
# 快速启动脚本 - 服务器重启后运行
# 重新安装系统依赖（Chrome、字体等）并激活环境
#
# 使用方法:
#   /workspace/pdfcollecter/quick_start.sh
#

echo "========================================================"
echo "  快速启动脚本（服务器重启后使用）"
echo "========================================================"

# 安装系统依赖
echo "[1/4] 安装系统依赖..."
apt update > /dev/null 2>&1
apt install -y fonts-noto-cjk libpq-dev > /dev/null 2>&1

# 安装 Chrome
echo "[2/4] 安装 Chrome..."
if ! command -v google-chrome &> /dev/null; then
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    dpkg -i google-chrome-stable_current_amd64.deb > /dev/null 2>&1 || apt --fix-broken install -y > /dev/null 2>&1
    rm -f google-chrome-stable_current_amd64.deb
fi

# 配置 PATH
echo "[3/4] 配置环境..."
export PATH="/workspace/.local/bin:/workspace/miniconda/bin:$PATH"

# 激活 conda
echo "[4/4] 激活 Conda 环境..."
source /workspace/miniconda/bin/activate overview

echo ""
echo "========================================================"
echo "  环境就绪!"
echo "========================================================"
echo ""
echo "  运行爬虫:   python run_crawler.py --chrome 4"
echo "  运行重命名: python run_renamer.py --docling 3"
echo "  查看进度:   python run_crawler.py --status"
echo ""
echo "  使用 tmux 后台运行:"
echo "    tmux new -s crawler"
echo "    python run_crawler.py --chrome 30"
echo "    # 按 Ctrl+B D 分离"
echo ""
echo "========================================================"

# 进入项目目录
cd /workspace/pdfcollecter

# 启动交互式 shell
exec bash
