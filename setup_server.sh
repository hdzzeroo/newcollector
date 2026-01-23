#!/bin/bash
#
# 一键服务器配置脚本
# 适用于 RunPod 等 /workspace 持久化的云服务器
#
# 使用方法:
#   curl -sSL https://raw.githubusercontent.com/hdzzeroo/pdfcollecter/main/setup_server.sh | bash
#
# 或者:
#   wget -qO- https://raw.githubusercontent.com/hdzzeroo/pdfcollecter/main/setup_server.sh | bash
#

set -e

echo "========================================================"
echo "  PDF Collecter 服务器一键配置脚本"
echo "========================================================"
echo ""

# ============================================================
# 配置
# ============================================================

WORKSPACE="/workspace"
PROJECT_NAME="pdfcollecter"
PROJECT_DIR="$WORKSPACE/$PROJECT_NAME"
MINICONDA_DIR="$WORKSPACE/miniconda"
LOCAL_BIN="$WORKSPACE/.local/bin"
CONDA_ENV="overview"
GITHUB_REPO="https://github.com/hdzzeroo/pdfcollecter.git"

# ============================================================
# 函数
# ============================================================

log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

check_workspace() {
    if [ ! -d "$WORKSPACE" ]; then
        echo "错误: $WORKSPACE 目录不存在"
        echo "请确保在支持持久化存储的云服务器上运行此脚本"
        exit 1
    fi
}

# ============================================================
# 1. 安装系统依赖
# ============================================================

install_system_deps() {
    log "安装系统依赖..."

    apt update
    apt install -y \
        wget curl git \
        fonts-noto-cjk \
        libpq-dev \
        build-essential \
        libevent-dev \
        libncurses-dev \
        bison

    log "系统依赖安装完成"
}

# ============================================================
# 2. 安装 Chrome
# ============================================================

install_chrome() {
    log "安装 Google Chrome..."

    if command -v google-chrome &> /dev/null; then
        log "Chrome 已安装: $(google-chrome --version)"
        return
    fi

    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    dpkg -i google-chrome-stable_current_amd64.deb || apt --fix-broken install -y
    rm -f google-chrome-stable_current_amd64.deb

    log "Chrome 安装完成: $(google-chrome --version)"
}

# ============================================================
# 3. 安装 Miniconda
# ============================================================

install_miniconda() {
    log "安装 Miniconda..."

    if [ -f "$MINICONDA_DIR/bin/conda" ]; then
        log "Miniconda 已安装"
        return
    fi

    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$MINICONDA_DIR"
    rm -f /tmp/miniconda.sh

    # 初始化
    "$MINICONDA_DIR/bin/conda" init bash

    log "Miniconda 安装完成"
}

# ============================================================
# 4. 创建 Conda 环境
# ============================================================

setup_conda_env() {
    log "配置 Conda 环境..."

    export PATH="$MINICONDA_DIR/bin:$PATH"

    # 检查环境是否存在
    if conda env list | grep -q "^$CONDA_ENV "; then
        log "环境 $CONDA_ENV 已存在"
    else
        log "创建环境 $CONDA_ENV..."
        conda create -n "$CONDA_ENV" python=3.10 -y
    fi

    log "Conda 环境配置完成"
}

# ============================================================
# 5. 克隆/更新项目
# ============================================================

setup_project() {
    log "配置项目..."

    if [ -d "$PROJECT_DIR" ]; then
        log "项目已存在，更新中..."
        cd "$PROJECT_DIR"
        git pull origin main
    else
        log "克隆项目..."
        git clone "$GITHUB_REPO" "$PROJECT_DIR"
        cd "$PROJECT_DIR"
    fi

    log "项目配置完成"
}

# ============================================================
# 6. 安装 Python 依赖
# ============================================================

install_python_deps() {
    log "安装 Python 依赖..."

    export PATH="$MINICONDA_DIR/bin:$PATH"
    source "$MINICONDA_DIR/bin/activate" "$CONDA_ENV"

    cd "$PROJECT_DIR"
    pip install --upgrade pip
    pip install -r requirements.txt

    log "Python 依赖安装完成"
}

# ============================================================
# 7. 安装 tmux
# ============================================================

install_tmux() {
    log "安装 tmux..."

    mkdir -p "$LOCAL_BIN"

    if [ -f "$LOCAL_BIN/tmux" ]; then
        log "tmux 已安装"
        return
    fi

    cd /tmp
    wget -q https://github.com/tmux/tmux/releases/download/3.4/tmux-3.4.tar.gz
    tar -xzf tmux-3.4.tar.gz
    cd tmux-3.4
    ./configure --prefix="$WORKSPACE/.local"
    make -j$(nproc)
    make install
    cd /tmp
    rm -rf tmux-3.4 tmux-3.4.tar.gz

    log "tmux 安装完成"
}

# ============================================================
# 8. 配置环境变量
# ============================================================

setup_bashrc() {
    log "配置环境变量..."

    # 添加到 .bashrc
    cat >> ~/.bashrc << 'BASHRC_END'

# === PDF Collecter 环境配置 ===
export PATH="/workspace/.local/bin:/workspace/miniconda/bin:$PATH"

# 快捷命令
alias activate-overview='source /workspace/miniconda/bin/activate overview'
alias cd-project='cd /workspace/pdfcollecter'
alias run-crawler='cd /workspace/pdfcollecter && source /workspace/miniconda/bin/activate overview && python run_crawler.py'
alias run-renamer='cd /workspace/pdfcollecter && source /workspace/miniconda/bin/activate overview && python run_renamer.py'
alias status='cd /workspace/pdfcollecter && source /workspace/miniconda/bin/activate overview && python run_crawler.py --status'

# 自动激活环境
if [ -f "/workspace/miniconda/bin/activate" ]; then
    source /workspace/miniconda/bin/activate overview 2>/dev/null
fi
# === END ===
BASHRC_END

    log "环境变量配置完成"
}

# ============================================================
# 9. 创建目录
# ============================================================

setup_dirs() {
    log "创建必要目录..."

    cd "$PROJECT_DIR"
    mkdir -p MemMD logs temp_downloads _debug

    log "目录创建完成"
}

# ============================================================
# 10. 配置 .env 文件
# ============================================================

setup_env_file() {
    log "检查 .env 文件..."

    cd "$PROJECT_DIR"

    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            cp .env.example .env
            log "已创建 .env 文件，请编辑填入配置"
        else
            cat > .env << 'ENV_END'
# === 数据库配置 ===
SOURCE_DB_URL=postgresql://user:password@host:port/database
TARGET_DB_URL=postgresql://user:password@host:port/database

# === Supabase 配置 ===
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
SUPABASE_BUCKET=university-files

# === LLM API 配置 ===
DOUBAO_API_KEY=your-doubao-api-key
ENV_END
            log "已创建 .env 模板，请编辑填入配置"
        fi
    else
        log ".env 文件已存在"
    fi
}

# ============================================================
# 11. 测试安装
# ============================================================

test_installation() {
    log "测试安装..."

    export PATH="$LOCAL_BIN:$MINICONDA_DIR/bin:$PATH"
    source "$MINICONDA_DIR/bin/activate" "$CONDA_ENV"

    # 测试 Chrome
    echo -n "  Chrome: "
    if google-chrome --version &> /dev/null; then
        echo "OK"
    else
        echo "FAILED"
    fi

    # 测试 Python
    echo -n "  Python: "
    python --version

    # 测试 Selenium
    echo -n "  Selenium: "
    if python -c "from selenium import webdriver; print('OK')" 2>/dev/null; then
        :
    else
        echo "FAILED"
    fi

    # 测试 tmux
    echo -n "  tmux: "
    if "$LOCAL_BIN/tmux" -V &> /dev/null; then
        "$LOCAL_BIN/tmux" -V
    else
        echo "FAILED"
    fi
}

# ============================================================
# 主程序
# ============================================================

main() {
    check_workspace

    echo ""
    log "开始配置..."
    echo ""

    install_system_deps
    install_chrome
    install_miniconda
    setup_conda_env
    setup_project
    install_python_deps
    install_tmux
    setup_bashrc
    setup_dirs
    setup_env_file

    echo ""
    log "=========================================="
    log "配置完成!"
    log "=========================================="
    echo ""

    test_installation

    echo ""
    echo "========================================================"
    echo "  下一步操作"
    echo "========================================================"
    echo ""
    echo "  1. 编辑 .env 文件填入配置:"
    echo "     nano $PROJECT_DIR/.env"
    echo ""
    echo "  2. 重新加载环境:"
    echo "     source ~/.bashrc"
    echo ""
    echo "  3. 运行爬虫:"
    echo "     run-crawler --chrome 4 --batch 20"
    echo ""
    echo "  4. 运行重命名:"
    echo "     run-renamer --docling 3 --llm 25"
    echo ""
    echo "  5. 后台运行 (使用 tmux):"
    echo "     tmux new -s crawler"
    echo "     run-crawler --chrome 30"
    echo "     # 按 Ctrl+B D 分离"
    echo ""
    echo "  快捷命令:"
    echo "     activate-overview  - 激活环境"
    echo "     cd-project         - 进入项目目录"
    echo "     run-crawler        - 运行爬虫"
    echo "     run-renamer        - 运行重命名"
    echo "     status             - 查看进度"
    echo ""
    echo "========================================================"
}

# 运行
main "$@"
