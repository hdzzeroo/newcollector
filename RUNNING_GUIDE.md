# OverView V0.3 运行指南

## 项目概述

自动化大学招考信息采集系统，从 Railway 数据库获取链接，爬取网页内容，下载 PDF/DOC 文件，使用 LLM 进行智能重命名。

---

## 文件结构

```
OverView-V0.2/
├── main_v3.py              # 主程序入口
├── run_batch.py            # 批量处理脚本（推荐使用）
├── run_forever.sh          # 自动重启脚本
├── check_status.py         # 进度查看脚本
├── reprocess_pending.py    # 重处理失败文件
│
├── db/
│   ├── source_db.py        # Railway 源数据库连接
│   └── target_db.py        # Supabase 目标数据库连接
│
├── processor/
│   ├── llm_renamer.py      # LLM 重命名器
│   ├── pdf_processor.py    # PDF 文本提取
│   └── doc_processor.py    # DOC 文本提取
│
├── sync/
│   └── incremental_sync.py # 增量同步检测
│
├── storage/
│   ├── supabase_storage.py # Supabase Storage 操作
│   └── downloader.py       # 文件下载器
│
├── config/
│   └── create_tables.sql   # 数据库表结构
│
├── AIPmt/
│   └── Rename.txt          # LLM Prompt 模板
│
└── logs/                   # 运行日志目录
```

---

## 环境准备

### 1. 激活 Conda 环境
```bash
conda activate takumi
```

### 2. 确认数据库连接
```bash
# 测试源数据库连接
python -c "from db.source_db import SourceDatabase; db = SourceDatabase(); db.connect(); print('源数据库连接成功')"

# 测试目标数据库连接
python -c "from db.target_db import TargetDatabase; db = TargetDatabase(); db.connect(); print('目标数据库连接成功')"
```

### 3. 确认 LLM API
```bash
python -c "import Sdata; print(f'API Key: {Sdata.Dou_Bao_Key[:20]}...')"
```

---

## 运行方式

### 方式1: 批量处理（推荐）

```bash
# 基本运行
python run_batch.py

# 完整参数
python run_batch.py --batch-size 5 --workers 3 --rest-time 30

# 只处理 graduate 类型
python run_batch.py --batch-size 5 --workers 3 --type graduate
```

**参数说明：**
| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--batch-size, -b` | 5 | 每批处理任务数 |
| `--workers, -w` | 1 | LLM 并行线程数（建议3-5） |
| `--rest-time, -r` | 60 | 批次间休息秒数 |
| `--type, -t` | 全部 | 只处理 graduate 或 undergraduate |
| `--max-batches, -m` | 0 | 最大批次数（0=无限） |
| `--status, -s` | - | 只显示进度，不运行 |

### 方式2: 单次运行

```bash
# 处理单个任务
python main_v3.py --type graduate --max 1

# 使用并行 LLM
python main_v3.py --type graduate --max 1 --workers 3

# 完整参数
python main_v3.py --type graduate --max 10 --depth 1 --workers 3
```

### 方式3: 重处理失败文件

```bash
# 查看待处理文件
python reprocess_pending.py --dry-run

# 重处理所有待处理文件
python reprocess_pending.py --workers 3

# 重处理指定任务
python reprocess_pending.py --task 5 --workers 3
```

---

## 使用 tmux 后台运行

### 启动

```bash
# 1. 创建 tmux 会话
tmux new -s overview

# 2. 进入项目目录
cd /Users/huangdizhi/Desktop/projects/takumi/OverView-V0.2

# 3. 激活环境
conda activate takumi

# 4. 启动批量处理
python run_batch.py --batch-size 5 --workers 3 --rest-time 30

# 5. 离开会话（程序继续运行）
# 按 Ctrl+B，然后按 D
```

### 常用操作

| 操作 | 命令 |
|------|------|
| 重新连接会话 | `tmux attach -t overview` |
| 离开会话(保持运行) | `Ctrl+B` 然后 `D` |
| 列出所有会话 | `tmux ls` |
| 杀死会话 | `tmux kill-session -t overview` |
| 滚动查看历史 | `Ctrl+B` 然后 `[`，方向键滚动，`q` 退出 |

### 安全停止

```bash
# 重新连接
tmux attach -t overview

# 按 Ctrl+C 一次 → 当前任务完成后安全退出
# 再按一次 → 立即停止
```

---

## 查看进度

### 方式1: 快速查看
```bash
python run_batch.py --status
```

输出示例：
```
========== 处理进度 ==========
总任务数:   5834
已完成:     120
已失败:     2
剩余:       5714
完成率:     2.1%
==============================
```

### 方式2: 详细报告
```bash
python check_status.py
```

输出示例：
```
=======================================================
  处理进度报告 - 2025-01-22 15:30:00
=======================================================

  总任务数:            5834
    - graduate:        2701
    - undergraduate:   3133

  已完成:               120  (2.1%)
  已失败:                 2
  处理中:                 1
  剩余:                5714

-------------------------------------------------------
  预估剩余时间: 1428 小时 (59.5 天)
-------------------------------------------------------

  最近完成的任务:
    - task_id=15, files=8, 北海道大学
    - task_id=14, files=12, 東京大学
    - task_id=13, files=5, 京都大学
```

### 方式3: 查看实时日志
```bash
# 查看最新日志文件
tail -f logs/batch_*.log

# 或查看最近100行
tail -100 logs/batch_*.log
```

---

## 配置说明

### LLM 模型配置
文件: `processor/llm_renamer.py`
```python
# 当前使用 doubao-seed-1.6-lite（更快更便宜）
DEFAULT_MODEL = "doubao-seed-1-6-lite-251015"

# 如需切换回 1.5 pro 版本：
# DEFAULT_MODEL = "doubao-1-5-pro-32k-250115"
```

### 爬取深度配置
文件: `main_v3.py` 或命令行参数 `--depth`
```python
self.crawl_depth = 1  # 1 = 起始页 + 子页面
```

### Prompt 模板
文件: `AIPmt/Rename.txt`
- 包含命名规则和格式说明
- `{school_name}` 会被替换为确定的学校名称

---

## 数据流程

```
1. 增量检测
   Railway links表 → 检测新增/变更 → 创建 task

2. 网页爬取
   task URL → Chrome 爬取 → 保存节点到 crawl_nodes

3. 文件下载
   crawl_nodes (is_file=true) → 下载 → 上传 Supabase Storage

4. LLM 重命名
   下载文件 → 提取文本 → 调用豆包 API → 保存命名结果

5. 补充 Unknown
   同任务文件 → 统计已识别字段 → 填补 Unknown
```

---

## 故障排除

### 问题1: Chrome 崩溃
```bash
# 检查 Chrome 进程
ps aux | grep chrome

# 杀死残留进程
pkill -f chrome
```

### 问题2: 数据库连接超时
- 检查网络连接
- 重新运行脚本会自动重连

### 问题3: LLM API 报错
- 检查 API Key 是否有效
- 降低 `--workers` 数量避免限流
- 查看日志确认具体错误

### 问题4: 内存不足
- 减少 `--workers` 数量
- 减少 `--batch-size`
- 关闭其他应用

---

## 推荐运行配置

### 稳定运行（推荐）
```bash
python run_batch.py --batch-size 5 --workers 3 --rest-time 30
```

### 快速运行
```bash
python run_batch.py --batch-size 10 --workers 5 --rest-time 20
```

### 保守运行（机器性能较差）
```bash
python run_batch.py --batch-size 3 --workers 2 --rest-time 60
```

---

## 日志位置

| 日志类型 | 位置 |
|---------|------|
| 批量处理日志 | `logs/batch_YYYYMMDD_HHMMSS.log` |
| 自动重启日志 | `logs/forever_YYYYMMDD_HHMMSS.log` |

---

## 快速命令参考

```bash
# 查看进度
python run_batch.py --status
python check_status.py

# 启动处理
python run_batch.py --batch-size 5 --workers 3

# 连接 tmux
tmux attach -t overview

# 查看日志
tail -f logs/batch_*.log

# 重处理失败文件
python reprocess_pending.py --workers 3
```

---

*文档更新时间: 2025-01-22*
