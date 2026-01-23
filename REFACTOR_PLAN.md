# OverView V0.3 项目重构方案

## 一、项目概述

### 1.1 重构目标

将 OverView 从本地文件驱动的爬虫系统，升级为**云端数据库驱动的自动化数据采集与文件处理系统**。

### 1.2 核心变更

| 模块 | V0.2 (当前) | V0.3 (目标) |
|------|-------------|-------------|
| 输入源 | 本地 CSV 文件 | Railway PostgreSQL |
| 输出存储 | 本地文件夹 | Supabase PostgreSQL |
| 文件存储 | 本地磁盘 | Supabase Storage |
| 文件处理 | 无 | LLM 智能重命名 |

### 1.3 系统架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              OverView V0.3                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │
│   │   Railway   │         │   OverView  │         │  Supabase   │          │
│   │ PostgreSQL  │────────▶│    Engine   │────────▶│ PostgreSQL  │          │
│   │  (输入源)    │         │   (爬取器)   │         │  (输出库)    │          │
│   └─────────────┘         └──────┬──────┘         └─────────────┘          │
│                                  │                                          │
│                                  │ 下载文件                                  │
│                                  ▼                                          │
│                           ┌─────────────┐         ┌─────────────┐          │
│                           │   文件处理   │────────▶│  Supabase   │          │
│                           │  (LLM重命名) │         │  Storage    │          │
│                           └─────────────┘         └─────────────┘          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、数据库设计

### 2.1 输入数据库 (Railway PostgreSQL)

**连接信息**：
```
postgresql://postgres:bwNYWrIfZgkGxdfmKObdhZjJPXVuDGAi@maglev.proxy.rlwy.net:43262/railway
```

**现有表 `links`** (只读，不修改)：

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| table_name | VARCHAR | 类型 (undergraduate/graduate/vocational) |
| row_id | INTEGER | 关联ID |
| url | TEXT | 目标URL |
| user_id | INTEGER | 用户ID |
| created_at | TIMESTAMP | 创建时间 |
| has_guideline | BOOLEAN | 是否有募集要项 |
| has_past_exam | BOOLEAN | 是否有过去问 |
| has_result | BOOLEAN | 是否有结果 |
| has_material_check | BOOLEAN | 是否有材料检查 |
| has_pdf | BOOLEAN | 是否有PDF |
| is_page_info | BOOLEAN | 是否为页面信息 |

---

### 2.2 输出数据库 (Supabase PostgreSQL)

**连接信息**：
```
postgresql://postgres:bnly4zU3k4pRmerH@db.orqthdhhyqtksrtxweoc.supabase.co:5432/postgres
```

#### 表1: `crawl_tasks` (爬取任务状态)

```sql
CREATE TABLE crawl_tasks (
    id              SERIAL PRIMARY KEY,
    source_link_id  INTEGER NOT NULL,          -- 对应 Railway.links.id
    source_url      TEXT NOT NULL,             -- 原始URL
    url_hash        VARCHAR(64),               -- URL的MD5哈希 (变更检测用)
    status          VARCHAR(20) DEFAULT 'pending',  -- pending/crawling/completed/failed
    node_count      INTEGER DEFAULT 0,         -- 爬取到的节点数
    pruned_count    INTEGER DEFAULT 0,         -- 剪枝后保留的节点数
    file_count      INTEGER DEFAULT 0,         -- 下载的文件数
    error_message   TEXT,                      -- 错误信息
    started_at      TIMESTAMP,                 -- 开始时间
    completed_at    TIMESTAMP,                 -- 完成时间
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),

    UNIQUE(source_link_id)
);

CREATE INDEX idx_crawl_tasks_status ON crawl_tasks(status);
CREATE INDEX idx_crawl_tasks_source ON crawl_tasks(source_link_id);
```

#### 表2: `crawl_nodes` (爬取节点数据)

```sql
CREATE TABLE crawl_nodes (
    id              SERIAL PRIMARY KEY,
    task_id         INTEGER REFERENCES crawl_tasks(id) ON DELETE CASCADE,
    node_index      INTEGER NOT NULL,          -- 节点索引
    father_index    INTEGER,                   -- 父节点索引
    depth           INTEGER,                   -- 深度
    title           TEXT,                      -- 标题
    breadcrumb      TEXT,                      -- 面包屑路径
    url             TEXT NOT NULL,             -- 完整URL
    father_title    TEXT,                      -- 父节点标题
    is_pruned       BOOLEAN DEFAULT FALSE,     -- 是否通过剪枝保留
    is_file         BOOLEAN DEFAULT FALSE,     -- 是否为文件链接
    file_extension  VARCHAR(10),               -- 文件扩展名 (pdf/doc/xls等)
    created_at      TIMESTAMP DEFAULT NOW(),

    UNIQUE(task_id, node_index)
);

CREATE INDEX idx_crawl_nodes_task ON crawl_nodes(task_id);
CREATE INDEX idx_crawl_nodes_pruned ON crawl_nodes(is_pruned);
CREATE INDEX idx_crawl_nodes_file ON crawl_nodes(is_file);
```

#### 表3: `crawl_files` (下载文件记录)

```sql
CREATE TABLE crawl_files (
    id              SERIAL PRIMARY KEY,
    task_id         INTEGER REFERENCES crawl_tasks(id) ON DELETE CASCADE,
    node_id         INTEGER REFERENCES crawl_nodes(id) ON DELETE CASCADE,
    original_url    TEXT NOT NULL,             -- 原始下载URL
    original_name   TEXT,                      -- 原始文件名
    renamed_name    TEXT,                      -- LLM重命名后的文件名
    file_extension  VARCHAR(10),               -- 扩展名
    file_size       BIGINT,                    -- 文件大小(bytes)
    storage_path    TEXT,                      -- Supabase Storage 路径
    storage_bucket  VARCHAR(50),               -- Storage Bucket名称

    -- LLM处理相关
    llm_processed   BOOLEAN DEFAULT FALSE,     -- 是否已LLM处理
    llm_model       VARCHAR(50),               -- 使用的LLM模型
    llm_confidence  FLOAT,                     -- LLM置信度
    llm_raw_response TEXT,                     -- LLM原始响应

    -- 状态
    download_status VARCHAR(20) DEFAULT 'pending',  -- pending/downloading/completed/failed
    process_status  VARCHAR(20) DEFAULT 'pending',  -- pending/processing/completed/failed
    error_message   TEXT,

    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_crawl_files_task ON crawl_files(task_id);
CREATE INDEX idx_crawl_files_download ON crawl_files(download_status);
CREATE INDEX idx_crawl_files_process ON crawl_files(process_status);
```

#### 表4: `sync_log` (同步日志)

```sql
CREATE TABLE sync_log (
    id              SERIAL PRIMARY KEY,
    sync_type       VARCHAR(20),               -- full/incremental
    source_count    INTEGER,                   -- 源数据总数
    new_count       INTEGER,                   -- 新增数量
    changed_count   INTEGER,                   -- 变更数量
    synced_at       TIMESTAMP DEFAULT NOW()
);
```

---

### 2.3 Supabase Storage 结构

```
Bucket: university-files
├── {task_id}/
│   ├── raw/                    # 原始下载文件
│   │   ├── file_001.pdf
│   │   ├── file_002.docx
│   │   └── ...
│   └── renamed/                # LLM重命名后的文件
│       ├── 东京大学_理学研究科_2025_募集要項.pdf
│       ├── 京都大学_工学研究科_2024_過去問_数学.pdf
│       └── ...
```

---

## 三、模块设计

### 3.1 新增模块结构

```
OverView-V0.3/
├── main.py                     # 主入口 (重构)
├── OverView.py                 # 爬虫引擎 (小改)
├── Sdata.py                    # 配置管理 (扩展)
│
├── db/                         # 新增: 数据库模块
│   ├── __init__.py
│   ├── source_db.py            # Railway 输入源连接
│   ├── target_db.py            # Supabase 输出库连接
│   └── models.py               # 数据模型定义
│
├── storage/                    # 新增: 存储模块
│   ├── __init__.py
│   ├── downloader.py           # 文件下载器
│   └── supabase_storage.py     # Supabase Storage 操作
│
├── processor/                  # 新增: 文件处理模块
│   ├── __init__.py
│   ├── pdf_processor.py        # PDF前两页提取
│   ├── doc_processor.py        # DOC/DOCX处理
│   └── llm_renamer.py          # LLM重命名器
│
├── sync/                       # 新增: 同步模块
│   ├── __init__.py
│   └── incremental_sync.py     # 增量同步检测
│
├── AIPmt/                      # AI提示词 (保留+扩展)
│   ├── Cutf.txt                # 剪枝Prompt
│   ├── Get.txt                 # 分类Prompt
│   └── Rename.txt              # 新增: 文件重命名Prompt
│
└── config/                     # 新增: 配置文件
    ├── database.yaml           # 数据库连接配置
    └── storage.yaml            # 存储配置
```

---

### 3.2 模块详细设计

#### 3.2.1 `db/source_db.py` - 输入源数据库

```python
# 功能概述
class SourceDatabase:
    """Railway PostgreSQL 连接器 (只读)"""

    def connect() -> Engine
        # 建立数据库连接

    def get_all_links() -> List[LinkRecord]
        # 获取所有links记录

    def get_links_by_type(table_name: str) -> List[LinkRecord]
        # 按类型筛选 (undergraduate/graduate/vocational)

    def get_link_by_id(link_id: int) -> LinkRecord
        # 获取单条记录
```

#### 3.2.2 `db/target_db.py` - 输出数据库

```python
# 功能概述
class TargetDatabase:
    """Supabase PostgreSQL 连接器"""

    # 任务管理
    def create_task(source_link_id, source_url) -> int
    def update_task_status(task_id, status, **kwargs)
    def get_task_by_source_id(source_link_id) -> TaskRecord

    # 节点管理
    def batch_insert_nodes(task_id, nodes: List[dict])
    def mark_nodes_pruned(task_id, pruned_indices: List[int])
    def get_file_nodes(task_id) -> List[NodeRecord]

    # 文件管理
    def create_file_record(task_id, node_id, url, ...) -> int
    def update_file_status(file_id, status, **kwargs)
    def update_file_renamed(file_id, new_name, llm_response)
```

#### 3.2.3 `sync/incremental_sync.py` - 增量同步

```python
# 功能概述
class IncrementalSync:
    """增量同步检测器"""

    def detect_new_links() -> List[LinkRecord]
        # 检测未爬取的新链接
        # SQL: SELECT * FROM source.links WHERE id NOT IN (SELECT source_link_id FROM target.crawl_tasks)

    def detect_changed_links() -> List[LinkRecord]
        # 检测URL变更的链接
        # SQL: SELECT * FROM source.links l JOIN target.crawl_tasks t
        #      ON l.id = t.source_link_id WHERE MD5(l.url) != t.url_hash

    def detect_failed_tasks() -> List[TaskRecord]
        # 检测失败需重试的任务
        # SQL: SELECT * FROM target.crawl_tasks WHERE status = 'failed'

    def get_pending_tasks() -> List[TaskRecord]
        # 获取所有待处理任务 (合并以上三种)
```

#### 3.2.4 `storage/downloader.py` - 文件下载器

```python
# 功能概述
class FileDownloader:
    """文件下载器"""

    def __init__(self, chrome: WebDriver)
        # 复用现有的Chrome实例

    def download_file(url: str, save_path: str) -> DownloadResult
        # 下载单个文件
        # 支持: PDF, DOC, DOCX, XLS, XLSX
        # 处理: 重定向、登录墙、超时

    def batch_download(urls: List[str], save_dir: str) -> List[DownloadResult]
        # 批量下载

    def get_file_info(url: str) -> FileInfo
        # 获取文件信息 (大小、类型) 不下载
```

#### 3.2.5 `storage/supabase_storage.py` - Supabase存储

```python
# 功能概述
class SupabaseStorage:
    """Supabase Storage 操作器"""

    def __init__(self, url: str, key: str, bucket: str)

    def upload_file(local_path: str, remote_path: str) -> str
        # 上传文件，返回公开URL

    def rename_file(old_path: str, new_path: str) -> str
        # 重命名/移动文件

    def delete_file(remote_path: str) -> bool
        # 删除文件

    def get_public_url(remote_path: str) -> str
        # 获取公开访问URL
```

#### 3.2.6 `processor/pdf_processor.py` - PDF处理器

```python
# 功能概述
class PDFProcessor:
    """PDF文件处理器"""

    def extract_first_pages(pdf_path: str, num_pages: int = 2) -> bytes
        # 提取前N页为新PDF
        # 使用: PyPDF2 或 pdf2image

    def extract_text(pdf_path: str, num_pages: int = 2) -> str
        # 提取前N页文字
        # 使用: pdfplumber 或 PyMuPDF

    def pdf_to_images(pdf_path: str, num_pages: int = 2) -> List[bytes]
        # 将前N页转为图片 (供多模态LLM使用)
        # 使用: pdf2image
```

#### 3.2.7 `processor/llm_renamer.py` - LLM重命名器

```python
# 功能概述
class LLMRenamer:
    """LLM智能重命名器"""

    def __init__(self, model: str = "doubao-1-5-pro-32k")

    def rename_from_text(text: str, context: dict) -> RenameResult
        # 基于文字内容重命名
        # context: {url, breadcrumb, parent_title, ...}

    def rename_from_images(images: List[bytes], context: dict) -> RenameResult
        # 基于图片内容重命名 (多模态)

    def batch_rename(files: List[FileInfo]) -> List[RenameResult]
        # 批量重命名
```

---

## 四、工作流程

### 4.1 完整工作流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Phase 1: 同步检测                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. 连接 Railway 数据库，获取 links 表数据                                    │
│  2. 连接 Supabase 数据库，获取 crawl_tasks 表数据                             │
│  3. 执行增量检测:                                                            │
│     - 新增链接 (links中有，crawl_tasks中无)                                   │
│     - 变更链接 (URL哈希不一致)                                                │
│     - 失败重试 (status='failed')                                             │
│  4. 生成待处理任务列表                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Phase 2: 爬取阶段                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  for each task in pending_tasks:                                            │
│      1. 更新 crawl_tasks.status = 'crawling'                                │
│      2. 执行 ov.Seek() 广度优先爬取                                          │
│      3. 将节点数据批量写入 crawl_nodes 表                                     │
│      4. 执行 ov.Pruning() AI智能剪枝                                         │
│      5. 更新 crawl_nodes.is_pruned 字段                                      │
│      6. 识别文件节点，更新 is_file 和 file_extension                          │
│      7. 更新 crawl_tasks.status = 'completed'                               │
│      8. 异常时: status = 'failed', 记录 error_message                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Phase 3: 文件下载                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. 查询所有 is_pruned=true AND is_file=true 的节点                          │
│  2. 为每个文件创建 crawl_files 记录                                          │
│  3. 下载文件到本地临时目录                                                    │
│  4. 上传到 Supabase Storage (raw/ 目录)                                      │
│  5. 更新 crawl_files.storage_path                                           │
│  6. 更新 download_status = 'completed'                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Phase 4: LLM重命名                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  for each file in downloaded_files:                                         │
│      1. 从 Storage 下载文件 (或使用本地缓存)                                  │
│      2. 提取前2页内容:                                                       │
│         - PDF: 提取文字 或 转图片                                            │
│         - DOC: 提取文字                                                      │
│      3. 构建上下文: {url, breadcrumb, parent_title, original_name}           │
│      4. 调用 LLM 生成新文件名                                                 │
│      5. 在 Storage 中复制文件到 renamed/ 目录                                 │
│      6. 更新 crawl_files: renamed_name, llm_processed=true                  │
│      7. 更新 process_status = 'completed'                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 状态机

#### 任务状态 (crawl_tasks.status)

```
pending ──▶ crawling ──▶ completed
    │           │
    │           ▼
    └───────▶ failed ──▶ pending (重试)
```

#### 文件下载状态 (crawl_files.download_status)

```
pending ──▶ downloading ──▶ completed
                │
                ▼
              failed
```

#### 文件处理状态 (crawl_files.process_status)

```
pending ──▶ processing ──▶ completed
                │
                ▼
              failed
```

---

## 五、LLM重命名Prompt设计

### 5.1 新增文件 `AIPmt/Rename.txt`

```
# Role
你是一名专业的日本大学招考资料档案管理专家，精通日语和大学院入试文件的命名规范。

# Task
根据文件的前两页内容和上下文信息，为该文件生成一个标准化的文件名。

# Naming Schema (严格遵守)
格式: {大学名}_{研究科/学部}_{年度}_{文件类型}_{补充信息}.{扩展名}

## 字段说明:
- 大学名: 简称或正式名 (如: 東大, 京大, 東京大学)
- 研究科/学部: 所属院系 (如: 理学研究科, 工学部)
- 年度: 入試年度或発行年度 (如: 2025, R7, 令和7)
- 文件类型:
  - 募集要項 / 出願書類 / 入試日程
  - 過去問 / 解答例
  - 合格発表 / 入試結果
- 补充信息(可选): 科目名、入試種別等 (如: 数学, 外国人留学生, 前期)

## 示例:
- 東京大学_理学系研究科_2025_募集要項.pdf
- 京都大学_工学研究科_2024_過去問_数学.pdf
- 早稲田大学_政治経済学部_R7_入試日程_一般選抜.pdf
- 東北大学_医学系研究科_2025_出願書類_外国人留学生.pdf

# Context (参考信息)
- 原始URL: {url}
- 面包屑路径: {breadcrumb}
- 页面标题: {title}
- 父页面标题: {parent_title}
- 原始文件名: {original_name}

# Content (文件前两页内容)
{content}

# Output Format (严格遵守)
请仅返回JSON格式，不要包含任何解释文字:
{
  "renamed": "生成的文件名.扩展名",
  "university": "识别出的大学名",
  "department": "识别出的研究科/学部",
  "year": "识别出的年度",
  "doc_type": "识别出的文件类型",
  "confidence": 0.95,
  "reason": "命名依据的简要说明"
}

若无法确定某字段，使用 "Unknown" 占位，但仍需生成完整文件名。
```

---

## 六、配置管理

### 6.1 `config/database.yaml`

```yaml
# 输入源数据库 (Railway)
source:
  host: maglev.proxy.rlwy.net
  port: 43262
  database: railway
  user: postgres
  password: ${RAILWAY_DB_PASSWORD}  # 从环境变量读取

# 输出数据库 (Supabase)
target:
  host: db.orqthdhhyqtksrtxweoc.supabase.co
  port: 5432
  database: postgres
  user: postgres
  password: ${SUPABASE_DB_PASSWORD}  # 从环境变量读取
```

### 6.2 `config/storage.yaml`

```yaml
# Supabase Storage
supabase:
  url: https://orqthdhhyqtksrtxweoc.supabase.co
  anon_key: ${SUPABASE_ANON_KEY}
  service_key: ${SUPABASE_SERVICE_KEY}
  bucket: university-files

# 本地临时目录
local:
  temp_dir: ./temp_downloads
  cache_enabled: true
  cache_ttl: 3600  # 秒
```

### 6.3 扩展 `Sdata.py`

```python
# === 新增配置 ===

# 数据库连接
SOURCE_DB_URL = "postgresql://postgres:{password}@maglev.proxy.rlwy.net:43262/railway"
TARGET_DB_URL = "postgresql://postgres:{password}@db.orqthdhhyqtksrtxweoc.supabase.co:5432/postgres"

# Supabase Storage
SUPABASE_URL = "https://orqthdhhyqtksrtxweoc.supabase.co"
SUPABASE_KEY = "..."
SUPABASE_BUCKET = "university-files"

# 文件处理
DOWNLOAD_TIMEOUT = 60          # 下载超时(秒)
MAX_FILE_SIZE = 50 * 1024 * 1024  # 最大文件大小(50MB)
PDF_EXTRACT_PAGES = 2          # 提取前N页
SUPPORTED_EXTENSIONS = ['.pdf', '.doc', '.docx', '.xls', '.xlsx']

# LLM配置
RENAME_MODEL = "doubao-1-5-pro-32k-250115"
RENAME_TEMPERATURE = 0.3
```

---

## 七、错误处理策略

### 7.1 重试机制

| 错误类型 | 重试次数 | 重试间隔 | 处理方式 |
|----------|----------|----------|----------|
| 网络超时 | 3次 | 指数退避 | 自动重试 |
| 数据库连接失败 | 5次 | 10秒 | 自动重试 |
| 文件下载失败 | 3次 | 5秒 | 标记failed，人工处理 |
| LLM调用失败 | 3次 | 2秒 | 使用默认命名 |
| Storage上传失败 | 3次 | 5秒 | 保留本地文件 |

### 7.2 错误日志

```python
# 错误记录到数据库
crawl_tasks.error_message = "Timeout after 60s at depth 2"
crawl_files.error_message = "403 Forbidden - Login required"
```

---

## 八、性能优化

### 8.1 批量操作

| 操作 | 单条 vs 批量 | 优化方式 |
|------|-------------|----------|
| 节点插入 | 批量 | `executemany()` 或 COPY |
| 状态更新 | 批量 | `UPDATE ... WHERE id IN (...)` |
| 文件上传 | 并发 | 多线程 (max_workers=5) |

### 8.2 连接池

```python
# SQLAlchemy 连接池配置
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800
)
```

---

## 九、依赖新增

```txt
# requirements.txt 新增

# 数据库
sqlalchemy>=2.0
psycopg2-binary

# Supabase
supabase>=2.0

# PDF处理
PyPDF2
pdfplumber
pdf2image

# DOC处理
python-docx

# 并发
concurrent-futures
```

---

## 十、里程碑计划

### Phase 1: 数据库集成 (基础)
- [ ] 创建 Supabase 表结构
- [ ] 实现 source_db.py (Railway连接)
- [ ] 实现 target_db.py (Supabase连接)
- [ ] 实现 incremental_sync.py (增量检测)
- [ ] 改造 main.py 输入逻辑

### Phase 2: 爬取输出改造
- [ ] 改造 OverView.Seek() 输出到数据库
- [ ] 改造 OverView.Pruning() 更新数据库
- [ ] 移除本地CSV生成 (可选保留为调试)
- [ ] 测试完整爬取流程

### Phase 3: 文件下载
- [ ] 实现 downloader.py
- [ ] 实现 supabase_storage.py
- [ ] 集成到主流程
- [ ] 测试大文件处理

### Phase 4: LLM重命名
- [ ] 实现 pdf_processor.py
- [ ] 实现 doc_processor.py
- [ ] 实现 llm_renamer.py
- [ ] 编写 Rename.txt Prompt
- [ ] 测试重命名准确性

### Phase 5: 优化与监控
- [ ] 添加进度日志
- [ ] 实现错误重试
- [ ] 性能优化
- [ ] 文档完善

---

## 十一、注意事项

1. **API密钥安全**: 所有密钥使用环境变量，不硬编码
2. **数据库事务**: 批量操作使用事务，失败时回滚
3. **文件清理**: 定期清理本地临时文件
4. **并发控制**: 避免对大学网站造成过大压力
5. **日志记录**: 完整记录每个步骤，便于问题排查
