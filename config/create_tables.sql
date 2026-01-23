-- ============================================
-- OverView V0.3 Supabase 数据库表结构
-- 在 Supabase SQL Editor 中执行此脚本
-- ============================================

-- 表1: 爬取任务状态
CREATE TABLE IF NOT EXISTS crawl_tasks (
    id              SERIAL PRIMARY KEY,
    source_link_id  INTEGER NOT NULL,
    source_url      TEXT NOT NULL,
    url_hash        VARCHAR(64),
    school_name     TEXT,                              -- 学校名称（从源数据库获取）
    status          VARCHAR(20) DEFAULT 'pending',
    node_count      INTEGER DEFAULT 0,
    pruned_count    INTEGER DEFAULT 0,
    file_count      INTEGER DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),

    CONSTRAINT unique_source_link UNIQUE(source_link_id)
);

-- 如果表已存在，添加 school_name 字段（只需执行一次）
-- ALTER TABLE crawl_tasks ADD COLUMN IF NOT EXISTS school_name TEXT;

CREATE INDEX IF NOT EXISTS idx_crawl_tasks_status ON crawl_tasks(status);
CREATE INDEX IF NOT EXISTS idx_crawl_tasks_source ON crawl_tasks(source_link_id);

-- 表2: 爬取节点数据
CREATE TABLE IF NOT EXISTS crawl_nodes (
    id              SERIAL PRIMARY KEY,
    task_id         INTEGER REFERENCES crawl_tasks(id) ON DELETE CASCADE,
    node_index      INTEGER NOT NULL,
    father_index    INTEGER,
    depth           INTEGER,
    title           TEXT,
    breadcrumb      TEXT,
    url             TEXT NOT NULL,
    father_title    TEXT,
    is_pruned       BOOLEAN DEFAULT FALSE,
    is_file         BOOLEAN DEFAULT FALSE,
    file_extension  VARCHAR(10),
    created_at      TIMESTAMP DEFAULT NOW(),

    CONSTRAINT unique_task_node UNIQUE(task_id, node_index)
);

CREATE INDEX IF NOT EXISTS idx_crawl_nodes_task ON crawl_nodes(task_id);
CREATE INDEX IF NOT EXISTS idx_crawl_nodes_pruned ON crawl_nodes(is_pruned);
CREATE INDEX IF NOT EXISTS idx_crawl_nodes_file ON crawl_nodes(is_file);

-- 表3: 下载文件记录
CREATE TABLE IF NOT EXISTS crawl_files (
    id              SERIAL PRIMARY KEY,
    task_id         INTEGER REFERENCES crawl_tasks(id) ON DELETE CASCADE,
    node_id         INTEGER REFERENCES crawl_nodes(id) ON DELETE CASCADE,
    original_url    TEXT NOT NULL,
    original_name   TEXT,
    renamed_name    TEXT,
    file_extension  VARCHAR(10),
    file_size       BIGINT,
    storage_path    TEXT,
    storage_bucket  VARCHAR(50) DEFAULT 'university-files',

    llm_processed   BOOLEAN DEFAULT FALSE,
    llm_model       VARCHAR(50),
    llm_confidence  FLOAT,
    llm_raw_response TEXT,

    download_status VARCHAR(20) DEFAULT 'pending',
    process_status  VARCHAR(20) DEFAULT 'pending',
    error_message   TEXT,

    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_files_task ON crawl_files(task_id);
CREATE INDEX IF NOT EXISTS idx_crawl_files_download ON crawl_files(download_status);
CREATE INDEX IF NOT EXISTS idx_crawl_files_process ON crawl_files(process_status);

-- 表4: 同步日志
CREATE TABLE IF NOT EXISTS sync_log (
    id              SERIAL PRIMARY KEY,
    sync_type       VARCHAR(20),
    source_count    INTEGER,
    new_count       INTEGER,
    changed_count   INTEGER,
    synced_at       TIMESTAMP DEFAULT NOW()
);

-- 表5: HTML可视化文件存储记录
CREATE TABLE IF NOT EXISTS crawl_visualizations (
    id              SERIAL PRIMARY KEY,
    task_id         INTEGER REFERENCES crawl_tasks(id) ON DELETE CASCADE,
    viz_type        VARCHAR(20),  -- 'raw' or 'pruned'
    storage_path    TEXT,
    storage_bucket  VARCHAR(50) DEFAULT 'university-files',
    created_at      TIMESTAMP DEFAULT NOW(),

    CONSTRAINT unique_task_viz UNIQUE(task_id, viz_type)
);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为需要的表添加触发器
DROP TRIGGER IF EXISTS update_crawl_tasks_updated_at ON crawl_tasks;
CREATE TRIGGER update_crawl_tasks_updated_at
    BEFORE UPDATE ON crawl_tasks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_crawl_files_updated_at ON crawl_files;
CREATE TRIGGER update_crawl_files_updated_at
    BEFORE UPDATE ON crawl_files
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 输出创建结果
SELECT 'Tables created successfully!' as result;
