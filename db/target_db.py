"""
输出数据库连接器 (Supabase PostgreSQL)
负责存储爬取结果、文件记录等
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, DisconnectionError
from sqlalchemy.pool import NullPool
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime
from functools import wraps
import hashlib
import os
import time
import logging

logger = logging.getLogger(__name__)

# 连接模式配置
# Transaction 模式：每次事务结束后连接返回池，支持更多并发
# Session 模式：连接保持整个会话，连接数有限
USE_TRANSACTION_MODE = True  # 推荐开启


def with_retry(max_retries=3, delay=1):
    """数据库操作重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(self, *args, **kwargs)
                except (OperationalError, DisconnectionError, BrokenPipeError, OSError) as e:
                    last_error = e
                    logger.warning(f"数据库操作失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                    # 重置连接
                    self._reconnect()
                    time.sleep(delay * (attempt + 1))
            raise last_error
        return wrapper
    return decorator


@dataclass
class TaskRecord:
    """crawl_tasks表记录"""
    id: int
    source_link_id: int
    source_url: str
    url_hash: str
    school_name: Optional[str]  # 学校名称（从源数据库获取）
    status: str
    node_count: int
    pruned_count: int
    file_count: int
    error_message: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass
class NodeRecord:
    """crawl_nodes表记录"""
    id: int
    task_id: int
    node_index: int
    father_index: int
    depth: int
    title: str
    breadcrumb: str
    url: str
    father_title: str
    is_pruned: bool
    is_file: bool
    file_extension: Optional[str]


@dataclass
class FileRecord:
    """crawl_files表记录"""
    id: int
    task_id: int
    node_id: int
    original_url: str
    original_name: str
    renamed_name: Optional[str]
    file_extension: str
    file_size: Optional[int]
    storage_path: Optional[str]
    storage_bucket: str
    llm_processed: bool
    download_status: str
    process_status: str


class TargetDatabase:
    """Supabase PostgreSQL 连接器"""

    # Transaction 模式 (端口 6543)：每次事务结束后连接返回池，支持更多并发
    # Session 模式 (端口 5432)：连接持续整个会话，连接数有限
    DEFAULT_URL_TRANSACTION = "postgresql://postgres.orqthdhhyqtksrtxweoc:bnly4zU3k4pRmerH@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"
    DEFAULT_URL_SESSION = "postgresql://postgres.orqthdhhyqtksrtxweoc:bnly4zU3k4pRmerH@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

    def __init__(self, database_url: str = None):
        """
        初始化数据库连接

        Args:
            database_url: 数据库连接URL，默认使用环境变量或内置URL
        """
        # 优先使用环境变量，否则根据模式选择默认 URL
        env_url = os.getenv('TARGET_DB_URL')
        if env_url:
            # 如果启用 Transaction 模式，自动将端口 5432 替换为 6543
            if USE_TRANSACTION_MODE and ':5432/' in env_url:
                self.database_url = env_url.replace(':5432/', ':6543/')
                logger.info("已切换到 Transaction 模式 (端口 6543)")
            else:
                self.database_url = env_url
        else:
            self.database_url = self.DEFAULT_URL_TRANSACTION if USE_TRANSACTION_MODE else self.DEFAULT_URL_SESSION

        self.engine = None
        self.Session = None

    def connect(self):
        """建立数据库连接"""
        if self.engine is None:
            if USE_TRANSACTION_MODE:
                # Transaction 模式：使用 NullPool，不维护本地连接池
                # 每次操作创建连接，用完立即释放，避免连接数超限
                self.engine = create_engine(
                    self.database_url,
                    poolclass=NullPool,  # 不使用连接池
                    connect_args={
                        "connect_timeout": 10,
                        "options": "-c statement_timeout=30000"  # 30秒超时
                    }
                )
            else:
                # Session 模式：使用小型连接池
                self.engine = create_engine(
                    self.database_url,
                    pool_size=2,
                    max_overflow=3,
                    pool_timeout=30,
                    pool_recycle=300,
                    pool_pre_ping=True,
                    connect_args={
                        "keepalives": 1,
                        "keepalives_idle": 30,
                        "keepalives_interval": 10,
                        "keepalives_count": 5
                    }
                )
            self.Session = sessionmaker(bind=self.engine)
        return self.engine

    def _reconnect(self):
        """重新建立数据库连接"""
        logger.info("正在重新连接数据库...")
        try:
            if self.engine:
                self.engine.dispose()
        except:
            pass
        self.engine = None
        self.Session = None
        self.connect()

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.Session = None

    # ==================== 任务管理 ====================

    @with_retry(max_retries=3, delay=1)
    def create_task(self, source_link_id: int, source_url: str, school_name: str = None) -> int:
        """
        创建爬取任务

        Args:
            source_link_id: Railway数据库中的link ID
            source_url: 目标URL
            school_name: 学校名称（从源数据库获取）

        Returns:
            新创建的task ID
        """
        self.connect()
        url_hash = hashlib.md5(source_url.encode()).hexdigest()

        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO crawl_tasks (source_link_id, source_url, url_hash, school_name, status)
                    VALUES (:source_link_id, :source_url, :url_hash, :school_name, 'pending')
                    ON CONFLICT (source_link_id) DO UPDATE SET
                        source_url = EXCLUDED.source_url,
                        url_hash = EXCLUDED.url_hash,
                        school_name = EXCLUDED.school_name,
                        status = 'pending',
                        updated_at = NOW()
                    RETURNING id
                """),
                {
                    "source_link_id": source_link_id,
                    "source_url": source_url,
                    "url_hash": url_hash,
                    "school_name": school_name
                }
            )
            conn.commit()
            return result.scalar()

    @with_retry(max_retries=3, delay=1)
    def update_task_status(self, task_id: int, status: str, **kwargs):
        """
        更新任务状态

        Args:
            task_id: 任务ID
            status: 新状态 (pending/crawling/completed/failed)
            **kwargs: 其他要更新的字段 (node_count, pruned_count, file_count, error_message)
        """
        self.connect()

        # 构建动态更新语句
        updates = ["status = :status"]
        params = {"task_id": task_id, "status": status}

        if status == 'crawling':
            updates.append("started_at = NOW()")
        elif status in ('completed', 'failed'):
            updates.append("completed_at = NOW()")

        for key in ['node_count', 'pruned_count', 'file_count', 'error_message']:
            if key in kwargs:
                updates.append(f"{key} = :{key}")
                params[key] = kwargs[key]

        sql = f"UPDATE crawl_tasks SET {', '.join(updates)} WHERE id = :task_id"

        with self.engine.connect() as conn:
            conn.execute(text(sql), params)
            conn.commit()

    @with_retry(max_retries=3, delay=1)
    def get_task_by_source_id(self, source_link_id: int) -> Optional[TaskRecord]:
        """根据源link ID获取任务"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM crawl_tasks WHERE source_link_id = :id"),
                {"id": source_link_id}
            )
            row = result.fetchone()
            if row:
                return TaskRecord(
                    id=row.id,
                    source_link_id=row.source_link_id,
                    source_url=row.source_url,
                    url_hash=row.url_hash,
                    school_name=getattr(row, 'school_name', None),
                    status=row.status,
                    node_count=row.node_count,
                    pruned_count=row.pruned_count,
                    file_count=row.file_count,
                    error_message=row.error_message,
                    started_at=row.started_at,
                    completed_at=row.completed_at,
                    created_at=row.created_at,
                    updated_at=row.updated_at
                )
            return None

    def get_task_by_id(self, task_id: int) -> Optional[TaskRecord]:
        """根据任务ID获取任务"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM crawl_tasks WHERE id = :id"),
                {"id": task_id}
            )
            row = result.fetchone()
            if row:
                return TaskRecord(
                    id=row.id,
                    source_link_id=row.source_link_id,
                    source_url=row.source_url,
                    url_hash=row.url_hash,
                    school_name=getattr(row, 'school_name', None),
                    status=row.status,
                    node_count=row.node_count,
                    pruned_count=row.pruned_count,
                    file_count=row.file_count,
                    error_message=row.error_message,
                    started_at=row.started_at,
                    completed_at=row.completed_at,
                    created_at=row.created_at,
                    updated_at=row.updated_at
                )
            return None

    def get_all_task_source_ids(self) -> List[int]:
        """获取所有已存在任务的source_link_id列表"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT source_link_id FROM crawl_tasks"))
            return [row[0] for row in result]

    def get_tasks_by_status(self, status: str) -> List[TaskRecord]:
        """根据状态获取任务列表"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM crawl_tasks WHERE status = :status ORDER BY id"),
                {"status": status}
            )
            tasks = []
            for row in result:
                tasks.append(TaskRecord(
                    id=row.id,
                    source_link_id=row.source_link_id,
                    source_url=row.source_url,
                    url_hash=row.url_hash,
                    school_name=getattr(row, 'school_name', None),
                    status=row.status,
                    node_count=row.node_count,
                    pruned_count=row.pruned_count,
                    file_count=row.file_count,
                    error_message=row.error_message,
                    started_at=row.started_at,
                    completed_at=row.completed_at,
                    created_at=row.created_at,
                    updated_at=row.updated_at
                ))
            return tasks

    def get_changed_tasks(self, url_hashes: Dict[int, str]) -> List[int]:
        """
        检测URL变更的任务 (批量查询优化版)

        Args:
            url_hashes: {source_link_id: url_md5_hash} 字典

        Returns:
            变更的source_link_id列表
        """
        if not url_hashes:
            return []

        self.connect()

        # 一次性获取所有已存在任务的 source_link_id 和 url_hash
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT source_link_id, url_hash FROM crawl_tasks")
            )
            existing_tasks = {row[0]: row[1] for row in result}

        # 在内存中比较，找出变更的链接
        changed_ids = []
        for source_id, new_hash in url_hashes.items():
            if source_id in existing_tasks:
                if existing_tasks[source_id] != new_hash:
                    changed_ids.append(source_id)

        return changed_ids

    # ==================== 节点管理 ====================

    @with_retry(max_retries=3, delay=1)
    def batch_insert_nodes(self, task_id: int, nodes: List[Dict[str, Any]]):
        """
        批量插入节点数据

        Args:
            task_id: 任务ID
            nodes: 节点数据列表，每个节点包含:
                   node_index, father_index, depth, title, breadcrumb, url, father_title
        """
        if not nodes:
            return

        self.connect()

        # 检测文件类型
        file_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx']

        # 辅助函数：安全获取值（处理 0 等 falsy 值）
        def safe_get(d, key1, key2=None):
            val = d.get(key1)
            if val is not None:
                return val
            if key2:
                return d.get(key2)
            return None

        with self.engine.connect() as conn:
            for node in nodes:
                url_val = safe_get(node, 'Url', 'url') or ''
                url_lower = url_val.lower()
                is_file = any(url_lower.endswith(ext) for ext in file_extensions)
                file_ext = None
                if is_file:
                    for ext in file_extensions:
                        if url_lower.endswith(ext):
                            file_ext = ext.lstrip('.')
                            break

                conn.execute(
                    text("""
                        INSERT INTO crawl_nodes
                        (task_id, node_index, father_index, depth, title, breadcrumb, url, father_title, is_file, file_extension)
                        VALUES (:task_id, :node_index, :father_index, :depth, :title, :breadcrumb, :url, :father_title, :is_file, :file_ext)
                        ON CONFLICT (task_id, node_index) DO UPDATE SET
                            title = EXCLUDED.title,
                            breadcrumb = EXCLUDED.breadcrumb,
                            url = EXCLUDED.url,
                            father_title = EXCLUDED.father_title,
                            is_file = EXCLUDED.is_file,
                            file_extension = EXCLUDED.file_extension
                    """),
                    {
                        "task_id": task_id,
                        "node_index": safe_get(node, 'Index', 'node_index'),
                        "father_index": safe_get(node, 'FatherIndex', 'father_index'),
                        "depth": safe_get(node, 'Depth', 'depth'),
                        "title": safe_get(node, 'title'),
                        "breadcrumb": safe_get(node, 'Breadcrumb', 'breadcrumb'),
                        "url": url_val,
                        "father_title": safe_get(node, 'FatherTitle', 'father_title'),
                        "is_file": is_file,
                        "file_ext": file_ext
                    }
                )
            conn.commit()

    @with_retry(max_retries=3, delay=1)
    def mark_nodes_pruned(self, task_id: int, pruned_indices: List[int]):
        """
        标记剪枝保留的节点

        Args:
            task_id: 任务ID
            pruned_indices: 保留的节点索引列表
        """
        if not pruned_indices:
            return

        self.connect()
        with self.engine.connect() as conn:
            # 先重置所有节点
            conn.execute(
                text("UPDATE crawl_nodes SET is_pruned = FALSE WHERE task_id = :task_id"),
                {"task_id": task_id}
            )
            # 标记保留的节点
            conn.execute(
                text("""
                    UPDATE crawl_nodes SET is_pruned = TRUE
                    WHERE task_id = :task_id AND node_index = ANY(:indices)
                """),
                {"task_id": task_id, "indices": pruned_indices}
            )
            conn.commit()

    @with_retry(max_retries=3, delay=1)
    def get_file_nodes(self, task_id: int, pruned_only: bool = True) -> List[NodeRecord]:
        """
        获取文件类型的节点

        Args:
            task_id: 任务ID
            pruned_only: 是否只获取剪枝保留的

        Returns:
            NodeRecord列表
        """
        self.connect()
        sql = "SELECT * FROM crawl_nodes WHERE task_id = :task_id AND is_file = TRUE"
        if pruned_only:
            sql += " AND is_pruned = TRUE"
        sql += " ORDER BY node_index"

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), {"task_id": task_id})
            nodes = []
            for row in result:
                nodes.append(NodeRecord(
                    id=row.id,
                    task_id=row.task_id,
                    node_index=row.node_index,
                    father_index=row.father_index,
                    depth=row.depth,
                    title=row.title,
                    breadcrumb=row.breadcrumb,
                    url=row.url,
                    father_title=row.father_title,
                    is_pruned=row.is_pruned,
                    is_file=row.is_file,
                    file_extension=row.file_extension
                ))
            return nodes

    def get_all_nodes(self, task_id: int) -> List[Dict[str, Any]]:
        """获取任务的所有节点数据"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM crawl_nodes WHERE task_id = :task_id ORDER BY node_index"),
                {"task_id": task_id}
            )
            return [dict(row._mapping) for row in result]

    def get_pruned_nodes(self, task_id: int) -> List[Dict[str, Any]]:
        """获取剪枝后保留的节点数据"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM crawl_nodes WHERE task_id = :task_id AND is_pruned = TRUE ORDER BY node_index"),
                {"task_id": task_id}
            )
            return [dict(row._mapping) for row in result]

    # ==================== 文件管理 ====================

    @with_retry(max_retries=3, delay=1)
    def create_file_record(self, task_id: int, node_id: int, original_url: str,
                           original_name: str = None, file_extension: str = None) -> int:
        """
        创建文件下载记录

        Returns:
            新创建的file ID
        """
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO crawl_files
                    (task_id, node_id, original_url, original_name, file_extension)
                    VALUES (:task_id, :node_id, :original_url, :original_name, :file_ext)
                    RETURNING id
                """),
                {
                    "task_id": task_id,
                    "node_id": node_id,
                    "original_url": original_url,
                    "original_name": original_name,
                    "file_ext": file_extension
                }
            )
            conn.commit()
            return result.scalar()

    @with_retry(max_retries=3, delay=1)
    def update_file_download(self, file_id: int, status: str,
                             storage_path: str = None, file_size: int = None,
                             error_message: str = None):
        """更新文件下载状态"""
        self.connect()
        updates = ["download_status = :status"]
        params = {"file_id": file_id, "status": status}

        if storage_path:
            updates.append("storage_path = :storage_path")
            params["storage_path"] = storage_path
        if file_size:
            updates.append("file_size = :file_size")
            params["file_size"] = file_size
        if error_message:
            updates.append("error_message = :error_message")
            params["error_message"] = error_message

        sql = f"UPDATE crawl_files SET {', '.join(updates)} WHERE id = :file_id"

        with self.engine.connect() as conn:
            conn.execute(text(sql), params)
            conn.commit()

    def update_file_renamed(self, file_id: int, renamed_name: str,
                            llm_model: str = None, llm_confidence: float = None,
                            llm_raw_response: str = None):
        """更新文件重命名结果"""
        self.connect()
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE crawl_files SET
                        renamed_name = :renamed_name,
                        llm_processed = TRUE,
                        llm_model = :llm_model,
                        llm_confidence = :llm_confidence,
                        llm_raw_response = :llm_raw_response,
                        process_status = 'completed'
                    WHERE id = :file_id
                """),
                {
                    "file_id": file_id,
                    "renamed_name": renamed_name,
                    "llm_model": llm_model,
                    "llm_confidence": llm_confidence,
                    "llm_raw_response": llm_raw_response
                }
            )
            conn.commit()

    def get_pending_files(self, task_id: int = None) -> List[Dict[str, Any]]:
        """获取待下载的文件"""
        self.connect()
        sql = "SELECT * FROM crawl_files WHERE download_status = 'pending'"
        params = {}
        if task_id:
            sql += " AND task_id = :task_id"
            params["task_id"] = task_id
        sql += " ORDER BY id"

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            return [dict(row._mapping) for row in result]

    def get_pending_process_files(self, task_id: int = None) -> List[Dict[str, Any]]:
        """获取待处理(LLM重命名)的文件"""
        self.connect()
        sql = "SELECT * FROM crawl_files WHERE download_status = 'completed' AND process_status = 'pending'"
        params = {}
        if task_id:
            sql += " AND task_id = :task_id"
            params["task_id"] = task_id
        sql += " ORDER BY id"

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            return [dict(row._mapping) for row in result]

    def get_task_files_with_llm_result(self, task_id: int) -> List[Dict[str, Any]]:
        """
        获取任务中所有已LLM处理的文件（包含命名信息）

        Args:
            task_id: 任务ID

        Returns:
            文件记录列表，包含 id, renamed_name, llm_raw_response 等
        """
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id, renamed_name, llm_raw_response, llm_processed
                    FROM crawl_files
                    WHERE task_id = :task_id AND llm_processed = TRUE
                    ORDER BY id
                """),
                {"task_id": task_id}
            )
            return [dict(row._mapping) for row in result]

    def update_renamed_name_only(self, file_id: int, renamed_name: str):
        """
        仅更新文件的 renamed_name 字段（用于补充 Unknown）

        Args:
            file_id: 文件ID
            renamed_name: 新的文件名
        """
        self.connect()
        with self.engine.connect() as conn:
            conn.execute(
                text("UPDATE crawl_files SET renamed_name = :renamed_name WHERE id = :file_id"),
                {"file_id": file_id, "renamed_name": renamed_name}
            )
            conn.commit()

    def update_file_process_failed(self, file_id: int, error_message: str = None):
        """
        更新文件处理失败状态

        Args:
            file_id: 文件ID
            error_message: 错误信息
        """
        self.connect()
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE crawl_files SET
                        llm_processed = TRUE,
                        process_status = 'failed',
                        error_message = :error_message
                    WHERE id = :file_id
                """),
                {
                    "file_id": file_id,
                    "error_message": error_message
                }
            )
            conn.commit()

    # ==================== 可视化管理 ====================

    def save_visualization(self, task_id: int, viz_type: str, storage_path: str):
        """
        保存可视化文件记录

        Args:
            task_id: 任务ID
            viz_type: 类型 ('raw' 或 'pruned')
            storage_path: Storage路径
        """
        self.connect()
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO crawl_visualizations (task_id, viz_type, storage_path)
                    VALUES (:task_id, :viz_type, :storage_path)
                    ON CONFLICT (task_id, viz_type) DO UPDATE SET
                        storage_path = EXCLUDED.storage_path
                """),
                {
                    "task_id": task_id,
                    "viz_type": viz_type,
                    "storage_path": storage_path
                }
            )
            conn.commit()

    # ==================== 同步日志 ====================

    def log_sync(self, sync_type: str, source_count: int, new_count: int, changed_count: int):
        """记录同步日志"""
        self.connect()
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO sync_log (sync_type, source_count, new_count, changed_count)
                    VALUES (:sync_type, :source_count, :new_count, :changed_count)
                """),
                {
                    "sync_type": sync_type,
                    "source_count": source_count,
                    "new_count": new_count,
                    "changed_count": changed_count
                }
            )
            conn.commit()

    # ==================== 清理操作 ====================

    def delete_task_data(self, task_id: int):
        """删除任务相关的所有数据（用于重新爬取）"""
        self.connect()
        with self.engine.connect() as conn:
            # 由于有 ON DELETE CASCADE，只需删除task即可
            conn.execute(
                text("DELETE FROM crawl_tasks WHERE id = :task_id"),
                {"task_id": task_id}
            )
            conn.commit()


# 测试代码
if __name__ == "__main__":
    db = TargetDatabase()

    print("连接Supabase数据库...")
    db.connect()

    # 测试创建任务
    task_id = db.create_task(999, "https://example.com/test")
    print(f"创建任务: {task_id}")

    # 测试更新状态
    db.update_task_status(task_id, 'crawling')
    print("更新状态为 crawling")

    # 测试插入节点
    nodes = [
        {"Index": 0, "FatherIndex": -1, "Depth": 0, "title": "根节点", "Breadcrumb": "", "Url": "https://example.com", "FatherTitle": ""},
        {"Index": 1, "FatherIndex": 0, "Depth": 1, "title": "测试PDF", "Breadcrumb": "首页", "Url": "https://example.com/test.pdf", "FatherTitle": "根节点"},
    ]
    db.batch_insert_nodes(task_id, nodes)
    print("插入节点完成")

    # 清理测试数据
    db.delete_task_data(task_id)
    print("清理测试数据完成")

    db.close()
    print("连接已关闭")
