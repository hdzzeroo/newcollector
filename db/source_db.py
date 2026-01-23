"""
输入源数据库连接器 (Railway PostgreSQL)
只读操作，不修改源数据库
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dataclasses import dataclass
from typing import List, Optional
import os


@dataclass
class LinkRecord:
    """links表记录的数据结构"""
    id: int
    table_name: str
    row_id: int
    url: str
    user_id: int
    created_at: str
    has_guideline: bool
    has_past_exam: bool
    has_result: bool
    has_material_check: bool
    has_pdf: bool
    is_page_info: bool


class SourceDatabase:
    """Railway PostgreSQL 连接器 (只读)"""

    DEFAULT_URL = "postgresql://postgres:bwNYWrIfZgkGxdfmKObdhZjJPXVuDGAi@maglev.proxy.rlwy.net:43262/railway"

    def __init__(self, database_url: str = None):
        """
        初始化数据库连接

        Args:
            database_url: 数据库连接URL，默认使用环境变量或内置URL
        """
        self.database_url = database_url or os.getenv('SOURCE_DB_URL', self.DEFAULT_URL)
        self.engine = None
        self.Session = None

    def connect(self):
        """建立数据库连接"""
        if self.engine is None:
            self.engine = create_engine(
                self.database_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
            self.Session = sessionmaker(bind=self.engine)
        return self.engine

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.Session = None

    def _row_to_record(self, row) -> LinkRecord:
        """将数据库行转换为LinkRecord对象"""
        return LinkRecord(
            id=row.id,
            table_name=row.table_name,
            row_id=row.row_id,
            url=row.url,
            user_id=row.user_id,
            created_at=str(row.created_at),
            has_guideline=row.has_guideline,
            has_past_exam=row.has_past_exam,
            has_result=row.has_result,
            has_material_check=row.has_material_check,
            has_pdf=row.has_pdf,
            is_page_info=row.is_page_info
        )

    def get_all_links(self) -> List[LinkRecord]:
        """
        获取所有links记录（只返回 graduate/undergraduate，排除 vocational）

        Returns:
            LinkRecord列表
        """
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM links WHERE table_name IN ('graduate', 'undergraduate') ORDER BY id")
            )
            return [self._row_to_record(row) for row in result]

    def get_school_name(self, table_name: str, row_id: int) -> Optional[str]:
        """
        根据 table_name 和 row_id 获取学校名称

        Args:
            table_name: 表名 (graduate/undergraduate)
            row_id: 该表中的记录ID

        Returns:
            学校名称，如果无法获取则返回 None
        """
        if table_name not in ('graduate', 'undergraduate'):
            return None

        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT school FROM {table_name} WHERE id = :row_id"),
                {"row_id": row_id}
            )
            row = result.fetchone()
            return row[0] if row else None

    def get_links_by_type(self, table_name: str) -> List[LinkRecord]:
        """
        按类型筛选links

        Args:
            table_name: 类型名 (undergraduate/graduate/vocational)

        Returns:
            LinkRecord列表
        """
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM links WHERE table_name = :table_name ORDER BY id"),
                {"table_name": table_name}
            )
            return [self._row_to_record(row) for row in result]

    def get_link_by_id(self, link_id: int) -> Optional[LinkRecord]:
        """
        获取单条记录

        Args:
            link_id: 记录ID

        Returns:
            LinkRecord或None
        """
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM links WHERE id = :id"),
                {"id": link_id}
            )
            row = result.fetchone()
            return self._row_to_record(row) if row else None

    def get_links_by_ids(self, link_ids: List[int]) -> List[LinkRecord]:
        """
        批量获取记录

        Args:
            link_ids: ID列表

        Returns:
            LinkRecord列表
        """
        if not link_ids:
            return []
        self.connect()
        with self.engine.connect() as conn:
            # 使用 ANY 数组语法
            result = conn.execute(
                text("SELECT * FROM links WHERE id = ANY(:ids) ORDER BY id"),
                {"ids": link_ids}
            )
            return [self._row_to_record(row) for row in result]

    def get_total_count(self) -> int:
        """获取总记录数"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM links"))
            return result.scalar()

    def get_count_by_type(self, table_name: str) -> int:
        """获取指定类型的记录数"""
        self.connect()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM links WHERE table_name = :table_name"),
                {"table_name": table_name}
            )
            return result.scalar()


# 测试代码
if __name__ == "__main__":
    db = SourceDatabase()

    print("连接数据库...")
    db.connect()

    print(f"总记录数: {db.get_total_count()}")
    print(f"undergraduate: {db.get_count_by_type('undergraduate')}")
    print(f"graduate: {db.get_count_by_type('graduate')}")
    print(f"vocational: {db.get_count_by_type('vocational')}")

    print("\n前5条记录 (已过滤 vocational):")
    links = db.get_all_links()[:5]
    for link in links:
        # 获取学校名称
        school_name = db.get_school_name(link.table_name, link.row_id)
        print(f"  [{link.id}] {link.table_name}: {school_name} - {link.url[:50]}...")

    db.close()
    print("\n连接已关闭")
