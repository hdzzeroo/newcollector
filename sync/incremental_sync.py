"""
增量同步检测器
检测新增、变更、失败的任务
"""

from typing import List, Tuple, Dict
from dataclasses import dataclass
import hashlib

from db.source_db import SourceDatabase, LinkRecord
from db.target_db import TargetDatabase, TaskRecord


@dataclass
class SyncResult:
    """同步检测结果"""
    new_links: List[LinkRecord]       # 新增的链接
    changed_links: List[LinkRecord]   # URL变更的链接
    failed_tasks: List[TaskRecord]    # 失败需重试的任务
    total_source: int                 # 源数据总数
    total_synced: int                 # 已同步数量


class IncrementalSync:
    """增量同步检测器"""

    def __init__(self, source_db: SourceDatabase = None, target_db: TargetDatabase = None):
        """
        初始化同步器

        Args:
            source_db: 输入源数据库连接
            target_db: 输出目标数据库连接
        """
        self.source_db = source_db or SourceDatabase()
        self.target_db = target_db or TargetDatabase()

    def detect_new_links(self) -> List[LinkRecord]:
        """
        检测未爬取的新链接

        Returns:
            新链接列表
        """
        # 获取源数据库所有link ID
        all_links = self.source_db.get_all_links()
        all_source_ids = {link.id for link in all_links}

        # 获取目标数据库已存在的source_link_id
        existing_ids = set(self.target_db.get_all_task_source_ids())

        # 找出差集
        new_ids = all_source_ids - existing_ids

        # 返回新链接
        return [link for link in all_links if link.id in new_ids]

    def detect_changed_links(self) -> List[LinkRecord]:
        """
        检测URL变更的链接

        Returns:
            变更的链接列表
        """
        # 获取所有源链接
        all_links = self.source_db.get_all_links()

        # 构建 {source_link_id: url_hash} 字典
        url_hashes = {
            link.id: hashlib.md5(link.url.encode()).hexdigest()
            for link in all_links
        }

        # 检测变更
        changed_ids = self.target_db.get_changed_tasks(url_hashes)

        # 返回变更的链接
        return [link for link in all_links if link.id in changed_ids]

    def detect_failed_tasks(self) -> List[TaskRecord]:
        """
        检测失败需重试的任务

        Returns:
            失败的任务列表
        """
        return self.target_db.get_tasks_by_status('failed')

    def run_detection(self, include_failed: bool = True) -> SyncResult:
        """
        运行完整的增量检测

        Args:
            include_failed: 是否包含失败任务的重试

        Returns:
            SyncResult 检测结果
        """
        print("[Sync] 开始增量检测...")

        # 连接数据库
        self.source_db.connect()
        self.target_db.connect()

        # 获取源数据总数
        total_source = self.source_db.get_total_count()
        print(f"[Sync] 源数据总数: {total_source}")

        # 检测新增
        new_links = self.detect_new_links()
        print(f"[Sync] 新增链接: {len(new_links)}")

        # 检测变更
        changed_links = self.detect_changed_links()
        print(f"[Sync] 变更链接: {len(changed_links)}")

        # 检测失败
        failed_tasks = []
        if include_failed:
            failed_tasks = self.detect_failed_tasks()
            print(f"[Sync] 失败任务: {len(failed_tasks)}")

        # 已同步数量
        total_synced = total_source - len(new_links)

        # 记录同步日志
        self.target_db.log_sync(
            sync_type='incremental',
            source_count=total_source,
            new_count=len(new_links),
            changed_count=len(changed_links)
        )

        return SyncResult(
            new_links=new_links,
            changed_links=changed_links,
            failed_tasks=failed_tasks,
            total_source=total_source,
            total_synced=total_synced
        )

    def get_pending_links(self, include_failed: bool = True,
                          include_changed: bool = True,
                          link_type: str = None,
                          deduplicate: bool = True) -> List[LinkRecord]:
        """
        获取所有待处理的链接（合并新增、变更、失败）

        Args:
            include_failed: 是否包含失败重试
            include_changed: 是否包含变更重爬
            link_type: 筛选类型 (undergraduate/graduate/vocational)
            deduplicate: 是否按URL去重（默认True）

        Returns:
            待处理的LinkRecord列表
        """
        result = self.run_detection(include_failed=include_failed)

        # 合并所有待处理链接
        pending_links = list(result.new_links)

        if include_changed:
            # 变更的链接直接添加
            for link in result.changed_links:
                if link not in pending_links:
                    pending_links.append(link)

        if include_failed:
            # 失败任务需要通过source_link_id获取对应的LinkRecord
            failed_source_ids = [task.source_link_id for task in result.failed_tasks]
            if failed_source_ids:
                failed_links = self.source_db.get_links_by_ids(failed_source_ids)
                for link in failed_links:
                    if link not in pending_links:
                        pending_links.append(link)

        # 类型筛选
        if link_type:
            pending_links = [link for link in pending_links if link.table_name == link_type]

        # URL 去重：相同 URL 只保留第一条
        if deduplicate:
            before_count = len(pending_links)
            seen_urls = set()
            unique_links = []
            for link in pending_links:
                if link.url not in seen_urls:
                    seen_urls.add(link.url)
                    unique_links.append(link)
            pending_links = unique_links
            duplicates_removed = before_count - len(pending_links)
            if duplicates_removed > 0:
                print(f"[Sync] URL去重: 移除 {duplicates_removed} 个重复链接")

        print(f"[Sync] 待处理链接总数: {len(pending_links)}")
        return pending_links

    def prepare_task_for_link(self, link: LinkRecord) -> int:
        """
        为链接准备任务记录

        Args:
            link: 链接记录

        Returns:
            task_id
        """
        # 检查是否需要清理旧数据
        existing_task = self.target_db.get_task_by_source_id(link.id)
        if existing_task:
            # 如果是变更或失败重试，删除旧数据
            print(f"[Sync] 清理旧任务数据: task_id={existing_task.id}")
            self.target_db.delete_task_data(existing_task.id)

        # 获取学校名称
        school_name = self.source_db.get_school_name(link.table_name, link.row_id)
        if school_name:
            print(f"[Sync] 获取学校名称: {school_name}")

        # 创建新任务（包含学校名称）
        task_id = self.target_db.create_task(link.id, link.url, school_name)
        print(f"[Sync] 创建任务: source_link_id={link.id} -> task_id={task_id}")
        return task_id

    def close(self):
        """关闭数据库连接"""
        self.source_db.close()
        self.target_db.close()


# 测试代码
if __name__ == "__main__":
    sync = IncrementalSync()

    print("=" * 50)
    print("运行增量检测")
    print("=" * 50)

    result = sync.run_detection()

    print(f"\n检测结果:")
    print(f"  - 源数据总数: {result.total_source}")
    print(f"  - 已同步数量: {result.total_synced}")
    print(f"  - 新增链接: {len(result.new_links)}")
    print(f"  - 变更链接: {len(result.changed_links)}")
    print(f"  - 失败任务: {len(result.failed_tasks)}")

    if result.new_links:
        print(f"\n新增链接示例 (前3条):")
        for link in result.new_links[:3]:
            print(f"  [{link.id}] {link.table_name}: {link.url[:60]}...")

    sync.close()
