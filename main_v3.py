"""
OverView V0.3 主程序
云端数据库驱动的自动化数据采集与文件处理系统
"""

import os
import sys
import time
import io

# 确保 stdout/stderr 使用 UTF-8 编码
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入模块
from OverView import OverView, overViewInit, OKNoise, CHECK_Noise, ERROR_Noise
from OverView import DEBPrint, DEBAnimaPrint, _DP

from db.source_db import SourceDatabase
from db.target_db import TargetDatabase
from sync.incremental_sync import IncrementalSync
from storage.supabase_storage import SupabaseStorage
from storage.downloader import FileDownloader
from processor.llm_renamer import LLMRenamer

import Sdata


class OverViewV3:
    """OverView V0.3 主控制器"""

    def __init__(self):
        """初始化"""
        # 数据库连接
        self.source_db = SourceDatabase()
        self.target_db = TargetDatabase()
        self.sync = IncrementalSync(self.source_db, self.target_db)

        # 存储和处理 (is_public=False 表示私有bucket，使用签名URL访问)
        self.storage = SupabaseStorage(is_public=False)
        self.downloader = FileDownloader()
        self.renamer = LLMRenamer(api_key=Sdata.Dou_Bao_Key)

        # Chrome 实例
        self.chrome = None

        # 配置
        self.crawl_depth = 1  # 爬取深度
        self.enable_download = True  # 是否下载文件
        self.enable_rename = True  # 是否LLM重命名
        self.llm_workers = 1  # LLM 并行处理数量

    def initialize(self):
        """初始化所有连接"""
        print("=" * 60)
        print("OverView V0.3 初始化")
        print("=" * 60)

        # 连接数据库
        print("[Init] 连接源数据库 (Railway)...")
        self.source_db.connect()

        print("[Init] 连接目标数据库 (Supabase)...")
        self.target_db.connect()

        # 初始化 Storage
        print("[Init] 初始化 Supabase Storage...")
        try:
            self.storage.connect()
            self.storage.ensure_bucket_exists()
        except Exception as e:
            print(f"[Warning] Storage 初始化失败: {e}")
            print("[Warning] 文件上传功能将不可用")

        # 初始化 Chrome
        print("[Init] 初始化 Chrome 浏览器...")
        self.chrome = overViewInit()

        print("[Init] 初始化完成!")
        print("=" * 60)

    def run_sync_detection(self) -> list:
        """
        运行增量同步检测

        Returns:
            待处理的 LinkRecord 列表
        """
        print("\n[Phase 1] 增量同步检测")
        print("-" * 40)

        pending_links = self.sync.get_pending_links(
            include_failed=True,
            include_changed=True
        )

        if not pending_links:
            print("[Sync] 没有待处理的任务")
            return []

        print(f"[Sync] 发现 {len(pending_links)} 个待处理任务")
        return pending_links

    def crawl_single_link(self, link, task_id: int):
        """
        爬取单个链接

        Args:
            link: LinkRecord
            task_id: 任务ID
        """
        print(f"\n[Crawl] 开始爬取: {link.url[:60]}...")

        try:
            # 更新状态为 crawling
            self.target_db.update_task_status(task_id, 'crawling')

            # 创建 OverView 实例
            sign = f"task_{task_id}"
            ov = OverView(link.url, self.crawl_depth, sign)
            ov.SetOriUrl(link.url)

            # 启动爬取
            ov.start(self.chrome)

            # 执行爬取 (修改后的 Seek 会返回节点数据)
            nodes_data = self._seek_to_db(ov, task_id)

            # 执行剪枝
            pruned_indices = self._pruning_to_db(ov, task_id)

            # 上传可视化 HTML
            self._upload_visualization(ov, task_id)

            # 更新任务状态
            self.target_db.update_task_status(
                task_id, 'completed',
                node_count=len(nodes_data),
                pruned_count=len(pruned_indices)
            )

            # 清理
            ov.end()

            print(f"[Crawl] 完成! 节点: {len(nodes_data)}, 剪枝后: {len(pruned_indices)}")
            CHECK_Noise()

        except Exception as e:
            import traceback
            print(f"[Crawl] 错误: {e}")
            print("[Crawl] 详细堆栈:")
            traceback.print_exc()
            self.target_db.update_task_status(
                task_id, 'failed',
                error_message=str(e)[:500]  # 限制长度避免数据库问题
            )
            ERROR_Noise()

    def _seek_to_db(self, ov: OverView, task_id: int) -> list:
        """
        执行爬取并将结果写入数据库

        Returns:
            节点数据列表
        """
        # 调用原始 Seek
        ov.Seek()

        # 从 URL_RLAB 提取节点数据
        nodes_data = []
        for key in ov.URL_RLAB.keys():
            node = ov.URL_RLAB[key]
            # [url, index, fatherIndex, depth, title, breadcrumb, message]
            nodes_data.append({
                'Index': node[1],
                'FatherIndex': node[2],
                'Depth': node[3],
                'title': node[4],
                'Breadcrumb': node[5],
                'Url': node[0],
                'FatherTitle': ov.URL_RLAB[str(node[2])][4] if node[2] != -1 else ""
            })

        # 批量写入数据库
        self.target_db.batch_insert_nodes(task_id, nodes_data)

        return nodes_data

    def _pruning_to_db(self, ov: OverView, task_id: int) -> list:
        """
        执行剪枝并更新数据库

        Returns:
            剪枝保留的节点索引列表
        """
        # 调用原始 Pruning
        ov.Pruning()

        # 读取剪枝后的 CSV 获取保留的索引
        import csv
        cleaned_csv = ov.MemPath + "/" + Sdata.CSVCLEANED_FILENAME
        pruned_indices = []

        try:
            with open(cleaned_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pruned_indices.append(int(row['Index']))
        except Exception as e:
            print(f"[Pruning] 读取剪枝结果失败: {e}")

        # 更新数据库
        self.target_db.mark_nodes_pruned(task_id, pruned_indices)

        return pruned_indices

    def _upload_visualization(self, ov: OverView, task_id: int):
        """上传可视化 HTML 到 Storage"""
        try:
            # 原始拓扑图
            raw_html_path = ov.MemPath + "/" + Sdata.HTML_FILENAME
            if os.path.exists(raw_html_path):
                with open(raw_html_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                remote_path = f"task_{task_id}/visualization_raw.html"
                storage_path = self.storage.upload_html(html_content, remote_path)
                self.target_db.save_visualization(task_id, 'raw', storage_path)
                print(f"[Upload] 原始拓扑图已上传: {remote_path}")

            # 剪枝后拓扑图
            pruned_html_path = ov.MemPath + "/" + Sdata.HTMLED_FILENAME
            if os.path.exists(pruned_html_path):
                with open(pruned_html_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                remote_path = f"task_{task_id}/visualization_pruned.html"
                storage_path = self.storage.upload_html(html_content, remote_path)
                self.target_db.save_visualization(task_id, 'pruned', storage_path)
                print(f"[Upload] 剪枝拓扑图已上传: {remote_path}")

        except Exception as e:
            print(f"[Upload] 可视化上传失败: {e}")

    def download_files(self, task_id: int):
        """
        下载任务的文件

        Args:
            task_id: 任务ID
        """
        if not self.enable_download:
            return

        print(f"\n[Download] 开始下载文件 (task_id={task_id})")

        # 获取剪枝后的文件节点
        file_nodes = self.target_db.get_file_nodes(task_id, pruned_only=True)

        if not file_nodes:
            print("[Download] 没有需要下载的文件")
            return

        print(f"[Download] 发现 {len(file_nodes)} 个文件")

        for node in file_nodes:
            try:
                # 创建文件记录
                file_id = self.target_db.create_file_record(
                    task_id=task_id,
                    node_id=node.id,
                    original_url=node.url,
                    original_name=node.title,
                    file_extension=node.file_extension
                )

                # 下载文件
                result = self.downloader.download_file(
                    node.url,
                    task_folder=f"task_{task_id}"
                )

                if result.success:
                    # 上传到 Storage
                    remote_path = f"task_{task_id}/raw/{result.file_name}"
                    storage_path = self.storage.upload_file(result.local_path, remote_path)

                    # 更新数据库 (存储路径格式: bucket/path)
                    self.target_db.update_file_download(
                        file_id, 'completed',
                        storage_path=storage_path,
                        file_size=result.file_size
                    )
                    print(f"[Download] 成功: {result.file_name}")
                else:
                    self.target_db.update_file_download(
                        file_id, 'failed',
                        error_message=result.error_message
                    )
                    print(f"[Download] 失败: {result.error_message}")

            except Exception as e:
                print(f"[Download] 错误: {e}")

        # 更新任务的文件数
        file_count = len([n for n in file_nodes])
        self.target_db.update_task_status(task_id, 'completed', file_count=file_count)

    def _process_single_file(self, file_record: dict, task_id: int, school_name: str):
        """
        处理单个文件的 LLM 重命名（供并行调用）

        Args:
            file_record: 文件记录
            task_id: 任务ID
            school_name: 学校名称
        """
        from processor.llm_renamer import LLMRenamer
        from storage.supabase_storage import SupabaseStorage
        from db.target_db import TargetDatabase

        # 每个线程创建独立的连接（线程安全）
        thread_db = TargetDatabase()
        thread_db.connect()

        thread_storage = SupabaseStorage(is_public=False)
        thread_storage.connect()

        thread_renamer = LLMRenamer(api_key=Sdata.Dou_Bao_Key)

        try:
            # 构建上下文
            context = {
                'url': file_record['original_url'],
                'original_name': file_record['original_name'] or '',
                'breadcrumb': '',
                'title': file_record['original_name'] or '',
                'parent_title': '',
                'school_name': school_name or 'Unknown'
            }

            # 下载文件到本地
            local_path = os.path.join(
                self.downloader.download_dir,
                f"task_{task_id}",
                "raw",
                f"thread_{file_record['id']}_{os.path.basename(file_record['storage_path'] or 'temp.pdf')}"
            )

            # 从 Storage 下载
            if file_record['storage_path']:
                remote_path = file_record['storage_path'].split(thread_storage.bucket + '/')[-1]
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                thread_storage.download_file(remote_path, local_path)

            # LLM 重命名
            result = thread_renamer.rename_file(local_path, context)

            if result.success and result.renamed_name:
                thread_db.update_file_renamed(
                    file_record['id'],
                    renamed_name=result.renamed_name,
                    llm_model=thread_renamer.model,
                    llm_confidence=result.confidence,
                    llm_raw_response=result.raw_response
                )
                print(f"[Process] ✓ {result.renamed_name}")
            else:
                error_msg = result.error_message or "LLM 返回空文件名"
                thread_db.update_file_process_failed(file_record['id'], error_message=error_msg)
                print(f"[Process] ✗ {error_msg}")

            # 删除临时文件
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass

            return True

        except Exception as e:
            thread_db.update_file_process_failed(
                file_record['id'],
                error_message=f"处理异常: {str(e)[:200]}"
            )
            print(f"[Process] ✗ 错误: {e}")
            return False

        finally:
            thread_db.close()

    def process_files(self, task_id: int):
        """
        处理文件 (LLM重命名) - 支持并行处理

        Args:
            task_id: 任务ID
        """
        if not self.enable_rename:
            return

        print(f"\n[Process] 开始 LLM 重命名 (task_id={task_id})")

        # 获取任务的学校名称
        task = self.target_db.get_task_by_id(task_id)
        school_name = task.school_name if task else None
        if school_name:
            print(f"[Process] 使用确定学校名: {school_name}")

        # 获取待处理的文件
        pending_files = self.target_db.get_pending_process_files(task_id)

        if not pending_files:
            print("[Process] 没有待处理的文件")
            return

        print(f"[Process] 发现 {len(pending_files)} 个待处理文件")

        # 根据配置选择串行或并行处理
        if self.llm_workers <= 1:
            # 串行处理（原有逻辑）
            for file_record in pending_files:
                self._process_single_file(file_record, task_id, school_name)
        else:
            # 并行处理
            from concurrent.futures import ThreadPoolExecutor, as_completed

            print(f"[Process] 使用 {self.llm_workers} 个并行线程")

            with ThreadPoolExecutor(max_workers=self.llm_workers) as executor:
                futures = {
                    executor.submit(self._process_single_file, f, task_id, school_name): f
                    for f in pending_files
                }

                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    print(f"[Process] 进度: {completed}/{len(pending_files)}")

        print(f"[Process] LLM 重命名完成")

    def fill_unknown_names(self, task_id: int):
        """
        补充任务中 Unknown 的字段

        新命名格式: {大学名}_{所属}_{専攻}_{課程}_{年度}_{入学時期}_{文書種別}_{詳細}.{拡張子}
        字段索引:      0       1      2      3      4       5          6        7

        同一个任务爬取的是同一个学校/专业的页面，
        所以可以用已成功识别的信息来填补 Unknown 的部分。
        主要补充: 大学名(0)、所属(1)、専攻(2)

        Args:
            task_id: 任务ID
        """
        print(f"\n[FillUnknown] 检查并补充 Unknown 字段 (task_id={task_id})")

        # 获取任务中所有已处理的文件
        files = self.target_db.get_task_files_with_llm_result(task_id)

        if not files:
            print("[FillUnknown] 没有已处理的文件")
            return

        # 收集已识别的各字段值（使用 Counter 统计频率）
        from collections import Counter
        universities = Counter()
        departments = Counter()
        majors = Counter()

        for f in files:
            if f['llm_raw_response']:
                try:
                    from json_repair import loads as json_loads
                    result = json_loads(f['llm_raw_response'])

                    univ = result.get('university', '')
                    dept = result.get('department', '')
                    major = result.get('major', '')

                    # 只收集非 Unknown 的值
                    if univ and univ != 'Unknown':
                        universities[univ] += 1
                    if dept and dept != 'Unknown':
                        departments[dept] += 1
                    if major and major != 'Unknown' and major != '全専攻':
                        majors[major] += 1
                except:
                    pass

        if not universities and not departments and not majors:
            print("[FillUnknown] 没有可用的补充信息")
            return

        # 选择最常见的值
        fill_university = universities.most_common(1)[0][0] if universities else None
        fill_department = departments.most_common(1)[0][0] if departments else None
        fill_major = majors.most_common(1)[0][0] if majors else None

        print(f"[FillUnknown] 可用补充信息:")
        print(f"  大学: {fill_university}")
        print(f"  所属: {fill_department}")
        print(f"  専攻: {fill_major}")

        # 补充 Unknown 的文件名
        updated_count = 0
        for f in files:
            renamed = f['renamed_name']
            if not renamed or 'Unknown' not in renamed:
                continue

            # 分离文件名和扩展名
            if '.' in renamed:
                name_part, ext_part = renamed.rsplit('.', 1)
                ext_part = '.' + ext_part
            else:
                name_part = renamed
                ext_part = ''

            parts = name_part.split('_')
            needs_update = False

            # 补充各字段 (按新格式的索引位置)
            # 索引 0: 大学名
            if fill_university and len(parts) > 0 and parts[0] == 'Unknown':
                parts[0] = fill_university
                needs_update = True

            # 索引 1: 所属
            if fill_department and len(parts) > 1 and parts[1] == 'Unknown':
                parts[1] = fill_department
                needs_update = True

            # 索引 2: 専攻
            if fill_major and len(parts) > 2 and parts[2] == 'Unknown':
                parts[2] = fill_major
                needs_update = True

            if needs_update:
                new_name = '_'.join(parts) + ext_part
                if new_name != renamed:
                    self.target_db.update_renamed_name_only(f['id'], new_name)
                    print(f"[FillUnknown] 更新:")
                    print(f"  旧: {renamed}")
                    print(f"  新: {new_name}")
                    updated_count += 1

        print(f"[FillUnknown] 完成，更新了 {updated_count} 个文件名")

    def cleanup_task_temp_files(self, task_id: int):
        """
        清理任务的临时下载文件

        Args:
            task_id: 任务ID
        """
        import shutil

        task_temp_dir = os.path.join(self.downloader.download_dir, f"task_{task_id}")

        if os.path.exists(task_temp_dir):
            try:
                # 统计要删除的文件
                file_count = sum(len(files) for _, _, files in os.walk(task_temp_dir))
                dir_size = sum(
                    os.path.getsize(os.path.join(dirpath, filename))
                    for dirpath, _, filenames in os.walk(task_temp_dir)
                    for filename in filenames
                )

                # 删除整个目录
                shutil.rmtree(task_temp_dir)
                print(f"[Cleanup] 已清理任务临时目录: task_{task_id} ({file_count} 个文件, {dir_size / 1024:.1f}KB)")
            except Exception as e:
                print(f"[Cleanup] 清理任务临时目录失败: {e}")

    def run(self, link_type: str = None, max_tasks: int = None):
        """
        运行主流程

        Args:
            link_type: 筛选类型 (undergraduate/graduate/vocational)
            max_tasks: 最大处理任务数
        """
        start_time = time.time()

        try:
            # 初始化
            self.initialize()

            # Phase 1: 增量检测
            pending_links = self.run_sync_detection()

            if link_type:
                pending_links = [l for l in pending_links if l.table_name == link_type]

            if max_tasks:
                pending_links = pending_links[:max_tasks]

            if not pending_links:
                print("\n没有待处理任务，程序结束")
                return

            # Phase 2: 爬取
            print(f"\n[Phase 2] 开始爬取 ({len(pending_links)} 个任务)")
            print("-" * 40)

            for i, link in enumerate(pending_links):
                print(f"\n{'=' * 50}")
                print(f"任务 {i + 1}/{len(pending_links)}")
                print(f"{'=' * 50}")

                # 准备任务
                task_id = self.sync.prepare_task_for_link(link)

                # 爬取
                self.crawl_single_link(link, task_id)

                # Phase 3: 下载文件
                if self.enable_download:
                    self.download_files(task_id)

                # Phase 4: LLM 处理
                if self.enable_rename:
                    self.process_files(task_id)
                    # Phase 4.5: 补充 Unknown 字段
                    self.fill_unknown_names(task_id)

                # Phase 5: 清理临时文件
                self.cleanup_task_temp_files(task_id)

                OKNoise()

            # 完成
            elapsed = (time.time() - start_time) / 60
            print(f"\n{'=' * 60}")
            print(f"所有任务完成! 耗时: {elapsed:.2f} 分钟")
            print(f"{'=' * 60}")
            CHECK_Noise()

        except KeyboardInterrupt:
            print("\n用户中断")
        except Exception as e:
            print(f"\n程序错误: {e}")
            ERROR_Noise()
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """清理资源"""
        print("\n[Cleanup] 清理资源...")

        # 关闭数据库连接
        self.source_db.close()
        self.target_db.close()

        # 关闭浏览器
        if self.chrome:
            try:
                self.chrome.quit()
            except:
                pass

        # 清理临时文件 (可选)
        # self.downloader.cleanup_temp_files()

        print("[Cleanup] 完成")


def main():
    """主入口"""
    import argparse

    parser = argparse.ArgumentParser(description='OverView V0.3 - 大学招考信息采集系统')
    parser.add_argument('--type', '-t', choices=['undergraduate', 'graduate', 'vocational'],
                        help='筛选链接类型')
    parser.add_argument('--max', '-m', type=int, help='最大处理任务数')
    parser.add_argument('--depth', '-d', type=int, default=1, help='爬取深度 (默认1)')
    parser.add_argument('--no-download', action='store_true', help='禁用文件下载')
    parser.add_argument('--no-rename', action='store_true', help='禁用LLM重命名')
    parser.add_argument('--workers', '-w', type=int, default=1,
                        help='LLM 并行处理线程数 (默认1，建议3-5)')

    args = parser.parse_args()

    # 创建控制器
    controller = OverViewV3()
    controller.crawl_depth = args.depth
    controller.enable_download = not args.no_download
    controller.enable_rename = not args.no_rename
    controller.llm_workers = args.workers

    if args.workers > 1:
        print(f"[Config] LLM 并行处理: {args.workers} 线程")

    # 运行
    controller.run(link_type=args.type, max_tasks=args.max)


if __name__ == "__main__":
    main()
