#!/usr/bin/env python
"""
爬虫脚本 - 只做爬取和文件下载
适合在大内存服务器上运行（不需要 GPU）

功能：
1. 爬取网页
2. 剪枝
3. 下载文件到 Supabase Storage
4. 记录到数据库（等待后续重命名）
"""

import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime
from multiprocessing import Process, Queue, Manager, cpu_count
from queue import Empty
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 日志配置
# ============================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging(level: str = "INFO"):
    log_file = os.path.join(LOG_DIR, f"crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s [%(levelname)s] [%(processName)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


# ============================================================
# Chrome 爬虫 Worker
# ============================================================

def chrome_worker(worker_id, task_queue, result_dict, config, stop_event):
    """Chrome 爬虫 Worker - 只做爬取和下载"""
    logger = logging.getLogger(f"Chrome-{worker_id}")
    logger.info(f"Chrome Worker {worker_id} 启动")

    from OverView import OverView, overViewInit
    from db.target_db import TargetDatabase
    from storage.downloader import FileDownloader
    from storage.supabase_storage import SupabaseStorage
    import Sdata

    chrome = None
    target_db = None

    try:
        chrome = overViewInit()
        target_db = TargetDatabase()
        target_db.connect()

        downloader = FileDownloader()
        storage = SupabaseStorage(is_public=False)
        storage.connect()

        while not stop_event.is_set():
            try:
                link_data = task_queue.get(timeout=5)

                if link_data is None:
                    logger.info(f"Chrome Worker {worker_id} 收到停止信号")
                    break

                link_id, link_url, link_type = link_data
                logger.info(f"[Worker-{worker_id}] 开始爬取 link_id={link_id}")

                # 检查/创建任务
                existing_task = target_db.get_task_by_source_id(link_id)
                if existing_task:
                    task_id = existing_task.id
                else:
                    task_id = target_db.create_task(
                        source_link_id=link_id,
                        source_url=link_url,
                        school_name=None
                    )

                target_db.update_task_status(task_id, 'crawling')

                # 爬取
                sign = f"task_{task_id}"
                ov = OverView(link_url, depth=config['crawl_depth'], sign=sign)
                ov.SetOriUrl(link_url)
                ov.start(chrome)
                ov.Seek()
                ov.Pruning()

                node_count = len(ov.URL_RLAB)

                # 读取剪枝结果
                import csv
                cleaned_csv = ov.MemPath + "/" + Sdata.CSVCLEANED_FILENAME
                pruned_indices = []
                try:
                    with open(cleaned_csv, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            pruned_indices.append(int(row['Index']))
                except Exception as e:
                    logger.warning(f"[Worker-{worker_id}] 读取剪枝结果失败: {e}")

                # 保存节点
                nodes_data = []
                for key in ov.URL_RLAB.keys():
                    node = ov.URL_RLAB[key]
                    nodes_data.append({
                        'Index': node[1],
                        'FatherIndex': node[2],
                        'Depth': node[3],
                        'title': node[4],
                        'Breadcrumb': node[5],
                        'Url': node[0],
                        'FatherTitle': ov.URL_RLAB[str(node[2])][4] if node[2] != -1 and str(node[2]) in ov.URL_RLAB else ""
                    })

                if nodes_data:
                    target_db.batch_insert_nodes(task_id, nodes_data)

                if pruned_indices:
                    target_db.mark_nodes_pruned(task_id, pruned_indices)

                ov.end()
                logger.info(f"[Worker-{worker_id}] 爬取完成, 节点数={node_count}")

                # 下载文件
                file_nodes = target_db.get_file_nodes(task_id, pruned_only=True)
                downloaded_count = 0

                for node in file_nodes:
                    try:
                        file_id = target_db.create_file_record(
                            task_id=task_id,
                            node_id=node.id,
                            original_url=node.url,
                            original_name=node.title,
                            file_extension=node.file_extension
                        )

                        result = downloader.download_file(node.url, task_folder=f"task_{task_id}")

                        if result.success:
                            remote_path = f"task_{task_id}/raw/{result.file_name}"
                            storage_path = storage.upload_file(result.local_path, remote_path)

                            # 状态设为 downloaded（等待重命名）
                            target_db.update_file_download(
                                file_id, 'downloaded',
                                storage_path=storage_path,
                                file_size=result.file_size
                            )
                            downloaded_count += 1
                            logger.info(f"[Worker-{worker_id}] 下载成功: {result.file_name}")

                            # 删除本地临时文件
                            try:
                                os.remove(result.local_path)
                            except:
                                pass
                        else:
                            target_db.update_file_download(file_id, 'failed', error_message=result.error_message)

                    except Exception as e:
                        logger.error(f"[Worker-{worker_id}] 下载失败: {e}")

                # 更新任务状态为 downloaded（等待重命名）
                target_db.update_task_status(task_id, 'downloaded', node_count=node_count, file_count=downloaded_count)

                result_dict[link_id] = {
                    'success': True,
                    'task_id': task_id,
                    'node_count': node_count,
                    'file_count': downloaded_count
                }

                logger.info(f"[Worker-{worker_id}] 完成 task_id={task_id}, 文件数={downloaded_count}")

            except Empty:
                continue
            except Exception as e:
                logger.error(f"[Worker-{worker_id}] 错误: {e}")
                import traceback
                traceback.print_exc()
                continue

    except Exception as e:
        logger.error(f"Chrome Worker {worker_id} 初始化失败: {e}")

    finally:
        if chrome:
            try:
                chrome.quit()
            except:
                pass
        if target_db:
            try:
                target_db.close()
            except:
                pass
        logger.info(f"Chrome Worker {worker_id} 退出")


# ============================================================
# 主程序
# ============================================================

class Crawler:
    """爬虫协调器"""

    def __init__(self, chrome_workers=4, crawl_depth=1, batch_size=20,
                 link_type=None, max_batches=0, rest_time=30):
        self.chrome_workers = chrome_workers
        self.crawl_depth = crawl_depth
        self.batch_size = batch_size
        self.link_type = link_type
        self.max_batches = max_batches
        self.rest_time = rest_time

        self.logger = setup_logging()
        self.manager = Manager()
        self.task_queue = Queue()
        self.result_dict = self.manager.dict()
        self.stop_event = self.manager.Event()
        self.processes = []

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.logger.warning("收到中断信号，正在安全退出...")
        self.stop()

    def start_workers(self):
        config = {'crawl_depth': self.crawl_depth}

        self.logger.info(f"启动 {self.chrome_workers} 个 Chrome Worker...")
        for i in range(self.chrome_workers):
            p = Process(
                target=chrome_worker,
                args=(i, self.task_queue, self.result_dict, config, self.stop_event),
                name=f"Chrome-{i}"
            )
            p.start()
            self.processes.append(p)
            time.sleep(2)

        self.logger.info("所有 Worker 已启动")

    def stop(self):
        self.logger.info("正在停止所有 Worker...")
        self.stop_event.set()

        for _ in range(self.chrome_workers):
            self.task_queue.put(None)

        for p in self.processes:
            p.join(timeout=30)

        self.logger.info("所有 Worker 已停止")

    def get_pending_links(self, limit):
        from db.source_db import SourceDatabase
        from db.target_db import TargetDatabase
        from sync.incremental_sync import IncrementalSync

        source_db = SourceDatabase()
        source_db.connect()
        target_db = TargetDatabase()
        target_db.connect()

        sync = IncrementalSync(source_db, target_db)
        pending = sync.get_pending_links(include_failed=True, include_changed=True)

        if self.link_type:
            pending = [l for l in pending if l.table_name == self.link_type]

        pending = pending[:limit]
        result = [(l.id, l.url, l.table_name) for l in pending]

        source_db.close()
        target_db.close()

        return result

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("爬虫启动（只爬取，不重命名）")
        self.logger.info(f"  Chrome Workers: {self.chrome_workers}")
        self.logger.info(f"  Crawl Depth:    {self.crawl_depth}")
        self.logger.info(f"  Batch Size:     {self.batch_size}")
        self.logger.info("=" * 60)

        self.start_workers()

        batch_count = 0
        total_queued = 0
        start_time = time.time()

        try:
            while not self.stop_event.is_set():
                if self.max_batches > 0 and batch_count >= self.max_batches:
                    self.logger.info(f"已达到最大批次数 {self.max_batches}")
                    break

                queue_size = self.task_queue.qsize()

                if queue_size < self.chrome_workers * 2:
                    pending = self.get_pending_links(self.batch_size)

                    if not pending:
                        if queue_size == 0 and len(self.result_dict) >= total_queued:
                            self.logger.info("没有更多待处理任务")
                            break
                        else:
                            time.sleep(5)
                            continue

                    batch_count += 1
                    self.logger.info(f"\n===== 批次 {batch_count} ({len(pending)} 个任务) =====")

                    for link_data in pending:
                        self.task_queue.put(link_data)

                    total_queued += len(pending)

                time.sleep(10)

                completed = len(self.result_dict)
                elapsed = (time.time() - start_time) / 60
                rate = completed / elapsed if elapsed > 0 else 0

                self.logger.info(f"[进度] 完成: {completed}/{total_queued} | 速率: {rate:.1f}/分钟")

        except KeyboardInterrupt:
            self.logger.info("用户中断")

        finally:
            self.logger.info("等待剩余任务完成...")
            time.sleep(10)
            self.stop()

            elapsed = (time.time() - start_time) / 60
            self.logger.info("\n" + "=" * 60)
            self.logger.info("爬虫结束")
            self.logger.info(f"总批次: {batch_count}")
            self.logger.info(f"总处理: {len(self.result_dict)}")
            self.logger.info(f"总耗时: {elapsed:.1f} 分钟")
            self.logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='爬虫脚本 - 只做爬取和下载')

    parser.add_argument('--chrome', '-c', type=int, default=4, help='Chrome 进程数')
    parser.add_argument('--depth', '-d', type=int, default=1, help='爬取深度')
    parser.add_argument('--batch', '-b', type=int, default=20, help='每批任务数')
    parser.add_argument('--type', '-t', choices=['graduate', 'undergraduate'], help='只处理指定类型')
    parser.add_argument('--max-batches', '-m', type=int, default=0, help='最大批次数')
    parser.add_argument('--rest', '-r', type=int, default=30, help='批次间休息秒数')
    parser.add_argument('--status', '-s', action='store_true', help='显示进度')

    args = parser.parse_args()

    if args.status:
        from db.source_db import SourceDatabase
        from db.target_db import TargetDatabase

        source_db = SourceDatabase()
        source_db.connect()
        target_db = TargetDatabase()
        target_db.connect()

        graduate = source_db.get_count_by_type('graduate')
        undergraduate = source_db.get_count_by_type('undergraduate')
        total = graduate + undergraduate
        completed = len(target_db.get_tasks_by_status('completed'))
        downloaded = len(target_db.get_tasks_by_status('downloaded'))
        crawling = len(target_db.get_tasks_by_status('crawling'))
        failed = len(target_db.get_tasks_by_status('failed'))

        print(f"""
========== 爬虫进度 ==========
总任务数:     {total}
已完成重命名: {completed}
已下载待重命名: {downloaded}
正在爬取:     {crawling}
已失败:       {failed}
剩余:         {total - completed - downloaded}
==============================
        """)
        return

    crawler = Crawler(
        chrome_workers=args.chrome,
        crawl_depth=args.depth,
        batch_size=args.batch,
        link_type=args.type,
        max_batches=args.max_batches,
        rest_time=args.rest
    )
    crawler.run()


if __name__ == "__main__":
    main()
