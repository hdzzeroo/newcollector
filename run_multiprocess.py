#!/usr/bin/env python
"""
多进程批量处理脚本
支持多个 Chrome 实例并行爬取
"""

import os
import sys
import time
import signal
import logging
from multiprocessing import Pool, Manager, cpu_count
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# 配置日志
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"multi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [PID:%(process)d] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def crawl_worker(args):
    """
    单个爬虫 worker（每个进程独立的 Chrome 实例）
    """
    link_id, link_url, link_type, worker_id = args

    # 每个进程独立导入和初始化
    from OverView import OverView, overViewInit
    from db.target_db import TargetDatabase
    from storage.downloader import FileDownloader
    from storage.supabase_storage import SupabaseStorage
    from processor.llm_renamer import LLMRenamer
    import Sdata

    logger.info(f"[Worker-{worker_id}] 开始处理 link_id={link_id}, url={link_url[:50]}...")

    # 创建独立的 Chrome 实例
    chrome = None
    target_db = None

    try:
        chrome = overViewInit()

        # 创建独立的数据库连接
        target_db = TargetDatabase()
        target_db.connect()

        # 检查是否已有任务，没有则创建
        existing_task = target_db.get_task_by_source_id(link_id)
        if existing_task:
            task_id = existing_task.id
            logger.info(f"[Worker-{worker_id}] 使用已有任务 task_id={task_id}")
        else:
            task_id = target_db.create_task(
                source_link_id=link_id,
                source_url=link_url,
                school_name=None
            )
            logger.info(f"[Worker-{worker_id}] 创建新任务 task_id={task_id}")

        target_db.update_task_status(task_id, 'crawling')

        # 爬取
        sign = f"task_{task_id}"
        ov = OverView(link_url, depth=1, sign=sign)
        ov.SetOriUrl(link_url)
        ov.start(chrome)
        ov.Seek()

        # 剪枝
        ov.Pruning()

        # 获取节点数
        node_count = len(ov.URL_RLAB)

        # 读取剪枝结果，获取保留的节点索引
        import csv
        cleaned_csv = ov.MemPath + "/" + Sdata.CSVCLEANED_FILENAME
        pruned_indices = []
        try:
            with open(cleaned_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pruned_indices.append(int(row['Index']))
            logger.info(f"[Worker-{worker_id}] 剪枝保留 {len(pruned_indices)} 个节点")
        except Exception as e:
            logger.warning(f"[Worker-{worker_id}] 读取剪枝结果失败: {e}")

        # 保存节点到数据库
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

        # 标记剪枝保留的节点
        if pruned_indices:
            target_db.mark_nodes_pruned(task_id, pruned_indices)
            logger.info(f"[Worker-{worker_id}] 已标记 {len(pruned_indices)} 个剪枝节点")

        ov.end()
        logger.info(f"[Worker-{worker_id}] 爬取完成, 节点数={node_count}")

        # ========== 文件下载 ==========
        file_nodes = target_db.get_file_nodes(task_id, pruned_only=True)
        downloaded_files = []

        if file_nodes:
            logger.info(f"[Worker-{worker_id}] 开始下载 {len(file_nodes)} 个文件...")
            downloader = FileDownloader()
            storage = SupabaseStorage(is_public=False)
            storage.connect()

            for node in file_nodes:
                try:
                    # 创建文件记录
                    file_id = target_db.create_file_record(
                        task_id=task_id,
                        node_id=node.id,
                        original_url=node.url,
                        original_name=node.title,
                        file_extension=node.file_extension
                    )

                    # 下载文件
                    result = downloader.download_file(node.url, task_folder=f"task_{task_id}")

                    if result.success:
                        # 上传到 Storage
                        remote_path = f"task_{task_id}/raw/{result.file_name}"
                        storage_path = storage.upload_file(result.local_path, remote_path)

                        target_db.update_file_download(
                            file_id, 'completed',
                            storage_path=storage_path,
                            file_size=result.file_size
                        )
                        downloaded_files.append({
                            'id': file_id,
                            'local_path': result.local_path,
                            'storage_path': storage_path,
                            'original_url': node.url,
                            'original_name': node.title
                        })
                        logger.info(f"[Worker-{worker_id}] 下载成功: {result.file_name}")
                    else:
                        target_db.update_file_download(file_id, 'failed', error_message=result.error_message)
                except Exception as e:
                    logger.error(f"[Worker-{worker_id}] 下载失败: {e}")

        # ========== LLM 重命名 ==========
        if downloaded_files:
            logger.info(f"[Worker-{worker_id}] 开始 LLM 重命名 {len(downloaded_files)} 个文件...")
            renamer = LLMRenamer(api_key=Sdata.Dou_Bao_Key)

            for file_info in downloaded_files:
                try:
                    context = {
                        'url': file_info['original_url'],
                        'original_name': file_info['original_name'] or '',
                        'breadcrumb': '',
                        'title': file_info['original_name'] or '',
                        'parent_title': '',
                        'school_name': ''
                    }

                    rename_result = renamer.rename_file(file_info['local_path'], context)

                    if rename_result.success and rename_result.renamed_name:
                        target_db.update_file_renamed(
                            file_info['id'],
                            renamed_name=rename_result.renamed_name,
                            llm_model=renamer.model,
                            llm_confidence=rename_result.confidence,
                            llm_raw_response=rename_result.raw_response
                        )
                        logger.info(f"[Worker-{worker_id}] 重命名: {rename_result.renamed_name}")
                    else:
                        logger.warning(f"[Worker-{worker_id}] 重命名失败: {rename_result.error_message}")
                except Exception as e:
                    logger.error(f"[Worker-{worker_id}] LLM 重命名错误: {e}")

                # 删除本地临时文件
                try:
                    import os
                    if os.path.exists(file_info['local_path']):
                        os.remove(file_info['local_path'])
                except:
                    pass

        # 更新状态
        target_db.update_task_status(task_id, 'completed', node_count=node_count, file_count=len(downloaded_files))

        logger.info(f"[Worker-{worker_id}] ✅ 完成 task_id={task_id}, 节点={node_count}, 文件={len(downloaded_files)}")
        return {'success': True, 'task_id': task_id, 'link_id': link_id, 'node_count': node_count, 'file_count': len(downloaded_files)}

    except Exception as e:
        logger.error(f"[Worker-{worker_id}] ❌ 失败 link_id={link_id}: {e}")
        import traceback
        traceback.print_exc()

        # 尝试更新失败状态
        if target_db:
            try:
                target_db.update_task_status(task_id, 'failed', error_message=str(e)[:500])
            except:
                pass

        return {'success': False, 'link_id': link_id, 'error': str(e)}

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


def get_pending_links(limit=100, link_type=None):
    """获取待处理的链接"""
    from db.source_db import SourceDatabase
    from db.target_db import TargetDatabase
    from sync.incremental_sync import IncrementalSync

    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()

    sync = IncrementalSync(source_db, target_db)
    pending = sync.get_pending_links(include_failed=True, include_changed=True)

    if link_type:
        pending = [l for l in pending if l.table_name == link_type]

    pending = pending[:limit]

    # 转换为可序列化的格式
    result = [(l.id, l.url, l.table_name) for l in pending]

    source_db.close()
    target_db.close()

    return result


def get_progress():
    """获取当前进度"""
    from db.source_db import SourceDatabase
    from db.target_db import TargetDatabase

    source_db = SourceDatabase()
    source_db.connect()
    target_db = TargetDatabase()
    target_db.connect()

    # 统计
    graduate = source_db.get_count_by_type('graduate')
    undergraduate = source_db.get_count_by_type('undergraduate')
    total_needed = graduate + undergraduate

    completed = len(target_db.get_tasks_by_status('completed'))
    failed = len(target_db.get_tasks_by_status('failed'))
    crawling = len(target_db.get_tasks_by_status('crawling'))

    source_db.close()
    target_db.close()

    return {
        'total': total_needed,
        'completed': completed,
        'failed': failed,
        'crawling': crawling,
        'remaining': total_needed - completed
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description='多进程批量爬取')
    parser.add_argument('--workers', '-w', type=int, default=6,
                        help='并行 Chrome 进程数 (默认6)')
    parser.add_argument('--tasks', '-t', type=int, default=50,
                        help='每批处理任务数 (默认50)')
    parser.add_argument('--type', choices=['graduate', 'undergraduate'],
                        help='只处理指定类型')
    parser.add_argument('--batches', '-b', type=int, default=0,
                        help='最大批次数 (0=无限)')
    parser.add_argument('--rest', '-r', type=int, default=30,
                        help='批次间休息时间(秒)')
    parser.add_argument('--status', '-s', action='store_true',
                        help='只显示进度状态')

    args = parser.parse_args()

    # 只显示状态
    if args.status:
        progress = get_progress()
        print(f"""
========== 处理进度 ==========
总任务数:   {progress['total']}
已完成:     {progress['completed']}
已失败:     {progress['failed']}
处理中:     {progress['crawling']}
剩余:       {progress['remaining']}
完成率:     {progress['completed']/progress['total']*100:.1f}%
==============================
        """)
        return

    logger.info("=" * 60)
    logger.info("多进程批量处理启动")
    logger.info(f"配置: workers={args.workers}, tasks_per_batch={args.tasks}, rest={args.rest}s")
    logger.info("=" * 60)

    # 显示初始进度
    progress = get_progress()
    logger.info(f"总任务: {progress['total']}, 已完成: {progress['completed']}, 剩余: {progress['remaining']}")

    batch_count = 0
    total_success = 0
    total_failed = 0
    start_time = time.time()

    while True:
        # 检查批次限制
        if args.batches > 0 and batch_count >= args.batches:
            logger.info(f"已达到最大批次数 {args.batches}")
            break

        # 获取待处理链接
        pending = get_pending_links(limit=args.tasks, link_type=args.type)

        if not pending:
            logger.info("没有更多待处理任务")
            break

        batch_count += 1
        logger.info(f"\n===== 批次 {batch_count} 开始 ({len(pending)} 个任务) =====")

        # 准备参数 (link_id, link_url, link_type, worker_id)
        work_items = [
            (link_id, link_url, link_type, i % args.workers)
            for i, (link_id, link_url, link_type) in enumerate(pending)
        ]

        # 多进程执行
        try:
            with Pool(processes=args.workers) as pool:
                results = pool.map(crawl_worker, work_items)
        except KeyboardInterrupt:
            logger.info("用户中断，正在退出...")
            break
        except Exception as e:
            logger.error(f"批次执行出错: {e}")
            time.sleep(60)
            continue

        # 统计结果
        batch_success = sum(1 for r in results if r['success'])
        batch_failed = len(results) - batch_success
        total_success += batch_success
        total_failed += batch_failed

        elapsed = (time.time() - start_time) / 60
        rate = total_success / elapsed if elapsed > 0 else 0

        logger.info(f"批次 {batch_count} 完成: 成功={batch_success}, 失败={batch_failed}")
        logger.info(f"累计: 成功={total_success}, 失败={total_failed}, 速率={rate:.1f}个/分钟")

        # 更新进度
        progress = get_progress()
        logger.info(f"总进度: {progress['completed']}/{progress['total']} ({progress['completed']/progress['total']*100:.1f}%)")

        # 批次间休息
        if len(pending) >= args.tasks // 2:
            logger.info(f"休息 {args.rest} 秒...")
            time.sleep(args.rest)

    # 最终统计
    elapsed = (time.time() - start_time) / 60
    logger.info("\n" + "=" * 60)
    logger.info("处理完成!")
    logger.info(f"总批次: {batch_count}")
    logger.info(f"成功: {total_success}, 失败: {total_failed}")
    logger.info(f"总耗时: {elapsed:.1f} 分钟")
    if elapsed > 0:
        logger.info(f"平均速率: {total_success/elapsed:.1f} 个/分钟")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
