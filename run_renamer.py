#!/usr/bin/env python
"""
重命名脚本 - 只做 PDF 解析和 LLM 重命名
适合在 GPU 服务器上运行

功能：
1. 从数据库获取已下载但未重命名的文件
2. 使用 Docling GPU 解析 PDF
3. 调用 LLM 进行智能重命名
"""

import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime
from multiprocessing import Process, Queue, Manager, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 日志配置
# ============================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging(level: str = "INFO"):
    log_file = os.path.join(LOG_DIR, f"renamer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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
# Docling GPU Worker
# ============================================================

def docling_worker(worker_id, file_queue, text_queue, config, stop_event):
    """Docling GPU Worker - PDF 解析"""
    logger = logging.getLogger(f"Docling-{worker_id}")
    logger.info(f"Docling Worker {worker_id} 启动")

    if config['use_gpu']:
        os.environ['CUDA_VISIBLE_DEVICES'] = '0'

    from processor.pdf_processor import PDFProcessor
    from processor.doc_processor import DocProcessor

    pdf_processor = PDFProcessor(
        max_pages=config['max_pages'],
        use_docling=config['use_docling'],
        force_ocr=False
    )
    doc_processor = DocProcessor(max_paragraphs=50)

    processed_count = 0

    while not stop_event.is_set():
        try:
            file_task = file_queue.get(timeout=10)

            if file_task is None:
                logger.info(f"Docling Worker {worker_id} 收到停止信号")
                break

            file_id = file_task['file_id']
            local_path = file_task['local_path']

            logger.info(f"[Docling-{worker_id}] 开始解析 file_id={file_id}")

            start_time = time.time()
            ext = os.path.splitext(local_path)[1].lower()

            if ext == '.pdf':
                result = pdf_processor.extract_text(local_path)
            elif ext in ['.doc', '.docx']:
                result = doc_processor.extract_text(local_path)
            else:
                result = type('obj', (object,), {
                    'success': False, 'text': '', 'error_message': f'不支持的文件类型: {ext}'
                })()

            extract_time = time.time() - start_time

            text_result = {
                'file_id': file_id,
                'task_id': file_task['task_id'],
                'success': result.success,
                'text': result.text if result.success else None,
                'error_message': result.error_message if not result.success else None,
                'context': file_task['context'],
                'local_path': local_path,
                'extract_time': extract_time
            }
            text_queue.put(text_result)

            processed_count += 1
            logger.info(f"[Docling-{worker_id}] 解析完成 file_id={file_id}, 耗时={extract_time:.1f}s")

        except Empty:
            continue
        except Exception as e:
            logger.error(f"[Docling-{worker_id}] 错误: {e}")
            import traceback
            traceback.print_exc()
            continue

    logger.info(f"Docling Worker {worker_id} 退出, 共处理 {processed_count} 个文件")


# ============================================================
# LLM 重命名 Worker
# ============================================================

def llm_worker(text_queue, config, stop_event):
    """LLM 重命名 Worker"""
    logger = logging.getLogger("LLM-Pool")
    logger.info(f"LLM Worker 启动, 线程数={config['llm_workers']}")

    import Sdata
    from db.target_db import TargetDatabase
    from processor.llm_renamer import LLMRenamer

    target_db = TargetDatabase()
    target_db.connect()

    renamer = LLMRenamer(api_key=Sdata.Dou_Bao_Key)
    renamer.connect()
    renamer.load_prompt_template()

    processed_count = 0

    def process_single(text_result):
        file_id = text_result['file_id']

        try:
            if not text_result['success'] or not text_result['text']:
                return {
                    'file_id': file_id,
                    'success': False,
                    'error': text_result.get('error_message', '文本提取失败')
                }

            rename_result = renamer.rename_from_text(
                text_result['text'],
                text_result['context'],
                os.path.splitext(text_result['local_path'])[1]
            )

            if rename_result.success and rename_result.renamed_name:
                target_db.update_file_renamed(
                    file_id,
                    renamed_name=rename_result.renamed_name,
                    llm_model=renamer.model,
                    llm_confidence=rename_result.confidence,
                    llm_raw_response=rename_result.raw_response
                )

                return {
                    'file_id': file_id,
                    'success': True,
                    'renamed_name': rename_result.renamed_name
                }
            else:
                return {
                    'file_id': file_id,
                    'success': False,
                    'error': rename_result.error_message
                }

        except Exception as e:
            return {
                'file_id': file_id,
                'success': False,
                'error': str(e)
            }
        finally:
            try:
                if os.path.exists(text_result['local_path']):
                    os.remove(text_result['local_path'])
            except:
                pass

    with ThreadPoolExecutor(max_workers=config['llm_workers']) as executor:
        futures = {}

        while not stop_event.is_set():
            try:
                try:
                    text_result = text_queue.get(timeout=2)

                    if text_result is None:
                        logger.info("LLM Worker 收到停止信号")
                        break

                    future = executor.submit(process_single, text_result)
                    futures[future] = text_result['file_id']

                except Empty:
                    pass

                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    file_id = futures.pop(future)
                    try:
                        result = future.result()
                        processed_count += 1
                        if result['success']:
                            logger.info(f"[LLM] 重命名成功 file_id={file_id}: {result.get('renamed_name', '')}")
                        else:
                            logger.warning(f"[LLM] 重命名失败 file_id={file_id}: {result.get('error', '')}")
                    except Exception as e:
                        logger.error(f"[LLM] 处理错误 file_id={file_id}: {e}")

            except Exception as e:
                logger.error(f"[LLM] 主循环错误: {e}")
                continue

        logger.info(f"等待剩余 {len(futures)} 个 LLM 任务完成...")
        for future in as_completed(futures):
            try:
                result = future.result()
                processed_count += 1
            except:
                pass

    target_db.close()
    logger.info(f"LLM Worker 退出, 共处理 {processed_count} 个文件")


# ============================================================
# 主程序
# ============================================================

class Renamer:
    """重命名协调器"""

    def __init__(self, docling_workers=3, llm_workers=25, use_gpu=True,
                 use_docling=True, max_pages=2, batch_size=50):
        self.docling_workers = docling_workers
        self.llm_workers = llm_workers
        self.use_gpu = use_gpu
        self.use_docling = use_docling
        self.max_pages = max_pages
        self.batch_size = batch_size

        self.logger = setup_logging()
        self.manager = Manager()
        self.file_queue = Queue()
        self.text_queue = Queue()
        self.stop_event = self.manager.Event()
        self.docling_processes = []
        self.llm_process = None

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.logger.warning("收到中断信号，正在安全退出...")
        self.stop()

    def start_workers(self):
        config = {
            'use_gpu': self.use_gpu,
            'use_docling': self.use_docling,
            'max_pages': self.max_pages,
            'llm_workers': self.llm_workers
        }

        self.logger.info(f"启动 {self.docling_workers} 个 Docling Worker...")
        for i in range(self.docling_workers):
            p = Process(
                target=docling_worker,
                args=(i, self.file_queue, self.text_queue, config, self.stop_event),
                name=f"Docling-{i}"
            )
            p.start()
            self.docling_processes.append(p)
            time.sleep(1)

        self.logger.info(f"启动 LLM Worker (线程数={self.llm_workers})...")
        self.llm_process = Process(
            target=llm_worker,
            args=(self.text_queue, config, self.stop_event),
            name="LLM-Pool"
        )
        self.llm_process.start()

        self.logger.info("所有 Worker 已启动")

    def stop(self):
        self.logger.info("正在停止所有 Worker...")
        self.stop_event.set()

        for _ in range(self.docling_workers):
            self.file_queue.put(None)
        self.text_queue.put(None)

        for p in self.docling_processes:
            p.join(timeout=30)
        if self.llm_process:
            self.llm_process.join(timeout=30)

        self.logger.info("所有 Worker 已停止")

    def get_pending_files(self, limit):
        """获取已下载但未重命名的文件"""
        from db.target_db import TargetDatabase
        from storage.supabase_storage import SupabaseStorage

        target_db = TargetDatabase()
        target_db.connect()

        # 获取状态为 downloaded 的文件
        files = target_db.get_files_by_status('downloaded', limit=limit)

        result = []
        for f in files:
            result.append({
                'file_id': f.id,
                'task_id': f.task_id,
                'storage_path': f.storage_path,
                'original_url': f.original_url,
                'original_name': f.original_name,
                'file_extension': f.file_extension
            })

        target_db.close()
        return result

    def download_file_from_storage(self, storage_path, task_id):
        """从 Supabase Storage 下载文件到本地"""
        from storage.supabase_storage import SupabaseStorage

        storage = SupabaseStorage(is_public=False)
        storage.connect()

        local_dir = f"temp_downloads/task_{task_id}"
        os.makedirs(local_dir, exist_ok=True)

        filename = os.path.basename(storage_path)
        local_path = os.path.join(local_dir, filename)

        try:
            storage.download_file(storage_path, local_path)
            return local_path
        except Exception as e:
            self.logger.error(f"下载文件失败: {e}")
            return None

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("重命名脚本启动（只做 PDF 解析和 LLM 重命名）")
        self.logger.info(f"  Docling Workers: {self.docling_workers}")
        self.logger.info(f"  LLM Workers:     {self.llm_workers}")
        self.logger.info(f"  Use GPU:         {self.use_gpu}")
        self.logger.info(f"  Use Docling:     {self.use_docling}")
        self.logger.info("=" * 60)

        self.start_workers()

        total_processed = 0
        start_time = time.time()

        try:
            while not self.stop_event.is_set():
                # 获取待处理文件
                pending_files = self.get_pending_files(self.batch_size)

                if not pending_files:
                    self.logger.info("没有更多待重命名的文件")
                    break

                self.logger.info(f"\n===== 处理 {len(pending_files)} 个文件 =====")

                # 下载文件并放入队列
                for file_info in pending_files:
                    try:
                        # 从 Storage 下载
                        local_path = self.download_file_from_storage(
                            file_info['storage_path'],
                            file_info['task_id']
                        )

                        if local_path:
                            file_task = {
                                'file_id': file_info['file_id'],
                                'task_id': file_info['task_id'],
                                'local_path': local_path,
                                'context': {
                                    'url': file_info['original_url'],
                                    'original_name': file_info['original_name'] or '',
                                    'breadcrumb': '',
                                    'title': file_info['original_name'] or '',
                                    'parent_title': '',
                                    'school_name': ''
                                }
                            }
                            self.file_queue.put(file_task)
                            total_processed += 1

                    except Exception as e:
                        self.logger.error(f"处理文件失败: {e}")

                # 等待处理完成
                while self.file_queue.qsize() > 0 or self.text_queue.qsize() > 0:
                    time.sleep(5)
                    self.logger.info(f"队列状态: 解析={self.file_queue.qsize()}, 重命名={self.text_queue.qsize()}")

                elapsed = (time.time() - start_time) / 60
                rate = total_processed / elapsed if elapsed > 0 else 0
                self.logger.info(f"[进度] 已处理: {total_processed} | 速率: {rate:.1f}/分钟")

        except KeyboardInterrupt:
            self.logger.info("用户中断")

        finally:
            self.logger.info("等待剩余任务完成...")
            time.sleep(10)
            self.stop()

            elapsed = (time.time() - start_time) / 60
            self.logger.info("\n" + "=" * 60)
            self.logger.info("重命名脚本结束")
            self.logger.info(f"总处理: {total_processed}")
            self.logger.info(f"总耗时: {elapsed:.1f} 分钟")
            self.logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='重命名脚本 - PDF 解析和 LLM 重命名')

    parser.add_argument('--docling', '-g', type=int, default=3, help='Docling GPU 进程数')
    parser.add_argument('--llm', '-l', type=int, default=25, help='LLM 并发线程数')
    parser.add_argument('--batch', '-b', type=int, default=50, help='每批文件数')
    parser.add_argument('--no-gpu', action='store_true', help='禁用 GPU')
    parser.add_argument('--no-docling', action='store_true', help='禁用 Docling，使用 pdfplumber')
    parser.add_argument('--max-pages', type=int, default=2, help='PDF 最大提取页数')
    parser.add_argument('--status', '-s', action='store_true', help='显示进度')

    args = parser.parse_args()

    if args.status:
        from db.target_db import TargetDatabase

        target_db = TargetDatabase()
        target_db.connect()

        completed = len(target_db.get_tasks_by_status('completed'))
        downloaded = len(target_db.get_tasks_by_status('downloaded'))

        # 获取文件统计
        downloaded_files = len(target_db.get_files_by_status('downloaded'))
        renamed_files = len(target_db.get_files_by_status('completed'))

        print(f"""
========== 重命名进度 ==========
已完成任务:     {completed}
待重命名任务:   {downloaded}
待重命名文件:   {downloaded_files}
已重命名文件:   {renamed_files}
================================
        """)
        return

    renamer = Renamer(
        docling_workers=args.docling,
        llm_workers=args.llm,
        use_gpu=not args.no_gpu,
        use_docling=not args.no_docling,
        max_pages=args.max_pages,
        batch_size=args.batch
    )
    renamer.run()


if __name__ == "__main__":
    main()
