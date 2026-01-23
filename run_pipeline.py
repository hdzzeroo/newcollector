#!/usr/bin/env python
"""
高性能并行处理 Pipeline

支持配置:
- Chrome 爬虫进程数
- Docling GPU 解析进程数
- LLM 并发线程数
- 自动资源检测模式 (--auto)

架构:
Chrome Workers → PDF Queue → Docling GPU Workers → LLM Thread Pool → 结果
"""

import os
import sys
import time
import signal
import logging
import argparse
import subprocess
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Process, Queue, Manager, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# 配置（必须在最前面定义）
# ============================================================

@dataclass
class PipelineConfig:
    """Pipeline 配置"""
    # Chrome 爬虫配置
    chrome_workers: int = 4
    crawl_depth: int = 1

    # Docling GPU 配置
    docling_workers: int = 3
    use_gpu: bool = True
    max_pages: int = 2

    # LLM 配置
    llm_workers: int = 25

    # 任务配置
    batch_size: int = 20
    link_type: Optional[str] = None
    max_batches: int = 0
    rest_time: int = 30

    # 其他
    log_level: str = "INFO"


# ============================================================
# 资源监控和自动配置
# ============================================================

class ResourceMonitor:
    """资源监控器 - 监控 CPU、内存、GPU 使用情况"""

    @staticmethod
    def get_cpu_info() -> Dict:
        """获取 CPU 信息"""
        try:
            import psutil
            return {
                'count': psutil.cpu_count(),
                'percent': psutil.cpu_percent(interval=1),
                'available': psutil.cpu_count() * (100 - psutil.cpu_percent()) / 100
            }
        except ImportError:
            # psutil 未安装，使用基础方法
            return {
                'count': cpu_count(),
                'percent': 0,
                'available': cpu_count()
            }

    @staticmethod
    def get_memory_info() -> Dict:
        """获取内存信息 (单位: GB)"""
        try:
            import psutil
            mem = psutil.virtual_memory()
            return {
                'total': mem.total / (1024**3),
                'available': mem.available / (1024**3),
                'percent': mem.percent,
                'used': mem.used / (1024**3)
            }
        except ImportError:
            # 尝试从 /proc/meminfo 读取
            try:
                with open('/proc/meminfo', 'r') as f:
                    lines = f.readlines()
                    total = int(lines[0].split()[1]) / (1024**2)
                    available = int(lines[2].split()[1]) / (1024**2)
                    return {
                        'total': total,
                        'available': available,
                        'percent': (total - available) / total * 100,
                        'used': total - available
                    }
            except:
                return {'total': 64, 'available': 50, 'percent': 20, 'used': 14}

    @staticmethod
    def get_gpu_info() -> Dict:
        """获取 GPU 信息"""
        try:
            # 使用 nvidia-smi 命令获取 GPU 信息
            result = subprocess.run(
                ['nvidia-smi', '--query-gpu=name,memory.total,memory.used,memory.free,utilization.gpu',
                 '--format=csv,noheader,nounits'],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                gpus = []
                for line in lines:
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 5:
                        gpus.append({
                            'name': parts[0],
                            'memory_total': float(parts[1]) / 1024,  # GB
                            'memory_used': float(parts[2]) / 1024,   # GB
                            'memory_free': float(parts[3]) / 1024,   # GB
                            'utilization': float(parts[4])
                        })

                if gpus:
                    gpu = gpus[0]  # 使用第一个 GPU
                    return {
                        'available': True,
                        'name': gpu['name'],
                        'memory_total': gpu['memory_total'],
                        'memory_free': gpu['memory_free'],
                        'memory_used': gpu['memory_used'],
                        'utilization': gpu['utilization'],
                        'count': len(gpus)
                    }
        except Exception as e:
            pass

        return {
            'available': False,
            'name': 'N/A',
            'memory_total': 0,
            'memory_free': 0,
            'memory_used': 0,
            'utilization': 0,
            'count': 0
        }

    @classmethod
    def get_all_resources(cls) -> Dict:
        """获取所有资源信息"""
        return {
            'cpu': cls.get_cpu_info(),
            'memory': cls.get_memory_info(),
            'gpu': cls.get_gpu_info()
        }

    @classmethod
    def print_resources(cls):
        """打印资源信息"""
        res = cls.get_all_resources()

        print("\n" + "=" * 60)
        print("  系统资源检测")
        print("=" * 60)

        # CPU
        cpu = res['cpu']
        print(f"\n  CPU:")
        print(f"    核心数:     {cpu['count']}")
        print(f"    当前使用率: {cpu['percent']:.1f}%")
        print(f"    可用核心:   {cpu['available']:.1f}")

        # 内存
        mem = res['memory']
        print(f"\n  内存:")
        print(f"    总量:       {mem['total']:.1f} GB")
        print(f"    已使用:     {mem['used']:.1f} GB ({mem['percent']:.1f}%)")
        print(f"    可用:       {mem['available']:.1f} GB")

        # GPU
        gpu = res['gpu']
        if gpu['available']:
            print(f"\n  GPU:")
            print(f"    型号:       {gpu['name']}")
            print(f"    显存总量:   {gpu['memory_total']:.1f} GB")
            print(f"    显存已用:   {gpu['memory_used']:.1f} GB")
            print(f"    显存可用:   {gpu['memory_free']:.1f} GB")
            print(f"    GPU 使用率: {gpu['utilization']:.1f}%")
        else:
            print(f"\n  GPU: 未检测到")

        print("=" * 60)
        return res


class AutoConfig:
    """自动配置计算器 - 根据资源自动计算最优配置"""

    # 每个组件的资源消耗估算
    CHROME_MEMORY_GB = 1.5      # 每个 Chrome 进程约 1.5GB 内存
    CHROME_CPU_CORES = 0.8     # 每个 Chrome 进程约 0.8 CPU 核心
    DOCLING_MEMORY_GB = 2.0    # 每个 Docling 进程约 2GB 内存
    DOCLING_GPU_GB = 4.0       # 每个 Docling 进程约 4GB 显存
    DOCLING_CPU_CORES = 0.5    # 每个 Docling 进程约 0.5 CPU 核心

    # 安全余量 (保留一部分资源给系统)
    MEMORY_SAFETY_MARGIN = 0.8   # 使用 80% 的可用内存
    GPU_SAFETY_MARGIN = 0.85     # 使用 85% 的显存
    CPU_SAFETY_MARGIN = 0.9      # 使用 90% 的 CPU

    @classmethod
    def calculate_optimal_config(cls, resources: Dict = None) -> PipelineConfig:
        """
        根据系统资源计算最优配置

        Returns:
            PipelineConfig 对象
        """
        if resources is None:
            resources = ResourceMonitor.get_all_resources()

        cpu = resources['cpu']
        mem = resources['memory']
        gpu = resources['gpu']

        # 可用资源（考虑安全余量）
        available_memory = mem['available'] * cls.MEMORY_SAFETY_MARGIN
        available_cpu = cpu['available'] * cls.CPU_SAFETY_MARGIN
        available_gpu_memory = gpu['memory_free'] * cls.GPU_SAFETY_MARGIN if gpu['available'] else 0

        # 计算各组件最大数量

        # 1. Chrome workers (受内存和 CPU 限制)
        max_chrome_by_memory = int(available_memory / cls.CHROME_MEMORY_GB)
        max_chrome_by_cpu = int(available_cpu / cls.CHROME_CPU_CORES)
        chrome_workers = min(max_chrome_by_memory, max_chrome_by_cpu, 8)  # 最多 8 个
        chrome_workers = max(chrome_workers, 1)  # 最少 1 个

        # 更新剩余资源
        remaining_memory = available_memory - (chrome_workers * cls.CHROME_MEMORY_GB)
        remaining_cpu = available_cpu - (chrome_workers * cls.CHROME_CPU_CORES)

        # 2. Docling workers (受显存、内存和 CPU 限制)
        if gpu['available'] and available_gpu_memory > cls.DOCLING_GPU_GB:
            max_docling_by_gpu = int(available_gpu_memory / cls.DOCLING_GPU_GB)
            max_docling_by_memory = int(remaining_memory / cls.DOCLING_MEMORY_GB)
            max_docling_by_cpu = int(remaining_cpu / cls.DOCLING_CPU_CORES)
            docling_workers = min(max_docling_by_gpu, max_docling_by_memory, max_docling_by_cpu, 6)
            docling_workers = max(docling_workers, 1)
            use_gpu = True
        else:
            # 无 GPU，使用 CPU 模式
            max_docling_by_memory = int(remaining_memory / cls.DOCLING_MEMORY_GB)
            max_docling_by_cpu = int(remaining_cpu / cls.DOCLING_CPU_CORES)
            docling_workers = min(max_docling_by_memory, max_docling_by_cpu, 4)
            docling_workers = max(docling_workers, 1)
            use_gpu = False

        # 3. LLM workers (I/O 密集，主要受 API 限制)
        # 一般建议 20-50 个并发
        llm_workers = min(chrome_workers * 8, 50)
        llm_workers = max(llm_workers, 10)

        # 4. Batch size (基于 Chrome workers)
        batch_size = chrome_workers * 5

        config = PipelineConfig(
            chrome_workers=chrome_workers,
            docling_workers=docling_workers,
            llm_workers=llm_workers,
            use_gpu=use_gpu,
            batch_size=batch_size,
            crawl_depth=1,
            max_pages=2,
            rest_time=30
        )

        return config

    @classmethod
    def print_recommendation(cls, config: PipelineConfig, resources: Dict):
        """打印推荐配置"""
        mem = resources['memory']
        gpu = resources['gpu']

        print("\n" + "=" * 60)
        print("  自动推荐配置")
        print("=" * 60)

        print(f"\n  Chrome Workers:  {config.chrome_workers}")
        print(f"    └─ 预计内存占用: {config.chrome_workers * cls.CHROME_MEMORY_GB:.1f} GB")

        print(f"\n  Docling Workers: {config.docling_workers}")
        if config.use_gpu:
            print(f"    └─ 预计显存占用: {config.docling_workers * cls.DOCLING_GPU_GB:.1f} GB")
        print(f"    └─ 预计内存占用: {config.docling_workers * cls.DOCLING_MEMORY_GB:.1f} GB")

        print(f"\n  LLM Workers:     {config.llm_workers}")
        print(f"    └─ (I/O 密集型，资源消耗极小)")

        print(f"\n  Batch Size:      {config.batch_size}")
        print(f"  Use GPU:         {'是' if config.use_gpu else '否'}")

        total_memory = (config.chrome_workers * cls.CHROME_MEMORY_GB +
                       config.docling_workers * cls.DOCLING_MEMORY_GB)
        print(f"\n  预计总内存占用:  {total_memory:.1f} GB / {mem['available']:.1f} GB 可用")

        if config.use_gpu:
            total_gpu = config.docling_workers * cls.DOCLING_GPU_GB
            print(f"  预计总显存占用:  {total_gpu:.1f} GB / {gpu['memory_free']:.1f} GB 可用")

        print("=" * 60)


def auto_detect_config() -> Tuple[PipelineConfig, Dict]:
    """自动检测资源并返回最优配置"""
    resources = ResourceMonitor.print_resources()
    config = AutoConfig.calculate_optimal_config(resources)
    AutoConfig.print_recommendation(config, resources)
    return config, resources


# ============================================================
# 日志配置
# ============================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging(level: str = "INFO"):
    """配置日志"""
    log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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
# 数据结构
# ============================================================

@dataclass
class FileTask:
    """文件处理任务"""
    task_id: int
    file_id: int
    local_path: str
    original_url: str
    original_name: str
    context: Dict[str, str]


@dataclass
class ExtractResult:
    """提取结果"""
    file_id: int
    task_id: int
    success: bool
    text: Optional[str] = None
    error_message: Optional[str] = None
    context: Optional[Dict] = None


# ============================================================
# Chrome 爬虫 Worker
# ============================================================

def chrome_worker(
    worker_id: int,
    task_queue: Queue,
    file_queue: Queue,
    result_dict: Dict,
    config: Dict,
    stop_event
):
    """
    Chrome 爬虫 Worker

    从 task_queue 获取链接，爬取后将文件放入 file_queue
    """
    logger = logging.getLogger(f"Chrome-{worker_id}")
    logger.info(f"Chrome Worker {worker_id} 启动")

    # 导入依赖
    from OverView import OverView, overViewInit
    from db.target_db import TargetDatabase
    from storage.downloader import FileDownloader
    from storage.supabase_storage import SupabaseStorage
    import Sdata

    chrome = None
    target_db = None

    try:
        # 初始化 Chrome
        chrome = overViewInit()

        # 初始化数据库连接
        target_db = TargetDatabase()
        target_db.connect()

        # 初始化下载器和存储
        downloader = FileDownloader()
        storage = SupabaseStorage(is_public=False)
        storage.connect()

        while not stop_event.is_set():
            try:
                # 从队列获取任务（超时 5 秒）
                link_data = task_queue.get(timeout=5)

                if link_data is None:  # 停止信号
                    logger.info(f"Chrome Worker {worker_id} 收到停止信号")
                    break

                link_id, link_url, link_type = link_data
                logger.info(f"[Worker-{worker_id}] 开始处理 link_id={link_id}")

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

                # 获取节点数
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

                if pruned_indices:
                    target_db.mark_nodes_pruned(task_id, pruned_indices)

                ov.end()
                logger.info(f"[Worker-{worker_id}] 爬取完成, 节点数={node_count}")

                # 下载文件并放入队列
                file_nodes = target_db.get_file_nodes(task_id, pruned_only=True)
                downloaded_count = 0

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

                            # 放入文件队列供 Docling 处理
                            file_task = {
                                'task_id': task_id,
                                'file_id': file_id,
                                'local_path': result.local_path,
                                'original_url': node.url,
                                'original_name': node.title,
                                'context': {
                                    'url': node.url,
                                    'original_name': node.title or '',
                                    'breadcrumb': node.breadcrumb or '',
                                    'title': node.title or '',
                                    'parent_title': '',
                                    'school_name': ''
                                }
                            }
                            file_queue.put(file_task)
                            downloaded_count += 1
                            logger.info(f"[Worker-{worker_id}] 下载成功: {result.file_name}")
                        else:
                            target_db.update_file_download(file_id, 'failed', error_message=result.error_message)

                    except Exception as e:
                        logger.error(f"[Worker-{worker_id}] 下载失败: {e}")

                # 更新任务状态
                target_db.update_task_status(task_id, 'processing', node_count=node_count, file_count=downloaded_count)

                # 记录结果
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
# Docling GPU Worker
# ============================================================

def docling_worker(
    worker_id: int,
    file_queue: Queue,
    text_queue: Queue,
    config: Dict,
    stop_event
):
    """
    Docling GPU Worker

    从 file_queue 获取文件，解析后将文本放入 text_queue
    """
    logger = logging.getLogger(f"Docling-{worker_id}")
    logger.info(f"Docling Worker {worker_id} 启动")

    # 设置 GPU
    if config['use_gpu']:
        os.environ['CUDA_VISIBLE_DEVICES'] = '0'  # 所有 worker 共享 GPU

    # 初始化 Docling
    from processor.pdf_processor import PDFProcessor
    from processor.doc_processor import DocProcessor

    pdf_processor = PDFProcessor(
        max_pages=config['max_pages'],
        use_docling=True,
        force_ocr=False
    )
    doc_processor = DocProcessor(max_paragraphs=50)

    processed_count = 0

    while not stop_event.is_set():
        try:
            # 从队列获取文件（超时 10 秒）
            file_task = file_queue.get(timeout=10)

            if file_task is None:  # 停止信号
                logger.info(f"Docling Worker {worker_id} 收到停止信号")
                break

            file_id = file_task['file_id']
            local_path = file_task['local_path']

            logger.info(f"[Docling-{worker_id}] 开始解析 file_id={file_id}")

            start_time = time.time()

            # 根据文件类型选择处理器
            ext = os.path.splitext(local_path)[1].lower()

            if ext == '.pdf':
                result = pdf_processor.extract_text(local_path)
            elif ext in ['.doc', '.docx']:
                result = doc_processor.extract_text(local_path)
            else:
                result = type('obj', (object,), {'success': False, 'text': '', 'error_message': f'不支持的文件类型: {ext}'})()

            extract_time = time.time() - start_time

            # 放入文本队列
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
            logger.info(f"[Docling-{worker_id}] 解析完成 file_id={file_id}, 耗时={extract_time:.1f}s, 累计={processed_count}")

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

def llm_worker(
    text_queue: Queue,
    config: Dict,
    stop_event
):
    """
    LLM 重命名 Worker

    从 text_queue 获取文本，使用线程池并行调用 LLM
    """
    logger = logging.getLogger("LLM-Pool")
    logger.info(f"LLM Worker 启动, 线程数={config['llm_workers']}")

    import Sdata
    from db.target_db import TargetDatabase
    from processor.llm_renamer import LLMRenamer

    # 初始化
    target_db = TargetDatabase()
    target_db.connect()

    renamer = LLMRenamer(api_key=Sdata.Dou_Bao_Key)
    renamer.connect()
    renamer.load_prompt_template()

    pending_tasks = []  # 待处理的任务
    processed_count = 0

    def process_single(text_result):
        """处理单个文件的 LLM 调用"""
        file_id = text_result['file_id']

        try:
            if not text_result['success'] or not text_result['text']:
                return {
                    'file_id': file_id,
                    'success': False,
                    'error': text_result.get('error_message', '文本提取失败')
                }

            # 调用 LLM
            rename_result = renamer.rename_from_text(
                text_result['text'],
                text_result['context'],
                os.path.splitext(text_result['local_path'])[1]
            )

            if rename_result.success and rename_result.renamed_name:
                # 更新数据库
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
            # 清理本地文件
            try:
                if os.path.exists(text_result['local_path']):
                    os.remove(text_result['local_path'])
            except:
                pass

    # 使用线程池
    with ThreadPoolExecutor(max_workers=config['llm_workers']) as executor:
        futures = {}

        while not stop_event.is_set():
            try:
                # 尝试获取新任务
                try:
                    text_result = text_queue.get(timeout=2)

                    if text_result is None:  # 停止信号
                        logger.info("LLM Worker 收到停止信号")
                        break

                    # 提交到线程池
                    future = executor.submit(process_single, text_result)
                    futures[future] = text_result['file_id']

                except Empty:
                    pass

                # 检查已完成的任务
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

        # 等待剩余任务完成
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
# 主 Pipeline
# ============================================================

class Pipeline:
    """主 Pipeline 协调器"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = setup_logging(config.log_level)

        # 进程管理
        self.manager = Manager()
        self.task_queue = Queue()      # 待爬取的链接
        self.file_queue = Queue()      # 待解析的文件
        self.text_queue = Queue()      # 待重命名的文本
        self.result_dict = self.manager.dict()  # 结果
        self.stop_event = self.manager.Event()

        self.chrome_processes = []
        self.docling_processes = []
        self.llm_process = None

        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.logger.warning("收到中断信号，正在安全退出...")
        self.stop()

    def start_workers(self):
        """启动所有 Worker"""
        config_dict = {
            'crawl_depth': self.config.crawl_depth,
            'use_gpu': self.config.use_gpu,
            'max_pages': self.config.max_pages,
            'llm_workers': self.config.llm_workers
        }

        # 启动 Chrome Workers
        self.logger.info(f"启动 {self.config.chrome_workers} 个 Chrome Worker...")
        for i in range(self.config.chrome_workers):
            p = Process(
                target=chrome_worker,
                args=(i, self.task_queue, self.file_queue, self.result_dict, config_dict, self.stop_event),
                name=f"Chrome-{i}"
            )
            p.start()
            self.chrome_processes.append(p)
            time.sleep(2)  # 错开启动，避免资源争抢

        # 启动 Docling GPU Workers
        self.logger.info(f"启动 {self.config.docling_workers} 个 Docling GPU Worker...")
        for i in range(self.config.docling_workers):
            p = Process(
                target=docling_worker,
                args=(i, self.file_queue, self.text_queue, config_dict, self.stop_event),
                name=f"Docling-{i}"
            )
            p.start()
            self.docling_processes.append(p)
            time.sleep(1)

        # 启动 LLM Worker
        self.logger.info(f"启动 LLM Worker (线程数={self.config.llm_workers})...")
        self.llm_process = Process(
            target=llm_worker,
            args=(self.text_queue, config_dict, self.stop_event),
            name="LLM-Pool"
        )
        self.llm_process.start()

        self.logger.info("所有 Worker 已启动")

    def stop(self):
        """停止所有 Worker"""
        self.logger.info("正在停止所有 Worker...")
        self.stop_event.set()

        # 发送停止信号
        for _ in range(self.config.chrome_workers):
            self.task_queue.put(None)
        for _ in range(self.config.docling_workers):
            self.file_queue.put(None)
        self.text_queue.put(None)

        # 等待进程结束
        for p in self.chrome_processes:
            p.join(timeout=30)
        for p in self.docling_processes:
            p.join(timeout=30)
        if self.llm_process:
            self.llm_process.join(timeout=30)

        self.logger.info("所有 Worker 已停止")

    def get_pending_links(self, limit: int) -> List:
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

        if self.config.link_type:
            pending = [l for l in pending if l.table_name == self.config.link_type]

        pending = pending[:limit]
        result = [(l.id, l.url, l.table_name) for l in pending]

        source_db.close()
        target_db.close()

        return result

    def run(self):
        """运行 Pipeline（流水线模式，Chrome 和 Docling 交叉运行）"""
        self.logger.info("=" * 60)
        self.logger.info("Pipeline 启动 (流水线模式)")
        self.logger.info(f"配置:")
        self.logger.info(f"  Chrome Workers:  {self.config.chrome_workers}")
        self.logger.info(f"  Docling Workers: {self.config.docling_workers}")
        self.logger.info(f"  LLM Workers:     {self.config.llm_workers}")
        self.logger.info(f"  Batch Size:      {self.config.batch_size}")
        self.logger.info(f"  Crawl Depth:     {self.config.crawl_depth}")
        self.logger.info(f"  Use GPU:         {self.config.use_gpu}")
        self.logger.info("=" * 60)

        # 启动 Workers
        self.start_workers()

        batch_count = 0
        total_queued = 0
        start_time = time.time()

        try:
            while not self.stop_event.is_set():
                # 检查批次限制
                if self.config.max_batches > 0 and batch_count >= self.config.max_batches:
                    self.logger.info(f"已达到最大批次数 {self.config.max_batches}")
                    break

                # 检查队列是否需要补充任务
                # 保持队列中有足够的任务，让 workers 持续工作
                queue_size = self.task_queue.qsize()

                # 当队列中任务少于 chrome_workers * 2 时，补充新任务
                if queue_size < self.config.chrome_workers * 2:
                    # 获取待处理链接
                    pending = self.get_pending_links(self.config.batch_size)

                    if not pending:
                        # 没有新任务了，等待现有任务完成
                        if queue_size == 0 and len(self.result_dict) >= total_queued:
                            self.logger.info("没有更多待处理任务")
                            break
                        else:
                            # 还有任务在处理中，等待
                            time.sleep(5)
                            continue

                    batch_count += 1
                    self.logger.info(f"\n===== 补充批次 {batch_count} ({len(pending)} 个任务) =====")

                    # 将任务放入队列（不等待完成）
                    for link_data in pending:
                        self.task_queue.put(link_data)

                    total_queued += len(pending)
                    self.logger.info(f"队列状态: 已投放 {total_queued}, 已完成 {len(self.result_dict)}")

                # 显示进度（每 10 秒）
                time.sleep(10)

                completed = len(self.result_dict)
                elapsed = (time.time() - start_time) / 60
                rate = completed / elapsed if elapsed > 0 else 0

                # 获取队列状态
                task_q = self.task_queue.qsize()
                file_q = self.file_queue.qsize()
                text_q = self.text_queue.qsize()

                self.logger.info(
                    f"[进度] 完成: {completed}/{total_queued} | "
                    f"队列: 爬取={task_q}, 解析={file_q}, 重命名={text_q} | "
                    f"速率: {rate:.1f}/分钟"
                )

        except KeyboardInterrupt:
            self.logger.info("用户中断")

        finally:
            # 等待队列处理完
            self.logger.info("等待剩余任务完成...")

            # 等待所有队列清空
            wait_count = 0
            while wait_count < 30:  # 最多等待 5 分钟
                task_q = self.task_queue.qsize()
                file_q = self.file_queue.qsize()
                text_q = self.text_queue.qsize()

                if task_q == 0 and file_q == 0 and text_q == 0:
                    self.logger.info("所有队列已清空")
                    break

                self.logger.info(f"等待队列清空: 爬取={task_q}, 解析={file_q}, 重命名={text_q}")
                time.sleep(10)
                wait_count += 1

            self.stop()

            # 最终统计
            elapsed = (time.time() - start_time) / 60
            total_processed = len(self.result_dict)
            self.logger.info("\n" + "=" * 60)
            self.logger.info("Pipeline 结束")
            self.logger.info(f"总批次: {batch_count}")
            self.logger.info(f"总处理: {total_processed}")
            self.logger.info(f"总耗时: {elapsed:.1f} 分钟")
            if elapsed > 0:
                self.logger.info(f"平均速率: {total_processed/elapsed:.1f} 个/分钟")
            self.logger.info("=" * 60)


# ============================================================
# 命令行入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='高性能并行处理 Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 自动检测资源并配置（推荐）
  python run_pipeline.py --auto

  # 只查看资源和推荐配置
  python run_pipeline.py --resources

  # 手动配置
  python run_pipeline.py --chrome 4 --docling 3 --llm 25 --batch 20

  # 高性能配置 (大内存服务器)
  python run_pipeline.py --chrome 6 --docling 4 --llm 30 --batch 30

  # 低资源配置
  python run_pipeline.py --chrome 2 --docling 2 --llm 10 --batch 10
        """
    )

    # 自动模式
    parser.add_argument('--auto', '-a', action='store_true',
                        help='自动检测资源并配置最优参数（推荐）')
    parser.add_argument('--resources', action='store_true',
                        help='只显示系统资源和推荐配置，不运行')

    # Chrome 配置
    parser.add_argument('--chrome', '-c', type=int, default=None,
                        help='Chrome 爬虫进程数 (默认: 自动)')
    parser.add_argument('--depth', '-d', type=int, default=1,
                        help='爬取深度 (默认: 1)')

    # Docling 配置
    parser.add_argument('--docling', '-g', type=int, default=None,
                        help='Docling GPU 解析进程数 (默认: 自动)')
    parser.add_argument('--no-gpu', action='store_true',
                        help='禁用 GPU，使用 CPU 解析')
    parser.add_argument('--max-pages', type=int, default=2,
                        help='PDF 最大提取页数 (默认: 2)')

    # LLM 配置
    parser.add_argument('--llm', '-l', type=int, default=None,
                        help='LLM 并发线程数 (默认: 自动)')

    # 任务配置
    parser.add_argument('--batch', '-b', type=int, default=None,
                        help='每批处理任务数 (默认: 自动)')
    parser.add_argument('--type', '-t', choices=['graduate', 'undergraduate'],
                        help='只处理指定类型')
    parser.add_argument('--max-batches', '-m', type=int, default=0,
                        help='最大批次数 (0=无限制)')
    parser.add_argument('--rest', '-r', type=int, default=30,
                        help='批次间休息时间(秒) (默认: 30)')

    # 其他
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO', help='日志级别')
    parser.add_argument('--status', '-s', action='store_true',
                        help='只显示当前进度')

    args = parser.parse_args()

    # 只显示资源信息
    if args.resources:
        config, resources = auto_detect_config()
        print("\n使用以下命令运行:")
        print(f"  python run_pipeline.py --chrome {config.chrome_workers} --docling {config.docling_workers} --llm {config.llm_workers} --batch {config.batch_size}")
        print("\n或使用自动模式:")
        print("  python run_pipeline.py --auto")
        return

    # 显示状态
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
        failed = len(target_db.get_tasks_by_status('failed'))
        processing = len(target_db.get_tasks_by_status('processing'))

        print(f"""
========== 处理进度 ==========
总任务数:   {total}
已完成:     {completed}
处理中:     {processing}
已失败:     {failed}
剩余:       {total - completed}
完成率:     {completed/total*100:.1f}%
==============================
        """)

        source_db.close()
        target_db.close()
        return

    # 自动模式或任何参数未指定时，使用自动检测
    if args.auto or (args.chrome is None and args.docling is None and args.llm is None):
        print("\n🔍 自动检测系统资源...")
        auto_config, resources = auto_detect_config()

        # 使用自动配置，但允许命令行参数覆盖
        config = PipelineConfig(
            chrome_workers=args.chrome if args.chrome is not None else auto_config.chrome_workers,
            crawl_depth=args.depth,
            docling_workers=args.docling if args.docling is not None else auto_config.docling_workers,
            use_gpu=not args.no_gpu and auto_config.use_gpu,
            max_pages=args.max_pages,
            llm_workers=args.llm if args.llm is not None else auto_config.llm_workers,
            batch_size=args.batch if args.batch is not None else auto_config.batch_size,
            link_type=args.type,
            max_batches=args.max_batches,
            rest_time=args.rest,
            log_level=args.log_level
        )
    else:
        # 手动模式，使用指定的参数（默认值作为后备）
        config = PipelineConfig(
            chrome_workers=args.chrome if args.chrome is not None else 4,
            crawl_depth=args.depth,
            docling_workers=args.docling if args.docling is not None else 3,
            use_gpu=not args.no_gpu,
            max_pages=args.max_pages,
            llm_workers=args.llm if args.llm is not None else 25,
            batch_size=args.batch if args.batch is not None else 20,
            link_type=args.type,
            max_batches=args.max_batches,
            rest_time=args.rest,
            log_level=args.log_level
        )

    # 显示最终配置
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║              Pipeline 最终配置                                ║
╠══════════════════════════════════════════════════════════════╣
║  Chrome 爬虫进程:    {config.chrome_workers:3d}                                  ║
║  Docling GPU 进程:   {config.docling_workers:3d}                                  ║
║  LLM 并发线程:       {config.llm_workers:3d}                                  ║
║  每批任务数:         {config.batch_size:3d}                                  ║
║  爬取深度:           {config.crawl_depth:3d}                                  ║
║  使用 GPU:           {'是' if config.use_gpu else '否':3s}                                  ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # 运行
    pipeline = Pipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
