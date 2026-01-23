"""
重新处理未完成的文件重命名任务
支持并行处理
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()

from db.target_db import TargetDatabase
from storage.supabase_storage import SupabaseStorage
from processor.llm_renamer import LLMRenamer
import Sdata


def process_single_file_worker(file_record: dict, school_name: str, download_dir: str):
    """
    单个文件处理（供并行调用）
    每个线程创建独立的连接
    """
    # 独立连接
    thread_db = TargetDatabase()
    thread_db.connect()

    thread_storage = SupabaseStorage(is_public=False)
    thread_storage.connect()

    thread_renamer = LLMRenamer(api_key=Sdata.Dou_Bao_Key)

    try:
        context = {
            'url': file_record['original_url'],
            'original_name': file_record['original_name'] or '',
            'breadcrumb': '',
            'title': file_record['original_name'] or '',
            'parent_title': '',
            'school_name': school_name or 'Unknown'
        }

        if not file_record['storage_path']:
            thread_db.update_file_process_failed(file_record['id'], error_message="没有 storage_path")
            return False, "没有 storage_path"

        # 下载文件
        remote_path = file_record['storage_path'].split(thread_storage.bucket + '/')[-1]
        local_path = os.path.join(download_dir, f"temp_{file_record['id']}.pdf")

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
            # 清理临时文件
            if os.path.exists(local_path):
                os.remove(local_path)
            return True, result.renamed_name
        else:
            error_msg = result.error_message or "LLM 返回空文件名"
            thread_db.update_file_process_failed(file_record['id'], error_message=error_msg)
            if os.path.exists(local_path):
                os.remove(local_path)
            return False, error_msg

    except Exception as e:
        thread_db.update_file_process_failed(
            file_record['id'],
            error_message=f"处理异常: {str(e)[:200]}"
        )
        return False, str(e)

    finally:
        thread_db.close()


def reprocess_pending_files(task_id: int = None, dry_run: bool = False, workers: int = 1):
    """
    重新处理所有 pending 状态的文件

    Args:
        task_id: 指定任务ID，None 表示处理所有
        dry_run: 仅显示待处理文件，不实际处理
        workers: 并行线程数
    """
    # 初始化
    target_db = TargetDatabase()
    target_db.connect()

    download_dir = "temp_downloads"
    os.makedirs(download_dir, exist_ok=True)

    # 获取所有待处理文件
    pending_files = target_db.get_pending_process_files(task_id)

    if not pending_files:
        print("没有待处理的文件")
        return

    print(f"发现 {len(pending_files)} 个待处理文件")

    if dry_run:
        print("\n[Dry Run] 以下文件将被处理:")
        for f in pending_files:
            print(f"  id={f['id']}, task_id={f['task_id']}, name={f['original_name']}")
        return

    # 缓存任务的学校名称
    task_school_cache = {}
    for f in pending_files:
        file_task_id = f['task_id']
        if file_task_id not in task_school_cache:
            task = target_db.get_task_by_id(file_task_id)
            task_school_cache[file_task_id] = task.school_name if task else None

    target_db.close()

    success_count = 0
    fail_count = 0

    if workers <= 1:
        # 串行处理
        for i, file_record in enumerate(pending_files):
            print(f"\n[{i+1}/{len(pending_files)}] 处理文件 id={file_record['id']}")
            school_name = task_school_cache.get(file_record['task_id'])
            success, msg = process_single_file_worker(file_record, school_name, download_dir)
            if success:
                print(f"  ✓ {msg}")
                success_count += 1
            else:
                print(f"  ✗ {msg}")
                fail_count += 1
            time.sleep(0.5)  # 短暂间隔
    else:
        # 并行处理
        print(f"\n使用 {workers} 个并行线程处理")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for f in pending_files:
                school_name = task_school_cache.get(f['task_id'])
                future = executor.submit(process_single_file_worker, f, school_name, download_dir)
                futures[future] = f

            completed = 0
            for future in as_completed(futures):
                completed += 1
                file_record = futures[future]
                try:
                    success, msg = future.result()
                    if success:
                        print(f"[{completed}/{len(pending_files)}] ✓ {msg[:50]}...")
                        success_count += 1
                    else:
                        print(f"[{completed}/{len(pending_files)}] ✗ {msg[:50]}...")
                        fail_count += 1
                except Exception as e:
                    print(f"[{completed}/{len(pending_files)}] ✗ 异常: {e}")
                    fail_count += 1

    print(f"\n处理完成: 成功 {success_count}, 失败 {fail_count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='重新处理 pending 状态的文件')
    parser.add_argument('--task', '-t', type=int, help='指定任务ID')
    parser.add_argument('--dry-run', '-d', action='store_true', help='仅显示待处理文件')
    parser.add_argument('--workers', '-w', type=int, default=1,
                        help='并行处理线程数 (默认1，建议3-5)')

    args = parser.parse_args()

    reprocess_pending_files(task_id=args.task, dry_run=args.dry_run, workers=args.workers)
