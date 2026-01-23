#!/usr/bin/env python
"""
重置所有数据 - 从头开始运行
警告：此操作不可逆！
"""

import os
import shutil
from db.target_db import TargetDatabase
from sqlalchemy import text


def reset_database():
    """清空目标数据库所有表"""
    print("=" * 50)
    print("清空 Supabase 数据库")
    print("=" * 50)

    db = TargetDatabase()
    db.connect()

    with db.engine.connect() as conn:
        # 按顺序删除（考虑外键约束）
        tables = [
            'crawl_visualizations',
            'crawl_files',
            'crawl_nodes',
            'crawl_tasks',
            'sync_log'
        ]

        for table in tables:
            try:
                result = conn.execute(text(f"DELETE FROM {table}"))
                conn.commit()
                print(f"  ✓ 已清空 {table}")
            except Exception as e:
                print(f"  ✗ 清空 {table} 失败: {e}")

        # 重置序列（让 ID 从 1 开始）
        sequences = [
            'crawl_tasks_id_seq',
            'crawl_nodes_id_seq',
            'crawl_files_id_seq',
            'crawl_visualizations_id_seq',
            'sync_log_id_seq'
        ]

        print("\n重置 ID 序列...")
        for seq in sequences:
            try:
                conn.execute(text(f"ALTER SEQUENCE {seq} RESTART WITH 1"))
                conn.commit()
                print(f"  ✓ 重置 {seq}")
            except Exception as e:
                print(f"  ✗ 重置 {seq} 失败: {e}")

    db.close()
    print("\n数据库清理完成!")


def reset_local_files():
    """清理本地临时文件"""
    print("\n" + "=" * 50)
    print("清理本地文件")
    print("=" * 50)

    dirs_to_clean = [
        'temp_downloads',
        'logs',
        'MemMD',
        '_debug'
    ]

    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            try:
                shutil.rmtree(dir_name)
                print(f"  ✓ 已删除 {dir_name}/")
            except Exception as e:
                print(f"  ✗ 删除 {dir_name}/ 失败: {e}")
        else:
            print(f"  - {dir_name}/ 不存在，跳过")

    print("\n本地文件清理完成!")


def main():
    print("\n" + "!" * 50)
    print("警告：此操作将删除所有已处理的数据！")
    print("!" * 50)

    confirm = input("\n确定要清除所有数据吗？输入 'YES' 确认: ")

    if confirm != 'YES':
        print("\n已取消操作")
        return

    print("\n开始清理...\n")

    # 清理数据库
    reset_database()

    # 清理本地文件
    reset_local_files()

    print("\n" + "=" * 50)
    print("全部清理完成！现在可以从头开始运行了")
    print("=" * 50)
    print("\n运行命令:")
    print("  python run_batch.py --batch-size 5 --workers 3")


if __name__ == "__main__":
    main()
