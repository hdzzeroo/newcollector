"""
内存占用测试脚本
测量 OverView 单线程运行时的内存使用情况
"""

import os
import sys
import time
import tracemalloc
import psutil
from dotenv import load_dotenv

load_dotenv()

def get_process_memory():
    """获取当前进程内存占用 (MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def format_size(size_bytes):
    """格式化字节数"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def test_import_memory():
    """测试导入模块的内存占用"""
    print("=" * 60)
    print("阶段 1: 测试模块导入内存占用")
    print("=" * 60)

    tracemalloc.start()
    mem_before = get_process_memory()

    # 导入核心模块
    from OverView import OverView, overViewInit, OKNoise, CHECK_Noise
    from OverView import DEBPrint, DEBAnimaPrint

    mem_after = get_process_memory()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"导入前进程内存: {mem_before:.2f} MB")
    print(f"导入后进程内存: {mem_after:.2f} MB")
    print(f"导入增量: {mem_after - mem_before:.2f} MB")
    print(f"Python 对象内存 (当前): {format_size(current)}")
    print(f"Python 对象内存 (峰值): {format_size(peak)}")
    print()

    return mem_after

def test_chrome_memory():
    """测试 Chrome 浏览器初始化的内存占用"""
    print("=" * 60)
    print("阶段 2: 测试 Chrome 浏览器初始化内存占用")
    print("=" * 60)

    from OverView import overViewInit

    mem_before = get_process_memory()
    tracemalloc.start()

    print("正在初始化 Chrome...")
    chrome = overViewInit()

    mem_after = get_process_memory()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"Chrome 初始化前: {mem_before:.2f} MB")
    print(f"Chrome 初始化后: {mem_after:.2f} MB")
    print(f"Chrome 增量: {mem_after - mem_before:.2f} MB")
    print(f"Python 对象内存 (当前): {format_size(current)}")
    print(f"Python 对象内存 (峰值): {format_size(peak)}")
    print()

    return chrome, mem_after

def test_crawl_memory(chrome, test_url="https://www.u-tokyo.ac.jp/ja/admissions/index.html"):
    """测试爬取过程的内存占用"""
    print("=" * 60)
    print("阶段 3: 测试爬取过程内存占用")
    print("=" * 60)

    from OverView import OverView

    mem_before = get_process_memory()
    tracemalloc.start()

    print(f"测试 URL: {test_url}")
    print("正在创建 OverView 实例...")

    ov = OverView(test_url, depth=1, sign="memory_test")
    mem_after_init = get_process_memory()
    print(f"OverView 实例创建后: {mem_after_init:.2f} MB (+{mem_after_init - mem_before:.2f} MB)")

    print("正在启动爬取...")
    ov.start(chrome)
    mem_after_start = get_process_memory()
    print(f"启动后: {mem_after_start:.2f} MB (+{mem_after_start - mem_after_init:.2f} MB)")

    print("正在执行 Seek (BFS 爬取)...")
    start_time = time.time()

    # 记录爬取过程中的内存峰值
    max_mem = mem_after_start

    ov.Seek()

    elapsed = time.time() - start_time
    mem_after_seek = get_process_memory()

    current, peak = tracemalloc.get_traced_memory()

    print(f"\n爬取完成!")
    print(f"耗时: {elapsed:.2f} 秒")
    print(f"爬取后内存: {mem_after_seek:.2f} MB (+{mem_after_seek - mem_after_start:.2f} MB)")
    print(f"节点数量: {len(ov.URL_LAB)}")
    print(f"Python 对象内存 (当前): {format_size(current)}")
    print(f"Python 对象内存 (峰值): {format_size(peak)}")
    print()

    # 测试剪枝
    print("正在执行 Pruning (AI 剪枝)...")
    mem_before_prune = get_process_memory()

    try:
        ov.Pruning()
        mem_after_prune = get_process_memory()
        print(f"剪枝后内存: {mem_after_prune:.2f} MB (+{mem_after_prune - mem_before_prune:.2f} MB)")
    except Exception as e:
        print(f"剪枝失败 (可能缺少 API Key): {e}")

    tracemalloc.stop()

    # 清理
    ov.end()
    mem_after_cleanup = get_process_memory()
    print(f"清理后内存: {mem_after_cleanup:.2f} MB")

    return mem_after_seek

def main():
    print("\n" + "=" * 60)
    print("OverView 内存占用测试")
    print("=" * 60 + "\n")

    # 初始内存
    initial_mem = get_process_memory()
    print(f"Python 进程初始内存: {initial_mem:.2f} MB\n")

    # 阶段 1: 导入测试
    mem_after_import = test_import_memory()

    # 阶段 2: Chrome 测试
    try:
        chrome, mem_after_chrome = test_chrome_memory()
    except Exception as e:
        print(f"Chrome 初始化失败: {e}")
        return

    # 阶段 3: 爬取测试
    try:
        mem_after_crawl = test_crawl_memory(chrome)
    except Exception as e:
        print(f"爬取测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 关闭 Chrome
        print("\n正在关闭 Chrome...")
        try:
            chrome.quit()
        except:
            pass

    # 最终报告
    final_mem = get_process_memory()
    print("\n" + "=" * 60)
    print("内存占用汇总")
    print("=" * 60)
    print(f"初始内存:     {initial_mem:.2f} MB")
    print(f"导入后:       {mem_after_import:.2f} MB (+{mem_after_import - initial_mem:.2f} MB)")
    print(f"Chrome 后:    {mem_after_chrome:.2f} MB (+{mem_after_chrome - mem_after_import:.2f} MB)")
    print(f"最终内存:     {final_mem:.2f} MB")
    print(f"总增量:       {final_mem - initial_mem:.2f} MB")
    print("=" * 60)


if __name__ == "__main__":
    main()
