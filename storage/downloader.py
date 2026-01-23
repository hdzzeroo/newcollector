"""
文件下载器
支持 PDF, DOC, DOCX 文件的下载
"""

import os
import time
import requests
from urllib.parse import urlparse, unquote
from dataclasses import dataclass
from typing import Optional, List, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


@dataclass
class DownloadResult:
    """下载结果"""
    success: bool
    url: str
    local_path: Optional[str]
    file_size: Optional[int]
    file_name: Optional[str]
    error_message: Optional[str] = None
    file_data: Optional[bytes] = None  # 内存中的文件数据（流式上传用）
    content_type: Optional[str] = None  # MIME类型


class FileDownloader:
    """文件下载器"""

    SUPPORTED_EXTENSIONS = ['.pdf', '.doc', '.docx', '.xls', '.xlsx']
    DEFAULT_TIMEOUT = 60  # 秒
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

    def __init__(self, download_dir: str = "./temp_downloads",
                 timeout: int = None, max_size: int = None):
        """
        初始化下载器

        Args:
            download_dir: 下载目录
            timeout: 下载超时时间(秒)
            max_size: 最大文件大小(字节)
        """
        self.download_dir = download_dir
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.max_size = max_size or self.MAX_FILE_SIZE

        # 确保下载目录存在
        os.makedirs(self.download_dir, exist_ok=True)

        # 请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document,*/*',
            'Accept-Language': 'ja,en;q=0.9,zh;q=0.8',
        }

    def _get_filename_from_url(self, url: str) -> str:
        """从URL提取文件名"""
        parsed = urlparse(url)
        path = unquote(parsed.path)
        filename = os.path.basename(path)

        # 如果没有文件名，使用URL哈希
        if not filename or '.' not in filename:
            import hashlib
            filename = hashlib.md5(url.encode()).hexdigest()[:16]

        return filename

    def _get_filename_from_headers(self, headers: dict) -> Optional[str]:
        """从响应头提取文件名"""
        content_disposition = headers.get('Content-Disposition', '')
        if 'filename=' in content_disposition:
            # 处理 filename="xxx.pdf" 或 filename*=UTF-8''xxx.pdf
            import re
            match = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', content_disposition)
            if match:
                return unquote(match.group(1))
        return None

    def _get_extension(self, url: str, content_type: str = None) -> str:
        """获取文件扩展名"""
        # 先从URL获取
        filename = self._get_filename_from_url(url)
        for ext in self.SUPPORTED_EXTENSIONS:
            if filename.lower().endswith(ext):
                return ext

        # 从 Content-Type 推断
        if content_type:
            type_map = {
                'application/pdf': '.pdf',
                'application/msword': '.doc',
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
                'application/vnd.ms-excel': '.xls',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
            }
            for mime, ext in type_map.items():
                if mime in content_type:
                    return ext

        return '.pdf'  # 默认

    def get_file_info(self, url: str) -> Tuple[Optional[int], Optional[str]]:
        """
        获取文件信息（不下载）

        Args:
            url: 文件URL

        Returns:
            (文件大小, Content-Type) 或 (None, None) 如果失败
        """
        try:
            response = requests.head(url, headers=self.headers,
                                     timeout=10, allow_redirects=True)
            if response.status_code == 200:
                size = response.headers.get('Content-Length')
                content_type = response.headers.get('Content-Type')
                return int(size) if size else None, content_type
        except Exception:
            pass
        return None, None

    def download_file(self, url: str, save_name: str = None,
                      task_folder: str = None) -> DownloadResult:
        """
        下载单个文件

        Args:
            url: 文件URL
            save_name: 保存的文件名（不含路径）
            task_folder: 任务子文件夹名

        Returns:
            DownloadResult
        """
        try:
            # 构建保存路径
            if task_folder:
                save_dir = os.path.join(self.download_dir, task_folder, "raw")
            else:
                save_dir = os.path.join(self.download_dir, "raw")
            os.makedirs(save_dir, exist_ok=True)

            # 发起请求
            print(f"[Download] 开始下载: {url[:80]}...")
            response = requests.get(url, headers=self.headers,
                                    timeout=self.timeout, stream=True,
                                    allow_redirects=True)

            if response.status_code != 200:
                return DownloadResult(
                    success=False,
                    url=url,
                    local_path=None,
                    file_size=None,
                    file_name=None,
                    error_message=f"HTTP {response.status_code}"
                )

            # 检查文件大小
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) > self.max_size:
                return DownloadResult(
                    success=False,
                    url=url,
                    local_path=None,
                    file_size=int(content_length),
                    file_name=None,
                    error_message=f"文件过大: {int(content_length) / 1024 / 1024:.1f}MB"
                )

            # 确定文件名
            if save_name:
                filename = save_name
            else:
                filename = self._get_filename_from_headers(response.headers)
                if not filename:
                    filename = self._get_filename_from_url(url)

            # 确保有正确的扩展名
            ext = self._get_extension(url, response.headers.get('Content-Type'))
            if not any(filename.lower().endswith(e) for e in self.SUPPORTED_EXTENSIONS):
                filename += ext

            # 保存文件
            local_path = os.path.join(save_dir, filename)
            file_size = 0

            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        file_size += len(chunk)

                        # 检查是否超过大小限制
                        if file_size > self.max_size:
                            f.close()
                            os.remove(local_path)
                            return DownloadResult(
                                success=False,
                                url=url,
                                local_path=None,
                                file_size=file_size,
                                file_name=filename,
                                error_message=f"文件过大: {file_size / 1024 / 1024:.1f}MB"
                            )

            print(f"[Download] 完成: {filename} ({file_size / 1024:.1f}KB)")

            return DownloadResult(
                success=True,
                url=url,
                local_path=local_path,
                file_size=file_size,
                file_name=filename
            )

        except requests.Timeout:
            return DownloadResult(
                success=False,
                url=url,
                local_path=None,
                file_size=None,
                file_name=None,
                error_message=f"下载超时 ({self.timeout}s)"
            )
        except requests.RequestException as e:
            return DownloadResult(
                success=False,
                url=url,
                local_path=None,
                file_size=None,
                file_name=None,
                error_message=f"请求错误: {str(e)}"
            )
        except Exception as e:
            return DownloadResult(
                success=False,
                url=url,
                local_path=None,
                file_size=None,
                file_name=None,
                error_message=f"未知错误: {str(e)}"
            )

    def download_to_memory(self, url: str) -> DownloadResult:
        """
        下载文件到内存（不写入磁盘）
        用于流式上传到 Supabase Storage

        Args:
            url: 文件URL

        Returns:
            DownloadResult (file_data 包含文件字节数据)
        """
        try:
            print(f"[Download] 开始下载到内存: {url[:80]}...")
            response = requests.get(url, headers=self.headers,
                                    timeout=self.timeout, stream=True,
                                    allow_redirects=True)

            if response.status_code != 200:
                return DownloadResult(
                    success=False,
                    url=url,
                    local_path=None,
                    file_size=None,
                    file_name=None,
                    error_message=f"HTTP {response.status_code}"
                )

            # 检查文件大小
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) > self.max_size:
                return DownloadResult(
                    success=False,
                    url=url,
                    local_path=None,
                    file_size=int(content_length),
                    file_name=None,
                    error_message=f"文件过大: {int(content_length) / 1024 / 1024:.1f}MB"
                )

            # 确定文件名
            filename = self._get_filename_from_headers(response.headers)
            if not filename:
                filename = self._get_filename_from_url(url)

            # 获取 Content-Type
            content_type = response.headers.get('Content-Type', 'application/octet-stream')

            # 确保有正确的扩展名
            ext = self._get_extension(url, content_type)
            if not any(filename.lower().endswith(e) for e in self.SUPPORTED_EXTENSIONS):
                filename += ext

            # 读取到内存
            chunks = []
            file_size = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    chunks.append(chunk)
                    file_size += len(chunk)

                    if file_size > self.max_size:
                        return DownloadResult(
                            success=False,
                            url=url,
                            local_path=None,
                            file_size=file_size,
                            file_name=filename,
                            error_message=f"文件过大: {file_size / 1024 / 1024:.1f}MB"
                        )

            file_data = b''.join(chunks)
            print(f"[Download] 完成: {filename} ({file_size / 1024:.1f}KB)")

            return DownloadResult(
                success=True,
                url=url,
                local_path=None,
                file_size=file_size,
                file_name=filename,
                file_data=file_data,
                content_type=content_type
            )

        except requests.Timeout:
            return DownloadResult(
                success=False,
                url=url,
                local_path=None,
                file_size=None,
                file_name=None,
                error_message=f"下载超时 ({self.timeout}s)"
            )
        except requests.RequestException as e:
            return DownloadResult(
                success=False,
                url=url,
                local_path=None,
                file_size=None,
                file_name=None,
                error_message=f"请求错误: {str(e)}"
            )
        except Exception as e:
            return DownloadResult(
                success=False,
                url=url,
                local_path=None,
                file_size=None,
                file_name=None,
                error_message=f"未知错误: {str(e)}"
            )

    def batch_download(self, urls: List[str], task_folder: str = None,
                       delay: float = 1.0) -> List[DownloadResult]:
        """
        批量下载文件

        Args:
            urls: URL列表
            task_folder: 任务子文件夹名
            delay: 请求间隔(秒)，避免被封

        Returns:
            DownloadResult列表
        """
        results = []
        total = len(urls)

        for i, url in enumerate(urls):
            print(f"[Download] 进度: {i + 1}/{total}")
            result = self.download_file(url, task_folder=task_folder)
            results.append(result)

            # 添加延迟，避免被封
            if i < total - 1:
                time.sleep(delay)

        success_count = sum(1 for r in results if r.success)
        print(f"[Download] 批量下载完成: {success_count}/{total} 成功")

        return results

    def is_supported_file(self, url: str) -> bool:
        """检查URL是否为支持的文件类型"""
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in self.SUPPORTED_EXTENSIONS)

    def cleanup_temp_files(self, task_folder: str = None):
        """
        清理临时文件

        Args:
            task_folder: 指定任务文件夹，None表示清理所有
        """
        import shutil

        if task_folder:
            path = os.path.join(self.download_dir, task_folder)
            if os.path.exists(path):
                shutil.rmtree(path)
                print(f"[Download] 已清理: {path}")
        else:
            if os.path.exists(self.download_dir):
                shutil.rmtree(self.download_dir)
                os.makedirs(self.download_dir)
                print(f"[Download] 已清理所有临时文件")


# 测试代码
if __name__ == "__main__":
    downloader = FileDownloader()

    # 测试URL (替换为实际的测试URL)
    test_url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"

    print("测试文件下载...")
    result = downloader.download_file(test_url, task_folder="test_task")

    print(f"\n下载结果:")
    print(f"  成功: {result.success}")
    print(f"  文件名: {result.file_name}")
    print(f"  大小: {result.file_size}")
    print(f"  路径: {result.local_path}")

    if result.error_message:
        print(f"  错误: {result.error_message}")

    # 清理测试文件
    downloader.cleanup_temp_files("test_task")
