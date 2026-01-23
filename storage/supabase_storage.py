"""
Supabase Storage 操作模块
负责文件的上传、下载、管理
"""

from supabase import create_client, Client
from typing import Optional, List
import os
import mimetypes


class SupabaseStorage:
    """Supabase Storage 操作器"""

    DEFAULT_URL = "https://orqthdhhyqtksrtxweoc.supabase.co"
    DEFAULT_BUCKET = "university-files"
    DEFAULT_SIGNED_URL_EXPIRES = 3600  # 签名URL有效期(秒)，默认1小时

    def __init__(self, url: str = None, key: str = None, bucket: str = None,
                 is_public: bool = False, signed_url_expires: int = None):
        """
        初始化 Supabase Storage 连接

        Args:
            url: Supabase 项目 URL
            key: Supabase service_role key (需要有Storage权限)
            bucket: 存储桶名称
            is_public: 是否为公开bucket (False=私有，使用签名URL)
            signed_url_expires: 签名URL有效期(秒)
        """
        self.url = url or os.getenv('SUPABASE_URL', self.DEFAULT_URL)
        self.key = key or os.getenv('SUPABASE_KEY')
        self.bucket = bucket or os.getenv('SUPABASE_BUCKET', self.DEFAULT_BUCKET)
        self.is_public = is_public
        self.signed_url_expires = signed_url_expires or self.DEFAULT_SIGNED_URL_EXPIRES

        self.client: Optional[Client] = None

    def connect(self) -> Client:
        """建立连接"""
        if self.client is None:
            if not self.key:
                raise ValueError("Supabase key is required. Set SUPABASE_KEY environment variable.")
            self.client = create_client(self.url, self.key)
        return self.client

    def ensure_bucket_exists(self):
        """确保存储桶存在"""
        self.connect()
        try:
            # 尝试获取桶信息
            self.client.storage.get_bucket(self.bucket)
        except Exception:
            # 桶不存在，创建它
            self.client.storage.create_bucket(
                self.bucket,
                options={
                    "public": self.is_public,  # 根据配置决定是否公开
                    "file_size_limit": 52428800,  # 50MB
                    "allowed_mime_types": [
                        "application/pdf",
                        "application/msword",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        "application/vnd.ms-excel",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "text/html",
                    ]
                }
            )
            bucket_type = "公开" if self.is_public else "私有"
            print(f"[Storage] 创建{bucket_type}存储桶: {self.bucket}")

    def upload_file(self, local_path: str, remote_path: str,
                    content_type: str = None) -> str:
        """
        上传文件到 Storage

        Args:
            local_path: 本地文件路径
            remote_path: 远程存储路径 (如 "task_123/raw/file.pdf")
            content_type: MIME类型，自动检测如果未指定

        Returns:
            存储路径标识 (bucket/path 格式，用于数据库存储)
        """
        self.connect()

        # 自动检测 MIME 类型
        if content_type is None:
            content_type, _ = mimetypes.guess_type(local_path)
            content_type = content_type or 'application/octet-stream'

        # 读取文件
        with open(local_path, 'rb') as f:
            file_data = f.read()

        # 上传
        response = self.client.storage.from_(self.bucket).upload(
            path=remote_path,
            file=file_data,
            file_options={"content-type": content_type}
        )

        # 返回存储路径标识
        return self.get_storage_path(remote_path)

    def upload_bytes(self, data: bytes, remote_path: str,
                     content_type: str = 'application/octet-stream') -> str:
        """
        上传字节数据到 Storage

        Args:
            data: 文件字节数据
            remote_path: 远程存储路径
            content_type: MIME类型

        Returns:
            存储路径标识 (bucket/path 格式)
        """
        self.connect()

        response = self.client.storage.from_(self.bucket).upload(
            path=remote_path,
            file=data,
            file_options={"content-type": content_type}
        )

        return self.get_storage_path(remote_path)

    def upload_html(self, html_content: str, remote_path: str) -> str:
        """
        上传HTML内容到 Storage

        Args:
            html_content: HTML字符串
            remote_path: 远程存储路径 (如 "task_123/visualization.html")

        Returns:
            存储路径标识 (bucket/path 格式)
        """
        return self.upload_bytes(
            data=html_content.encode('utf-8'),
            remote_path=remote_path,
            content_type='text/html; charset=utf-8'
        )

    def download_file(self, remote_path: str, local_path: str) -> str:
        """
        从 Storage 下载文件

        Args:
            remote_path: 远程存储路径
            local_path: 本地保存路径

        Returns:
            本地文件路径
        """
        self.connect()

        response = self.client.storage.from_(self.bucket).download(remote_path)

        # 确保目录存在
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with open(local_path, 'wb') as f:
            f.write(response)

        return local_path

    def download_bytes(self, remote_path: str) -> bytes:
        """
        下载文件为字节数据

        Args:
            remote_path: 远程存储路径

        Returns:
            文件字节数据
        """
        self.connect()
        return self.client.storage.from_(self.bucket).download(remote_path)

    def copy_file(self, source_path: str, dest_path: str) -> str:
        """
        复制文件 (用于重命名)

        Args:
            source_path: 源路径
            dest_path: 目标路径

        Returns:
            新文件的存储路径标识
        """
        self.connect()

        # Supabase Storage 不支持直接复制，需要下载再上传
        data = self.download_bytes(source_path)

        # 获取原文件的 content-type
        content_type, _ = mimetypes.guess_type(source_path)
        content_type = content_type or 'application/octet-stream'

        return self.upload_bytes(data, dest_path, content_type)

    def move_file(self, source_path: str, dest_path: str) -> str:
        """
        移动文件 (复制后删除原文件)

        Args:
            source_path: 源路径
            dest_path: 目标路径

        Returns:
            新文件的存储路径标识
        """
        storage_path = self.copy_file(source_path, dest_path)
        self.delete_file(source_path)
        return storage_path

    def delete_file(self, remote_path: str) -> bool:
        """
        删除文件

        Args:
            remote_path: 远程存储路径

        Returns:
            是否成功
        """
        self.connect()
        try:
            self.client.storage.from_(self.bucket).remove([remote_path])
            return True
        except Exception as e:
            print(f"[Storage] 删除失败: {e}")
            return False

    def delete_folder(self, folder_path: str) -> bool:
        """
        删除文件夹及其所有内容

        Args:
            folder_path: 文件夹路径

        Returns:
            是否成功
        """
        self.connect()
        try:
            # 列出文件夹内所有文件
            files = self.list_files(folder_path)
            if files:
                paths = [f"{folder_path}/{f['name']}" for f in files]
                self.client.storage.from_(self.bucket).remove(paths)
            return True
        except Exception as e:
            print(f"[Storage] 删除文件夹失败: {e}")
            return False

    def list_files(self, folder_path: str = "") -> List[dict]:
        """
        列出文件夹内的文件

        Args:
            folder_path: 文件夹路径，空字符串表示根目录

        Returns:
            文件信息列表
        """
        self.connect()
        response = self.client.storage.from_(self.bucket).list(folder_path)
        return response

    def get_public_url(self, remote_path: str) -> str:
        """
        获取文件的公开访问URL (仅适用于公开bucket)

        Args:
            remote_path: 远程存储路径

        Returns:
            公开URL
        """
        self.connect()
        response = self.client.storage.from_(self.bucket).get_public_url(remote_path)
        return response

    def create_signed_url(self, remote_path: str, expires_in: int = None) -> str:
        """
        创建签名URL (适用于私有bucket)

        Args:
            remote_path: 远程存储路径
            expires_in: 有效期(秒)，默认使用 self.signed_url_expires

        Returns:
            签名URL (有时效性)
        """
        self.connect()
        expires_in = expires_in or self.signed_url_expires
        response = self.client.storage.from_(self.bucket).create_signed_url(
            remote_path,
            expires_in
        )
        return response.get('signedURL') or response.get('signedUrl')

    def get_url(self, remote_path: str, expires_in: int = None) -> str:
        """
        获取文件访问URL (自动根据bucket类型选择)

        Args:
            remote_path: 远程存储路径
            expires_in: 签名URL有效期(秒)，仅私有bucket有效

        Returns:
            访问URL (公开URL或签名URL)
        """
        if self.is_public:
            return self.get_public_url(remote_path)
        else:
            return self.create_signed_url(remote_path, expires_in)

    def get_storage_path(self, remote_path: str) -> str:
        """
        获取存储路径标识 (用于数据库存储，不是访问URL)

        Args:
            remote_path: 远程存储路径

        Returns:
            格式: bucket_name/remote_path
        """
        return f"{self.bucket}/{remote_path}"

    def file_exists(self, remote_path: str) -> bool:
        """
        检查文件是否存在

        Args:
            remote_path: 远程存储路径

        Returns:
            是否存在
        """
        self.connect()
        try:
            # 尝试获取文件信息
            folder = os.path.dirname(remote_path)
            filename = os.path.basename(remote_path)
            files = self.list_files(folder)
            return any(f['name'] == filename for f in files)
        except Exception:
            return False


# 测试代码
if __name__ == "__main__":
    # 需要设置环境变量 SUPABASE_KEY
    # is_public=False 表示私有bucket，使用签名URL
    storage = SupabaseStorage(is_public=False)

    print("测试 Supabase Storage 连接 (私有模式)...")

    try:
        storage.connect()
        print("连接成功!")

        # 确保桶存在
        storage.ensure_bucket_exists()

        # 测试上传HTML
        test_html = "<html><body><h1>Test</h1></body></html>"
        storage_path = storage.upload_html(test_html, "test/test.html")
        print(f"上传HTML成功, 存储路径: {storage_path}")

        # 获取访问URL (签名URL，1小时有效)
        access_url = storage.get_url("test/test.html")
        print(f"访问URL (1小时有效): {access_url}")

        # 测试列出文件
        files = storage.list_files("test")
        print(f"test文件夹内容: {files}")

        # 清理测试文件
        storage.delete_file("test/test.html")
        print("清理完成")

    except Exception as e:
        print(f"错误: {e}")
        print("请确保设置了 SUPABASE_KEY 环境变量")
