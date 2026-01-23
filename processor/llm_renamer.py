"""
LLM 智能文件重命名器
使用豆包 API 根据文件内容生成标准化文件名
"""

import os
import re
from typing import Optional, Dict, Any
from dataclasses import dataclass
from openai import OpenAI
from json_repair import loads as json_loads

from .pdf_processor import PDFProcessor
from .doc_processor import DocProcessor


@dataclass
class RenameResult:
    """重命名结果

    新命名格式: {大学名}_{所属}_{専攻}_{課程}_{年度}_{入学時期}_{文書種別}_{詳細}.{拡張子}
    """
    success: bool
    original_name: str
    renamed_name: Optional[str]
    university: Optional[str]      # 大学名
    department: Optional[str]      # 所属（研究科または学部）
    major: Optional[str]           # 専攻
    course: Optional[str]          # 課程：学部/修士/博士
    year: Optional[str]            # 年度（西暦）
    semester: Optional[str]        # 入学時期：4月/10月/4月10月
    doc_type: Optional[str]        # 文書種別
    detail: Optional[str]          # 詳細信息
    confidence: float
    reason: Optional[str]
    raw_response: Optional[str]
    error_message: Optional[str] = None


class LLMRenamer:
    """LLM 智能重命名器"""

    # 默认使用豆包
    # DEFAULT_MODEL = "doubao-1-5-pro-32k-250115"  # 旧版本
    DEFAULT_MODEL = "doubao-seed-1-6-lite-251015"  # 1.6 lite 版本，更快更便宜
    DEFAULT_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"

    def __init__(self, api_key: str = None, model: str = None, base_url: str = None):
        """
        初始化 LLM 重命名器

        Args:
            api_key: API 密钥
            model: 模型名称
            base_url: API 基础 URL
        """
        self.api_key = api_key or os.getenv('DOUBAO_API_KEY')
        self.model = model or self.DEFAULT_MODEL
        self.base_url = base_url or self.DEFAULT_BASE_URL

        self.client: Optional[OpenAI] = None
        self.prompt_template: Optional[str] = None

        # 处理器
        self.pdf_processor = PDFProcessor(max_pages=2)
        self.doc_processor = DocProcessor(max_paragraphs=50)

    def connect(self):
        """建立 API 连接"""
        if self.client is None:
            if not self.api_key:
                raise ValueError("API key is required. Set DOUBAO_API_KEY environment variable.")
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
        return self.client

    def load_prompt_template(self, prompt_path: str = None):
        """加载 Prompt 模板"""
        if prompt_path is None:
            # 默认路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            prompt_path = os.path.join(current_dir, '..', 'AIPmt', 'Rename.txt')

        with open(prompt_path, 'r', encoding='utf-8') as f:
            self.prompt_template = f.read()

        return self.prompt_template

    def _extract_file_content(self, file_path: str) -> str:
        """提取文件内容"""
        ext = os.path.splitext(file_path)[1].lower()

        if ext == '.pdf':
            result = self.pdf_processor.extract_text(file_path)
            if result.success:
                return result.text
            else:
                return f"[PDF提取失败: {result.error_message}]"

        elif ext in ['.doc', '.docx']:
            result = self.doc_processor.extract_text(file_path)
            if result.success:
                return result.text
            else:
                return f"[DOC提取失败: {result.error_message}]"

        else:
            return f"[不支持的文件类型: {ext}]"

    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名，移除非法字符"""
        # 移除非法字符
        illegal_chars = r'[/\\:*?"<>|]'
        cleaned = re.sub(illegal_chars, '_', filename)

        # 移除连续的下划线
        cleaned = re.sub(r'_+', '_', cleaned)

        # 移除首尾下划线
        cleaned = cleaned.strip('_')

        return cleaned

    def _build_prompt(self, content: str, context: Dict[str, str]) -> str:
        """构建完整的 Prompt"""
        if self.prompt_template is None:
            self.load_prompt_template()

        prompt = self.prompt_template
        prompt = prompt.replace('{school_name}', context.get('school_name', 'Unknown'))
        prompt = prompt.replace('{url}', context.get('url', 'Unknown'))
        prompt = prompt.replace('{breadcrumb}', context.get('breadcrumb', 'Unknown'))
        prompt = prompt.replace('{title}', context.get('title', 'Unknown'))
        prompt = prompt.replace('{parent_title}', context.get('parent_title', 'Unknown'))
        prompt = prompt.replace('{original_name}', context.get('original_name', 'Unknown'))
        prompt = prompt.replace('{content}', content[:8000])  # 限制内容长度

        return prompt

    def rename_file(self, file_path: str, context: Dict[str, str] = None) -> RenameResult:
        """
        为文件生成新名称

        Args:
            file_path: 文件路径
            context: 上下文信息 {url, breadcrumb, title, parent_title, original_name}

        Returns:
            RenameResult
        """
        context = context or {}
        original_name = context.get('original_name') or os.path.basename(file_path)
        file_ext = os.path.splitext(file_path)[1].lower()

        try:
            # 提取文件内容
            print(f"[LLM] 提取文件内容: {original_name}")
            content = self._extract_file_content(file_path)

            if content.startswith('[') and '失败' in content:
                return RenameResult(
                    success=False,
                    original_name=original_name,
                    renamed_name=None,
                    university=None,
                    department=None,
                    major=None,
                    course=None,
                    year=None,
                    semester=None,
                    doc_type=None,
                    detail=None,
                    confidence=0.0,
                    reason=None,
                    raw_response=None,
                    error_message=content
                )

            # 构建 Prompt
            prompt = self._build_prompt(content, context)

            # 调用 LLM
            print(f"[LLM] 调用 AI 进行重命名...")
            self.connect()

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )

            raw_response = response.choices[0].message.content

            # 解析响应
            result_dict = json_loads(raw_response)

            # 获取确定的学校名称（如果有）
            confirmed_school = context.get('school_name')
            if confirmed_school and confirmed_school != 'Unknown':
                # 使用确定的学校名覆盖 LLM 识别的结果
                result_dict['university'] = confirmed_school
                print(f"[LLM] 使用确定学校名: {confirmed_school}")

            renamed = result_dict.get('renamed', '')
            if renamed:
                # 如果有确定的学校名，替换文件名中的大学名部分
                if confirmed_school and confirmed_school != 'Unknown':
                    parts = renamed.rsplit('.', 1)  # 分离扩展名
                    name_part = parts[0]
                    ext_part = '.' + parts[1] if len(parts) > 1 else file_ext

                    name_fields = name_part.split('_')
                    if len(name_fields) >= 1:
                        # 第一个字段是大学名，用确定的学校名替换
                        name_fields[0] = confirmed_school
                        renamed = '_'.join(name_fields) + ext_part

                # 确保扩展名正确
                if not renamed.lower().endswith(file_ext):
                    renamed = renamed.rsplit('.', 1)[0] + file_ext
                renamed = self._sanitize_filename(renamed)

            print(f"[LLM] 重命名结果: {renamed}")

            return RenameResult(
                success=True,
                original_name=original_name,
                renamed_name=renamed,
                university=result_dict.get('university'),
                department=result_dict.get('department'),
                major=result_dict.get('major'),
                course=result_dict.get('course'),
                year=result_dict.get('year'),
                semester=result_dict.get('semester'),
                doc_type=result_dict.get('doc_type'),
                detail=result_dict.get('detail'),
                confidence=float(result_dict.get('confidence', 0.5)),
                reason=result_dict.get('reason'),
                raw_response=raw_response
            )

        except Exception as e:
            return RenameResult(
                success=False,
                original_name=original_name,
                renamed_name=None,
                university=None,
                department=None,
                major=None,
                course=None,
                year=None,
                semester=None,
                doc_type=None,
                detail=None,
                confidence=0.0,
                reason=None,
                raw_response=None,
                error_message=f"LLM 调用失败: {str(e)}"
            )

    def rename_from_text(self, text: str, context: Dict[str, str],
                         file_extension: str = '.pdf') -> RenameResult:
        """
        根据已提取的文本生成文件名

        Args:
            text: 文件文本内容
            context: 上下文信息
            file_extension: 文件扩展名

        Returns:
            RenameResult
        """
        original_name = context.get('original_name', 'unknown' + file_extension)

        try:
            # 构建 Prompt
            prompt = self._build_prompt(text, context)

            # 调用 LLM
            self.connect()

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )

            raw_response = response.choices[0].message.content
            result_dict = json_loads(raw_response)

            # 获取确定的学校名称（如果有）
            confirmed_school = context.get('school_name')
            if confirmed_school and confirmed_school != 'Unknown':
                result_dict['university'] = confirmed_school

            renamed = result_dict.get('renamed', '')
            if renamed:
                # 如果有确定的学校名，替换文件名中的大学名部分
                if confirmed_school and confirmed_school != 'Unknown':
                    parts = renamed.rsplit('.', 1)
                    name_part = parts[0]
                    ext_part = '.' + parts[1] if len(parts) > 1 else file_extension

                    name_fields = name_part.split('_')
                    if len(name_fields) >= 1:
                        name_fields[0] = confirmed_school
                        renamed = '_'.join(name_fields) + ext_part

                if not renamed.lower().endswith(file_extension):
                    renamed = renamed.rsplit('.', 1)[0] + file_extension
                renamed = self._sanitize_filename(renamed)

            return RenameResult(
                success=True,
                original_name=original_name,
                renamed_name=renamed,
                university=result_dict.get('university'),
                department=result_dict.get('department'),
                major=result_dict.get('major'),
                course=result_dict.get('course'),
                year=result_dict.get('year'),
                semester=result_dict.get('semester'),
                doc_type=result_dict.get('doc_type'),
                detail=result_dict.get('detail'),
                confidence=float(result_dict.get('confidence', 0.5)),
                reason=result_dict.get('reason'),
                raw_response=raw_response
            )

        except Exception as e:
            return RenameResult(
                success=False,
                original_name=original_name,
                renamed_name=None,
                university=None,
                department=None,
                major=None,
                course=None,
                year=None,
                semester=None,
                doc_type=None,
                detail=None,
                confidence=0.0,
                reason=None,
                raw_response=None,
                error_message=f"LLM 调用失败: {str(e)}"
            )

    def batch_rename(self, files: list, delay: float = 1.0) -> list:
        """
        批量重命名文件

        Args:
            files: 文件信息列表 [{path, context}, ...]
            delay: 请求间隔(秒)

        Returns:
            RenameResult 列表
        """
        import time
        results = []
        total = len(files)

        for i, file_info in enumerate(files):
            print(f"[LLM] 处理 {i + 1}/{total}")

            file_path = file_info.get('path')
            context = file_info.get('context', {})

            result = self.rename_file(file_path, context)
            results.append(result)

            if i < total - 1:
                time.sleep(delay)

        success_count = sum(1 for r in results if r.success)
        print(f"[LLM] 批量重命名完成: {success_count}/{total} 成功")

        return results


# 测试代码
if __name__ == "__main__":
    import sys

    # 需要设置环境变量 DOUBAO_API_KEY
    # 或在 Sdata.py 中读取

    # 尝试从 Sdata 读取 key
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import Sdata
        api_key = Sdata.Dou_Bao_Key
    except ImportError:
        api_key = os.getenv('DOUBAO_API_KEY')

    if not api_key:
        print("请设置 DOUBAO_API_KEY 环境变量")
        exit(1)

    renamer = LLMRenamer(api_key=api_key)

    # 测试文本重命名
    test_text = """
    令和7年度 東京大学大学院理学系研究科
    修士課程学生募集要項

    出願資格
    大学を卒業した者、または令和7年3月までに卒業見込みの者

    出願期間
    令和6年7月1日～7月15日
    """

    context = {
        'url': 'https://www.s.u-tokyo.ac.jp/admission/master/',
        'breadcrumb': '理学系研究科 > 入試情報 > 募集要項',
        'title': '修士課程募集要項',
        'parent_title': '入試情報',
        'original_name': 'yoko_2025.pdf'
    }

    print("测试 LLM 重命名...")
    result = renamer.rename_from_text(test_text, context, '.pdf')

    print(f"\n重命名结果:")
    print(f"  成功: {result.success}")
    print(f"  原名: {result.original_name}")
    print(f"  新名: {result.renamed_name}")
    print(f"  大学: {result.university}")
    print(f"  研究科: {result.department}")
    print(f"  専攻: {result.major}")
    print(f"  課程: {result.course}")
    print(f"  年度: {result.year}")
    print(f"  入学時期: {result.semester}")
    print(f"  文書種別: {result.doc_type}")
    print(f"  詳細: {result.detail}")
    print(f"  置信度: {result.confidence}")
    print(f"  理由: {result.reason}")
