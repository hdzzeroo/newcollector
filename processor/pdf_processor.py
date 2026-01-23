"""
PDF 文件处理器
提取 PDF 前几页的文字内容
支持 Docling (带OCR) 和 pdfplumber (纯文本) 两种模式
"""

import io
from typing import Optional, List, Literal
from dataclasses import dataclass


@dataclass
class PDFContent:
    """PDF内容提取结果"""
    success: bool
    text: str
    page_count: int
    extracted_pages: int
    error_message: Optional[str] = None
    extractor_used: Optional[str] = None  # 记录使用的提取器


class PDFProcessor:
    """PDF 文件处理器"""

    def __init__(
        self,
        max_pages: int = 2,
        use_docling: bool = True,
        force_ocr: bool = False,
        ocr_engine: Literal["easyocr", "tesseract", "rapidocr"] = "easyocr"
    ):
        """
        初始化 PDF 处理器

        Args:
            max_pages: 最大提取页数
            use_docling: 是否优先使用 Docling（支持OCR）
            force_ocr: 是否强制使用 OCR（针对扫描PDF）
            ocr_engine: OCR 引擎选择 ("easyocr", "tesseract", "rapidocr")
        """
        self.max_pages = max_pages
        self.use_docling = use_docling
        self.force_ocr = force_ocr
        self.ocr_engine = ocr_engine

        # Docling 相关对象（懒加载）
        self._docling_converter = None

    def _init_docling_converter(self):
        """初始化 Docling 转换器（懒加载）"""
        if self._docling_converter is not None:
            return self._docling_converter

        try:
            from docling.document_converter import DocumentConverter, PdfFormatOption
            from docling.datamodel.base_models import InputFormat
            from docling.datamodel.pipeline_options import PdfPipelineOptions

            # 配置 Pipeline 选项
            pdf_options = PdfPipelineOptions(
                do_ocr=True,
                do_table_structure=True,
            )

            # 配置 OCR 引擎
            if self.ocr_engine == "easyocr":
                from docling.datamodel.pipeline_options import EasyOcrOptions
                pdf_options.ocr_options = EasyOcrOptions(
                    force_full_page_ocr=self.force_ocr
                )
            elif self.ocr_engine == "tesseract":
                from docling.datamodel.pipeline_options import TesseractOcrOptions
                pdf_options.ocr_options = TesseractOcrOptions(
                    force_full_page_ocr=self.force_ocr
                )
            elif self.ocr_engine == "rapidocr":
                from docling.datamodel.pipeline_options import RapidOcrOptions
                pdf_options.ocr_options = RapidOcrOptions(
                    force_full_page_ocr=self.force_ocr
                )
            else:
                # 默认使用 EasyOCR
                from docling.datamodel.pipeline_options import EasyOcrOptions
                pdf_options.ocr_options = EasyOcrOptions(
                    force_full_page_ocr=self.force_ocr
                )

            self._docling_converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options)
                }
            )
            return self._docling_converter

        except ImportError as e:
            print(f"[PDF] Docling 未安装: {e}")
            return None
        except Exception as e:
            print(f"[PDF] Docling 初始化失败: {e}")
            return None

    def _extract_with_docling(self, pdf_path: str, num_pages: int) -> PDFContent:
        """使用 Docling 提取 PDF 文字（支持OCR）"""
        try:
            converter = self._init_docling_converter()
            if converter is None:
                return PDFContent(
                    success=False,
                    text="",
                    page_count=0,
                    extracted_pages=0,
                    error_message="Docling 初始化失败，请安装: pip install docling",
                    extractor_used="docling"
                )

            # 转换 PDF
            result = converter.convert(pdf_path)
            doc = result.document

            # 导出为 Markdown（保留结构）
            full_text = doc.export_to_markdown()

            # 获取页数信息
            total_pages = len(doc.pages) if hasattr(doc, 'pages') else 0

            # 如果需要限制页数，截取内容
            if num_pages and total_pages > num_pages:
                # Docling 按页导出比较复杂，这里简单截取前 N 页的估算字符数
                # 一般日本大学文档每页约 1500-2000 字符
                estimated_chars = num_pages * 2000
                if len(full_text) > estimated_chars:
                    full_text = full_text[:estimated_chars] + "\n\n[... 内容已截断 ...]"

            return PDFContent(
                success=True,
                text=full_text,
                page_count=total_pages,
                extracted_pages=min(num_pages, total_pages) if num_pages else total_pages,
                extractor_used="docling"
            )

        except Exception as e:
            return PDFContent(
                success=False,
                text="",
                page_count=0,
                extracted_pages=0,
                error_message=f"Docling 提取失败: {str(e)}",
                extractor_used="docling"
            )

    def extract_text(self, pdf_path: str, num_pages: int = None) -> PDFContent:
        """
        提取 PDF 前 N 页的文字

        优先使用 Docling（支持OCR），失败时回退到 pdfplumber

        Args:
            pdf_path: PDF 文件路径
            num_pages: 提取页数，默认使用 self.max_pages

        Returns:
            PDFContent
        """
        num_pages = num_pages or self.max_pages

        # 优先使用 Docling（支持 OCR）
        if self.use_docling:
            result = self._extract_with_docling(pdf_path, num_pages)
            if result.success:
                return result
            # Docling 失败，回退到 pdfplumber
            print(f"[PDF] Docling 失败，回退到 pdfplumber: {result.error_message}")

        # 使用 pdfplumber（纯文本PDF）
        return self._extract_with_pdfplumber(pdf_path, num_pages)

    def _extract_with_pdfplumber(self, pdf_path: str, num_pages: int) -> PDFContent:
        """使用 pdfplumber 提取 PDF 文字（仅支持纯文本PDF）"""
        try:
            import pdfplumber

            text_parts = []
            total_pages = 0
            extracted = 0

            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                pages_to_extract = min(num_pages, total_pages)

                for i in range(pages_to_extract):
                    page = pdf.pages[i]
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(f"--- Page {i + 1} ---\n{page_text}")
                        extracted += 1

            full_text = "\n\n".join(text_parts)

            return PDFContent(
                success=True,
                text=full_text,
                page_count=total_pages,
                extracted_pages=extracted,
                extractor_used="pdfplumber"
            )

        except ImportError:
            return self._extract_with_pypdf(pdf_path, num_pages)
        except Exception as e:
            return PDFContent(
                success=False,
                text="",
                page_count=0,
                extracted_pages=0,
                error_message=f"pdfplumber 提取失败: {str(e)}",
                extractor_used="pdfplumber"
            )

    def _extract_with_pypdf(self, pdf_path: str, num_pages: int) -> PDFContent:
        """使用 PyPDF2 作为备选方案"""
        try:
            from PyPDF2 import PdfReader

            text_parts = []
            reader = PdfReader(pdf_path)
            total_pages = len(reader.pages)
            pages_to_extract = min(num_pages, total_pages)
            extracted = 0

            for i in range(pages_to_extract):
                page = reader.pages[i]
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(f"--- Page {i + 1} ---\n{page_text}")
                    extracted += 1

            full_text = "\n\n".join(text_parts)

            return PDFContent(
                success=True,
                text=full_text,
                page_count=total_pages,
                extracted_pages=extracted,
                extractor_used="pypdf2"
            )

        except ImportError:
            return PDFContent(
                success=False,
                text="",
                page_count=0,
                extracted_pages=0,
                error_message="需要安装 pdfplumber 或 PyPDF2: pip install pdfplumber",
                extractor_used="pypdf2"
            )
        except Exception as e:
            return PDFContent(
                success=False,
                text="",
                page_count=0,
                extracted_pages=0,
                error_message=f"PyPDF2 提取失败: {str(e)}",
                extractor_used="pypdf2"
            )

    def extract_text_from_bytes(self, pdf_bytes: bytes, num_pages: int = None) -> PDFContent:
        """
        从字节数据提取 PDF 文字

        注意：Docling 不支持直接从字节提取，需要先保存为临时文件

        Args:
            pdf_bytes: PDF 字节数据
            num_pages: 提取页数

        Returns:
            PDFContent
        """
        num_pages = num_pages or self.max_pages

        # 如果使用 Docling，需要先保存为临时文件
        if self.use_docling:
            import tempfile
            import os
            try:
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                    tmp_file.write(pdf_bytes)
                    tmp_path = tmp_file.name

                result = self._extract_with_docling(tmp_path, num_pages)

                # 清理临时文件
                os.unlink(tmp_path)

                if result.success:
                    return result
                print(f"[PDF] Docling 失败，回退到 pdfplumber: {result.error_message}")
            except Exception as e:
                print(f"[PDF] Docling 临时文件处理失败: {e}")

        # 回退到 pdfplumber
        try:
            import pdfplumber

            text_parts = []
            total_pages = 0
            extracted = 0

            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                total_pages = len(pdf.pages)
                pages_to_extract = min(num_pages, total_pages)

                for i in range(pages_to_extract):
                    page = pdf.pages[i]
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(f"--- Page {i + 1} ---\n{page_text}")
                        extracted += 1

            full_text = "\n\n".join(text_parts)

            return PDFContent(
                success=True,
                text=full_text,
                page_count=total_pages,
                extracted_pages=extracted,
                extractor_used="pdfplumber"
            )

        except ImportError:
            return PDFContent(
                success=False,
                text="",
                page_count=0,
                extracted_pages=0,
                error_message="需要安装 pdfplumber: pip install pdfplumber",
                extractor_used="pdfplumber"
            )
        except Exception as e:
            return PDFContent(
                success=False,
                text="",
                page_count=0,
                extracted_pages=0,
                error_message=f"提取失败: {str(e)}",
                extractor_used="pdfplumber"
            )

    def extract_first_pages_as_pdf(self, pdf_path: str, num_pages: int = None) -> Optional[bytes]:
        """
        提取前 N 页为新的 PDF 字节数据

        Args:
            pdf_path: 源 PDF 路径
            num_pages: 提取页数

        Returns:
            新 PDF 的字节数据，失败返回 None
        """
        num_pages = num_pages or self.max_pages

        try:
            from PyPDF2 import PdfReader, PdfWriter

            reader = PdfReader(pdf_path)
            writer = PdfWriter()

            pages_to_extract = min(num_pages, len(reader.pages))

            for i in range(pages_to_extract):
                writer.add_page(reader.pages[i])

            output = io.BytesIO()
            writer.write(output)
            return output.getvalue()

        except Exception as e:
            print(f"[PDF] 提取页面失败: {e}")
            return None

    def get_page_count(self, pdf_path: str) -> int:
        """获取 PDF 总页数"""
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except Exception:
            return 0

    def is_pdf_valid(self, pdf_path: str) -> bool:
        """检查 PDF 是否有效"""
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(pdf_path)
            return len(reader.pages) > 0
        except Exception:
            return False


# 测试代码
if __name__ == "__main__":
    import sys

    # 默认测试参数
    test_path = sys.argv[1] if len(sys.argv) > 1 else "test.pdf"
    use_ocr = "--ocr" in sys.argv or "-o" in sys.argv

    print("=" * 50)
    print("PDF 处理器测试")
    print("=" * 50)
    print(f"测试文件: {test_path}")
    print(f"强制 OCR: {use_ocr}")
    print()

    # 创建处理器（优先使用 Docling）
    processor = PDFProcessor(
        max_pages=2,
        use_docling=True,
        force_ocr=use_ocr,
        ocr_engine="easyocr"
    )

    import os
    if os.path.exists(test_path):
        print("测试 PDF 文本提取...")
        result = processor.extract_text(test_path)

        print(f"\n提取结果:")
        print(f"  成功: {result.success}")
        print(f"  提取器: {result.extractor_used}")
        print(f"  总页数: {result.page_count}")
        print(f"  提取页数: {result.extracted_pages}")
        print(f"  文本长度: {len(result.text)}")

        if result.error_message:
            print(f"  错误信息: {result.error_message}")

        if result.text:
            print(f"\n文本预览 (前800字):")
            print("-" * 40)
            print(result.text[:800])
            print("-" * 40)

        # 测试禁用 Docling 的情况
        print("\n\n测试仅使用 pdfplumber...")
        processor_legacy = PDFProcessor(max_pages=2, use_docling=False)
        result_legacy = processor_legacy.extract_text(test_path)
        print(f"  成功: {result_legacy.success}")
        print(f"  提取器: {result_legacy.extractor_used}")
        print(f"  文本长度: {len(result_legacy.text)}")

    else:
        print(f"测试文件不存在: {test_path}")
        print("\n用法:")
        print(f"  python {sys.argv[0]} <pdf_path> [--ocr]")
        print("\n示例:")
        print(f"  python {sys.argv[0]} document.pdf")
        print(f"  python {sys.argv[0]} scanned.pdf --ocr  # 强制使用 OCR")
