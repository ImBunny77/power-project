"""PDF parsing utilities using pdfplumber with pymupdf fallback."""
from __future__ import annotations

import io
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def extract_pdf_tables(
    content: bytes,
    pages: Optional[list[int]] = None,
) -> list[pd.DataFrame]:
    """
    Extract tables from a PDF using pdfplumber.
    Falls back to pymupdf if pdfplumber fails.
    Returns list of DataFrames (one per table found).
    """
    tables = []

    # Try pdfplumber first
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            page_list = pdf.pages
            if pages:
                page_list = [pdf.pages[i] for i in pages if i < len(pdf.pages)]
            for page in page_list:
                try:
                    page_tables = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 5,
                            "join_tolerance": 5,
                        }
                    )
                    if not page_tables:
                        # Try with looser settings
                        page_tables = page.extract_tables(
                            table_settings={
                                "vertical_strategy": "text",
                                "horizontal_strategy": "text",
                            }
                        )
                    for table in (page_tables or []):
                        if table and len(table) > 1:
                            df = pd.DataFrame(table[1:], columns=table[0])
                            # Clean up
                            df = df.dropna(how="all")
                            df = df[~df.apply(
                                lambda row: all(str(v).strip() == "" for v in row), axis=1
                            )]
                            if not df.empty:
                                tables.append(df)
                except Exception as e:
                    logger.debug(f"pdfplumber page extraction error: {e}")
        if tables:
            return tables
    except ImportError:
        logger.debug("pdfplumber not available, trying pymupdf")
    except Exception as e:
        logger.warning(f"pdfplumber failed: {e}, trying pymupdf")

    # Fallback: pymupdf
    try:
        import fitz  # pymupdf
        doc = fitz.open(stream=content, filetype="pdf")
        for page_num in range(len(doc)):
            if pages and page_num not in pages:
                continue
            page = doc[page_num]
            tab = page.find_tables()
            for table in tab.tables:
                data = table.extract()
                if data and len(data) > 1:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df = df.dropna(how="all")
                    if not df.empty:
                        tables.append(df)
        doc.close()
    except ImportError:
        logger.debug("pymupdf not available")
    except Exception as e:
        logger.warning(f"pymupdf table extraction failed: {e}")

    return tables


def extract_pdf_text(
    content: bytes,
    max_pages: int = 20,
    pages: Optional[list[int]] = None,
) -> str:
    """Extract raw text from PDF. Returns concatenated text."""
    text_parts = []

    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            page_list = pdf.pages[:max_pages]
            if pages:
                page_list = [pdf.pages[i] for i in pages if i < len(pdf.pages)]
            for page in page_list:
                try:
                    t = page.extract_text() or ""
                    text_parts.append(t)
                except Exception:
                    pass
        if text_parts:
            return "\n\n".join(text_parts)
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"pdfplumber text extraction failed: {e}")

    # Fallback pymupdf
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        for page_num in range(min(max_pages, len(doc))):
            if pages and page_num not in pages:
                continue
            page = doc[page_num]
            text_parts.append(page.get_text())
        doc.close()
    except ImportError:
        pass
    except Exception as e:
        logger.warning(f"pymupdf text extraction failed: {e}")

    return "\n\n".join(text_parts)


def find_tables_with_keyword(content: bytes, keywords: list[str]) -> list[tuple[int, pd.DataFrame]]:
    """
    Find tables in PDF that contain any of the given keywords.
    Returns list of (page_num, DataFrame) tuples.
    """
    try:
        import pdfplumber
    except ImportError:
        return []

    results = []
    keywords_lower = [k.lower() for k in keywords]

    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_text = (page.extract_text() or "").lower()
                if not any(kw in page_text for kw in keywords_lower):
                    continue
                page_tables = page.extract_tables() or []
                for table in page_tables:
                    if not table:
                        continue
                    table_str = str(table).lower()
                    if any(kw in table_str for kw in keywords_lower):
                        df = pd.DataFrame(table[1:], columns=table[0])
                        df = df.dropna(how="all")
                        if not df.empty:
                            results.append((page_num, df))
    except Exception as e:
        logger.warning(f"keyword table search failed: {e}")

    return results


def extract_hyperlinks(content: bytes) -> list[dict]:
    """Extract all hyperlinks from PDF."""
    links = []
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        for page_num, page in enumerate(doc):
            for link in page.get_links():
                if link.get("uri"):
                    links.append({"page": page_num, "url": link["uri"]})
        doc.close()
    except Exception as e:
        logger.debug(f"Link extraction failed: {e}")
    return links
