from __future__ import annotations

import functools
import hashlib
import html
import re
import shutil
import subprocess
import zipfile
from collections import deque
from pathlib import Path

from .models import ContainerKind


TEXT_EXTENSIONS = {".txt", ".md", ".rtf", ".fb2", ".html", ".htm"}
BOOK_LIKE_EXTENSIONS = {".txt", ".md", ".rtf", ".fb2", ".html", ".htm", ".pdf", ".epub", ".djvu", ".doc", ".docx"}
SPLIT_STANDALONE_BOOK_EXTENSIONS = {
    ".fb2",
    ".epub",
    ".pdf",
    ".djvu",
    ".doc",
    ".docx",
    ".rtf",
    ".txt",
    ".mobi",
    ".azw",
    ".azw3",
}
HTML_COLLECTION_ASSET_EXTENSIONS = {
    ".css",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".webp",
    ".svg",
    ".js",
}
NON_BOOK_BOOKSHELF_NAMES = {
    "readme",
    "about",
    "license",
    "licence",
    "notes",
    "note",
    "toc",
    "contents",
    "content",
    "index",
}
NON_BOOK_EXTENSIONS = {
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hh",
    ".hpp",
    ".hxx",
    ".java",
    ".kt",
    ".kts",
    ".cs",
    ".go",
    ".rs",
    ".swift",
    ".scala",
    ".vb",
    ".py",
    ".pyw",
    ".js",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".jsx",
    ".php",
    ".rb",
    ".pl",
    ".pm",
    ".lua",
    ".r",
    ".pas",
    ".asm",
    ".s",
    ".y",
    ".yy",
    ".cup",
    ".lex",
    ".l",
    ".mak",
    ".mk",
    ".cmake",
    ".gradle",
    ".vcxproj",
    ".sln",
    ".csproj",
    ".fsproj",
}
SOURCE_CODE_MARKERS = (
    "#include",
    "using namespace",
    "public static void main",
    "int main(",
    "def __init__(",
    "fn main(",
    "package main",
    "console.log(",
    "import java.",
    "select ",
    "create table",
    "typedef ",
)
NORMALIZABLE_TITLE_SUFFIXES = (
    ".zip",
    ".rar",
    ".7z",
    ".cbz",
    ".epub",
    ".fb2",
    ".pdf",
    ".chm",
    ".iso",
    ".html",
    ".htm",
    ".txt",
    ".rtf",
)

MAGIC_SIGNATURES: list[tuple[bytes, str]] = [
    (b"PK\x03\x04", "zip"),
    (b"PK\x05\x06", "zip"),
    (b"PK\x07\x08", "zip"),
    (b"%PDF-", "pdf"),
    (b"Rar!\x1a\x07\x00", "rar"),
    (b"Rar!\x1a\x07\x01\x00", "rar"),
    (b"7z\xbc\xaf\x27\x1c", "7z"),
    (b"\xff\xd8\xff", "image"),
    (b"\x89PNG\r\n\x1a\n", "image"),
    (b"GIF87a", "image"),
    (b"GIF89a", "image"),
    (b"BM", "image"),
    (b"RIFF", "riff"),
    (b"ID3", "audio"),
    (b"MZ", "executable"),
    (b"ITSF", "chm"),
    (b"SQLite format 3\x00", "sqlite"),
]

EMPTY_ZIP_SHA256 = "8739c76e681f900923b900c9df0ef75cf421d39cabb54650c4b9ad19b6a76d85"


def should_unpack_with_agent(kind: ContainerKind) -> bool:
    tool = find_archive_tool()
    if kind in {ContainerKind.ZIP, ContainerKind.EPUB}:
        return True
    if kind in {ContainerKind.RAR, ContainerKind.SEVEN_Z}:
        return tool is not None
    return False


def detect_container_kind(path: Path) -> ContainerKind:
    if path.is_dir():
        return ContainerKind.DIRECTORY
    magic_kind = detect_magic_kind(path)
    if magic_kind == "zip":
        return ContainerKind.ZIP
    if magic_kind == "pdf":
        return ContainerKind.PDF
    if magic_kind == "rar":
        return ContainerKind.RAR
    if magic_kind == "7z":
        return ContainerKind.SEVEN_Z
    suffix = path.suffix.lower()
    if suffix in {".zip", ".cbz"}:
        return ContainerKind.ZIP
    if suffix == ".epub":
        return ContainerKind.EPUB
    if suffix == ".fb2":
        return ContainerKind.FB2
    if suffix == ".pdf":
        return ContainerKind.PDF
    if suffix == ".rar":
        return ContainerKind.RAR
    if suffix == ".7z":
        return ContainerKind.SEVEN_Z
    return ContainerKind.FILE


def detect_magic_kind(path: Path) -> str | None:
    if not path.is_file():
        return None
    header = read_file_header(path, 64)
    if len(header) < 12 and header.startswith(b"\x00"):
        return None
    for signature, name in MAGIC_SIGNATURES:
        if header.startswith(signature):
            if name == "riff":
                return _detect_riff_subtype(header)
            return name
    # MP4 family: `ftyp` marker usually starts at offset 4.
    if len(header) >= 12 and header[4:8] == b"ftyp":
        return "video"
    # WebP is RIFF-based with WEBP marker at offset 8.
    if len(header) >= 12 and header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image"
    # ISO images often contain CD001 at sector 16 offset 1; this is a light probe.
    if is_iso_image(path):
        return "iso"
    # Plain text heuristic for extensionless or odd-suffixed files.
    if looks_like_text(header):
        return "text"
    return None


def classify_file_role(path: Path) -> str:
    if path.is_dir():
        return "directory"
    suffix = path.suffix.lower()
    magic_kind = detect_magic_kind(path)
    name = path.name.lower()
    if name.startswith("~$"):
        return "trash"
    if suffix in {".tmp", ".bak", ".part", ".crdownload", ".jccfg3"}:
        return "trash"
    if magic_kind in {"image", "video", "audio", "executable", "sqlite"}:
        return "trash"
    if suffix in NON_BOOK_EXTENSIONS:
        return "non_book"
    if magic_kind == "text" and _looks_like_source_code(path):
        return "non_book"
    if magic_kind in {"zip", "pdf", "rar", "7z", "text", "chm", "iso"}:
        return "book_or_container"
    if suffix in BOOK_LIKE_EXTENSIONS:
        return "book_or_container"
    if suffix in {".zip", ".rar", ".7z", ".cbz", ".epub", ".fb2", ".pdf", ".chm", ".iso"}:
        return "book_or_container"
    return "unknown"


def is_supported_unpack_kind(kind: ContainerKind) -> bool:
    tool = find_archive_tool()
    return kind in {
        ContainerKind.DIRECTORY,
        ContainerKind.FILE,
        ContainerKind.ZIP,
        ContainerKind.EPUB,
        ContainerKind.FB2,
        ContainerKind.PDF,
    } or (kind in {ContainerKind.RAR, ContainerKind.SEVEN_Z} and tool is not None)


def is_unpackable_archive_kind(kind: ContainerKind) -> bool:
    return kind in {ContainerKind.ZIP, ContainerKind.EPUB, ContainerKind.RAR, ContainerKind.SEVEN_Z}


def stage_source(source_path: Path, destination_dir: Path, max_nested_depth: int = 0) -> tuple[Path, int]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    staged_dir = destination_dir / "payload"
    staged_dir.mkdir(parents=True, exist_ok=True)
    if source_path.is_dir():
        shutil.copytree(source_path, staged_dir, dirs_exist_ok=True)
        nested_count = expand_nested_archives(staged_dir, max_nested_depth)
        return staged_dir, nested_count
    shutil.copy2(source_path, staged_dir / source_path.name)
    nested_count = expand_nested_archives(staged_dir, max_nested_depth)
    return staged_dir, nested_count


def unpack_source(source_path: Path, destination_dir: Path, max_nested_depth: int = 0) -> tuple[Path, int]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    kind = detect_container_kind(source_path)
    unpack_dir = destination_dir / "payload"
    unpack_dir.mkdir(parents=True, exist_ok=True)
    if kind == ContainerKind.DIRECTORY:
        shutil.copytree(source_path, unpack_dir, dirs_exist_ok=True)
        nested_count = expand_nested_archives(unpack_dir, max_nested_depth)
        return unpack_dir, nested_count
    if kind in {ContainerKind.ZIP, ContainerKind.EPUB}:
        with zipfile.ZipFile(source_path) as archive:
            archive.extractall(unpack_dir)
        nested_count = expand_nested_archives(unpack_dir, max_nested_depth)
        return unpack_dir, nested_count
    if kind in {ContainerKind.RAR, ContainerKind.SEVEN_Z}:
        extract_with_external_tool(source_path, unpack_dir)
        nested_count = expand_nested_archives(unpack_dir, max_nested_depth)
        return unpack_dir, nested_count
    shutil.copy2(source_path, unpack_dir / source_path.name)
    nested_count = expand_nested_archives(unpack_dir, max_nested_depth)
    return unpack_dir, nested_count


def collect_excerpt(source_path: Path, max_words: int) -> str:
    kind = detect_container_kind(source_path)
    if kind == ContainerKind.DIRECTORY:
        for file_path in sorted(source_path.rglob("*")):
            if file_path.is_file():
                excerpt = _excerpt_from_file(file_path, max_words)
                if excerpt:
                    return excerpt
        return ""
    if kind in {ContainerKind.ZIP, ContainerKind.EPUB}:
        return _excerpt_from_zip(source_path, max_words)
    if kind in {ContainerKind.RAR, ContainerKind.SEVEN_Z}:
        return ""
    return _excerpt_from_file(source_path, max_words)


def detect_book_candidates(container_dir: Path) -> list[Path]:
    if not container_dir.exists() or not container_dir.is_dir():
        return []

    root = container_dir.resolve()
    directory_candidates: list[Path] = []
    chosen_dirs: set[Path] = set()
    for dir_path in sorted(path for path in root.rglob("*") if path.is_dir()):
        if any(parent in chosen_dirs for parent in dir_path.parents):
            continue
        if _is_html_collection_dir(dir_path):
            directory_candidates.append(dir_path)
            chosen_dirs.add(dir_path)

    file_candidates: list[Path] = []
    for file_path in sorted(path for path in root.rglob("*") if path.is_file()):
        if any(parent in chosen_dirs for parent in file_path.parents):
            continue
        if _is_split_book_file_candidate(file_path):
            file_candidates.append(file_path)

    # If the container itself looks like a single HTML book, keep it whole.
    if not directory_candidates and len(file_candidates) <= 1 and _is_html_collection_dir(root):
        return []

    return directory_candidates + file_candidates


def pack_directory_to_zip(source_dir: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        output_path,
        mode="w",
        compression=zipfile.ZIP_LZMA,
        compresslevel=9,
    ) as archive:
        for file_path in sorted(source_dir.rglob("*")):
            if file_path.is_file():
                archive.write(file_path, file_path.relative_to(source_dir))


def is_valid_packed_archive(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        with zipfile.ZipFile(path) as archive:
            return any(not info.is_dir() for info in archive.infolist())
    except (OSError, zipfile.BadZipFile):
        return False


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    if path.is_file():
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    for file_path in sorted(p for p in path.rglob("*") if p.is_file()):
        digest.update(str(file_path.relative_to(path)).encode("utf-8", errors="ignore"))
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def sanitize_name(value: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', "_", value.strip())
    return cleaned.strip(". ") or "Unknown"


def normalize_title(value: str) -> str:
    title = value.strip()
    changed = True
    while changed and title:
        changed = False
        lower = title.lower()
        for suffix in NORMALIZABLE_TITLE_SUFFIXES:
            if lower.endswith(suffix):
                title = title[: -len(suffix)].rstrip(" ._-")
                changed = True
                break
    return title or "Unknown"


def author_initial(author: str) -> str:
    author = author.strip()
    if not author:
        return "#"
    return author[0].upper()


def _excerpt_from_zip(path: Path, max_words: int) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                suffix = Path(info.filename).suffix.lower()
                if suffix not in TEXT_EXTENSIONS:
                    continue
                with archive.open(info) as handle:
                    data = handle.read(1024 * 256)
                return _normalize_text(data.decode("utf-8", errors="ignore"), max_words)
    except zipfile.BadZipFile:
        return ""
    return ""


def _excerpt_from_file(path: Path, max_words: int) -> str:
    if path.suffix.lower() not in TEXT_EXTENSIONS:
        return ""
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    return _normalize_text(content, max_words)


def _looks_like_source_code(path: Path) -> bool:
    if path.suffix.lower() in {".html", ".htm", ".fb2", ".rtf"}:
        return False
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    except OSError:
        return False
    if not content.strip():
        return False
    lowered = content.lower()
    marker_hits = sum(1 for marker in SOURCE_CODE_MARKERS if marker in lowered)
    syntax_hits = sum(
        token in content
        for token in ("{", "}", ";", "/*", "*/", "->", "::", "<?php", "class ", "struct ")
    )
    return marker_hits >= 1 and syntax_hits >= 2


def _is_html_collection_dir(path: Path) -> bool:
    try:
        children = list(path.iterdir())
    except OSError:
        return False
    html_files = [child for child in children if child.is_file() and child.suffix.lower() in {".html", ".htm"}]
    if not html_files:
        return False
    if any(child.name.lower() in {"index.html", "index.htm", "toc.html", "toc.htm"} for child in html_files):
        return True
    asset_files = [
        child for child in children
        if child.is_file() and child.suffix.lower() in HTML_COLLECTION_ASSET_EXTENSIONS
    ]
    return len(html_files) >= 2 and (len(asset_files) >= 1 or len(children) > len(html_files))


def _is_split_book_file_candidate(path: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix not in SPLIT_STANDALONE_BOOK_EXTENSIONS:
        return False
    stem = path.stem.strip().lower()
    if stem in NON_BOOK_BOOKSHELF_NAMES:
        return False
    if suffix not in {".txt", ".rtf"}:
        return True
    try:
        sample = path.read_text(encoding="utf-8", errors="ignore")[:8192]
    except OSError:
        return False
    words = re.findall(r"\S+", sample)
    return len(words) >= 40


def expand_nested_archives(root_dir: Path, max_depth: int) -> int:
    if max_depth <= 0 or not root_dir.exists():
        return 0

    expanded_count = 0
    pending: deque[tuple[Path, int]] = deque((path, 1) for path in sorted(root_dir.rglob("*")) if path.is_file())
    seen: set[str] = set()
    while pending:
        archive_path, depth = pending.popleft()
        if not archive_path.exists() or not archive_path.is_file():
            continue
        archive_key = str(archive_path.resolve())
        if archive_key in seen:
            continue
        seen.add(archive_key)
        kind = detect_container_kind(archive_path)
        if not is_unpackable_archive_kind(kind) or not is_supported_unpack_kind(kind):
            continue
        if depth > max_depth:
            continue

        expanded_dir = _nested_expanded_dir(archive_path)
        temp_dir = archive_path.parent / f".__extracting_{archive_path.stem}"
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        payload_dir, _ = unpack_source(archive_path, temp_dir, max_nested_depth=0)
        shutil.move(str(payload_dir), expanded_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)
        archive_path.unlink(missing_ok=True)
        expanded_count += 1

        if depth < max_depth:
            nested_files = [path for path in sorted(expanded_dir.rglob("*")) if path.is_file()]
            pending.extend((path, depth + 1) for path in nested_files)
    return expanded_count


def _nested_expanded_dir(archive_path: Path) -> Path:
    base_name = normalize_title(archive_path.stem) or archive_path.stem or "nested_archive"
    candidate = archive_path.with_name(base_name)
    if not candidate.exists() and candidate != archive_path:
        return candidate
    index = 2
    while True:
        fallback = archive_path.with_name(f"{base_name} ({index})")
        if not fallback.exists():
            return fallback
        index += 1


def _normalize_text(content: str, max_words: int) -> str:
    content = html.unescape(content)
    content = re.sub(r"<[^>]+>", " ", content)
    words = re.findall(r"\S+", content)
    return " ".join(words[:max_words])


def read_file_header(path: Path, size: int = 64) -> bytes:
    try:
        with path.open("rb") as handle:
            return handle.read(size)
    except OSError:
        return b""


def looks_like_text(header: bytes) -> bool:
    if not header:
        return False
    if b"\x00" in header:
        return False
    printable = sum(
        1
        for byte in header
        if byte in b"\t\n\r" or 32 <= byte <= 126 or byte >= 128
    )
    return (printable / len(header)) >= 0.85


def _detect_riff_subtype(header: bytes) -> str:
    if len(header) >= 12 and header[8:12] == b"WAVE":
        return "audio"
    if len(header) >= 12 and header[8:12] in {b"AVI ", b"WEBP"}:
        return "video" if header[8:12] == b"AVI " else "image"
    return "riff"


def is_iso_image(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            handle.seek(16 * 2048)
            marker = handle.read(6)
        return marker[1:6] == b"CD001"
    except OSError:
        return False


@functools.lru_cache(maxsize=1)
def find_archive_tool() -> Path | None:
    candidates = [
        Path(r"C:\Program Files\7-Zip\7z.exe"),
        Path(r"C:\Program Files (x86)\7-Zip\7z.exe"),
        Path(r"C:\Program Files\WinRAR\UnRAR.exe"),
        Path(r"C:\Program Files\WinRAR\WinRAR.exe"),
        Path(r"C:\Program Files (x86)\WinRAR\UnRAR.exe"),
        Path(r"C:\Program Files (x86)\WinRAR\WinRAR.exe"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def extract_with_external_tool(source_path: Path, output_dir: Path) -> None:
    tool = find_archive_tool()
    if tool is None:
        raise RuntimeError("No external extractor found for RAR/7Z archives.")
    output_dir.mkdir(parents=True, exist_ok=True)
    lower_name = tool.name.lower()
    if lower_name == "7z.exe":
        command = [str(tool), "x", "-y", f"-o{output_dir}", str(source_path)]
    elif lower_name == "unrar.exe":
        command = [str(tool), "x", "-o+", str(source_path), str(output_dir)]
    else:
        command = [str(tool), "x", "-ibck", "-y", str(source_path), str(output_dir)]
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(f"Archive extraction failed: {details}")
