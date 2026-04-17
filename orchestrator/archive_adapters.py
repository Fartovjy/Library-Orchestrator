from __future__ import annotations

import functools
import hashlib
import html
import re
import shutil
import subprocess
import zipfile
from pathlib import Path

from .models import ContainerKind


TEXT_EXTENSIONS = {".txt", ".md", ".rtf", ".fb2", ".html", ".htm"}
BOOK_LIKE_EXTENSIONS = {".txt", ".md", ".rtf", ".fb2", ".html", ".htm", ".pdf", ".epub", ".djvu", ".doc", ".docx"}
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


def unpack_source(source_path: Path, destination_dir: Path) -> Path:
    destination_dir.mkdir(parents=True, exist_ok=True)
    kind = detect_container_kind(source_path)
    unpack_dir = destination_dir / "payload"
    unpack_dir.mkdir(parents=True, exist_ok=True)
    if kind == ContainerKind.DIRECTORY:
        shutil.copytree(source_path, unpack_dir, dirs_exist_ok=True)
        return unpack_dir
    if kind in {ContainerKind.ZIP, ContainerKind.EPUB}:
        with zipfile.ZipFile(source_path) as archive:
            archive.extractall(unpack_dir)
        return unpack_dir
    if kind in {ContainerKind.RAR, ContainerKind.SEVEN_Z}:
        extract_with_external_tool(source_path, unpack_dir)
        return unpack_dir
    shutil.copy2(source_path, unpack_dir / source_path.name)
    return unpack_dir


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
