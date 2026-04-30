from __future__ import annotations


BLOCKED_MIME_PREFIXES = ("image/svg",)

_VIDEO_FTYP_BRANDS = {
    b"avc1",
    b"hvc1",
    b"hev1",
    b"qt  ",
    b"3gp4",
    b"3gp5",
    b"3g2a",
    b"M4V ",
}

_IMAGE_FTYP_BRANDS = {
    b"heic",
    b"heix",
    b"hevc",
    b"hevx",
    b"heif",
    b"mif1",
    b"msf1",
    b"avif",
    b"avis",
}

_AUDIO_FTYP_BRANDS = {
    b"M4A ",
    b"M4B ",
    b"M4P ",
    b"M4R ",
}


def normalize_mimetype(raw_mimetype: str | None) -> str:
    return (raw_mimetype or "").split(";", 1)[0].strip().lower()


def media_category_from_mimetype(raw_mimetype: str | None) -> str | None:
    mimetype = normalize_mimetype(raw_mimetype)
    if mimetype.startswith("image/"):
        return "image"
    if mimetype.startswith("video/"):
        return "video"
    if mimetype.startswith("audio/"):
        return "audio"
    return None


def is_blocked_declared_mimetype(raw_mimetype: str | None) -> bool:
    mimetype = normalize_mimetype(raw_mimetype)
    return any(mimetype.startswith(prefix) for prefix in BLOCKED_MIME_PREFIXES)


def is_allowed_declared_mimetype(
    raw_mimetype: str | None,
    allowed_categories: set[str] | None = None,
) -> bool:
    category = media_category_from_mimetype(raw_mimetype)
    if category is None:
        return False
    if allowed_categories is not None and category not in allowed_categories:
        return False
    return not is_blocked_declared_mimetype(raw_mimetype)


def validate_upload_content(
    file_storage,
    raw_mimetype: str | None,
    *,
    allowed_categories: set[str] | None = None,
    reject_active_text_payloads: bool = True,
    enforce_declared_category_match: bool = True,
    sniff_bytes: int = 2048,
) -> str | None:
    mimetype = normalize_mimetype(raw_mimetype)

    if not is_allowed_declared_mimetype(mimetype, allowed_categories=allowed_categories):
        return "unsupported_declared_type"

    head = _peek_stream_head(file_storage, size=max(256, int(sniff_bytes)))
    if head is None:
        return "unable_to_read_stream"
    if len(head) == 0:
        return "empty_file"

    if reject_active_text_payloads and _looks_like_active_text_content(head):
        return "blocked_active_content"

    detected_category = detect_media_category_from_header(head)
    declared_category = media_category_from_mimetype(mimetype)
    if (
        enforce_declared_category_match
        and
        detected_category is not None
        and declared_category is not None
        and detected_category != declared_category
    ):
        return "declared_type_mismatch"
    if allowed_categories is not None and detected_category is not None and detected_category not in allowed_categories:
        return "unsupported_detected_type"

    return None


def detect_media_category_from_header(head: bytes) -> str | None:
    if len(head) >= 3 and head[0:3] == b"\xFF\xD8\xFF":
        return "image"
    if len(head) >= 8 and head[0:8] == b"\x89PNG\r\n\x1A\n":
        return "image"
    if len(head) >= 6 and head[0:6] in {b"GIF87a", b"GIF89a"}:
        return "image"
    if len(head) >= 12 and head[0:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image"
    if len(head) >= 2 and head[0:2] == b"BM":
        return "image"
    if len(head) >= 4 and head[0:4] in {b"II*\x00", b"MM\x00*"}:
        return "image"

    ftyp_brand = _extract_ftyp_brand(head)
    if ftyp_brand is not None:
        if ftyp_brand in _IMAGE_FTYP_BRANDS:
            return "image"
        if ftyp_brand in _AUDIO_FTYP_BRANDS:
            return "audio"
        if ftyp_brand in _VIDEO_FTYP_BRANDS:
            return "video"

    if len(head) >= 4 and head[0:4] == b"OggS":
        return "audio"
    if len(head) >= 4 and head[0:4] == b"fLaC":
        return "audio"
    if len(head) >= 3 and head[0:3] == b"ID3":
        return "audio"
    if len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xE0) == 0xE0:
        return "audio"
    if len(head) >= 12 and head[0:4] == b"RIFF" and head[8:12] == b"WAVE":
        return "audio"

    if len(head) >= 4 and head[0:4] == b"\x1A\x45\xDF\xA3":
        # Matroska/WebM can be either audio-only or video; keep it ambiguous.
        return None

    return None


def _extract_ftyp_brand(head: bytes) -> bytes | None:
    if len(head) < 12:
        return None
    if head[4:8] != b"ftyp":
        return None
    return head[8:12]


def _peek_stream_head(file_storage, size: int = 2048) -> bytes | None:
    stream = getattr(file_storage, "stream", file_storage)
    try:
        original_position = stream.tell()
    except Exception:
        original_position = None

    try:
        if original_position is not None:
            stream.seek(0)
        return stream.read(size)
    except Exception:
        return None
    finally:
        if original_position is not None:
            try:
                stream.seek(original_position)
            except Exception:
                pass


def _looks_like_active_text_content(head: bytes) -> bool:
    sample = head[:512].lstrip().lower()
    markers = (b"<svg", b"<html", b"<!doctype", b"<?xml", b"<script")
    return any(marker in sample for marker in markers)
