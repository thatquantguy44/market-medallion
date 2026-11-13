"""Source-owned PNG/GIF/JPEG container framing; no image decoding or network access."""


def asset_framing_valid(payload, source_url):
    """Match the reviewed URL format and reject missing GIF framing/dimensions.

    GIF framing: https://www.w3.org/Graphics/GIF/spec-gif89a.txt.
    JPEG SOI/EOI: https://www.w3.org/Graphics/JPEG/itu-t81.pdf, Table B.1.
    Raw bytes stay canonical; this does not certify rendered chart content.
    """
    if source_url.endswith(".png"):
        return payload.startswith(b"\x89PNG\r\n\x1a\n")
    if source_url.endswith(".jpg"):
        return (
            len(payload) >= 4 and payload.startswith(b"\xff\xd8") and payload.endswith(b"\xff\xd9")
        )
    if source_url.endswith(".gif"):
        return (
            len(payload) >= 14
            and payload[:6] in {b"GIF87a", b"GIF89a"}
            and int.from_bytes(payload[6:8], "little") > 0
            and int.from_bytes(payload[8:10], "little") > 0
            and payload.endswith(b";")
        )
    return False
