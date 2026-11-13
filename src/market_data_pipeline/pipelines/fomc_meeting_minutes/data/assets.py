"""Source-owned PNG/GIF container framing; no image decoding or network access."""


def asset_framing_valid(payload, source_url):
    """Match the reviewed URL format and reject missing GIF framing/dimensions.

    GIF header/LSD/trailer: https://www.w3.org/Graphics/GIF/spec-gif89a.txt.
    Raw bytes stay canonical; this does not certify rendered chart content.
    """
    if source_url.endswith(".png"):
        return payload.startswith(b"\x89PNG\r\n\x1a\n")
    if source_url.endswith(".gif"):
        return (
            len(payload) >= 14
            and payload[:6] in {b"GIF87a", b"GIF89a"}
            and int.from_bytes(payload[6:8], "little") > 0
            and int.from_bytes(payload[8:10], "little") > 0
            and payload.endswith(b";")
        )
    return False
