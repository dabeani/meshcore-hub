"""Tests for channel label helpers."""

from meshcore_hub.collector.letsmesh_decoder import LetsMeshPacketDecoder
from meshcore_hub.common.channel_labels import build_channel_labels, format_channel_label


def test_format_channel_label_resolves_prefixed_two_byte_hash(
    monkeypatch,
) -> None:
    """0x-prefixed 2-byte hashes resolve to configured channel labels."""
    key_hex = "EB50A1BCB3E4E5D7BF69A57C9DADA211"
    channel_hash = LetsMeshPacketDecoder._compute_channel_hash(key_hex, hash_bytes=2)
    monkeypatch.setenv("COLLECTOR_LETSMESH_DECODER_KEYS", f"bot={key_hex}")
    build_channel_labels.cache_clear()

    try:
        assert (
            format_channel_label(
                channel_name=None,
                channel_hash=f" 0x{channel_hash.lower()} ",
                channel_idx=None,
            )
            == "#bot"
        )
    finally:
        build_channel_labels.cache_clear()


def test_format_channel_label_normalizes_prefixed_two_byte_hash_fallback() -> None:
    """Fallback display strips 0x prefixes from 2-byte channel hashes."""
    assert (
        format_channel_label(
            channel_name=None,
            channel_hash=" 0x00ca ",
            channel_idx=None,
        )
        == "Ch 00CA"
    )
