from opsiq_runtime.domain.activation_policy.identity import (
    build_activation_item,
    resolve_item_group_id,
)


def test_resolve_item_group_id_linkcode_wins():
    """Test that linkcode takes precedence over gtin."""
    result = resolve_item_group_id(linkcode="LINK001", gtin="GTIN001")
    assert result == "LINK001"


def test_resolve_item_group_id_gtin_fallback():
    """Test that gtin is used when linkcode is None."""
    result = resolve_item_group_id(linkcode=None, gtin="GTIN001")
    assert result == "GTIN001"


def test_resolve_item_group_id_gtin_fallback_empty_string():
    """Test that gtin is used when linkcode is empty string."""
    result = resolve_item_group_id(linkcode="", gtin="GTIN001")
    assert result == "GTIN001"


def test_resolve_item_group_id_returns_none_when_both_none():
    """Test that None is returned when both linkcode and gtin are None."""
    result = resolve_item_group_id(linkcode=None, gtin=None)
    assert result is None


def test_resolve_item_group_id_returns_none_when_both_empty():
    """Test that None is returned when both linkcode and gtin are empty strings."""
    result = resolve_item_group_id(linkcode="", gtin="")
    assert result is None


def test_build_activation_item_with_item_group_id():
    """Test building item with pre-resolved item_group_id."""
    item = build_activation_item(
        item_group_id="PRE_RESOLVED",
        linkcode="LINK001",
        gtin="GTIN001",
        score=0.9,
    )
    assert item.item_group_id == "PRE_RESOLVED"
    assert item.linkcode == "LINK001"
    assert item.gtin == "GTIN001"
    assert item.score == 0.9


def test_build_activation_item_resolves_from_linkcode():
    """Test that item_group_id is resolved from linkcode when not provided."""
    item = build_activation_item(linkcode="LINK001", gtin="GTIN001", score=0.8)
    assert item.item_group_id == "LINK001"
    assert item.linkcode == "LINK001"
    assert item.gtin == "GTIN001"


def test_build_activation_item_resolves_from_gtin_when_no_linkcode():
    """Test that item_group_id is resolved from gtin when linkcode is None."""
    item = build_activation_item(linkcode=None, gtin="GTIN001", score=0.7)
    assert item.item_group_id == "GTIN001"
    assert item.gtin == "GTIN001"
    assert item.linkcode is None


def test_build_activation_item_with_metadata():
    """Test building item with metadata."""
    metadata = {"promo_price": 10.0, "title": "Milk"}
    item = build_activation_item(
        linkcode="LINK001", gtin="GTIN001", metadata=metadata
    )
    assert item.metadata == metadata
    assert item.metadata["promo_price"] == 10.0


def test_build_activation_item_raises_when_cannot_resolve():
    """Test that ValueError is raised when item_group_id cannot be resolved."""
    import pytest

    with pytest.raises(ValueError, match="Cannot resolve item_group_id"):
        build_activation_item(linkcode=None, gtin=None)


def test_build_activation_item_raises_when_empty_strings():
    """Test that ValueError is raised when both identifiers are empty strings."""
    import pytest

    with pytest.raises(ValueError, match="Cannot resolve item_group_id"):
        build_activation_item(linkcode="", gtin="")


def test_build_activation_item_with_category():
    """Test building item with category."""
    item = build_activation_item(
        linkcode="LINK001", category="Dairy", score=0.9
    )
    assert item.category == "Dairy"
    assert item.item_group_id == "LINK001"


def test_build_activation_item_default_score():
    """Test that score defaults to 0.0."""
    item = build_activation_item(linkcode="LINK001")
    assert item.score == 0.0


def test_build_activation_item_empty_metadata_default():
    """Test that metadata defaults to empty dict."""
    item = build_activation_item(linkcode="LINK001")
    assert item.metadata == {}
