from lxml import objectify, etree


def test_local_metadata_is_up_to_date(local_metadata, live_metadata):
    """ Ensure local metadata is semantically the same as the current live one."""

    # Simple, imperfect but hopefully good enough normalization
    local_metadata_normalized = etree.tostring(objectify.fromstring(local_metadata))
    live_metadata_normalized = etree.tostring(objectify.fromstring(live_metadata))
    assert local_metadata_normalized == live_metadata_normalized
