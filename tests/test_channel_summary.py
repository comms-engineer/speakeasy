from channel_summary import build_channel_summary, channel_maybe_present


def test_channel_summary_matches_known_channels():
    summary = build_channel_summary(["general", "tech", "events"])

    assert channel_maybe_present("general", summary)
    assert channel_maybe_present("tech", summary)
    assert channel_maybe_present("events", summary)


def test_channel_summary_normalizes_channel_names():
    summary = build_channel_summary(["#TeCh"])

    assert channel_maybe_present("tech", summary)
    assert channel_maybe_present("#TECH", summary)


def test_channel_summary_rejects_empty_or_invalid_summary():
    assert not channel_maybe_present("tech", b"")
    assert not channel_maybe_present("tech", None)