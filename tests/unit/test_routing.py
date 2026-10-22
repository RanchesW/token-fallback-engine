from app.routing import GiveUp, Leg, NextLeg, after_failure, build_plan

IMAGE_FAST = [
    Leg(1, "alpha", 1, 4, False),
    Leg(2, "beta", 2, 10, False),
]
VIDEO_LONG = [
    Leg(3, "beta", 1, 2, False),
    Leg(4, "alpha", 2, 1, True),
]


def test_plan_filters_by_ref_images_and_sorts_by_priority():
    shuffled = list(reversed(IMAGE_FAST))
    assert [leg.provider for leg in build_plan(shuffled, 0)] == ["alpha", "beta"]
    assert [leg.provider for leg in build_plan(IMAGE_FAST, 5)] == ["beta"]
    assert build_plan(IMAGE_FAST, 11) == []
    assert [leg.provider for leg in build_plan(VIDEO_LONG, 2)] == ["beta"]


def test_technical_like_errors_go_to_next_leg():
    for error in ("technical", "submit_failed", "timeout"):
        assert after_failure(IMAGE_FAST, 0, error) == NextLeg(1)


def test_last_leg_failure_gives_up():
    assert after_failure(IMAGE_FAST, 1, "technical") == GiveUp("legs_exhausted")


def test_payload_fails_immediately():
    assert after_failure(IMAGE_FAST, 0, "payload") == GiveUp("payload_rejected")


def test_moderation_skips_flagged_legs():
    assert after_failure(VIDEO_LONG, 0, "moderation") == GiveUp("legs_exhausted")
    assert after_failure(VIDEO_LONG, 0, "technical") == NextLeg(1)
    # у image-fast нет ног со skip_on_moderation
    assert after_failure(IMAGE_FAST, 0, "moderation") == NextLeg(1)


def test_moderation_flag_sticks_through_later_failures():
    plan = [
        Leg(1, "a", 1, 1, False),
        Leg(2, "b", 2, 1, False),
        Leg(3, "c", 3, 1, True),
        Leg(4, "d", 4, 1, False),
    ]
    # a: moderation -> b; b: technical -> c всё равно пропускаем -> d
    assert after_failure(plan, 0, "moderation") == NextLeg(1)
    assert after_failure(plan, 1, "technical", moderation_seen=True) == NextLeg(3)
    assert after_failure(plan, 1, "technical", moderation_seen=False) == NextLeg(2)


def test_leg_json_roundtrip():
    leg = IMAGE_FAST[0]
    assert Leg.from_json(leg.to_json()) == leg
