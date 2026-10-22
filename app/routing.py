"""выбор следующей ноги. без бд и сети, чтоб правила можно было тестить юнитами"""
from dataclasses import asdict, dataclass
from typing import Iterable

# ошибки которые присылает провайдер в вебхуке
PROVIDER_ERRORS = ("technical", "moderation", "payload")
# ошибки которые ставим сами: 5xx/таймаут на POST /tasks и когда сторож сдался.
# обрабатываются так же как technical
OWN_ERRORS = ("submit_failed", "timeout")


@dataclass(frozen=True)
class Leg:
    leg_id: int
    provider: str
    priority: int
    max_ref_images: int
    skip_on_moderation: bool

    def to_json(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json(cls, data: dict) -> "Leg":
        return cls(**data)


@dataclass(frozen=True)
class NextLeg:
    index: int


@dataclass(frozen=True)
class GiveUp:
    reason: str


def build_plan(legs: Iterable[Leg], ref_images: int) -> list[Leg]:
    """ноги которые принимают столько референсов, по приоритету"""
    return sorted(
        (leg for leg in legs if leg.max_ref_images >= ref_images),
        key=lambda leg: leg.priority,
    )


def after_failure(
    plan: list[Leg],
    failed_index: int,
    error_type: str,
    moderation_seen: bool = False,
) -> NextLeg | GiveUp:
    """что делать после того как упала попытка на plan[failed_index].

    moderation_seen - раньше в этой задаче уже был отказ по модерации.
    флаг не сбрасывается: если потом упало по technical, контент чище не стал,
    так что ноги со skip_on_moderation пропускаем до конца цепочки
    """
    if error_type == "payload":
        # кривой запрос, его любая нога отвергнет
        return GiveUp("payload_rejected")

    flagged = moderation_seen or error_type == "moderation"
    for index in range(failed_index + 1, len(plan)):
        if flagged and plan[index].skip_on_moderation:
            continue
        return NextLeg(index)
    return GiveUp("legs_exhausted")
