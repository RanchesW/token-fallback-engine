import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://app:app@localhost:5432/app"
    webhook_secret: str = "dev-secret"
    # по этому адресу провайдеры стучатся к нам в вебхуки
    public_base_url: str = "http://localhost:8000"
    provider_urls: dict[str, str] = field(default_factory=dict)
    provider_timeout: float = 60.0
    submit_timeout: float = 5.0
    watchdog_interval: float = 1.0
    max_active_tasks: int = 3
    # сколько раз переотправляем зависшую submitting попытку, потом считаем submit_failed
    max_dispatches: int = 3
    db_pool_size: int = 10

    @property
    def dispatch_lease(self) -> float:
        # хватает на один запрос к провайдеру, после этого попытку может забрать сторож
        return self.submit_timeout + 5.0


def load_settings() -> Settings:
    env = os.environ
    # PROVIDER_ALPHA_URL=http://alpha:8000 -> {"alpha": "http://alpha:8000"}
    providers = {
        key[len("PROVIDER_"):-len("_URL")].lower(): value.rstrip("/")
        for key, value in env.items()
        if key.startswith("PROVIDER_") and key.endswith("_URL")
    }
    defaults = Settings()
    return Settings(
        database_url=env.get("DATABASE_URL", defaults.database_url),
        webhook_secret=env.get("WEBHOOK_SECRET", defaults.webhook_secret),
        public_base_url=env.get("PUBLIC_BASE_URL", defaults.public_base_url).rstrip("/"),
        provider_urls=providers,
        provider_timeout=float(env.get("PROVIDER_TIMEOUT", defaults.provider_timeout)),
        submit_timeout=float(env.get("SUBMIT_TIMEOUT", defaults.submit_timeout)),
        watchdog_interval=float(env.get("WATCHDOG_INTERVAL", defaults.watchdog_interval)),
        max_active_tasks=int(env.get("MAX_ACTIVE_TASKS", defaults.max_active_tasks)),
        max_dispatches=int(env.get("MAX_DISPATCHES", defaults.max_dispatches)),
        db_pool_size=int(env.get("DB_POOL_SIZE", defaults.db_pool_size)),
    )
