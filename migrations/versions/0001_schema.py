"""схема: юзеры, журнал, каталог, задачи, попытки, оплаты

Revision ID: 0001
Revises:
Create Date: 2026-10-22
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


UPGRADE = r"""
CREATE TABLE users (
    id          bigint PRIMARY KEY,
    balance     bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE models (
    id      serial PRIMARY KEY,
    slug    text NOT NULL UNIQUE,
    price   bigint NOT NULL CHECK (price > 0)
);

CREATE TABLE legs (
    id                  serial PRIMARY KEY,
    model_id            int NOT NULL REFERENCES models (id),
    provider            text NOT NULL,
    priority            int NOT NULL,
    max_ref_images      int NOT NULL CHECK (max_ref_images >= 0),
    skip_on_moderation  boolean NOT NULL DEFAULT false,
    UNIQUE (model_id, priority)
);

CREATE TABLE tasks (
    id               uuid PRIMARY KEY,
    user_id          bigint NOT NULL REFERENCES users (id),
    model_id         int NOT NULL REFERENCES models (id),
    prompt           text NOT NULL,
    ref_images       jsonb NOT NULL,
    price            bigint NOT NULL CHECK (price > 0),
    -- подходящие ноги, сохраняем при создании, чтоб правка каталога не ломала уже запущенные задачи
    plan             jsonb NOT NULL,
    status           text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    result_url       text,
    failure_reason   text,
    idempotency_key  text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CHECK ((status = 'completed') = (result_url IS NOT NULL))
);
CREATE INDEX tasks_user_running ON tasks (user_id) WHERE status = 'running';
CREATE UNIQUE INDEX tasks_user_idempotency ON tasks (user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE attempts (
    id                    uuid PRIMARY KEY,
    task_id               uuid NOT NULL REFERENCES tasks (id),
    seq                   int NOT NULL,
    leg_index             int NOT NULL,
    provider              text NOT NULL,
    status                text NOT NULL
        CHECK (status IN ('submitting', 'waiting', 'completed', 'failed', 'superseded')),
    error_type            text
        CHECK (error_type IN ('technical', 'moderation', 'payload', 'submit_failed', 'timeout')),
    external_id           text,
    result_url            text,
    -- completed пришёл, когда мы уже забили на эту попытку
    late                  boolean NOT NULL DEFAULT false,
    dispatch_lease_until  timestamptz,
    dispatch_count        int NOT NULL DEFAULT 0,
    deadline_at           timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    submitted_at          timestamptz,
    finished_at           timestamptz,
    UNIQUE (task_id, seq),
    UNIQUE (provider, external_id)
);
-- у задачи максимум одна активная попытка
CREATE UNIQUE INDEX attempts_one_active ON attempts (task_id)
    WHERE status IN ('submitting', 'waiting');
CREATE INDEX attempts_waiting_deadline ON attempts (deadline_at) WHERE status = 'waiting';
CREATE INDEX attempts_submitting_lease ON attempts (dispatch_lease_until) WHERE status = 'submitting';

CREATE TABLE payments (
    payment_id   text PRIMARY KEY,
    user_id      bigint NOT NULL REFERENCES users (id),
    tokens       bigint NOT NULL CHECK (tokens > 0),
    status       text NOT NULL,
    credited_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ledger (
    id             bigserial PRIMARY KEY,
    user_id        bigint NOT NULL REFERENCES users (id),
    delta          bigint NOT NULL CHECK (delta <> 0),
    kind           text NOT NULL CHECK (kind IN ('payment', 'charge', 'refund')),
    -- одна строка на одно движение денег: payment:<id>, charge:<task>, refund:<task>
    op_key         text NOT NULL UNIQUE,
    task_id        uuid REFERENCES tasks (id),
    payment_id     text REFERENCES payments (payment_id),
    balance_after  bigint NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'payment') = (payment_id IS NOT NULL)),
    CHECK ((kind IN ('charge', 'refund')) = (task_id IS NOT NULL))
);
CREATE INDEX ledger_user ON ledger (user_id, id DESC);


-- баланс это по сути кэш SUM(ledger.delta), поменять его можно только вставкой в ledger
CREATE FUNCTION ledger_apply() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kind = 'refund' AND NOT EXISTS (
        SELECT 1 FROM tasks WHERE id = NEW.task_id AND status = 'failed'
    ) THEN
        RAISE EXCEPTION 'refund for task % which is not failed', NEW.task_id;
    END IF;

    UPDATE users SET balance = balance + NEW.delta
     WHERE id = NEW.user_id
    RETURNING balance INTO NEW.balance_after;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'user % does not exist', NEW.user_id USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END $$;

-- триггер BEFORE, поэтому ON CONFLICT DO NOTHING на ledger юзать нельзя:
-- баланс уже успеет поменяться. дубли отсекаем раньше (лок на оплате, смена статуса задачи),
-- а UNIQUE на op_key просто чтоб баг упал с ошибкой, а не молча прошёл
CREATE TRIGGER ledger_apply BEFORE INSERT ON ledger
    FOR EACH ROW EXECUTE FUNCTION ledger_apply();

CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger is append-only';
END $$;

CREATE TRIGGER ledger_append_only BEFORE UPDATE OR DELETE ON ledger
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();

CREATE FUNCTION users_balance_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.balance <> 0 THEN
        RAISE EXCEPTION 'new users start with zero balance';
    END IF;
    -- depth 1 = кто-то напрямую делает UPDATE, depth 2 = это наш триггер ledger_apply
    IF TG_OP = 'UPDATE' AND NEW.balance IS DISTINCT FROM OLD.balance AND pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'users.balance changes only through ledger';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER users_balance_guard BEFORE INSERT OR UPDATE OF balance ON users
    FOR EACH ROW EXECUTE FUNCTION users_balance_guard();

-- из конечного статуса выйти нельзя: failed (с возвратом) уже никогда не станет completed
CREATE FUNCTION tasks_final_status() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status <> 'running' AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'task % is already %', OLD.id, OLD.status;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER tasks_final_status BEFORE UPDATE OF status ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_final_status();
"""

DOWNGRADE = r"""
DROP TABLE ledger;
DROP TABLE payments;
DROP TABLE attempts;
DROP TABLE tasks;
DROP TABLE legs;
DROP TABLE models;
DROP TABLE users;
DROP FUNCTION ledger_apply();
DROP FUNCTION ledger_append_only();
DROP FUNCTION users_balance_guard();
DROP FUNCTION tasks_final_status();
"""


def upgrade() -> None:
    op.execute(UPGRADE)


def downgrade() -> None:
    op.execute(DOWNGRADE)
