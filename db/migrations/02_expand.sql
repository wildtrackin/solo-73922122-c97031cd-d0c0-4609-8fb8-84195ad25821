-- 02 扩展阶段（Expand）：新长表 + 迁移状态表 + 双写触发器
-- 由应用首次启动时在单个事务里幂等执行；即使中途进程被杀，
-- 未提交的 DDL 随事务回滚，重启后重新执行，不会留下半套对象。

CREATE TABLE IF NOT EXISTS measurements (
    sample_id BIGINT NOT NULL,
    metric    TEXT   NOT NULL CHECK (metric IN ('temp', 'ph')),
    value     NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (sample_id, metric)
);

-- 回填进度以“已处理到的最大样本 id（水位）”记录，随批次事务提交。
-- 任何时刻都没有“迁了一半的行”：一行样本的两个指标在同一条 INSERT 里落库。
CREATE TABLE IF NOT EXISTS migration_state (
    id          INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    state       TEXT NOT NULL CHECK (state IN
                  ('init', 'backfilling', 'paused', 'ready', 'switching', 'switched')),
    last_id     BIGINT NOT NULL DEFAULT 0,
    message     TEXT NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO migration_state (id, state)
VALUES (1, 'init')
ON CONFLICT (id) DO NOTHING;

-- 旧表 -> 新表 的双写触发器。
-- 关键语义：
--   * INSERT 整组写入 temp/ph 两项，同一事务原子生效；
--   * UPDATE 同步全部列（含 id 变更，旧指标随旧 id 搬走，不留幽灵行）；
--   * DELETE 连带删除指标，行不会被回填“复活”；
--   * 回填采用 DO UPDATE 同步，直接 SQL 的修改不会被旧批次覆盖。
CREATE OR REPLACE FUNCTION samples_to_measurements_fn()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO measurements (sample_id, metric, value)
        VALUES (NEW.id, 'temp', NEW.temp),
               (NEW.id, 'ph',   NEW.ph)
        ON CONFLICT (sample_id, metric) DO UPDATE
            SET value = EXCLUDED.value;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.id IS DISTINCT FROM OLD.id THEN
            DELETE FROM measurements WHERE sample_id = OLD.id;
        END IF;
        INSERT INTO measurements (sample_id, metric, value)
        VALUES (NEW.id, 'temp', NEW.temp),
               (NEW.id, 'ph',   NEW.ph)
        ON CONFLICT (sample_id, metric) DO UPDATE
            SET value = EXCLUDED.value;
        RETURN NEW;
    END IF;

    -- DELETE
    DELETE FROM measurements WHERE sample_id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS samples_to_measurements ON samples;
CREATE TRIGGER samples_to_measurements
    AFTER INSERT OR UPDATE OR DELETE ON samples
    FOR EACH ROW EXECUTE FUNCTION samples_to_measurements_fn();
