-- 03 收缩阶段（Contract）：把 samples 由表替换为同名视图
-- 该文件的全部语句由应用在切换时放进“一个事务 + 一次 ACCESS EXCLUSIVE 锁定”执行，
-- 这是全程唯一一次短暂阻塞写入。事务未提交则一切原样保留，旧接口继续可用。

-- 原表的 identity 序列随 DROP TABLE 一并消失，这里重建同名序列并把水位
-- 对齐到现存最大 sample_id，保证切换后新样本继续拿到不冲突的 id。
CREATE SEQUENCE IF NOT EXISTS samples_id_seq;
SELECT setval(
    'samples_id_seq',
    COALESCE(max(sample_id), 1),
    max(sample_id) IS NOT NULL
) FROM measurements;

-- 视图保持原表名与原列名（temp/ph 均 NOT NULL 语义：缺任一指标的组不出现在视图中）。
CREATE OR REPLACE VIEW samples (id, temp, ph) AS
SELECT t.sample_id,
       t.value AS temp,
       p.value AS ph
FROM measurements t
JOIN measurements p
  ON p.sample_id = t.sample_id
 AND p.metric = 'ph'
WHERE t.metric = 'temp';

-- 视图写操作下沉到长表；每个 DML 在调用事务内整组生效。
CREATE OR REPLACE FUNCTION samples_view_iofn()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- 视图不携带 identity 默认值，在这里显式分配 id
        IF NEW.id IS NULL THEN
            NEW.id := nextval('samples_id_seq');
        ELSE
            -- 兼容旧采集程序显式指定 id（OVERRIDING）的写法：抬升序列水位
            PERFORM setval('samples_id_seq',
                           GREATEST((SELECT last_value FROM samples_id_seq), NEW.id),
                           true);
        END IF;
        INSERT INTO measurements (sample_id, metric, value)
        VALUES (NEW.id, 'temp', NEW.temp),
               (NEW.id, 'ph',   NEW.ph);
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

DROP TRIGGER IF EXISTS samples_io ON samples;
CREATE TRIGGER samples_io
    INSTEAD OF INSERT OR UPDATE OR DELETE ON samples
    FOR EACH ROW EXECUTE FUNCTION samples_view_iofn();
