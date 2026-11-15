-- 01 基线 schema（迁移前的旧世界）
-- 旧采集程序直接对 samples(id, temp, ph) 做 INSERT/UPDATE/DELETE，
-- 整个迁移过程中该表名与这两列始终存在且可写（切换后变为同名视图）。

CREATE TABLE samples (
    id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    temp NUMERIC(8, 2) NOT NULL,
    ph   NUMERIC(5, 2) NOT NULL
);

-- 虚构数据：12 个样本，温度与 pH 均非空
INSERT INTO samples (temp, ph) VALUES
    (22.50, 7.10),
    (23.10, 7.05),
    (19.80, 6.90),
    (25.00, 7.40),
    (24.20, 7.20),
    (18.60, 6.80),
    (26.70, 7.55),
    (21.30, 7.00),
    (20.90, 6.95),
    (27.40, 7.60),
    (22.00, 7.15),
    (23.80, 7.25);
