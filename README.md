# 宽表 → 指标长表在线迁移（FastAPI + PostgreSQL）

把 `samples(id, temp, ph)` 宽表在线迁移为
`measurements(sample_id, metric, value)` 长表（每样本恰有 `temp`/`ph` 两项），
迁移全程不中断旧采集程序，最终切换后旧表名与旧列名仍然可用。

采用经典 **Expand → Migrate → Contract（扩展-迁移-收缩）** 模式，
新旧两个入口始终读写**同一份数据**，不使用任何定时同步。

---

## 1. 需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 旧采集程序不中断 | 切换前 `samples` 一直是原表，直接 SQL 的 INSERT/UPDATE/DELETE 全程有效；行级双写触发器实时维护长表 |
| 回填时直接 SQL 修改不被旧扫描覆盖 | 回填按 id 水位推进，UPSERT 使用 `DO UPDATE SET value=EXCLUDED.value`；最新提交值永远胜出 |
| 删除不被复活、删除后复用 ID 不串数据 | 回填只扫描当前存在的行；删除经触发器级联清长表；复用的 id 作为新行按当前值写入 |
| 一次修改两项一起生效 | 所有写操作均在单事务内完成；触发器对一个样本整组写两行，约束失败整体回滚 |
| 只在最终切换时短暂阻塞写入 | 切换是唯一一次 `LOCK TABLE ... ACCESS EXCLUSIVE`，在单事务内完成校验与表→视图替换 |
| 切换失败继续提供旧接口 | 切换任一步失败/进程退出，事务回滚，`samples` 仍是原表、双写触发器完好，状态回到 `ready` |
| 进程中断重启可续办 | 回填水位 `last_id` 随批次事务提交；启动时自动把 `backfilling` 续跑、把残留 `switching` 回退为 `ready` |
| 无半迁移、不丢已提交修改 | DDL 引导与最终切换都是单事务；已提交数据在长表持久存在，崩溃只回滚未提交事务 |
| 切换后旧名称/列仍支持 SELECT 与增改删 | `samples` 变为同名视图（列仍为 `id,temp,ph`）+ `INSTEAD OF` 触发器把写下沉到长表 |
| 新旧入口读写同一份数据 | 切换前：表+触发器；切换后：视图+触发器。两个 HTTP 入口都经 `samples` 这一个写入面 |
| 可启动迁移、查进度的网页 | 内置控制台 `/`，含阶段徽章、水位、一致性校验、增改删、样本合并与崩溃演示按钮 |
| Docker Compose 可实际启动 | `docker compose up` 一键拉起 Postgres 16 + API，自带 12 行虚构数据 |
| 同一送检样本两编号可合并 | 网页选择保留/重复编号与温度、pH 来源；单事务内从两条**当前已提交**记录取值，原子更新保留行、删除重复行，回显合并前后数值 |
| 回填期间可合并、与旧 SQL 按提交顺序取值 | 合并 `FOR UPDATE`、回填 `FOR SHARE`，均按 id 升序；先提交者先被看到，已提交新值不被旧扫描覆盖 |
| 被删编号不在长表复活、合并整次失败 | 行锁 + READ COMMITTED 重检；任一编号已不存在则抛错回滚，长表 DELETE 触发器同步清理 |
| 合并必须完整先于最终切换 | 共享 advisory lock 与切换排他锁互斥；切换取不到锁立即拒绝，校验不可能读到半次合并 |

---

## 2. 三阶段架构

```
Expand（应用启动时，单事务幂等）
  samples(表) ──AFTER ROW 触发器（双写）──▶ measurements(长表)
  + migration_state(id=1, state, last_id)

Migrate（后台批次回填，可暂停/续办/重启续跑）
  按 last_id 水位分批把宽表行展开为 (sample_id,'temp',..)、(sample_id,'ph',..)
  期间旧程序的直接 SQL 由触发器实时进长表，回填只补缺、绝不覆盖新值

合并（任意切换前阶段，单事务）
  锁定保留/重复两个编号 → 各自从两条当前已提交记录取 temp、ph
  → 更新保留行 → 删除重复行（长表两项由触发器同步清理）

Contract（最终切换，全程唯一写阻塞窗口，单事务）
  LOCK samples → 最终一致性校验 → DROP 表 → 建同名视图 + INSTEAD OF 触发器
  提交后：samples 是 measurements 的视图，两入口同写长表
```

状态机：
`init → backfilling ⇄ paused → ready → switching → switched`
（`switching` 失败/崩溃 → 回退 `ready`，可重试）

---

## 3. 目录结构

```
.
├── docker-compose.yml          # 主交付：db + api
├── docker-compose.demo.yml     # 演示覆盖（小批次、开启崩溃端点）
├── Dockerfile
├── requirements.txt
├── db/
│   ├── init/01_baseline.sql    # 旧世界：samples 表 + 12 行虚构数据（容器首次启动自动执行）
│   └── migrations/
│       ├── 02_expand.sql       # 长表、状态表、双写触发器（应用引导时执行）
│       └── 03_contract.sql     # 同名视图 + INSTEAD OF 触发器（切换时执行）
├── app/
│   ├── main.py                 # FastAPI 路由（迁移控制 + 新旧两套入口 + 合并 + 网页）
│   ├── migration.py            # 回填引擎、状态机、一致性校验、原子切换
│   ├── merge.py                # 同一送检样本两编号的原子合并（行锁 + advisory lock）
│   ├── crud.py                 # 新旧入口的数据访问（全部事务化、整组写）
│   ├── db.py / config.py / schemas.py
│   └── static/index.html       # 迁移控制台（原生 JS，无构建步骤）
├── demo/run_demo.sh            # 端到端演示脚本
└── tests/                      # pytest 全生命周期测试
```

---

## 4. 快速启动（Docker Compose）

```bash
docker compose up -d --build
# 打开控制台
open http://localhost:8000          # 或手动访问 http://localhost:8000
# 健康检查与进度
curl -s localhost:8000/health
curl -s localhost:8000/migration/progress
```

数据库连接：`postgresql://postgres:postgres@localhost:5432/warehouse`，
旧采集程序可直接连库对 `samples` 执行任意 SQL。

网页 `/` 上除迁移控制与新/旧入口外，还有“样本合并”卡片：填入保留编号、重复编号，
分别选择温度与 pH 取自哪条记录，预览两条当前记录后提交，页面会显示合并前后的数值。
合并可在回填进行中或暂停时操作，最终切换后不可用。

---

## 5. 网页与 API

### 迁移控制

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/migration/start` | 启动（或继续）后台回填 |
| POST | `/migration/pause` | 暂停回填，已提交批次保留 |
| POST | `/migration/resume` | 续办 |
| GET  | `/migration/progress` | 阶段、水位、行数、一致性校验 |
| POST | `/migration/switch` | 最终切换（唯一写阻塞窗口） |
| POST | `/migration/switch?hold_seconds=5` | 演示：持锁 5s 后回滚，模拟切换中崩溃 |

### 旧入口（宽表语义，切换前后均可写）

`GET/POST /samples`，`GET/PUT/DELETE /samples/{id}`，请求体 `{"temp": 22.5, "ph": 7.1}`

### 样本合并（同一送检样本被记成两个编号）

为避免分析员在迁移切换后重复计数，可在最终切换前把重复编号并入保留编号。
网页“样本合并”卡片填写保留编号与重复编号，并分别选择温度、pH 取哪条记录。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/samples/{keep_id}/merge-preview?duplicate_id=…` | 只读预览两条当前已提交记录与长表指标，供网页选择 |
| POST | `/samples/merge` | 原子合并，请求体 `{"keep_id":3,"duplicate_id":7,"temp_from":"keep"或"duplicate","ph_from":同上}` |

请求体只提交“取哪条记录”的选择，温度/pH 的具体数值由服务端在事务内、拿到行锁后
从两条**当前已提交**记录读取。返回 `before`（两条合并前记录）、`after`（保留编号合并后
记录）、`removed_measurements`，供网页显示合并前后数值。

- 仅 `init/backfilling/paused/ready` 阶段可合并；`switching/switched` 一律 409 拒绝。
- 任一编号已不存在（例如被旧程序删除）→ 409 `sample_missing`，整次回滚，无部分写入。
- 合并与最终切换通过 advisory lock 互斥：合并进行中切换立即拒绝；切换进行中合并立即拒绝。

### 新入口（长表语义，按样本整组操作）

`GET /measurements`、`GET/DELETE /measurements/{sample_id}`、
`POST /measurements`、`PUT /measurements/{sample_id}`（整组替换两项）

### 崩溃演示

`POST /debug/crash`（仅 `ALLOW_CRASH_DEMO=1` 开启）：进程硬退出，
数据库侧等价 `kill -9`——未提交事务由 PostgreSQL 回滚，重启后续办。

---

## 6. 端到端演示

### Docker 环境

```bash
docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build
# 进入 api 容器或在宿主机用 psql/HTTP 复现；网页按钮即可操作
```

### 脚本演示（需本机有 psql 与一个可连的 PostgreSQL）

`demo/run_demo.sh` 自动覆盖全部验收场景：

- A 启动回填 → 中途暂停（确认停在 `paused`）
- B 暂停期间旧程序**直接 SQL**：修改、删除、删除后复用 ID
- C 续办完成，验证不覆盖、不复活、复用 ID 恰好两项
- D 两项修改的原子性（NOT NULL 失败整体回滚）
- D2 同一送检样本两编号合并：原子更新保留行、删除重复行并清理长表；
     重复编号再提交时整次失败
- E 切换中退出：NOWAIT 探针证明排他锁存在 → 回滚 409 → 旧表完好可写
- E2 进程硬退出后重启，从已提交水位自动续办
- F 最终切换：`samples` 变视图，旧名旧列仍支持 SELECT/增改删
- F1 视图显式 id 写入后序列水位自动抬升
- F2 新旧 HTTP 入口读写同一份数据
- F3 全局不变量：每样本恰两项、逐值一致、零不匹配

```bash
BASE_URL=http://127.0.0.1:8000 \
PSQL="psql -h localhost -U postgres -d warehouse -v ON_ERROR_STOP=1 -tA" \
bash demo/run_demo.sh
```

无 Docker 的本机环境可用 `scripts/local_dev.sh`（见第 8 节）先拉起依赖再演示。

---

## 7. 关键正确性设计说明

**为什么回填不会覆盖旧程序的新修改？**
回填语句对冲突指标执行 `ON CONFLICT DO UPDATE SET value = EXCLUDED.value`，
而 `EXCLUDED.value` 来自扫描时刻宽表的当前值。若某行已被双写触发器更新为新值，
回填要么扫不到（已越过水位），要么用同样的当前值 UPSERT，旧值不可能回写。

**为什么删除不会复活？**
回填源是 `FROM samples ... WHERE id > 水位` 的实时查询，已删除行不在结果集中；
触发器的 DELETE 分支同时清理长表，没有任何机制会再把它插回。

**为什么复用 ID 不会串？**
复用的 id 对数据库就是一行新数据，触发器/回填只认当前 `(id, temp, ph)`，
整组重写该 id 的两项指标，与它的“前世”无任何关联。

**为什么不会半迁移？**
- 引导（建长表/触发器）与最终切换（锁表、校验、换视图）各自是**一个事务**；
- 回填按批提交，水位与本批数据同事务落库；
- 进程崩溃只回滚未提交事务，重启后从水位继续，或把残留 `switching` 退回 `ready`。

**合并如何与旧采集程序、回填交错？**
- 合并事务内对两个编号 `SELECT ... FOR UPDATE`（按 id 升序逐行加锁），
  再读取当前值、更新保留行、删除重复行，全程一个事务。
- 回填扫描对源行加 `FOR SHARE`（同样按 id 升序）。两者都是行级锁，
  PostgreSQL 在 READ COMMITTED 下按提交顺序串行化：先提交的一方先被看到，
  另一方等到其提交后重新读取。
  * 旧程序 UPDATE 先提交 → 合并读到新值，不会用预览时的旧值覆盖；
  * 合并先提交 → 旧程序 UPDATE 在其后生效，语义就是“后提交者最后写入”；
  * 回填扫描遇到正被删除/合并的编号 → 等对方提交后重检，行已不存在即跳过，
    绝不把被删编号重新写进 measurements；
  * 已由触发器写入的新值参与 UPSERT 时，回填取的是锁定后重新读取的当前值。
- 水位只推进到本批实际写出的最大 sample_id，不会跳过未处理的行。
- 切换与合并用 advisory lock 互斥：切换用 `pg_try_advisory_xact_lock`（不排队），
  合并未提交时切换立即拒绝；切换持锁时合并的共享锁尝试立即失败。
  切换开启事务后先取锁、再做一致性校验，因此校验只可能看到已提交的完整合并。

**切换后 id 如何分配？**
原 identity 序列随表删除，切换事务重建 `samples_id_seq` 并对齐到最大 id；
视图 INSERT 触发器在 `NEW.id IS NULL` 时取序列，显式 id 写入时抬升序列水位。

---

## 8. 无 Docker 的本机运行（可选）

`scripts/local_dev.sh` 演示如何用本机 Python venv + 任意 PostgreSQL 运行：

```bash
# 1) 建库并加载基线
psql -d postgres -c "CREATE DATABASE warehouse;"
psql -d warehouse -f db/init/01_baseline.sql
# 2) 起 API
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL="postgresql://USER@/warehouse?host=/var/run/postgresql"
export BACKFILL_BATCH_SIZE=2 BACKFILL_IDLE_SECONDS=0.4 ALLOW_CRASH_DEMO=1
uvicorn app.main:app --port 8000
```

---

## 9. 自动化测试

```bash
# TEST_DATABASE 指向一个可创建的测试库；每用例自动 DROP/CREATE SCHEMA 重置
TEST_DATABASE=warehouse_pytest \
ADMIN_DATABASE_URL="postgresql:///postgres?host=%2Fvar%2Frun%2Fpostgresql&user=postgres" \
pytest tests/ -v
```

7 个用例覆盖：初始进度、回填完成且每组两项、暂停续办、
直接 SQL 改/删/复用 ID 的存活、两项原子更新、失败切换回滚保旧接口、
成功切换后视图与双入口同数据。

`tests/test_merge.py` 另有 15 个合并用例：取值来源与前后回显、缺任一编号整次失败、
切换后拒绝合并、回填期间合并不复活被删编号、预览后旧程序改值仍按提交顺序取值、
合并等待行锁期间旧程序提交的新值被采用、等待期间被删除则整次失败、
回填扫描与删除/合并交错的复活防护、直接驱动回填批次的锁等待重检、
切换与合并双向互斥、切换校验只看到已提交合并。
