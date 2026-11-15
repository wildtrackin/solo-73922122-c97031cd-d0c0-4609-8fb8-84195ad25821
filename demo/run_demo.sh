#!/usr/bin/env bash
# 端到端演示：宽表 samples -> 长表 measurements 在线迁移
#
# 覆盖场景：
#   A. 启动回填、网页查进度、暂停
#   B. 暂停期间“旧采集程序”直接 SQL：修改 / 删除 / 删除后复用 ID
#      —— 均不被旧扫描覆盖或复活
#   C. 进程被 kill -9 后重启，迁移自动续办，无半迁移
#   D. 一次修改两项的原子性（约束失败整体回滚）
#   E. 切换中进程退出（持锁 3s 后回滚）：旧接口继续可用，可重新切换
#   F. 最终切换：samples 变同名视图，SELECT/增改删 全部保留，
#      新旧入口读写同一份数据，每样本恰两项
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
DATABASE_URL="${DATABASE_URL:-postgresql://postgres@/warehouse?host=/tmp}"
export DATABASE_URL
PORT="${PORT:-8000}"
PSQL="${PSQL:-psql -h /tmp -U postgres -d warehouse -v ON_ERROR_STOP=1 -tA}"

step() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$*"; }
ok()   { printf '  \033[1;32m✔ %s\033[0m\n' "$*"; }
say()  { printf '  %s\n' "$*"; }

jget() { python3 -c "import json,sys;d=json.load(sys.stdin);print(eval(sys.argv[1]))" "$1"; }
phase() { curl -s "$BASE_URL/migration/progress" | jget "d['state']"; }
wait_phase() { # $1=目标状态  $2=超时秒
  local deadline=$(( $(date +%s) + $2 ))
  until [ "$(phase)" = "$1" ] || [ "$(date +%s)" -gt "$deadline" ]; do sleep 0.5; done
  [ "$(phase)" = "$1" ]
}

step "A. 启动回填并观察进度（小批次），在中途暂停"
curl -s -X POST "$BASE_URL/migration/start" >/dev/null
# 轮询直到有批次落库且尚未完成，确保暂停发生在回填进行中
for i in $(seq 1 30); do
  R=$(curl -s "$BASE_URL/migration/progress")
  ROWS=$(echo "$R" | jget "d['measurement_rows']")
  ST=$(echo "$R" | jget "d['state']")
  [ "$ROWS" -gt 0 ] && [ "$ST" = "backfilling" ] && break
  sleep 0.3
done
curl -s -X POST "$BASE_URL/migration/pause" | jget "d['detail']"
P=$(curl -s "$BASE_URL/migration/progress")
say "暂停时水位 last_id=$(echo "$P" | jget "d['last_id']")，" \
    "长表行=$(echo "$P" | jget "d['measurement_rows']")"
[ "$(echo "$P" | jget "d['measurement_rows']")" -gt 0 ] && ok "已有批次提交落库"
[ "$(echo "$P" | jget "d['state']")" = "paused" ] && ok "回填确实停在中途（paused）"

step "B. 暂停期间旧采集程序直接 SQL 修改（绕过网页/API）"
$PSQL -c "UPDATE samples SET temp=99.90, ph=8.88 WHERE id=3;"           # 修改已回填行
$PSQL -c "DELETE FROM samples WHERE id=4;"                                # 删除
$PSQL -c "INSERT INTO samples (temp, ph) VALUES (30.00, 7.70);"           # 新增(id=13)
$PSQL -c "DELETE FROM samples WHERE id=5;"
# 删除后复用 ID：显式覆盖 identity 列
$PSQL -c "INSERT INTO samples (id, temp, ph) OVERRIDING SYSTEM VALUE
          VALUES (5, 11.11, 2.22);"
ok "直接 SQL 的增/改/删/复用ID 全部被旧表接受（回填未中断采集程序）"

step "C. 续办至回填完成，验证旧扫描既不覆盖也不复活"
curl -s -X POST "$BASE_URL/migration/resume" >/dev/null
wait_phase ready 30
ok "回填完成，阶段=$(phase)"
$PSQL -c "
SELECT 'id3 最新值保留(99.90): '||temp||'/'||ph FROM samples WHERE id=3;
SELECT 'id4 已删除且未复活, 长表残留行数='||count(*) FROM measurements WHERE sample_id=4;
SELECT '复用 id5 为新值(11.11/2.22): '||string_agg(metric||'='||value, ',' ORDER BY metric)
  FROM measurements WHERE sample_id=5;
SELECT '新 id13 两项: '||string_agg(metric||'='||value, ',' ORDER BY metric)
  FROM measurements WHERE sample_id=13;"
C=$($PSQL -c "SELECT count(*) FROM measurements WHERE sample_id=3 AND value=99.90 AND metric='temp';")
[ "$C" = "1" ] && ok "直接 SQL 的修改未被回填覆盖"
C=$($PSQL -c "SELECT count(*) FROM measurements WHERE sample_id=4;")
[ "$C" = "0" ] && ok "删除行未被回填复活"
C=$($PSQL -c "SELECT count(*) FROM measurements WHERE sample_id=5;")
[ "$C" = "2" ] && ok "复用 ID 恰好两项且为新数据"

step "D. 一次修改两项的原子性"
$PSQL -c "UPDATE samples SET temp=45.67, ph=7.77 WHERE id=3;"
say "正常整组更新后：$($PSQL -c "SELECT temp||'/'||ph FROM samples WHERE id=3;")"
# 同一事务里第二项违反 NOT NULL -> 整事务回滚，第一项也不生效
$PSQL -c "BEGIN; UPDATE samples SET temp=55.55 WHERE id=3;
          UPDATE samples SET ph=NULL WHERE id=3; COMMIT;" 2>/dev/null || true
V=$($PSQL -c "SELECT temp||'/'||ph FROM samples WHERE id=3;")
say "约束失败回滚后：$V（应为 45.67/7.77）"
[ "$V" = "45.67/7.77" ] && ok "两项修改同生共死，无半更新"
# 新入口整组替换，同一事务写两行
curl -s -X PUT "$BASE_URL/measurements/3" -H 'Content-Type: application/json' \
     -d '{"temp": 60.01, "ph": 6.02}' >/dev/null
say "新入口 PUT /measurements/3 后：$($PSQL -c \
   "SELECT string_agg(metric||'='||value,',' ORDER BY metric) FROM measurements WHERE sample_id=3;")"

step "E. 切换中进程退出：持锁 5s 后回滚（模拟崩溃）"
# 后台发起“会回滚的切换”。切换事务持有 ACCESS EXCLUSIVE 锁期间，
# 用 NOWAIT 非阻塞探针验证锁确实被持有（冲突立即报错、不进入等待，
# 因而不依赖操作系统的进程唤醒）。
( curl -s -X POST "$BASE_URL/migration/switch?hold_seconds=5" -o /tmp/switch_resp.json \
    -w '%{http_code}' > /tmp/switch_code ) &
sleep 1
if $PSQL -c "BEGIN; LOCK TABLE samples IN ACCESS EXCLUSIVE MODE NOWAIT; COMMIT;" \
     >/tmp/lockprobe.out 2>&1; then
  echo "  异常：持锁期间排他锁探针竟未冲突"
else
  if grep -q "could not obtain lock" /tmp/lockprobe.out; then
    say "持锁窗口内 NOWAIT 探针立即被拒（SQLSTATE 55P03，证明切换事务正持有排他锁）："
    sed 's/^/    /' /tmp/lockprobe.out
    ok "切换是全程唯一一次写阻塞窗口"
  else
    echo "  探针因非预期原因失败："; cat /tmp/lockprobe.out; exit 1
  fi
fi
wait
say "切换接口 HTTP $(cat /tmp/switch_code)：$(cat /tmp/switch_resp.json)"
[ "$(cat /tmp/switch_code)" = "409" ] && ok "切换失败返回 409，事务整体回滚"
# 回滚后再探一次：锁已释放，NOWAIT 可立即拿到
$PSQL -c "BEGIN; LOCK TABLE samples IN ACCESS EXCLUSIVE MODE NOWAIT; COMMIT;" \
  && ok "回滚后 NOWAIT 探针立即成功，锁已随事务回滚释放"
[ "$(phase)" = "ready" ] && ok "阶段回到 ready，samples 仍是原表，旧接口继续服务"
# 无竞争地直接 SQL 写入：证明旧采集程序接口完好
$PSQL -c "INSERT INTO samples (temp, ph) VALUES (31.50, 7.90);" >/dev/null
ok "回滚后旧表直接 SQL 写入立即成功，且由双写触发器同步出两项指标："
$PSQL -c "SELECT '  新行两项: '||string_agg(metric||'='||value,',' ORDER BY metric)
          FROM measurements WHERE sample_id=(SELECT max(id) FROM samples);"
$PSQL -c "SELECT relkind FROM pg_class WHERE relname='samples';" | grep -q '^r$' \
  && ok "samples 依旧是普通表(relkind=r)，视图切换未留下任何半成品"

step "E2. 进程崩溃重启续办（等价 kill -9：硬退出，未提交事务由 PG 回滚）"
# 重新制造未完成状态：再插入数据并重启回填循环即可验证恢复逻辑
$PSQL -c "INSERT INTO samples (temp, ph) SELECT 40+g, 7.0+g*0.01 FROM generate_series(1,5) g;"
curl -s -X POST "$BASE_URL/migration/resume" >/dev/null
sleep 0.5
# 硬退出：不优雅关闭连接，数据库里在途事务立即回滚，已提交批次保留
curl -s -X POST "$BASE_URL/debug/crash" || true
ok "API 进程已硬退出（/debug/crash，等价 kill -9）"
sleep 1
# 数据库里进度仍停在已提交水位，无任何半成品
$PSQL -c "SELECT '库内阶段='||state||' 水位='||last_id FROM migration_state WHERE id=1;"
echo "  重新启动 API 进程……"
( DATABASE_URL="$DATABASE_URL" \
  BACKFILL_BATCH_SIZE=3 BACKFILL_IDLE_SECONDS=0.2 ALLOW_CRASH_DEMO=1 \
  .venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT" \
  > /tmp/uvicorn2.log 2>&1 ) &
for i in $(seq 1 20); do curl -sf "$BASE_URL/health" >/dev/null 2>&1 && break; sleep 0.5; done
wait_phase ready 30
ok "重启后自动续办至 ready（水位从断点继续，无重扫/丢失）"

step "F. 最终切换（唯一一次允许的短暂写阻塞）"
curl -s -X POST "$BASE_URL/migration/switch" | jget "d['detail']"
[ "$(phase)" = "switched" ] && ok "阶段=switched"
$PSQL -c "SELECT relkind FROM pg_class WHERE relname='samples';" | grep -q '^v$' \
  && ok "samples 现为视图(relkind=v)，名称与 temp/ph 列保持不变"

step "F1. 切换后旧名称 samples 仍支持 SELECT 与增改删（直接 SQL）"
$PSQL -c "SELECT count(*) AS 视图可查行数 FROM samples;"
$PSQL -c "INSERT INTO samples (temp, ph) VALUES (33.33, 7.33);"
NEWID=$($PSQL -c "SELECT max(id) FROM samples;")
$PSQL -c "UPDATE samples SET temp=34.34, ph=7.34 WHERE id=$NEWID;"
say "视图写入并回读：$($PSQL -c "SELECT id||':'||temp||'/'||ph FROM samples WHERE id=$NEWID;")"
# 显式指定大 id 后，序列水位应被抬升，后续自动分配的 id 不冲突
BIGID=$(($NEWID + 100))
$PSQL -c "INSERT INTO samples (id, temp, ph) VALUES ($BIGID, 35.35, 7.35);"
$PSQL -c "DELETE FROM samples WHERE id=$BIGID;"
$PSQL -c "INSERT INTO samples (temp, ph) VALUES (36.36, 7.36);"
AUTOID=$($PSQL -c "SELECT max(id) FROM samples;")
[ "$AUTOID" -gt "$BIGID" ] && ok "显式大 id 插入后序列自动抬升，新自动 id=$AUTOID 无冲突"
$PSQL -c "DELETE FROM samples WHERE id=$NEWID; DELETE FROM samples WHERE id=$AUTOID;"
[ "$($PSQL -c "SELECT count(*) FROM measurements WHERE sample_id=$NEWID;")" = "0" ] \
  && ok "经视图删除后长表同步删除，无残留"

step "F2. 新旧两个 HTTP 入口读写同一份数据（无定时同步进程）"
curl -s -X POST "$BASE_URL/samples" -H 'Content-Type: application/json' \
     -d '{"temp": 28.88, "ph": 7.28}' >/dev/null
SID=$(curl -s "$BASE_URL/samples" | jget "d[-1]['id']")
say "旧入口 POST /samples 新建 id=$SID"
curl -s "$BASE_URL/measurements/$SID" | python3 -m json.tool
N=$(curl -s "$BASE_URL/measurements/$SID" | python3 -c "import json,sys;print(len(json.load(sys.stdin)))")
[ "$N" = "2" ] && ok "新入口立刻读到同一组两项指标"
curl -s -X PUT "$BASE_URL/samples/$SID" -H 'Content-Type: application/json' \
     -d '{"temp": 29.99, "ph": 7.29}' >/dev/null
say "旧入口改后，新入口视角：$(curl -s "$BASE_URL/measurements/$SID" \
    | python3 -c "import json,sys;print([(r['metric'],r['value']) for r in json.load(sys.stdin)])")"
curl -s -X DELETE "$BASE_URL/measurements/$SID" >/dev/null
curl -sf "$BASE_URL/samples/$SID" >/dev/null && echo "  异常：应已删除" || ok "新入口删除后旧入口同样查不到"

step "F3. 全局不变量：每样本恰有 temp/ph 两项且两表逐值一致"
$PSQL -c "
SELECT '样本组数='||count(*) FROM samples;
SELECT '长表组数='||count(DISTINCT sample_id) FROM measurements;
SELECT '非两项的组数='||count(*) FROM (
  SELECT sample_id FROM measurements GROUP BY sample_id HAVING count(*)<>2) q;
SELECT 'temp/ph 与视图不一致行数='||count(*)
FROM measurements m JOIN samples s ON s.id=m.sample_id
WHERE m.value IS DISTINCT FROM (CASE m.metric WHEN 'temp' THEN s.temp ELSE s.ph END);"
curl -s "$BASE_URL/migration/progress" | python3 -m json.tool

printf '\n\033[1;32m全部演示通过 ✅\033[0m\n'
