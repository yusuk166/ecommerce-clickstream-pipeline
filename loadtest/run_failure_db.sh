# 장애 시나리오: 1,000건/s 로 보내는 도중 PostgreSQL 을 60초 멈췄다가 다시 켠다
# 기대: preprocess.py 가 재시도(5s → 10s → 20s → 30s)하다가 DB 가 돌아오면 이어서 저장. 유실 · 중복 없음
OFF=${1:-1447497}
R=1000; N=$((R*150))
LOG=data/metrics/failure_db.log
echo "$(date +%T) failure test start rate=$R limit=$N offset=$OFF" >> "$LOG"
python producer.py --rate $R --limit $N --offset $OFF --report-every $((R*30)) >> data/metrics/producer_failure.log 2>&1 &
sleep 30
echo "$(date +%T) docker compose stop postgres" >> "$LOG"
docker compose stop postgres >> "$LOG" 2>&1
sleep 60
echo "$(date +%T) docker compose start postgres" >> "$LOG"
docker compose start postgres >> "$LOG" 2>&1
wait
echo "$(date +%T) producer done" >> "$LOG"
