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
