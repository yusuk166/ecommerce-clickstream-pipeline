OFF=${1:-1597497}
R=1000; N=$((R*120))
LOG=data/metrics/failure_spark.log
PY310="C:/Users/USER/AppData/Local/Programs/Python/Python310/python.exe"
echo "$(date +%T) failure test start rate=$R limit=$N offset=$OFF" >> "$LOG"
python producer.py --rate $R --limit $N --offset $OFF --report-every $((R*30)) >> data/metrics/producer_failure_spark.log 2>&1 &
sleep 30
echo "$(date +%T) kill preprocess.py" >> "$LOG"
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { \$_.CommandLine -like '*preprocess.py*' -and \$_.Name -eq 'python.exe' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force }" >> "$LOG" 2>&1
sleep 15
echo "$(date +%T) restart preprocess.py" >> "$LOG"
PYTHONIOENCODING=utf-8 "$PY310" preprocess.py --continuous --trigger-seconds 10 --checkpoint checkpoint/loadtest --starting-offsets latest --parquet-dir data/parquet/loadtest --metrics-dir data/metrics >> data/metrics/preprocess.log 2>&1 &
wait %1
echo "$(date +%T) producer done" >> "$LOG"
