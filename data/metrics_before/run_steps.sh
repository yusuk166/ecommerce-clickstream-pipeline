for i in $(seq 1 90); do grep -q "continuous 모드" data/metrics/preprocess.log && break; sleep 2; done
grep -q "continuous 모드" data/metrics/preprocess.log || { echo "preprocess not started" >> data/metrics/steps.log; exit 1; }
sleep 5
OFF=919497
for R in 100 300 1000 3000; do
  N=$((R*120))
  echo "$(date +%T) step rate=$R limit=$N offset=$OFF" >> data/metrics/steps.log
  python producer.py --rate $R --limit $N --offset $OFF --report-every $((R*30)) >> data/metrics/producer.log 2>&1
  echo "$(date +%T) step rate=$R done" >> data/metrics/steps.log
  OFF=$((OFF+N))
done
echo "$(date +%T) ALL STEPS DONE offset_next=$OFF" >> data/metrics/steps.log
