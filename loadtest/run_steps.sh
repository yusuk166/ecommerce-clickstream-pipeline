OFF=${1:-1447497}; shift
for R in "$@"; do
  N=$((R*120))
  echo "$(date +%T) step rate=$R limit=$N offset=$OFF" >> data/metrics/steps.log
  python producer.py --rate $R --limit $N --offset $OFF --report-every $((R*30)) >> data/metrics/producer.log 2>&1
  echo "$(date +%T) step rate=$R done" >> data/metrics/steps.log
  OFF=$((OFF+N))
done
echo "$(date +%T) ALL STEPS DONE offset_next=$OFF" >> data/metrics/steps.log
