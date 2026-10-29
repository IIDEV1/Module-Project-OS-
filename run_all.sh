#!/usr/bin/env bash
# Единая команда запуска A -> B -> C + strace (если установлен).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p reports
printf '\n========= A: Bash errors.sh =========\n'
bash errors.sh | tee reports/A_errors.txt
printf '\n========= A: strace wc -l vs Python =========\n'
python3 project3.py a --file logs/access_01.log | tee reports/A_python.txt
if command -v strace >/dev/null 2>&1; then
  strace -c -o reports/A_wc_strace.txt wc -l logs/access_01.log
  strace -c -o reports/A_python_strace.txt python3 project3.py a --file logs/access_01.log
  echo 'Сводки: reports/A_wc_strace.txt и reports/A_python_strace.txt'
  echo 'Топ-3 системных вызовов (число вызовов, имя):' | tee reports/A_top3_syscalls.txt
  for trace in reports/A_wc_strace.txt reports/A_python_strace.txt; do
    echo "$trace" | tee -a reports/A_top3_syscalls.txt
    awk '$1 ~ /^[0-9.]+$/ && $4 ~ /^[0-9]+$/ && $NF != "total" {print $4, $NF}' "$trace" \
      | sort -nr | sed -n '1,3p' | tee -a reports/A_top3_syscalls.txt
  done
else
  echo 'ВНИМАНИЕ: strace не найден. Для части A и C: sudo apt-get update && sudo apt-get install -y strace'
fi
printf '\n========= B: fork + pipe, N=1/2/4 =========\n'
for n in 1 2 4; do
  python3 project3.py b -n "$n" | tee "reports/B_n${n}.txt"
done
printf '\n========= C: 3 способа IPC + strace =========\n'
for method in pipe shm file; do
  echo "---- $method ----"
  python3 project3.py c --method "$method" -n 4 | tee "reports/C_${method}.txt"
  if command -v strace >/dev/null 2>&1; then
    strace -f -c -o "reports/C_${method}_strace.txt" \
      python3 project3.py c --method "$method" -n 4 > "reports/C_${method}_straced_output.txt"
    echo "strace: reports/C_${method}_strace.txt"
    echo "Топ-3 вызова ${method}:"
    awk '$1 ~ /^[0-9.]+$/ && $4 ~ /^[0-9]+$/ && $NF != "total" {print $4, $NF}' \
      "reports/C_${method}_strace.txt" | sort -nr | sed -n '1,3p' | \
      tee "reports/C_${method}_top3_syscalls.txt"
  fi
done
printf '\n========= Проверка равенства ответов =========\n'
python3 project3.py verify | tee reports/VERIFY.txt
printf '\nГОТОВО. Все результаты: reports/\n'
