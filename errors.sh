#!/usr/bin/env bash
# Блок A: подсчёт строк и HTTP-ответов 5xx по файлам.
# Автор блока A (вписать участника команды перед сдачей): __________________
set -euo pipefail
LC_ALL=C
LOG_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/logs}"
shopt -s nullglob
files=("$LOG_DIR"/access_*.log)
if (( ${#files[@]} == 0 )); then
  printf 'Ошибка: в %s нет файлов access_*.log\n' "$LOG_DIR" >&2
  exit 1
fi
printf '%-19s %10s %10s\n' 'Файл' 'Строк' 'Ответов 5xx'
all_lines=0
all_errors=0
largest_count=-1
largest_file=''
for file in "${files[@]}"; do
  rows=$(wc -l < "$file")
  errors=$(awk '$6 ~ /^5[0-9][0-9]$/ {n++} END {print n+0}' "$file")
  printf '%-19s %10d %10d\n' "$(basename "$file")" "$rows" "$errors"
  all_lines=$((all_lines + rows))
  all_errors=$((all_errors + errors))
  if (( errors > largest_count )); then
    largest_count=$errors
    largest_file=$(basename "$file")
  fi
done
printf '\nИТОГО: %d строк, %d ошибок 5xx\n' "$all_lines" "$all_errors"
printf 'Больше всего 5xx: %s (%d ошибок)\n' "$largest_file" "$largest_count"
