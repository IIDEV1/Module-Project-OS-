#!/usr/bin/env python3
"""Проект 3, блоки A/B/C. Ubuntu / WSL2, Python 3.11+.

Авторы блоков (заполнить перед сдачей):
  A (count): __________________
  B (fork+pipe, analyze/report): __________________
  C (shared memory/temp, cleanup/outage): __________________

A: последовательный Python-подсчёт для сравнения strace с wc -l;
   отдельный обязательный по условию Bash-скрипт errors.sh.
B: fork + pipe, подсчёт ошибок, N=1/2/4, CSV-отчёты, waitpid.
C: fork + 3 канала IPC (pipe / shared memory / temporary files),
   очистка при Ctrl+C и определение точных границ сбоя.
"""

import argparse
import csv
import json
import os
import signal
import struct
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from multiprocessing import shared_memory

ROOT = Path(__file__).resolve().parent


def percent(bad, all_):
    return 100 * bad / all_ if all_ else 0


def examine_file(path):
    """Одна проходка по файлу. Поля: date time IP method path status duration."""
    paths = defaultdict(lambda: {"requests": 0, "errors_5xx": 0})
    count = errors = 0
    payment_errors = []
    with open(path, encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            fields = line.split()
            if len(fields) != 7:
                raise ValueError(f"{path}:{line_no}: ожидалось 7 полей, найдено {len(fields)}")
            date, clock, ip, method, url, status, duration = fields
            try:
                code = int(status)
            except ValueError as exc:
                raise ValueError(f"{path}:{line_no}: HTTP-код {status!r} не число") from exc
            failed = 500 <= code <= 599
            count += 1
            errors += int(failed)
            paths[url]["requests"] += 1
            paths[url]["errors_5xx"] += int(failed)
            if failed and url == "/api/payment":
                payment_errors.append(f"{date} {clock}")
    return {"requests": count, "errors_5xx": errors,
            "paths": dict(paths), "payment_errors": payment_errors}


def analyze_chunk(files):
    return {str(Path(f).name): examine_file(f) for f in files}


def worker_send(files, method, channel):
    """В дочернем процессе посчитать долю файлов и вернуть родителю JSON."""
    try:
        packed = json.dumps({"data": analyze_chunk(files)}, ensure_ascii=False,
                            separators=(",", ":")).encode("utf-8")
        if method == "pipe":
            with os.fdopen(channel, "wb", buffering=0) as out:
                out.write(packed)
        elif method == "shm":
            if len(packed) + 8 > channel.size:
                raise ValueError(f"Недостаточно shared memory: {len(packed)} > {channel.size - 8}")
            channel.buf[0:8] = struct.pack("!Q", len(packed))
            channel.buf[8:8 + len(packed)] = packed
            channel.close()
        elif method == "file":
            Path(channel).write_bytes(packed)
        return 0
    except BaseException as exc:
        try:
            print(f"[worker {os.getpid()}] {type(exc).__name__}: {exc}", file=sys.stderr,
                  flush=True)
        except BaseException:
            pass
        return 1


class ProcessGroup:
    """Учёт порождённых PID и ресурсов; корректная уборка даже после Ctrl+C."""
    def __init__(self):
        self.pids = []
        self.open_fds = set()
        self.shms = []
        self.temp = None
        self.reaped = []

    def cleanup(self, kill=False):
        # SIGINT игнорируем на короткий период очистки, чтобы двойной Ctrl+C
        # не оставил зомби и /dev/shm/psm_*.
        old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            if kill:
                for pid in self.pids:
                    if pid not in self.reaped:
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
            for fd in list(self.open_fds):
                try:
                    os.close(fd)
                except OSError:
                    pass
                self.open_fds.discard(fd)
            for pid in self.pids:
                if pid in self.reaped:
                    continue
                try:
                    os.waitpid(pid, 0)
                    self.reaped.append(pid)
                except ChildProcessError:
                    self.reaped.append(pid)
            for shm in self.shms:
                try:
                    shm.close()
                finally:
                    try:
                        shm.unlink()
                    except FileNotFoundError:
                        pass
            if self.temp is not None:
                self.temp.cleanup()
        finally:
            signal.signal(signal.SIGINT, old_sigint)


def parallel(files, n, method, slow=0.0):
    """Дочерние процессы с fork, передачи pipe/shm/file. Всегда reap + cleanup."""
    parts = [files[i::n] for i in range(n)]
    group = ProcessGroup()
    channels = []
    try:
        if method == "file":
            group.temp = tempfile.TemporaryDirectory(prefix="project3_", dir=None)
        for index, chunk in enumerate(parts):
            # Создаём канал ДО fork.
            if method == "pipe":
                readfd, writefd = os.pipe()
                group.open_fds.update((readfd, writefd))
                channel = writefd
            elif method == "shm":
                # 16000 байт на файл достаточно для краткой сводки, с запасом.
                shm = shared_memory.SharedMemory(create=True, size=max(65536, len(chunk) * 16000))
                group.shms.append(shm)
                channel = shm
            else:
                channel = str(Path(group.temp.name) / f"part_{index}.json")
            pid = os.fork()
            if pid == 0:
                # Обычные POSIX-сигналы убивают ребёнка. Родитель при Ctrl+C
                # всё равно вызовет waitpid и удалит разделяемую память.
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                if method == "pipe":
                    os.close(readfd)
                    for fd in group.open_fds:
                        if fd != writefd:
                            try:
                                os.close(fd)
                            except OSError:
                                pass
                if slow > 0:
                    time.sleep(slow)  # Для демонстрации Ctrl+C.
                code = worker_send(chunk, method, channel)
                os._exit(code)
            group.pids.append(pid)
            if method == "pipe":
                os.close(writefd)
                group.open_fds.discard(writefd)
                channels.append(readfd)
            else:
                channels.append(channel)
        # Не ждём дочерние процессы до чтения pipe: иначе возможен deadlock.
        result = {}
        for channel in channels:
            if method == "pipe":
                with os.fdopen(channel, "rb") as stream:
                    payload = stream.read()
                group.open_fds.discard(channel)
            elif method == "shm":
                # waitpid перед чтением shm, чтобы размер/содержимое были готовы.
                # Общий waitpid всех детей выполнен ниже: здесь polling-проверка
                # блока длины. Ноль означает, что ребёнок не записал результат.
                while True:
                    size = struct.unpack("!Q", bytes(channel.buf[0:8]))[0]
                    if size:
                        payload = bytes(channel.buf[8:8 + size])
                        break
                    if not any(_pid_alive(pid) for pid in group.pids):
                        raise RuntimeError("Рабочие процессы завершились без записи shared memory")
                    time.sleep(0.005)
            else:
                # По одному waitpid на соответствующий рабочий PID; читаем
                # файл после завершения записи.
                index = channels.index(channel)
                pid = group.pids[index]
                waited, status = os.waitpid(pid, 0)
                group.reaped.append(waited)
                if os.waitstatus_to_exitcode(status) != 0:
                    raise RuntimeError(f"worker {pid} завершился с ошибкой")
                payload = Path(channel).read_bytes()
            if not payload:
                raise RuntimeError("Пустой ответ рабочего процесса")
            item = json.loads(payload)
            result.update(item["data"])
        # После возврата из waitpid каждый PID исчез, в том числе Z.
        for pid in group.pids:
            if pid in group.reaped:
                continue
            waited, status = os.waitpid(pid, 0)
            group.reaped.append(waited)
            if os.waitstatus_to_exitcode(status) != 0:
                raise RuntimeError(f"worker {pid} завершился с ошибкой")
        return result, list(group.pids), [shm.name for shm in group.shms]
    except BaseException:
        group.cleanup(kill=True)
        raise
    finally:
        # На штатном пути unlink и удаление временных файлов тоже обязательны.
        if len(group.reaped) == len(group.pids):
            group.cleanup(kill=False)


def _pid_alive(pid):
    # Для shm в случае краха процесса не ждём вечно. Зомби имеют STAT=Z;
    # Linux сообщает его как работающий для kill(pid,0), поэтому читаем /proc.
    stat = Path(f"/proc/{pid}/stat")
    try:
        return stat.read_text().split(") ", 1)[1][0] != "Z"
    except FileNotFoundError:
        return False


def detect_outage(records):
    """Кластер ошибок оплаты; разделяем всплески паузой более 5 минут."""
    clusters = []
    for filename, record in records.items():
        times = sorted(datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                       for value in record["payment_errors"])
        parts = []
        for t in times:
            if not parts or t - parts[-1][-1] > timedelta(minutes=5):
                parts.append([t])
            else:
                parts[-1].append(t)
        for part in parts:
            clusters.append((len(part), filename, part[0], part[-1]))
    return max(clusters, default=None)


def report(records, pids, shm_names, elapsed, method, dest):
    dest.mkdir(parents=True, exist_ok=True)
    counts = defaultdict(lambda: {"requests": 0, "errors_5xx": 0})
    rows_files, rows_file_paths = [], []
    for filename in sorted(records):
        record = records[filename]
        rows_files.append((filename, record["requests"], record["errors_5xx"],
                           f"{percent(record['errors_5xx'], record['requests']):.4f}"))
        for url, item in record["paths"].items():
            counts[url]["requests"] += item["requests"]
            counts[url]["errors_5xx"] += item["errors_5xx"]
            rows_file_paths.append((filename, url, item["requests"], item["errors_5xx"],
                                    f"{percent(item['errors_5xx'], item['requests']):.4f}"))
    rows_paths = [(url, item["requests"], item["errors_5xx"],
                   f"{percent(item['errors_5xx'], item['requests']):.4f}")
                  for url, item in sorted(counts.items())]
    def csv_save(name, headers, rows):
        with (dest / name).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(headers)
            writer.writerows(rows)
    csv_save("files.csv", ("файл", "запросов", "5xx", "доля_5xx_%"), rows_files)
    csv_save("paths.csv", ("путь", "запросов", "5xx", "доля_5xx_%"), rows_paths)
    csv_save("files_paths.csv", ("файл", "путь", "запросов", "5xx", "доля_5xx_%"),
             sorted(rows_file_paths))
    (dest / "result.json").write_text(json.dumps(records, indent=2, ensure_ascii=False,
                                                  sort_keys=True) + "\n", encoding="utf-8")
    overall = sum(r[1] for r in rows_files)
    failed = sum(r[2] for r in rows_files)
    print(f"Режим: {method} | Дочерние процессы: {len(pids)} | PID: {pids}")
    print(f"Файлов: {len(records)} | Запросов: {overall} | Ошибок 5xx: {failed} ({percent(failed, overall):.3f}%)")
    print(f"Время (wall-clock): {elapsed:.4f} с")
    print("\n5xx по путям:")
    for url, n_all, n_bad, pct in rows_paths:
        print(f"  {url:<20} {n_all:>7} запросов, {n_bad:>5} ошибок ({pct}%)")
    worst = max(rows_file_paths, key=lambda r: float(r[4]))
    print(f"\nМаксимальная доля 5xx в комбинации файл + путь: {worst[0]}, {worst[1]}")
    print(f"  {worst[3]} ошибок из {worst[2]} запросов ({worst[4]}%)")
    outage = detect_outage(records)
    if outage:
        c, fn, t0, t1 = outage
        print(f"ВСПЛЕСК ОШИБОК ОПЛАТЫ: {fn}, {c} ответов 5xx")
        print(f"  НАЧАЛО: {t0:%Y-%m-%d %H:%M:%S}")
        print(f"  КОНЕЦ:  {t1:%Y-%m-%d %H:%M:%S}")
        print("  (границы по первым/последним 5xx, правило: пауза >5 мин образует новый кластер)")
    ps = subprocess.run(["ps", "-o", "pid,ppid,stat,cmd", "-p", ",".join(map(str, pids))],
                        capture_output=True, text=True)
    visible = (ps.stdout.splitlines()[1:] if ps.stdout else [])
    print("\nПроверка ps после waitpid:")
    print(ps.stdout.rstrip() if ps.stdout.strip() else "(процессы не обнаружены)")
    print("Зомби этих PID:", "НЕТ" if not visible else "ПРОВЕРЬТЕ")
    if shm_names:
        stale = [name for name in shm_names if Path("/dev/shm", name).exists()]
        print(f"Остатки собственных сегментов /dev/shm: {stale if stale else 'НЕТ'}")
    print(f"Отчёты сохранены в {dest}")


def command_analyze(args):
    files = sorted(args.logs.glob("access_*.log"))
    if not files:
        raise ValueError(f"Файлы access_*.log не найдены в {args.logs}")
    if args.workers < 1 or args.workers > len(files):
        raise ValueError(f"-n должно быть от 1 до {len(files)}")
    method = "pipe" if args.mode == "b" else args.method
    start = time.perf_counter()
    records, pids, shm_names = parallel(files, args.workers, method, args.slow)
    elapsed = time.perf_counter() - start
    target = args.out / f"{args.mode}_{method}_n{args.workers}"
    report(records, pids, shm_names, elapsed, method, target)


def command_verify(root):
    files = sorted(root.glob("*/result.json"))
    if not files:
        raise ValueError("Нет результатов: сначала запустите bash run_all.sh")
    reference = json.loads(files[0].read_text(encoding="utf-8"))
    for path in files:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data != reference:
            raise AssertionError(f"Различаются результаты {files[0]} и {path}")
    print(f"ПРОВЕРКА: {len(files)} результатов совпадают точно.")
    print("Все способы IPC и N=1/2/4 дают одинаковый ответ.")


def main():
    parser = argparse.ArgumentParser(description="Проект 3: блоки A, B, C в одном Python-файле")
    sub = parser.add_subparsers(dest="mode", required=True)
    p_a = sub.add_parser("a", help="Блок A: Python-подсчёт строк для strace")
    p_a.add_argument("--file", type=Path, default=ROOT / "logs/access_01.log")
    for mode in ("b", "c"):
        p = sub.add_parser(mode, help="Блок B: pipe; блок C: сравнение IPC")
        p.add_argument("-n", "--workers", type=int, default=4)
        p.add_argument("--logs", type=Path, default=ROOT / "logs")
        p.add_argument("--out", type=Path, default=ROOT / "reports")
        p.add_argument("--slow", type=float, default=0, help="Задержка дочерних процессов для Ctrl+C")
        if mode == "c":
            p.add_argument("--method", choices=("pipe", "shm", "file"), default="pipe")
    p_check = sub.add_parser("verify", help="Сверить данные из всех 6 прогонов")
    p_check.add_argument("--out", type=Path, default=ROOT / "reports")
    args = parser.parse_args()
    if args.mode == "a":
        n = sum(1 for _ in args.file.open(encoding="utf-8"))
        print(f"Python: {args.file.name}: {n} строк")
    elif args.mode == "verify":
        command_verify(args.out)
    else:
        if not hasattr(os, "fork"):
            parser.error("Нужен Linux / Ubuntu WSL2, а не Windows Python")
        command_analyze(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCtrl+C: дочерние процессы остановлены; waitpid и очистка выполнены.", file=sys.stderr)
        sys.exit(130)
    except (ValueError, OSError, RuntimeError, AssertionError) as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        sys.exit(1)
