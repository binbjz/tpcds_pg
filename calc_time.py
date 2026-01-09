"""
统计 TPC-DS 查询日志总执行时间。
"""
import argparse
import os
import re
from pathlib import Path


DEFAULT_LOG_FILE = (
    "/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/"
    "DSGen-software-code-3.2.0rc1/tpcds_query/tpcds.log"
)

TIME_PREFIX_RE = re.compile(r"[Tt]ime:[ \t]*(?P<body>.+)")
PAIR_RE = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*"
    r"(?P<unit>ns|us|µs|ms|s|sec|secs|second|seconds|m|min|mins|minute|minutes)\b",
    re.IGNORECASE,
)
HMS_RE = re.compile(
    r"^(?P<h>\d+):(?P<m>[0-5]?\d):(?P<s>[0-5]?\d(?:\.\d+)?)$",
    re.IGNORECASE,
)

UNIT_TO_MS = {
    "ns": 0.000001,
    "us": 0.001,
    "µs": 0.001,
    "ms": 1.0,
    "s": 1000.0,
    "sec": 1000.0,
    "secs": 1000.0,
    "second": 1000.0,
    "seconds": 1000.0,
    "m": 60000.0,
    "min": 60000.0,
    "mins": 60000.0,
    "minute": 60000.0,
    "minutes": 60000.0,
}


def parse_time_body_to_ms(time_body: str, default_unit: str = "ms") -> float | None:
    """
    解析 'Time:' 后面的时间内容并统一转换为毫秒。
    支持：
    - 12 ms / 12ms
    - 0.5 s / 2 min
    - 1m 2s 250ms
    - 00:01:02.345
    - 纯数字（按 default_unit）
    """
    body = time_body.strip().rstrip(",;")
    if not body:
        return None

    hms_match = HMS_RE.match(body)
    if hms_match:
        hours = float(hms_match.group("h"))
        minutes = float(hms_match.group("m"))
        seconds = float(hms_match.group("s"))
        return (hours * 3600 + minutes * 60 + seconds) * 1000.0

    pair_total = 0.0
    pair_found = False
    for match in PAIR_RE.finditer(body):
        unit = match.group("unit").lower()
        value = float(match.group("value"))
        factor = UNIT_TO_MS.get(unit)
        if factor is None:
            continue
        pair_total += value * factor
        pair_found = True

    if pair_found:
        return pair_total

    plain_number = re.fullmatch(r"\d+(?:\.\d+)?", body)
    if plain_number:
        factor = UNIT_TO_MS.get(default_unit.lower(), UNIT_TO_MS["ms"])
        return float(body) * factor

    # 兼容 calc_time.sh 的行为:
    # 当 Time 后第一个字段是数字但单位无法识别时,按默认单位累加。
    leading_number = re.match(r"^(?P<value>\d+(?:\.\d+)?)\b", body)
    if leading_number:
        factor = UNIT_TO_MS.get(default_unit.lower(), UNIT_TO_MS["ms"])
        return float(leading_number.group("value")) * factor

    return None


def calculate_total_time(log_file: Path, default_unit: str = "ms") -> tuple[float, int]:
    total_ms = 0.0
    matched = 0

    with log_file.open("r", encoding="utf-8", errors="replace") as file_handle:
        for line in file_handle:
            prefix_match = TIME_PREFIX_RE.search(line)
            if not prefix_match:
                continue

            ms_value = parse_time_body_to_ms(
                prefix_match.group("body"), default_unit=default_unit
            )
            if ms_value is None:
                continue

            total_ms += ms_value
            matched += 1

    return total_ms, matched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="统计日志中的 Time 记录总时长并输出毫秒(ms)。",
    )
    parser.add_argument(
        "log_file",
        nargs="?",
        default=DEFAULT_LOG_FILE,
        help="要分析的日志文件路径",
    )
    parser.add_argument(
        "--default-unit",
        default="ms",
        choices=sorted(UNIT_TO_MS.keys()),
        help="当 Time 后仅有数字时使用的默认单位（默认: ms）",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=3,
        help="输出小数位数（默认: 3）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_path = Path(args.log_file)

    if not log_path.exists():
        print(f"日志文件不存在: {log_path}")
        return 1

    if not log_path.is_file():
        print(f"日志文件不是普通文件: {log_path}")
        return 1

    if not os.access(log_path, os.R_OK):
        print(f"日志文件不可读: {log_path}")
        return 1

    try:
        total_ms, count = calculate_total_time(log_path, default_unit=args.default_unit)
    except OSError as error:
        print(f"读取日志文件失败: {log_path} ({error})")
        return 1

    if count == 0:
        print(f"日志中未匹配到可统计的 Time 记录: {log_path}")
        return 1

    precision = max(0, args.precision)
    print(f"Total Time: {total_ms:.{precision}f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
