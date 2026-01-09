"""
并发处理 .dat 文件，删除每一行末尾的 "|" 字符。

与 remove_last_pipe.sh 兼容的默认行为：
1) 默认目录来自 DAT_DIR 环境变量
2) MAX_JOBS/REMOVE_LAST_PIPE_LOG 环境变量继续生效
3) 失败任务存在时返回非零
"""

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import NamedTemporaryFile


DEFAULT_DAT_DIR = (
    "/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/"
    "DSGen-software-code-3.2.0rc1/tpcds_data"
)
DEFAULT_MAX_JOBS = "20"
DEFAULT_LOG_FILE = "./tpcds_remove_last_pipe.log"


def parse_positive_int(name: str, value: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise ValueError(f"{name} 必须是大于 0 的整数,当前值: {value}")
    return int(value)


def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("remove_last_pipe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.FileHandler(log_file, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def strip_last_pipe(line: bytes) -> bytes:
    if line.endswith(b"|\n"):
        return line[:-2] + b"\n"
    if line.endswith(b"|"):
        return line[:-1]
    return line


def process_dat_file(dat_file: Path, logger: logging.Logger) -> bool:
    if not dat_file.is_file():
        logger.error(f"指定的数据文件不存在: {dat_file}")
        return False

    try:
        with NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=str(dat_file.parent),
            prefix=f"{dat_file.name}.tmp.",
        ) as tmp:
            tmp_path = Path(tmp.name)
            with dat_file.open("rb") as src:
                for line in src:
                    tmp.write(strip_last_pipe(line))
    except OSError as error:
        logger.error(f"无法为 {dat_file} 创建或写入临时文件: {error}")
        return False

    try:
        os.replace(tmp_path, dat_file)
    except OSError as error:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        logger.error(f"处理 {dat_file} 失败,已保留原文件内容.. 原因: {error}")
        return False

    logger.info(f"已处理 {dat_file} 文件,删除了每一行最后的 | 字符..")
    return True


def process_dat_files(dat_dir: Path, max_jobs: int, logger: logging.Logger) -> int:
    if not dat_dir.is_dir():
        print(f"指定的目录不存在: {dat_dir}")
        return 1

    dat_files = sorted(dat_dir.glob("*.dat"))
    if not dat_files:
        print(f"目录 {dat_dir} 下未找到 .dat 文件")
        return 1

    failed_jobs = 0
    with ThreadPoolExecutor(max_workers=max_jobs) as executor:
        futures = {
            executor.submit(process_dat_file, dat_file, logger): dat_file
            for dat_file in dat_files
        }
        for future in as_completed(futures):
            ok = False
            try:
                ok = future.result()
            except Exception as error:  # noqa: BLE001
                logger.error(f"处理 {futures[future]} 发生异常: {error}")
            if not ok:
                failed_jobs += 1

    if failed_jobs > 0:
        print(f"处理完成, 失败任务数: {failed_jobs}")
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="并发处理目录下所有 .dat 文件，删除每行末尾的管道符。",
    )
    parser.add_argument(
        "dat_dir",
        nargs="?",
        default=os.getenv("DAT_DIR", DEFAULT_DAT_DIR),
        help="要处理的 .dat 文件目录（默认读取 DAT_DIR）",
    )
    parser.add_argument(
        "--max-jobs",
        default=os.getenv("MAX_JOBS", DEFAULT_MAX_JOBS),
        help="并发任务数（默认读取 MAX_JOBS）",
    )
    parser.add_argument(
        "--log-file",
        default=os.getenv("REMOVE_LAST_PIPE_LOG", DEFAULT_LOG_FILE),
        help="日志文件路径（默认读取 REMOVE_LAST_PIPE_LOG）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        max_jobs = parse_positive_int("MAX_JOBS", str(args.max_jobs))
    except ValueError as error:
        print(error)
        return 1

    logger = setup_logger(Path(args.log_file))
    rc = process_dat_files(Path(args.dat_dir), max_jobs, logger)
    if rc != 0:
        print("处理 .dat 文件失败..")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
