"""
在虚拟环境中安装依赖库,收集PG性能指标：
$ python -m venv metrics_venv
$ cd metrics_venv/ && source bin/activate
$ pip install asyncpg pandas pyarrow psutil loguru

$ nohup /usr/bin/time python sys_pg_metrics_collector.py &
或者自定义输出文件名
$ nohup /usr/bin/time python sys_pg_metrics_collector.py > sys_pg_metrics_collector.log 2>&1 &

# 查看执行进程
$ ps -eo pid,user,pcpu,pmem,vsz,rss,tty,stat,start,time,etime,cmd | grep '[s]ys_pg_metrics_collector.py'
"""

import asyncio
import os
import asyncpg
import psutil
import pandas as pd
from collections import Counter
from pathlib import Path
from asyncpg.pool import PoolConnectionProxy
from loguru import logger


def get_env_int(name: str, default: int, min_value: int = 1) -> int:
    """
    获取并校验环境变量中的整数配置；配置非法时回退默认值。
    """
    value_raw = os.getenv(name)
    if value_raw in (None, ""):
        return default

    try:
        value = int(value_raw)
    except ValueError:
        logger.warning(f"环境变量 {name} 不是整数,使用默认值 {default}")
        return default

    if value < min_value:
        logger.warning(f"环境变量 {name} 小于最小值 {min_value},使用默认值 {default}")
        return default
    return value


def build_pg_config() -> dict:
    """
    构造 PG 连接配置。默认值与原脚本保持一致,并允许通过环境变量覆盖。
    """
    return {
        "host": os.getenv("TPCDS_PG_HOST", "localhost"),
        "port": get_env_int("TPCDS_PG_PORT", 5432),
        "user": os.getenv("TPCDS_PG_USER", "postgres"),
        "password": os.getenv("TPCDS_PG_PASSWORD", "pg-auth"),
        "database": os.getenv("TPCDS_PG_DATABASE", "tpcds"),
    }


class CSVBuffer:
    """
    更高效地将数据写入到一个单一的 CSV 文件中,而不是每次循环都创建一个新的文件。
    同时,通过缓存和批量写入,写入性能也会得到提升。
    """

    def __init__(self, filename: str, buffer_size: int = 1000):
        self.buffer = []
        self.filename = filename
        self.buffer_size = buffer_size
        self.header_written = Path(filename).exists()

    def append(self, data: dict):
        self.buffer.append(data)
        if len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        if self.buffer:
            df = pd.DataFrame(self.buffer)
            if self.header_written:
                df.to_csv(self.filename, mode="a", header=False, index=False)
            else:
                df.to_csv(self.filename, mode="w", header=True, index=False)
                self.header_written = True
            self.buffer = []


async def collect_system_metrics():
    """
    在异步环境中运行阻塞性代码,收集系统性能指标。
    :return: 返回一个包含各种系统性能指标的字典。实际返回的是一个等待对象
    """

    def collect_metrics_blocking():
        """
        收集系统级别指标
        :return: 返回一个包含各种系统性能指标的字典
        """
        cpu_info = psutil.cpu_times_percent(interval=None)
        memory_info = psutil.virtual_memory()
        io_info = psutil.disk_io_counters()
        return {
            "cpu_user": cpu_info.user,
            "cpu_system": cpu_info.system,
            "memory_used": memory_info.used,
            "memory_free": memory_info.free,
            "io_read": io_info.read_count,
            "io_write": io_info.write_count,
        }

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, collect_metrics_blocking)


async def collect_pg_metrics(conn: PoolConnectionProxy, db_name: str = "tpcds"):
    """
    异步地收集PG内部性能指标。
    :param conn: 数据库连接
    :return: 返回一个包含各种PG性能指标的字典
    """
    metrics = {}

    # 收集会话数
    session_count = await conn.fetchval("SELECT count(*) FROM pg_stat_activity;")
    metrics["pg_session_count"] = session_count

    # 收集数据库级别统计信息
    db_stat = await conn.fetchrow(
        "SELECT datname, xact_commit, xact_rollback FROM pg_stat_database "
        "WHERE datname = $1 LIMIT 1;",
        db_name,
    )
    if db_stat is not None:
        metrics["pg_db_name"] = db_stat["datname"]
        metrics["pg_xact_commit"] = db_stat["xact_commit"]
        metrics["pg_xact_rollback"] = db_stat["xact_rollback"]
    else:
        metrics["pg_db_name"] = db_name
        metrics["pg_xact_commit"] = 0
        metrics["pg_xact_rollback"] = 0

    # 收集后台写入器统计信息
    bgwriter_stat = await conn.fetchrow(
        "SELECT buffers_alloc, buffers_backend FROM pg_stat_bgwriter;"
    )
    if bgwriter_stat is not None:
        metrics["pg_buffers_alloc"] = bgwriter_stat["buffers_alloc"]
        metrics["pg_buffers_backend"] = bgwriter_stat["buffers_backend"]
    else:
        metrics["pg_buffers_alloc"] = 0
        metrics["pg_buffers_backend"] = 0

    # 收集磁盘 I/O 操作
    disk_io_stat = await conn.fetchrow(
        "SELECT relname, heap_blks_read, heap_blks_hit FROM "
        "pg_statio_user_tables LIMIT 1;"
    )
    if disk_io_stat is not None:
        metrics["pg_disk_io_table_name"] = disk_io_stat["relname"]
        metrics["pg_heap_blks_read"] = disk_io_stat["heap_blks_read"]
        metrics["pg_heap_blks_hit"] = disk_io_stat["heap_blks_hit"]
    else:
        metrics["pg_disk_io_table_name"] = "N/A"
        metrics["pg_heap_blks_read"] = 0
        metrics["pg_heap_blks_hit"] = 0

    # 收集缓存命中率
    cache_hit_ratio = await conn.fetchval(
        "SELECT COALESCE(sum(heap_blks_hit)::numeric / "
        "NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 0) "
        "as ratio FROM pg_statio_user_tables;"
    )
    metrics["pg_cache_hit_ratio"] = (
        cache_hit_ratio if cache_hit_ratio is not None else 0
    )

    # 收集长时间运行的查询数量
    long_running_queries = await conn.fetchval(
        "SELECT count(*) FROM pg_stat_activity WHERE state != 'idle' "
        "AND now() - pg_stat_activity.query_start > interval '5 minutes';"
    )
    metrics["pg_long_running_queries"] = (
        long_running_queries if long_running_queries is not None else 0
    )

    # 收集未授权的锁数量
    ungranted_locks = await conn.fetchval(
        "SELECT count(*) FROM pg_locks WHERE granted = false;"
    )
    metrics["pg_ungranted_locks"] = (
        ungranted_locks if ungranted_locks is not None else 0
    )

    return metrics


def append_average_to_csv(filename: str):
    """
    读取CSV文件,计算每一列的平均值,并将平均值添加到CSV文件的最后一行。
    :param filename: CSV文件名
    """
    try:
        if Path(filename).exists():
            df = pd.read_csv(filename)
            avg_values_all = pd.Series(dtype="object", index=df.columns)

            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    avg_values_all[col] = df[col].mean()
                elif pd.api.types.is_string_dtype(df[col]):
                    most_common = Counter(df[col].dropna()).most_common(1)
                    if most_common:
                        avg_values_all[col] = most_common[0][0]
                    else:
                        avg_values_all[col] = "N/A"
            df_avg = pd.DataFrame([avg_values_all])
            df_avg.to_csv(filename, mode="a", header=False, index=False)
            logger.info("平均值(或其他适当的值)已成功写入到CSV文件..")
        else:
            logger.warning(f"{filename} 文件不存在,无法计算和写入平均值..")
    except Exception as e:
        logger.error(f"读取CSV文件或写入平均值时出错: {e}")


def check_process_running(process_name: str):
    """
    检查是否有一个名为 process_name 的进程是否正在运行。
    """
    for proc in psutil.process_iter():
        try:
            proc_info = proc.as_dict(attrs=["pid", "name", "cmdline"])
            cmdline = proc_info.get("cmdline") or []
            if process_name.lower() in " ".join(cmdline).lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


async def main(csv_file: str):
    interval_time = get_env_int("METRICS_INTERVAL_SECONDS", 60)
    monitored_file = os.getenv("MONITORED_FILE", "query_0.sql")
    pg_config = build_pg_config()
    pool_pg = None

    csv_buffer = CSVBuffer(csv_file)

    try:
        pool_pg = await asyncpg.create_pool(**pg_config)
        while True:
            if not check_process_running(monitored_file):
                logger.info(f"{monitored_file}不再运行,程序即将退出..")
                break

            async with pool_pg.acquire() as conn:
                system_metrics = await collect_system_metrics()
                pg_metrics = await collect_pg_metrics(conn, pg_config["database"])
                all_metrics = {**system_metrics, **pg_metrics}
                csv_buffer.append(all_metrics)
            await asyncio.sleep(interval_time)
    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        if pool_pg is not None:
            await pool_pg.close()
        csv_buffer.flush()
        append_average_to_csv(csv_file)


if __name__ == "__main__":
    _csv_file = "tpcds_metrics_data.csv"
    asyncio.run(main(_csv_file))
