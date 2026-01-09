import os
import threading
import multiprocessing
import numpy as np


def get_env_positive_int(name: str, default: int, allow_zero: bool = False) -> int:
    """
    从环境变量读取正整数配置,非法值回退到默认值。
    """
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if allow_zero and value == 0:
        return 0
    if value <= 0:
        return default
    return value


def stress_cpu_core(stop_event=None):
    """
    打满单个CPU核心
    """
    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            a = np.random.rand(500, 500)
            b = np.random.rand(500, 500)
            _ = np.dot(a, b)
    except KeyboardInterrupt:
        print("CPU压力测试被终止在单核上.")


def stress_cpu(worker_count=None, stop_event=None):
    """
    打满所有可用的CPU核心
    pkill -f "python.*server_stress.py"
    """
    try:
        num_cores = worker_count or (os.cpu_count() or 1)
        print(f"开始在 {num_cores} 个核心上打满CPU...")

        threads = []
        for _ in range(num_cores):
            t = threading.Thread(target=stress_cpu_core, args=(stop_event,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("CPU压力测试被终止.")


def stress_memory(max_blocks=None, block_size=None):
    """
    打满内存
    """
    max_blocks = (
        get_env_positive_int("STRESS_MEMORY_MAX_BLOCKS", 0, allow_zero=True)
        if max_blocks is None
        else max_blocks
    )
    block_size = (
        get_env_positive_int("STRESS_MEMORY_BLOCK_SIZE", 9000)
        if block_size is None
        else block_size
    )
    memory_fill = []
    try:
        print("开始打满内存...")
        while True:
            if max_blocks > 0 and len(memory_fill) >= max_blocks:
                print(f"达到内存压力块上限({max_blocks}),停止继续分配.")
                break
            memory_fill.append(np.zeros((block_size, block_size), dtype=np.float64))
    except MemoryError:
        print("内存不足,清空数组以避免崩溃..")
        memory_fill.clear()
    except KeyboardInterrupt:
        print("内存压力测试被终止.")


if __name__ == "__main__":
    processes = []
    try:
        choice = input("请选择要执行的操作(1: 打满CPU, 2: 打满内存)：")

        if choice == "1":
            cpu_processes = get_env_positive_int(
                "STRESS_CPU_PROCESSES", multiprocessing.cpu_count() or 1
            )
            print(f"启动 {cpu_processes} 个CPU压力进程...")
            for _ in range(cpu_processes):
                p = multiprocessing.Process(target=stress_cpu_core)
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
        elif choice == "2":
            stress_memory()
        else:
            print("无效的选择.")
    except KeyboardInterrupt:
        print("程序被用户终止.")
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
            p.join()
