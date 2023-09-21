import os
import threading
import multiprocessing
import numpy as np


def stress_cpu_core():
    """
    打满单个CPU核心
    """
    try:
        while True:
            a = np.random.rand(500, 500)
            b = np.random.rand(500, 500)
            c = np.dot(a, b)
    except KeyboardInterrupt:
        print("CPU压力测试被终止在单核上.")


def stress_cpu():
    """
    打满所有可用的CPU核心
    pkill -f "python.*server_stress.py"
    """
    try:
        num_cores = os.cpu_count()
        print(f"开始在 {num_cores} 个核心上打满CPU...")

        threads = []
        for _ in range(num_cores):
            t = threading.Thread(target=stress_cpu_core)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("CPU压力测试被终止.")


def stress_memory():
    """
    打满内存
    """
    memory_fill = []
    try:
        print("开始打满内存...")
        while True:
            memory_fill.append(np.zeros((9000, 9000), dtype=np.float64))
    except MemoryError:
        print("内存不足,清空数组以避免崩溃..")
        memory_fill.clear()
    except KeyboardInterrupt:
        print("内存压力测试被终止.")


if __name__ == "__main__":
    try:
        choice = input("请选择要执行的操作(1: 打满CPU, 2: 打满内存)：")

        if choice == "1":
            for _ in range(multiprocessing.cpu_count()):
                p = multiprocessing.Process(target=stress_cpu)
                p.start()
        elif choice == "2":
            stress_memory()
        else:
            print("无效的选择.")
    except KeyboardInterrupt:
        print("程序被用户终止.")

