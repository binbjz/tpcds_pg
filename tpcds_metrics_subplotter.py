import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from pathlib import Path
from loguru import logger


def plot_metrics_from_csv_chunked(csv_path: str, ncols: int, chunk_size: int = 50000,
                                  output_dir: Optional[Path] = None,
                                  output_image_name: Optional[str] = None):
    """
    从给定的 CSV 文件路径中读取数据,并实时绘制数值型指标的子图。
    逐块读取数据以优化内存使用。

    :param csv_path: 要读取的CSV文件的路径(类型：str)
    :param ncols: 子图的列数(类型：int)
    :param chunk_size: 每块数据的行数(类型：int,默认为50000)
    :param output_dir: 输出图像的目录(类型：Optional[Path],默认为None)
    :param output_image_name: 输出图像的文件名(类型：Optional[str],默认为None)
    """
    n = 0
    fig, axes = None, None

    rows_read, total_rows = 0, sum(1 for _ in open(csv_path)) - 1

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        if not any(chunk.dtypes.astype(str).isin(["float64", "int64"])):
            logger.info("没有数值型列可供绘图..")
            return

        logger.info(f"列名：{chunk.columns.tolist()}")
        logger.info(f"数据块最后一行数据：\n{chunk.iloc[-1].to_dict()}")

        rows_read += len(chunk)
        logger.info(f"total_rows：{total_rows}")
        logger.info(f"rows_read：{rows_read}")

        if rows_read == total_rows:
            chunk = chunk.iloc[:-1]
            logger.info(f"最后一个数据块中的数据(已排除最后一行)：\n{chunk}")

        numeric_cols_chunk = chunk.select_dtypes(include=["number"]).dropna(axis=1)

        if n == 0:
            n = len(numeric_cols_chunk.columns)
            nrows = n // ncols + (n % ncols > 0)
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(20, 6 * nrows))

        for ax, col in zip(axes.flatten(), numeric_cols_chunk.columns):
            ax.plot(numeric_cols_chunk[col], alpha=0.5)
            ax.set_xticks(np.arange(0, len(numeric_cols_chunk[col]), 0.5))
            ax.set_title(col, fontsize=12)
            ax.set_xlabel("Time (Minutes)", fontsize=9)
            ax.set_ylabel("Metric Value", fontsize=9)

    plt.subplots_adjust(wspace=0.3, hspace=0.5)
    plt.tight_layout()

    save_path = (output_dir if output_dir else Path(".")).joinpath(
        output_image_name if output_image_name else "tpcds_metrics_data.png")
    plt.savefig(save_path, dpi=300)

    logger.info(f"指标分析图已保存至 {save_path}")
    plt.show()


if __name__ == "__main__":
    _csv_file = "tpcds_metrics_data.csv"
    plot_metrics_from_csv_chunked(_csv_file, ncols=2)
