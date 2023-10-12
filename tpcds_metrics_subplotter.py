import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from pathlib import Path
from loguru import logger


def plot_metrics_from_csv_chunked(csv_path: str, ncols: int, chunk_size: int = 5000,
                                  output_dir: Optional[Path] = None,
                                  output_image_name: Optional[str] = None):
    """
    从给定的 CSV 文件路径中读取数据,并实时绘制数值型指标的子图。
    逐块读取数据以优化内存使用。
    """
    fig, axes = None, None
    cumulative_df = pd.DataFrame()

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

        numeric_cols_chunk = chunk.select_dtypes(include=["number"]).dropna(axis=1)
        cumulative_df = pd.concat([cumulative_df, numeric_cols_chunk])

        if fig is None and axes is None:
            num_columns = len(numeric_cols_chunk.columns)
            nrows = num_columns // ncols + (num_columns % ncols > 0)
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(20, 6 * nrows))

        for ax, col in zip(axes.flatten(), numeric_cols_chunk.columns):
            ax.clear()
            ax.plot(cumulative_df[col], alpha=0.5)
            ax.set_xticks(np.arange(0, len(cumulative_df[col]), 0.5))
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

