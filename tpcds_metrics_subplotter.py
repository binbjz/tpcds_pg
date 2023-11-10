import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from pathlib import Path
from loguru import logger


def plot_metrics_from_csv_chunked_continuous(csv_path: str, ncols: int, chunk_size: int = 5000,
                                             output_dir: Optional[Path] = None,
                                             output_image_name: Optional[str] = None):
    """
    从给定的 CSV 文件路径中分块读取数据,并且绘制数据指标的连续图表。
    使用全局索引偏移量来在 x 轴上实现连续性。
    """
    rows_read, total_rows = 0, sum(1 for _ in open(csv_path)) - 1

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        if rows_read + len(chunk) >= total_rows:
            last_row = chunk.iloc[-1]
            chunk = chunk.iloc[:-1]
            logger.info("最后一行数据：\n{}".format(last_row))

        numeric_cols_chunk = chunk.select_dtypes(include=["number"]).dropna(axis=1)
        if len(numeric_cols_chunk.columns) == 0:
            logger.info("没有数值型列可供绘图。")
            continue

        axes = None
        if rows_read == 0:
            num_columns = len(numeric_cols_chunk.columns)
            nrows = num_columns // ncols + (num_columns % ncols > 0)
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(20, 6 * nrows))

        for i, col in enumerate(numeric_cols_chunk.columns):
            ax = axes.flatten()[i]
            x_ticks = range(rows_read, rows_read + len(chunk))
            ax.plot(x_ticks, numeric_cols_chunk[col], alpha=0.5)
            ax.set_title(col, fontsize=12)

        rows_read += len(chunk)

    plt.tight_layout()
    save_path = (output_dir if output_dir else Path(".")).joinpath(
        output_image_name if output_image_name else "metrics_data_continuous.png")
    plt.savefig(save_path, dpi=300)
    logger.info(f"指标分析图已保存至 {save_path}")
    plt.show()


if __name__ == "__main__":
    _csv_file = "tpcds_metrics_data.csv"
    plot_metrics_from_csv_chunked_continuous(_csv_file, ncols=2)
