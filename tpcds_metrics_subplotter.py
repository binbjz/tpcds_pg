import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from typing import Optional, List
from pathlib import Path
from loguru import logger


def plot_metrics_from_csv_chunked_continuous(
        csv_path: str,
        ncols: int,
        chunk_size: int = 5000,
        output_dir: Optional[Path] = None,
        output_image_name: Optional[str] = None,
        downsample_step: int = 1,
        show: bool = True,
        close_after_save: bool = True,
        encoding: Optional[str] = None,
        sep: str = ",",
) -> None:
    """
    从给定的 CSV 文件路径分块读取数据,并绘制数值型列的连续图表。
    关键特性：
    1) x 轴使用全局行号偏移,跨 chunk 连续
    2) 最后一行数据仅打印日志,不参与绘图
    3) 支持降采样减少绘图点数,提升整体性能
    """

    if ncols <= 0:
        raise ValueError("ncols 必须大于 0")

    if chunk_size <= 1:
        raise ValueError("chunk_size 必须大于 1(因为最后一行要单独处理)")

    if downsample_step <= 0:
        raise ValueError("downsample_step 必须大于 0")

    csv_file_path = Path(csv_path)
    if not csv_file_path.exists():
        raise FileNotFoundError(f"CSV 文件不存在：{csv_file_path}")

    out_dir = output_dir if output_dir is not None else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_name = output_image_name if output_image_name else "metrics_data_continuous.png"
    save_path = out_dir / out_name

    rows_read = 0
    numeric_cols: List[str] = []
    fig = None
    axes = None
    axes_flat = None

    prev_chunk: Optional[pd.DataFrame] = None

    reader = pd.read_csv(
        csv_file_path,
        chunksize=chunk_size,
        encoding=encoding,
        sep=sep,
        low_memory=False,
    )

    for chunk in reader:
        if prev_chunk is None:
            prev_chunk = chunk
            continue

        df_to_plot = prev_chunk

        if not numeric_cols:
            numeric_cols = df_to_plot.select_dtypes(include=["number"]).columns.tolist()
            if not numeric_cols:
                logger.info("没有数值型列可供绘图。")
                return

            num_columns = len(numeric_cols)
            nrows = num_columns // ncols + (1 if num_columns % ncols else 0)
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(20, 6 * nrows))
            axes_flat = np.array(axes).reshape(-1)

            for j in range(num_columns, len(axes_flat)):
                axes_flat[j].set_visible(False)

            for i, col in enumerate(numeric_cols):
                axes_flat[i].set_title(col, fontsize=12)

        df_len = len(df_to_plot)
        if df_len > 0:
            x = np.arange(rows_read, rows_read + df_len, dtype=np.int64)

            x_sampled = x[::downsample_step]

            for i, col in enumerate(numeric_cols):
                if col not in df_to_plot.columns:
                    continue

                y = df_to_plot[col].to_numpy(copy=False)
                y_sampled = y[::downsample_step]

                axes_flat[i].plot(x_sampled, y_sampled, alpha=0.5)

        rows_read += df_len
        prev_chunk = chunk

    if prev_chunk is None:
        logger.info("CSV 文件为空或无法读取。")
        return

    last_chunk = prev_chunk
    if len(last_chunk) == 0:
        logger.info("最后一个 chunk 为空,无可绘图数据。")
        return

    last_row = last_chunk.iloc[-1]
    logger.info("最后一行数据：\n{}".format(last_row))

    last_chunk_to_plot = last_chunk.iloc[:-1]
    if len(last_chunk_to_plot) > 0:
        if not numeric_cols:
            numeric_cols = last_chunk_to_plot.select_dtypes(include=["number"]).columns.tolist()
            if not numeric_cols:
                logger.info("没有数值型列可供绘图。")
                return

            num_columns = len(numeric_cols)
            nrows = num_columns // ncols + (1 if num_columns % ncols else 0)
            fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(20, 6 * nrows))
            axes_flat = np.array(axes).reshape(-1)

            for j in range(num_columns, len(axes_flat)):
                axes_flat[j].set_visible(False)

            for i, col in enumerate(numeric_cols):
                axes_flat[i].set_title(col, fontsize=12)

        df_len = len(last_chunk_to_plot)
        x = np.arange(rows_read, rows_read + df_len, dtype=np.int64)
        x_sampled = x[::downsample_step]

        for i, col in enumerate(numeric_cols):
            if col not in last_chunk_to_plot.columns:
                continue

            y = last_chunk_to_plot[col].to_numpy(copy=False)
            y_sampled = y[::downsample_step]
            axes_flat[i].plot(x_sampled, y_sampled, alpha=0.5)

        rows_read += df_len

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    logger.info(f"指标分析图已保存至 {save_path}")

    if show:
        plt.show()

    if close_after_save:
        plt.close(fig)


if __name__ == "__main__":
    _csv_file = "tpcds_metrics_data.csv"
    plot_metrics_from_csv_chunked_continuous(_csv_file, ncols=2)
