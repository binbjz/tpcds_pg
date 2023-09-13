#!/bin/bash

# ==========================================================
# 函数: calculate_total_time
# 描述: 统计 TPC-DS 查询SQL总的执行时间
# 参数: $1 要分析的日志文件路径
# ==========================================================
calculate_total_time() {
    local log_file="$1"
    local total_time=0

    total_time=$(awk '/Time:/ { split($2, a, " "); sum += a[1] } END { print sum }' "$log_file")
    echo "Total Time: $total_time ms"
}

log_file="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tpcds_query_0901/tpcds.log"
calculate_total_time "$log_file"
