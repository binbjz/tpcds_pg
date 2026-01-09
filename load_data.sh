#!/bin/bash

set -u
set -o pipefail

DB_NAME="${DB_NAME:-tpcds}"
DB_USER="${DB_USER:-postgres}"
DB_PASS="${DB_PASS:-pg-auth}"
DB_HOST="${DB_HOST:-localhost}"

DAT_DIR="${DAT_DIR:-/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tpcds_data}"
MAX_JOBS="${MAX_JOBS:-20}"

# ==========================================================
# 函数: load_data_to_table
# 描述: 封装数据加载命令的函数,执行 COPY 命令
# 注意: 这里简化了密码处理,实际应用应更安全地处理密码
# 执行程序：nohup /usr/bin/time /bin/bash load_data.sh >>./tpcds_load_data.log 2>&1 &
# ==========================================================
load_data_to_table() {
    local dat_file="$1"
    local table_name
    local escaped_dat_file

    table_name=$(basename "$dat_file" .dat | awk -F'_' '{s=$1; for (i=2; i<NF-1; i++) s=s"_"$i; print s}')

    [[ -n "$table_name" ]] || {
        echo "无法从文件名解析表名: $dat_file"
        return 1
    }

    [[ "$table_name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
        echo "解析出的表名非法: $table_name (源文件: $dat_file)"
        return 1
    }

    escaped_dat_file=${dat_file//\'/\'\'}

    if PGPASSWORD="$DB_PASS" psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -v ON_ERROR_STOP=1 -c \
        "\COPY $table_name FROM '$escaped_dat_file' WITH DELIMITER '|' CSV"; then
        echo "成功将 $dat_file 加载到表 $table_name.."
        return 0
    else
        echo "加载 $dat_file 到表 $table_name 失败.."
        return 1
    fi
}

command -v psql >/dev/null 2>&1 || {
    echo "未找到 psql 命令,请先安装 PostgreSQL 客户端工具.."
    exit 1
}

[[ -d "$DAT_DIR" ]] || {
    echo "数据目录不存在: $DAT_DIR"
    exit 1
}

[[ "$MAX_JOBS" =~ ^[1-9][0-9]*$ ]] || {
    echo "MAX_JOBS 必须是大于 0 的整数,当前值: $MAX_JOBS"
    exit 1
}

shopt -s nullglob
dat_files=("$DAT_DIR"/*.dat)

if [ "${#dat_files[@]}" -eq 0 ]; then
    echo "目录 $DAT_DIR 下未找到 .dat 文件.."
    exit 1
fi

count=0
failed_jobs=0
for dat_file in "${dat_files[@]}"; do
    load_data_to_table "$dat_file" &
    count=$((count + 1))

    if [ "$count" -eq "$MAX_JOBS" ]; then
        if ! wait -n; then
            failed_jobs=$((failed_jobs + 1))
        fi
        count=$((count - 1))
    fi
done

while [ "$count" -gt 0 ]; do
    if ! wait -n; then
        failed_jobs=$((failed_jobs + 1))
    fi
    count=$((count - 1))
done

if [ "$failed_jobs" -gt 0 ]; then
    echo "全部数据加载完成, 失败任务数: $failed_jobs.."
    exit 1
fi

echo "全部数据加载完成.."
