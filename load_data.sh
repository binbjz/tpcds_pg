#!/bin/bash

DB_NAME="tpcds"
DB_USER="postgres"
DB_PASS="pg-auth"

DAT_DIR="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tpcds_data_0903"
MAX_JOBS=20

# ==========================================================
# 函数: load_data_to_table
# 描述: 封装数据加载命令的函数,执行 COPY 命令
# 注意: 这里简化了密码处理,实际应用应更安全地处理密码
# 执行程序：nohup /usr/bin/time /bin/bash load_data.sh >>./tpcds_load_data.log 2>&1 &
# ==========================================================
load_data_to_table() {
    local dat_file=$1
    local table_name
    table_name=$(basename "$dat_file" .dat | awk -F'_' '{s=$1; for (i=2; i<NF-1; i++) s=s"_"$i; print s}')
    if PGPASSWORD=$DB_PASS psql -U $DB_USER -d $DB_NAME -h localhost -c \
        "\COPY $table_name FROM '$dat_file' WITH DELIMITER '|' CSV"; then
        echo "成功将 $dat_file 加载到表 $table_name.."
    else
        echo "加载 $dat_file 到表 $table_name 失败.."
    fi
}

count=0
for dat_file in "$DAT_DIR"/*.dat; do
    load_data_to_table "$dat_file" &
    count=$((count + 1))

    if [ $count -eq $MAX_JOBS ]; then
        wait -n
        count=$((count - 1))
    fi
done

wait

echo "全部数据加载完成.."
