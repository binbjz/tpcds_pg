#!/bin/bash

DSGEN_PATH="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1"

# ==========================================================
# 函数: generate_dat_data
# 描述: 生成TPC-DS测试数据
# 确认程序在后台正确执行：
# ps -eo pid,user,%cpu,%mem,vsz,rss,tty,stat,start,time,etime,cmd | grep '[d]sdgen'
# ==========================================================
generate_dat_data() {
    local TOOLS_DIR=${1:-"${DSGEN_PATH}/tools"}
    local SCALE=${2:-10}
    local child

    cd "$TOOLS_DIR" || {
        echo "无法进入目录 $TOOLS_DIR"
        exit 1
    }

    for child in {1..4}; do
        nohup /usr/bin/time ./dsdgen -scale "$SCALE" -dir ../tpcds_data/ \
            -parallel 4 -child "${child}" >>../../tpcds_data.log 2>&1 &
    done
}

# ==========================================================
# 函数: generate_query_data
# 描述: 生成TPC-DS查询SQL
# 确认程序在后台正确执行：
# ps -eo pid,user,%cpu,%mem,vsz,rss,tty,stat,start,time,etime,cmd | grep '[d]sqgen'
# ==========================================================
generate_query_data() {
    local TPC_DIR=${DSGEN_PATH}

    cd "$TPC_DIR/tools" || {
        echo "无法进入目录 $TPC_DIR/tools"
        exit 1
    }

    nohup /usr/bin/time ./dsqgen -output_dir ../tpcds_query/ \
        -input ../query_templates/templates.lst -scale 1 -dialect postgresql \
        -directory ../query_templates/ >>../../tpcds_query.log 2>&1 &
}

generate_dat_data "${DSGEN_PATH}/tools" 10
generate_query_data
