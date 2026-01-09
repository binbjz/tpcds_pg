#!/bin/bash

set -u
set -o pipefail

DSGEN_PATH="${DSGEN_PATH:-/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1}"
DSDGEN_PARALLEL="${DSDGEN_PARALLEL:-4}"
DSQGEN_SCALE="${DSQGEN_SCALE:-1}"
DSQGEN_DIALECT="${DSQGEN_DIALECT:-postgresql}"

declare -a dsdgen_pids=()
declare -a dsqgen_pids=()

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

    [[ -x "./dsdgen" ]] || {
        echo "未找到可执行文件: $TOOLS_DIR/dsdgen"
        return 1
    }

    [[ -d "../tpcds_data" ]] || {
        echo "未找到数据输出目录: $TOOLS_DIR/../tpcds_data"
        return 1
    }

    [[ "$DSDGEN_PARALLEL" =~ ^[1-9][0-9]*$ ]] || {
        echo "DSDGEN_PARALLEL 必须是大于 0 的整数,当前值: $DSDGEN_PARALLEL"
        return 1
    }

    for ((child = 1; child <= DSDGEN_PARALLEL; child++)); do
        nohup /usr/bin/time ./dsdgen -scale "$SCALE" -dir ../tpcds_data/ \
            -parallel "$DSDGEN_PARALLEL" -child "${child}" >>../../tpcds_data.log 2>&1 &
        dsdgen_pids+=("$!")
    done

    echo "TPC-DS 数据生成任务已启动, 并发数: $DSDGEN_PARALLEL.."
    return 0
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

    [[ -x "./dsqgen" ]] || {
        echo "未找到可执行文件: $TPC_DIR/tools/dsqgen"
        return 1
    }

    [[ -f "../query_templates/templates.lst" ]] || {
        echo "未找到模板清单文件: $TPC_DIR/query_templates/templates.lst"
        return 1
    }

    [[ -r "../query_templates/templates.lst" ]] || {
        echo "模板清单文件不可读: $TPC_DIR/query_templates/templates.lst"
        return 1
    }

    nohup /usr/bin/time ./dsqgen -output_dir ../tpcds_query/ \
        -input ../query_templates/templates.lst -scale "$DSQGEN_SCALE" -dialect "$DSQGEN_DIALECT" \
        -directory ../query_templates/ >>../../tpcds_query.log 2>&1 &

    dsqgen_pids+=("$!")
    echo "TPC-DS 查询生成任务已启动.."
    return 0
}

# ==========================================================
# 函数: wait_generate_jobs
# 描述: 等待后台生成任务结束并汇总退出状态
# ==========================================================
wait_generate_jobs() {
    local pid
    local total_jobs=0
    local failed_jobs=0

    for pid in "${dsdgen_pids[@]}" "${dsqgen_pids[@]}"; do
        [ -n "$pid" ] || continue
        total_jobs=$((total_jobs + 1))
        if ! wait "$pid"; then
            failed_jobs=$((failed_jobs + 1))
        fi
    done

    if [ "$failed_jobs" -gt 0 ]; then
        echo "生成任务结束, 失败任务数: $failed_jobs/$total_jobs.."
        return 1
    fi

    echo "生成任务全部完成.."
    return 0
}

generate_dat_data "${DSGEN_PATH}/tools" 10 || exit 1
generate_query_data || exit 1
wait_generate_jobs || exit 1
