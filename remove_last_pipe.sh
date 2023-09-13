#!/bin/bash

# ==========================================================
# 函数: process_dat_file
# 描述: 处理单个 .dat 文件,删除每行最后的 "|" 字符
# 参数: $1 .dat 文件的路径
# ps -eo pid,user,%cpu,%mem,vsz,rss,tty,stat,start,time,etime,cmd | grep '[/]usr/bin/time'
# bash remove_last_pipe.sh &
# ==========================================================
process_dat_file() {
    local dat_file="$1"
    nohup /usr/bin/time /bin/bash -c "sed 's/|$//' $dat_file > ${dat_file}.tmp && \
    mv ${dat_file}.tmp $dat_file && \
    echo \"已处理 $dat_file 文件,删除了每一行最后的 | 字符..\"" >>./tpcds_remove_last_pipe.log 2>&1 &
}

# ==========================================================
# 函数: process_dat_files
# 描述: 并发地处理指定的测试数据目录中的所有 .dat 文件
# 参数: $1 指定的目录路径
# ==========================================================
process_dat_files() {
    local dat_dir="$1"
    local dat_file
    local count=0

    [[ -d "$dat_dir" ]] || {
        echo "指定的目录不存在: $dat_dir"
        return 1
    }

    for dat_file in "$dat_dir"/*.dat; do
        process_dat_file "$dat_file"
        ((count++))

        if ((count % 20 == 0)); then
            wait -n
        fi
    done
    wait
    return 0
}

# ==========================================================
# 函数: main
# 描述: 脚本的主执行函数
# ==========================================================
main() {
    local dat_dir
    dat_dir="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tpcds_data_0903"

    process_dat_files "$dat_dir" || {
        echo "处理 .dat 文件失败.."
        exit 1
    }
}

main
