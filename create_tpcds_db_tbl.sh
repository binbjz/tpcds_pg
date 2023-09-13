#!/bin/bash

# ==========================================================
# 函数: create_database
# 描述: 创建一个新的 PostgreSQL 数据库
# 参数: $1 数据库用户名, $2 数据库名称
# 返回: 成功时返回 0, 失败时返回 1
# ==========================================================
create_database() {
    local db_user="$1"
    local db_name="$2"
    if createdb -U "$db_user" "$db_name"; then
        echo "数据库 $db_name 成功创建."
        return 0
    else
        echo "数据库 $db_name 创建失败."
        return 1
    fi
}

# ==========================================================
# 函数: execute_sql_file
# 描述: 在指定的数据库中执行一个 SQL 文件以创建表
# 参数: $1 数据库用户名, $2 数据库名称, $3 SQL 文件路径
# 返回: 成功时返回 0, 失败时返回 1
# ==========================================================
execute_sql_file() {
    local db_user="$1"
    local db_name="$2"
    local sql_file="$3"
    if psql -U "$db_user" -d "$db_name" -a -f "$sql_file"; then
        echo "所有表成功创建."
        return 0
    else
        echo "创建表失败."
        return 1
    fi
}

# ==========================================================
# 函数: main
# 描述: 脚本的主执行函数
# ==========================================================
main() {
    local tools_dir="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tools"
    local db_name="tpcds"
    local db_user="postgres"
    local db_pass="pg-auth"
    local sql_file="${tools_dir}/tpcds.sql"

    export PGPASSWORD="$db_pass"

    cd "$tools_dir" || {
        echo "无法进入目录 $tools_dir"
        exit 1
    }

    if create_database "$db_user" "$db_name"; then
        execute_sql_file "$db_user" "$db_name" "$sql_file"
    else
        echo "数据库创建失败,终止程序并清除环境.."
        exit 1
    fi

    unset PGPASSWORD
}

main
