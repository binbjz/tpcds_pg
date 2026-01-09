#!/bin/bash

set -u
set -o pipefail

# ==========================================================
# 函数: create_database
# 描述: 创建一个新的 PostgreSQL 数据库
# 参数: $1 数据库用户名, $2 数据库名称
# 返回: 成功时返回 0, 失败时返回 1
# ==========================================================
create_database() {
    local db_user="$1"
    local db_name="$2"
    local escaped_db_name
    local db_exists

    escaped_db_name=${db_name//\'/\'\'}
    db_exists=$(psql -U "$db_user" -d postgres -tAc \
        "SELECT 1 FROM pg_database WHERE datname = '$escaped_db_name'" 2>/dev/null | tr -d '[:space:]')

    if [ "$db_exists" = "1" ]; then
        echo "数据库 $db_name 已存在,跳过创建."
        return 0
    fi

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
    if psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -a -f "$sql_file"; then
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
    local tools_dir="${TPCDS_TOOLS_DIR:-/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tools}"
    local db_name="${TPCDS_DB_NAME:-tpcds}"
    local db_user="${TPCDS_DB_USER:-postgres}"
    local db_pass="${TPCDS_DB_PASS:-pg-auth}"
    local sql_file="${tools_dir}/tpcds.sql"

    command -v createdb >/dev/null 2>&1 || {
        echo "未找到 createdb 命令,请先安装 PostgreSQL 客户端工具.."
        exit 1
    }

    command -v psql >/dev/null 2>&1 || {
        echo "未找到 psql 命令,请先安装 PostgreSQL 客户端工具.."
        exit 1
    }

    [[ -r "$sql_file" ]] || {
        echo "SQL 文件不存在或不可读: $sql_file"
        exit 1
    }

    export PGPASSWORD="$db_pass"
    trap 'unset PGPASSWORD' EXIT

    cd "$tools_dir" || {
        echo "无法进入目录 $tools_dir"
        exit 1
    }

    if create_database "$db_user" "$db_name"; then
        execute_sql_file "$db_user" "$db_name" "$sql_file" || {
            echo "创建表失败,终止程序并清除环境.."
            exit 1
        }
    else
        echo "数据库创建失败,终止程序并清除环境.."
        exit 1
    fi

    unset PGPASSWORD
}

main
