#!/usr/bin/env bats
# ==========================================================
# test_load_data.bats
#  ✓ 测试 psql 依赖缺失时失败
#  ✓ 测试 DAT_DIR 不存在时失败
#  ✓ 测试 MAX_JOBS 非法时失败
#  ✓ 测试 DAT_DIR 为空时失败
#  ✓ 测试成功加载并校验 psql 参数
#  ✓ 测试部分任务失败时返回失败并汇总失败数
#  ✓ 测试非法表名文件触发失败
#  ✓ 测试 DAT_DIR 含单引号时 COPY 路径被正确转义
# ==========================================================

setup() {
    TEST_DIR="$(mktemp -d)"
    MOCK_BIN="$TEST_DIR/mock_bin"
    mkdir -p "$MOCK_BIN"

    SCRIPT_PATH="$BATS_TEST_DIRNAME/load_data.sh"
    export PATH="$MOCK_BIN:$PATH"

    export MOCK_PSQL_ARGS_LOG="$TEST_DIR/mock_psql_args.log"
    export MOCK_PSQL_ENV_LOG="$TEST_DIR/mock_psql_env.log"
}

teardown() {
    rm -rf "$TEST_DIR"
}

create_mock_psql() {
    cat >"$MOCK_BIN/psql" <<'EOF'
#!/usr/bin/env bash
set -u
args_log="${MOCK_PSQL_ARGS_LOG:-/tmp/mock_psql_args.log}"
env_log="${MOCK_PSQL_ENV_LOG:-/tmp/mock_psql_env.log}"

printf '%s\n' "$*" >>"$args_log"
printf '%s\n' "${PGPASSWORD:-}" >>"$env_log"

if [ "${MOCK_PSQL_ALWAYS_FAIL:-0}" = "1" ]; then
    exit 1
fi

if [ -n "${MOCK_PSQL_FAIL_TABLE:-}" ]; then
    case "$*" in
        *"\\COPY ${MOCK_PSQL_FAIL_TABLE} FROM "*)
            exit 1
            ;;
    esac
fi

exit 0
EOF
    chmod +x "$MOCK_BIN/psql"
}

@test "测试 psql 依赖缺失时失败" {
    run env PATH="$MOCK_BIN" DAT_DIR="$TEST_DIR/not_used" /bin/bash "$SCRIPT_PATH"
    [ "$status" -ne 0 ]
    [[ "$output" == *"未找到 psql 命令"* ]]
}

@test "测试 DAT_DIR 不存在时失败" {
    create_mock_psql
    run env DAT_DIR="$TEST_DIR/not_exists" bash "$SCRIPT_PATH"
    [ "$status" -ne 0 ]
    [[ "$output" == *"数据目录不存在"* ]]
}

@test "测试 MAX_JOBS 非法时失败" {
    create_mock_psql
    dat_dir="$TEST_DIR/data"
    mkdir -p "$dat_dir"
    printf 'line|\n' >"$dat_dir/customer_1_4.dat"

    run env DAT_DIR="$dat_dir" MAX_JOBS="abc" bash "$SCRIPT_PATH"
    [ "$status" -ne 0 ]
    [[ "$output" == *"MAX_JOBS 必须是大于 0 的整数"* ]]
}

@test "测试 DAT_DIR 为空时失败" {
    create_mock_psql
    dat_dir="$TEST_DIR/empty_data"
    mkdir -p "$dat_dir"

    run env DAT_DIR="$dat_dir" bash "$SCRIPT_PATH"
    [ "$status" -ne 0 ]
    [[ "$output" == *"未找到 .dat 文件"* ]]
}

@test "测试成功加载并校验 psql 参数" {
    create_mock_psql
    dat_dir="$TEST_DIR/data_ok"
    mkdir -p "$dat_dir"
    printf 'a|\n' >"$dat_dir/customer_1_4.dat"
    printf 'b|\n' >"$dat_dir/store_sales_1_4.dat"

    run env \
        DAT_DIR="$dat_dir" \
        MAX_JOBS="2" \
        DB_NAME="tpcds_test" \
        DB_USER="postgres_user" \
        DB_PASS="pg-pass" \
        DB_HOST="127.0.0.1" \
        bash "$SCRIPT_PATH"

    [ "$status" -eq 0 ]
    [[ "$output" == *"全部数据加载完成.."* ]]

    [ -f "$MOCK_PSQL_ARGS_LOG" ]
    call_count="$(wc -l <"$MOCK_PSQL_ARGS_LOG" | tr -d ' ')"
    [ "$call_count" -eq 2 ]

    grep -q -- "-v ON_ERROR_STOP=1" "$MOCK_PSQL_ARGS_LOG"
    grep -q -- "-U postgres_user -d tpcds_test -h 127.0.0.1" "$MOCK_PSQL_ARGS_LOG"

    [ -f "$MOCK_PSQL_ENV_LOG" ]
    env_count="$(grep -c '^pg-pass$' "$MOCK_PSQL_ENV_LOG" || true)"
    [ "$env_count" -eq 2 ]
}

@test "测试部分任务失败时返回失败并汇总失败数" {
    create_mock_psql
    dat_dir="$TEST_DIR/data_partial_fail"
    mkdir -p "$dat_dir"
    printf 'a|\n' >"$dat_dir/customer_1_4.dat"
    printf 'b|\n' >"$dat_dir/store_sales_1_4.dat"

    run env \
        DAT_DIR="$dat_dir" \
        MAX_JOBS="2" \
        MOCK_PSQL_FAIL_TABLE="store_sales" \
        bash "$SCRIPT_PATH"

    [ "$status" -ne 0 ]
    [[ "$output" == *"失败任务数: 1"* ]]
}

@test "测试非法表名文件触发失败" {
    create_mock_psql
    dat_dir="$TEST_DIR/data_invalid_table"
    mkdir -p "$dat_dir"
    printf 'x|\n' >"$dat_dir/123table_1_4.dat"

    run env DAT_DIR="$dat_dir" MAX_JOBS="1" bash "$SCRIPT_PATH"
    [ "$status" -ne 0 ]
    [[ "$output" == *"解析出的表名非法"* ]]
    [[ "$output" == *"失败任务数: 1"* ]]
}

@test "测试 DAT_DIR 含单引号时 COPY 路径被正确转义" {
    create_mock_psql
    dat_dir="$TEST_DIR/dir'quoted"
    mkdir -p "$dat_dir"
    printf 'y|\n' >"$dat_dir/customer_1_4.dat"

    run env DAT_DIR="$dat_dir" MAX_JOBS="1" bash "$SCRIPT_PATH"
    [ "$status" -eq 0 ]

    [ -f "$MOCK_PSQL_ARGS_LOG" ]
    grep -q "dir''quoted" "$MOCK_PSQL_ARGS_LOG"
}
