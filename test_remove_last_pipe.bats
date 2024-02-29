#!/usr/bin/env bats
# ==========================================================
# test_remove_last_pipe_opt.bats
# ✓ 测试 process_dat_file 函数删除每行最后的 '|' 字符
# ✓ 测试 process_dat_files 函数处理目录中的所有 .dat 文件
#
# 2 tests, 0 failures
# ==========================================================
load "remove_last_pipe.sh"

setup() {
    TEST_DIR=$(mktemp -d)
}

teardown() {
    rm -rf "$TEST_DIR"
}

@test "测试 process_dat_file 函数删除每行最后的 '|' 字符" {
    # 在这个测试的独立环境中创建测试文件
    echo "Sample line 1|" >"$TEST_DIR/sample1.dat"

    process_dat_file "$TEST_DIR/sample1.dat"
    wait

    result=$(cat "$TEST_DIR/sample1.dat")
    [ "$result" = "Sample line 1" ]
}

@test "测试 process_dat_files 函数处理目录中的所有 .dat 文件" {
    # 在这个测试的独立环境中创建超过20个测试文件来触发 wait -n 逻辑
    for i in $(seq 1 25); do
        echo "Sample line $i|" >"$TEST_DIR/sample$i.dat"
    done

    process_dat_files "$TEST_DIR"
    wait

    for i in $(seq 1 25); do
        result=$(cat "$TEST_DIR/sample$i.dat")
        [ "$result" = "Sample line $i" ] || false
    done
}

