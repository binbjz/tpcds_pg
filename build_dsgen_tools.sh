#!/bin/bash

# ==========================================================
# 函数: compile_dsgen_tools
# 描述: 编译dsgen tools (用于TPC-DS)
# 执行程序：nohup /usr/bin/time /bin/bash build_dsgen_tools.sh >>./make_dsgen_tools.log 2>&1 &
# ==========================================================
compile_dsgen_tools() {
    local target_dir
    local dsgen_makefile

    target_dir="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/tools"
    dsgen_makefile="Makefile.suite"

    cd $target_dir || {
        echo "Failed to enter target directory."
        exit 1
    }

    [[ -f $dsgen_makefile ]] || {
        echo "$dsgen_makefile does not exist."
        exit 2
    }

    if make clean; then
        if make -f $dsgen_makefile OS=LINUX -j 2; then
            echo "Make succeeded."
        else
            echo "Make failed with $dsgen_makefile."
            exit 3
        fi
    else
        echo "Make clean failed."
        exit 3
    fi
}

compile_dsgen_tools
