#!/bin/bash

directory="/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/DSGen-software-code-3.2.0rc1/query_templates"

# ==========================================================
# 函数: update_query_templates_add
# 描述: 在文件末尾添加一行指定的文本
# 注意：<db_type>.tpl中已经加入了 define _BEGIN 和 define _END,
# 所以这里不需要在每个query tpl文件末尾 添加 define _END = ""了。
# ==========================================================
update_query_templates_add() {
    local text_to_add="define _END = \"\""
    for i in $(seq 1 99); do
        file="${directory}/query${i}.tpl"
        if [ -f "$file" ]; then
            echo "$text_to_add" >>"$file"
            echo "已经在 $file 的行尾添加了文本"
        else
            echo "$file 不存在"
        fi
    done
}

# ==========================================================
# 函数: update_query_templates_remove
# 描述: 从文件末尾删除一行(添加的指定文本)
# ==========================================================
update_query_templates_remove() {
    for i in $(seq 1 99); do
        file="${directory}/query${i}.tpl"
        if [ -f "$file" ]; then
            sed -i '$ d' "$file"
            echo "已经从 $file 的末尾删除了文本"
        else
            echo "$file 不存在"
        fi
    done
}

# 执行函数
update_query_templates_add
update_query_templates_remove
