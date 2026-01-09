"""
更新 TPC-DS query 模板文件。

与 update_query_templates.sh 兼容的默认行为：
1) 默认目录与索引范围来自同名环境变量：directory/start_index/end_index
2) 默认动作仍为 all（先 add 再 remove）
3) 对缺失文件仅计数与提示，不作为失败
"""
import argparse
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


DEFAULT_DIRECTORY = (
    "/home/parallels/prac_bin/TPC-DS-Tool_v3.2.0/"
    "DSGen-software-code-3.2.0rc1/query_templates"
)
DEFAULT_TEXT = 'define _END = ""'


def parse_non_negative_int(name: str, value: str) -> int:
    if not value.isdigit():
        raise ValueError(f"{name} 必须是非负整数, 当前值: {value}")
    return int(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量更新 query*.tpl 文件内容",
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="all",
        help="执行动作: add/remove/check/all（默认: all）",
    )
    parser.add_argument(
        "--directory",
        default=os.getenv("directory", DEFAULT_DIRECTORY),
        help="query 模板目录路径（默认读取环境变量 directory）",
    )
    parser.add_argument(
        "--start-index",
        default=os.getenv("start_index", "1"),
        help="起始索引（默认读取环境变量 start_index）",
    )
    parser.add_argument(
        "--end-index",
        default=os.getenv("end_index", "99"),
        help="结束索引（默认读取环境变量 end_index）",
    )
    parser.add_argument(
        "--text",
        default=DEFAULT_TEXT,
        help='目标文本（默认: define _END = ""）',
    )
    parser.add_argument(
        "--idempotent-add",
        action="store_true",
        help="add 时避免重复追加（默认关闭，保持 shell 兼容）",
    )
    parser.add_argument(
        "--remove-mode",
        choices=["legacy-last-line", "tail-target", "all-target"],
        default="legacy-last-line",
        help=(
            "remove 模式：legacy-last-line(兼容 shell，直接删末行) / "
            "tail-target(仅删末尾目标行) / all-target(删全部目标行)"
        ),
    )
    parser.add_argument(
        "--remove-all-matches",
        action="store_true",
        help="兼容参数，等价于 --remove-mode all-target",
    )
    return parser.parse_args()


def iter_query_files(directory: Path, start_index: int, end_index: int) -> list[Path]:
    return [directory / f"query{i}.tpl" for i in range(start_index, end_index + 1)]


def safe_write_lines(file_path: Path, lines: list[str]) -> None:
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        delete=False,
        dir=str(file_path.parent),
        prefix=f"{file_path.name}.tmp.",
    ) as tmp:
        tmp.writelines(lines)
        tmp_name = tmp.name
    os.replace(tmp_name, file_path)


def do_add(files: list[Path], text_to_add: str, idempotent_add: bool) -> int:
    updated_count = 0
    missing_count = 0
    unchanged_count = 0

    for file_path in files:
        if not file_path.exists():
            print(f"{file_path} 不存在")
            missing_count += 1
            continue
        if not os.access(file_path, os.W_OK):
            print(f"{file_path} 不可写")
            return 1

        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except OSError as error:
            print(f"读取 {file_path} 失败: {error}")
            return 1

        lines = content.splitlines()
        if idempotent_add and text_to_add in lines:
            print(f"{file_path} 已包含目标文本,跳过添加")
            unchanged_count += 1
            continue

        with file_path.open("a", encoding="utf-8", newline="") as file_handle:
            if content and not content.endswith("\n"):
                file_handle.write("\n")
            file_handle.write(text_to_add + "\n")
        print(f"已经在 {file_path} 的行尾添加了文本")
        updated_count += 1

    print(
        "add 操作完成: "
        f"已处理 {updated_count} 个文件, 缺失 {missing_count} 个文件, 跳过 {unchanged_count} 个文件"
    )
    return 0


def do_remove(files: list[Path], text_to_remove: str, remove_mode: str) -> int:
    updated_count = 0
    missing_count = 0
    unchanged_count = 0

    for file_path in files:
        if not file_path.exists():
            print(f"{file_path} 不存在")
            missing_count += 1
            continue
        if not os.access(file_path, os.W_OK):
            print(f"{file_path} 不可写")
            return 1

        try:
            lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines(
                keepends=True
            )
        except OSError as error:
            print(f"读取 {file_path} 失败: {error}")
            return 1

        original_lines = list(lines)
        if remove_mode == "legacy-last-line":
            if lines:
                lines = lines[:-1]
        elif remove_mode == "all-target":
            lines = [line for line in lines if line.rstrip("\r\n") != text_to_remove]
        else:
            while lines and lines[-1].rstrip("\r\n") == text_to_remove:
                lines.pop()

        if remove_mode != "legacy-last-line" and lines == original_lines:
            print(f"{file_path} 未发现可删除的目标文本,跳过")
            unchanged_count += 1
            continue

        try:
            safe_write_lines(file_path, lines)
        except OSError as error:
            print(f"处理 {file_path} 失败: {error}")
            return 1
        if remove_mode == "legacy-last-line":
            print(f"已经从 {file_path} 的末尾删除了文本")
        else:
            print(f"已经从 {file_path} 删除了目标文本")
        updated_count += 1

    print(
        "remove 操作完成: "
        f"已处理 {updated_count} 个文件, 缺失 {missing_count} 个文件, 跳过 {unchanged_count} 个文件"
    )
    return 0


def do_check(files: list[Path]) -> int:
    exists_count = 0
    missing_count = 0

    for file_path in files:
        if file_path.exists():
            print(f"{file_path} 存在")
            exists_count += 1
        else:
            print(f"{file_path} 不存在")
            missing_count += 1

    print(f"check 操作完成: 存在 {exists_count} 个文件, 缺失 {missing_count} 个文件")
    return 0


def main() -> int:
    args = parse_args()
    valid_actions = {"add", "remove", "check", "all"}

    try:
        start_index = parse_non_negative_int("start_index", str(args.start_index))
    except ValueError as error:
        print(error)
        return 1

    try:
        end_index = parse_non_negative_int("end_index", str(args.end_index))
    except ValueError as error:
        print(error)
        return 1

    if start_index > end_index:
        print("start_index 不能大于 end_index")
        return 1

    if args.action not in valid_actions:
        print(f"未知操作: {args.action}")
        print("Usage: python update_query_templates.py [add|remove|check|all]")
        return 1

    directory = Path(args.directory)
    if not directory.is_dir():
        print(f"目录不存在: {directory}")
        return 1

    files = iter_query_files(directory, start_index, end_index)
    remove_mode = "all-target" if args.remove_all_matches else args.remove_mode

    if args.action == "add":
        return do_add(files, args.text, args.idempotent_add)
    if args.action == "remove":
        return do_remove(files, args.text, remove_mode)
    if args.action == "check":
        return do_check(files)
    if args.action == "all":
        rc = do_add(files, args.text, args.idempotent_add)
        if rc != 0:
            return rc
        return do_remove(files, args.text, remove_mode)

    print(f"未知操作: {args.action}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
