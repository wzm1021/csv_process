"""
清理xlsx文件中的无效列
删除空列和值完全相同的列
"""

import pandas as pd
import argparse
from pathlib import Path


def clean_columns(input_file: str, output_file: str = None) -> tuple[int, int, list[str]]:
    """
    清理xlsx文件中的空列和值相同的列
    
    参数:
        input_file: 输入文件路径
        output_file: 输出文件路径（默认覆盖原文件）
    
    返回:
        (原列数, 清理后列数, 删除的列名列表)
    """
    if output_file is None:
        output_file = input_file

    df = pd.read_excel(input_file)
    original_cols = len(df.columns)
    removed_cols = []

    cols_to_keep = []
    for col in df.columns:
        # 检查是否为空列
        if df[col].isna().all():
            removed_cols.append(f"{col} (空列)")
            continue

        # 检查是否所有值相同（忽略空值）
        non_null = df[col].dropna()
        if len(non_null) > 0 and non_null.nunique() == 1:
            removed_cols.append(f"{col} (值相同: {non_null.iloc[0]})")
            continue

        cols_to_keep.append(col)

    df_clean = df[cols_to_keep]
    df_clean.to_excel(output_file, index=False)

    return original_cols, len(cols_to_keep), removed_cols


def main():
    parser = argparse.ArgumentParser(description='清理xlsx文件中的空列和值相同的列')
    parser.add_argument('input', help='输入xlsx文件路径')
    parser.add_argument('-o', '--output', help='输出文件路径（默认覆盖原文件）')
    args = parser.parse_args()

    input_file = args.input
    output_file = args.output

    if not Path(input_file).exists():
        print(f"文件不存在: {input_file}")
        return

    print(f"处理文件: {input_file}")
    original, remaining, removed = clean_columns(input_file, output_file)

    print(f"\n原列数: {original}")
    print(f"保留列数: {remaining}")
    print(f"删除列数: {len(removed)}")

    if removed:
        print("\n删除的列:")
        for col in removed:
            print(f"  - {col}")

    out_path = output_file or input_file
    print(f"\n结果已保存到: {out_path}")


if __name__ == '__main__':
    main()
