"""
A CSV排除指定列后，与B CSV的列取交集，保留交集列并保存为新CSV
"""

import pandas as pd
import json
import argparse
from pathlib import Path


def intersect_columns(a_csv: str, b_csv: str, exclude_columns: list[str], output: str) -> tuple[list[str], list[str], list[str]]:
    """
    A CSV排除指定列后，与B CSV的列取交集，保留交集列

    参数:
        a_csv: A CSV文件路径
        b_csv: B CSV文件路径
        exclude_columns: 需要从A中排除的列名列表
        output: 输出文件路径

    返回:
        (A排除后的列, 交集列, 被删除的列)
    """
    df_a = pd.read_csv(a_csv)
    df_b = pd.read_csv(b_csv)

    a_remaining = [col for col in df_a.columns if col not in exclude_columns]
    b_cols = set(df_b.columns)
    intersect = [col for col in a_remaining if col in b_cols]
    removed = [col for col in a_remaining if col not in b_cols]

    df_result = df_a[intersect]
    df_result.to_csv(output, index=False)

    return a_remaining, intersect, removed


def main():
    parser = argparse.ArgumentParser(description='A CSV排除列后与B CSV取列交集')
    parser.add_argument('-c', '--config', default='config_intersect.json', help='配置文件路径')
    args = parser.parse_args()

    config_path = args.config
    if not Path(config_path).exists():
        print(f"配置文件不存在: {config_path}")
        return

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    a_csv = config['a_csv']
    b_csv = config['b_csv']
    exclude_columns = config.get('exclude_columns', [])
    output = config.get('output', 'a_intersected.csv')

    if not Path(a_csv).exists():
        print(f"A文件不存在: {a_csv}")
        return
    if not Path(b_csv).exists():
        print(f"B文件不存在: {b_csv}")
        return

    print(f"A文件: {a_csv}")
    print(f"B文件: {b_csv}")
    print(f"排除列: {exclude_columns}")

    a_remaining, intersect, removed = intersect_columns(a_csv, b_csv, exclude_columns, output)

    print(f"\nA排除后剩余列数: {len(a_remaining)}")
    print(f"交集列数: {len(intersect)}")
    print(f"被删除列数: {len(removed)}")

    if removed:
        print("\n被删除的列(不在B中):")
        for col in removed:
            print(f"  - {col}")

    print(f"\n结果已保存到: {output}")


if __name__ == '__main__':
    main()
