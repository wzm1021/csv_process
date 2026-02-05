"""
通过wafer列匹配，将A文件的列合并到B文件中
使用inner join，只保留匹配的行
"""
import pandas as pd
import argparse
from pathlib import Path


def merge_csv_by_wafer(file_a: str, file_b: str, output_file: str, key_column: str = "wafer"):
    """
    通过指定列匹配，将A文件的列合并到B文件中
    
    Args:
        file_a: A文件路径（提供额外列的文件）
        file_b: B文件路径（主文件）
        output_file: 输出文件路径
        key_column: 匹配的列名，默认wafer
    """
    # 读取文件
    df_a = pd.read_csv(file_a)
    df_b = pd.read_csv(file_b)
    
    print(f"A文件: {len(df_a)} 行, {len(df_a.columns)} 列")
    print(f"B文件: {len(df_b)} 行, {len(df_b.columns)} 列")
    
    # 检查key列是否存在
    if key_column not in df_a.columns:
        raise ValueError(f"A文件中不存在列: {key_column}")
    if key_column not in df_b.columns:
        raise ValueError(f"B文件中不存在列: {key_column}")
    
    # inner join合并
    df_merged = pd.merge(df_b, df_a, on=key_column, how="inner", suffixes=("", "_from_A"))
    
    print(f"合并后: {len(df_merged)} 行, {len(df_merged.columns)} 列")
    
    # 输出
    df_merged.to_csv(output_file, index=False)
    print(f"已保存到: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="通过wafer列匹配合并CSV文件")
    parser.add_argument("file_a", help="A文件路径（提供额外列）")
    parser.add_argument("file_b", help="B文件路径（主文件）")
    parser.add_argument("-o", "--output", default="merged.csv", help="输出文件路径，默认merged.csv")
    parser.add_argument("-k", "--key", default="wafer", help="匹配列名，默认wafer")
    
    args = parser.parse_args()
    merge_csv_by_wafer(args.file_a, args.file_b, args.output, args.key)


if __name__ == "__main__":
    main()
