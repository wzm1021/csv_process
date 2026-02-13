"""
CSV/XLSX 缺失值分析工具
通过YAML配置排除指定列后，统计各列缺失值数量，输出TOP N列名
"""

import sys
import io
import yaml
import pandas as pd
from pathlib import Path
from typing import List, Optional

# Windows UTF-8 兼容
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass


def load_data(file_path: str, encoding: str = 'utf-8',
              sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    加载CSV或XLSX文件

    Args:
        file_path: 文件路径
        encoding: 文件编码
        sheet_name: xlsx的sheet名，None取第一个sheet

    Returns:
        加载后的DataFrame
    """
    path = Path(file_path)
    if path.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path,
                           sheet_name=sheet_name if sheet_name else 0,
                           engine='openpyxl')
    else:
        df = pd.read_csv(file_path, encoding=encoding)

    print(f"加载文件: {file_path}  ({len(df)}行, {len(df.columns)}列)")
    return df


def analyze_missing(df: pd.DataFrame,
                    exclude_columns: List[str] = None,
                    top_n: int = 20) -> pd.DataFrame:
    """
    排除指定列后，统计各列缺失值并返回TOP N

    Args:
        df: 原始数据
        exclude_columns: 要排除的列名列表
        top_n: 返回缺失值最多的前N列

    Returns:
        包含列名、缺失数、缺失比例的DataFrame
    """
    if exclude_columns:
        # 只排除实际存在的列，不存在的给出提示
        valid_exclude = [c for c in exclude_columns if c in df.columns]
        invalid = [c for c in exclude_columns if c not in df.columns]
        if invalid:
            print(f"[提示] 以下排除列不存在，已忽略: {invalid}")

        df_filtered = df.drop(columns=valid_exclude)
        print(f"排除 {len(valid_exclude)} 列后剩余 {len(df_filtered.columns)} 列")
    else:
        df_filtered = df

    # 统计每列缺失值
    missing_count = df_filtered.isnull().sum()
    missing_ratio = df_filtered.isnull().mean()

    result = pd.DataFrame({
        '列名': missing_count.index,
        '缺失数': missing_count.values,
        '缺失比例': missing_ratio.values
    })

    # 按缺失数降序，取TOP N
    result = result.sort_values('缺失数', ascending=False).head(top_n).reset_index(drop=True)
    result.index = result.index + 1  # 排名从1开始
    result.index.name = '排名'
    result['缺失比例'] = result['缺失比例'].apply(lambda x: f"{x:.2%}")

    return result


def run_from_config(config_path: str = "config_missing.yaml"):
    """
    从YAML配置文件执行缺失值分析

    配置文件格式:
        input_file: "data.csv"
        encoding: "utf-8"
        input_sheet: null
        exclude_columns:
          - "col_a"
          - "col_b"
        top_n: 20

    Args:
        config_path: 配置文件路径
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    input_file = config.get('input_file')
    encoding = config.get('encoding', 'utf-8')
    sheet_name = config.get('input_sheet')
    exclude_columns = config.get('exclude_columns', [])
    top_n = config.get('top_n', 20)

    if not input_file:
        raise ValueError("配置中缺少 input_file")

    df = load_data(input_file, encoding=encoding, sheet_name=sheet_name)
    result = analyze_missing(df, exclude_columns=exclude_columns, top_n=top_n)

    print(f"\n缺失值 TOP {top_n}:")
    print(result.to_string())
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='CSV/XLSX缺失值分析')
    parser.add_argument('-c', '--config', default='config_missing.yaml',
                        help='YAML配置文件路径')
    args = parser.parse_args()

    run_from_config(config_path=args.config)
