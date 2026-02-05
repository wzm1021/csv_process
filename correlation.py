"""
计算CSV文件中数值列两两之间的Pearson相关性
支持配置排除指定列
"""
import pandas as pd
import numpy as np
import argparse
import json
from pathlib import Path


def load_config(config_path: str) -> dict:
    """加载配置文件"""
    if Path(config_path).exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def calculate_correlation(input_file: str, output_file: str, exclude_columns: list = None):
    """
    计算CSV文件中数值列的Pearson相关性矩阵
    
    Args:
        input_file: 输入CSV文件路径
        output_file: 输出xlsx文件路径
        exclude_columns: 要排除的列名列表
    """
    # 读取数据
    df = pd.read_csv(input_file)
    print(f"读取文件: {len(df)} 行, {len(df.columns)} 列")
    
    # 排除指定列
    if exclude_columns:
        cols_to_drop = [col for col in exclude_columns if col in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            print(f"排除列: {cols_to_drop}")
    
    # 只保留数值列
    numeric_df = df.select_dtypes(include=[np.number])
    print(f"数值列数量: {len(numeric_df.columns)}")
    
    if len(numeric_df.columns) < 2:
        print("错误: 数值列少于2列，无法计算相关性")
        return
    
    # 计算Pearson相关性矩阵
    corr_matrix = numeric_df.corr(method="pearson")
    
    # 输出到xlsx
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # 相关性矩阵
        corr_matrix.to_excel(writer, sheet_name="相关性矩阵")
        
        # 高相关性对（|r| > 0.7，排除自身）
        high_corr = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                col1 = corr_matrix.columns[i]
                col2 = corr_matrix.columns[j]
                r = corr_matrix.iloc[i, j]
                if abs(r) > 0.7:
                    high_corr.append({
                        "列1": col1,
                        "列2": col2,
                        "相关系数": round(r, 4)
                    })
        
        if high_corr:
            high_corr_df = pd.DataFrame(high_corr)
            high_corr_df = high_corr_df.sort_values("相关系数", key=abs, ascending=False)
            high_corr_df.to_excel(writer, sheet_name="高相关性对", index=False)
            print(f"高相关性对(|r|>0.7): {len(high_corr)} 对")
    
    print(f"已保存到: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="计算CSV文件列的Pearson相关性")
    parser.add_argument("input_file", help="输入CSV文件路径")
    parser.add_argument("-o", "--output", default="correlation.xlsx", help="输出文件路径，默认correlation.xlsx")
    parser.add_argument("-c", "--config", default="config_correlation.json", help="配置文件路径")
    parser.add_argument("-e", "--exclude", nargs="*", help="要排除的列名（命令行指定）")
    
    args = parser.parse_args()
    
    # 优先使用命令行参数，否则从配置文件读取
    exclude_columns = args.exclude
    if not exclude_columns:
        config = load_config(args.config)
        exclude_columns = config.get("exclude_columns", [])
    
    calculate_correlation(args.input_file, args.output, exclude_columns)


if __name__ == "__main__":
    main()
