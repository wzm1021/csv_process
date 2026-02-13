"""
晶圆加权平均系数拟合工具
通过最小二乘回归，根据前N片晶圆值和目标值，自动求解每片的最优权重系数
支持3片、6片或任意片数，统一逻辑
"""

import sys
import io
import yaml
import pandas as pd
import numpy as np
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
    """加载CSV或XLSX文件"""
    path = Path(file_path)
    if path.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path,
                           sheet_name=sheet_name if sheet_name else 0,
                           engine='openpyxl')
    else:
        df = pd.read_csv(file_path, encoding=encoding)

    print(f"加载文件: {file_path}  ({len(df)}行, {len(df.columns)}列)")
    return df


def fit_wafer_weights(df: pd.DataFrame,
                      wafer_columns: List[str],
                      target_column: str,
                      fit_intercept: bool = True,
                      normalize_weights: bool = False) -> dict:
    """
    最小二乘拟合每片晶圆的加权系数

    Args:
        df: 数据
        wafer_columns: 各片晶圆对应的列名列表
        target_column: 目标值列名
        fit_intercept: 是否拟合截距项
        normalize_weights: 是否将系数归一化（使系数和为1）

    Returns:
        包含系数、截距、评估指标的字典
    """
    # 校验列名
    missing = [c for c in wafer_columns + [target_column] if c not in df.columns]
    if missing:
        raise ValueError(f"以下列不存在: {missing}")

    # 提取特征和目标，去除含缺失值的行
    cols = wafer_columns + [target_column]
    data = df[cols].dropna()
    n_dropped = len(df) - len(data)
    if n_dropped > 0:
        print(f"去除含缺失值的行: {n_dropped}行, 剩余 {len(data)}行")

    if len(data) < len(wafer_columns) + 1:
        raise ValueError(f"有效数据行数({len(data)})不足，至少需要 {len(wafer_columns) + 1} 行")

    X = data[wafer_columns].values
    y = data[target_column].values

    # 最小二乘拟合
    if fit_intercept:
        X_design = np.column_stack([X, np.ones(len(X))])
    else:
        X_design = X

    coeffs, residuals, rank, sv = np.linalg.lstsq(X_design, y, rcond=None)

    if fit_intercept:
        weights = coeffs[:-1]
        intercept = coeffs[-1]
    else:
        weights = coeffs
        intercept = 0.0

    # 归一化系数（可选）
    if normalize_weights:
        w_sum = np.sum(weights)
        if abs(w_sum) > 1e-10:
            weights = weights / w_sum
            intercept = intercept  # 截距不变

    # 预测与评估
    y_pred = X @ weights + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    mae = np.mean(np.abs(y - y_pred))
    rmse = np.sqrt(np.mean((y - y_pred) ** 2))

    result = {
        'wafer_columns': wafer_columns,
        'weights': {col: float(w) for col, w in zip(wafer_columns, weights)},
        'intercept': float(intercept),
        'fit_intercept': fit_intercept,
        'normalized': normalize_weights,
        'metrics': {
            'R2': round(r2, 6),
            'MAE': round(mae, 6),
            'RMSE': round(rmse, 6),
            'samples': len(data)
        }
    }

    return result


def print_result(result: dict):
    """格式化打印拟合结果"""
    print("\n" + "=" * 50)
    print("拟合结果")
    print("=" * 50)

    weights = result['weights']
    for i, (col, w) in enumerate(weights.items(), 1):
        print(f"  片{i} ({col}): {w:+.6f}")

    if result['fit_intercept']:
        print(f"  截距: {result['intercept']:+.6f}")

    if result['normalized']:
        print(f"  (系数已归一化，和为 {sum(weights.values()):.4f})")

    print(f"\n评估指标:")
    metrics = result['metrics']
    print(f"  R²:     {metrics['R2']}")
    print(f"  MAE:    {metrics['MAE']}")
    print(f"  RMSE:   {metrics['RMSE']}")
    print(f"  样本数: {metrics['samples']}")

    # 打印加权公式
    terms = []
    for col, w in weights.items():
        terms.append(f"{w:+.4f}*{col}")
    formula = " ".join(terms)
    if result['fit_intercept']:
        formula += f" {result['intercept']:+.4f}"
    print(f"\n加权公式: target = {formula}")
    print("=" * 50)


def run_from_config(config_path: str = "config_wafer_weight.yaml") -> dict:
    """
    从YAML配置文件执行加权系数拟合

    配置文件格式:
        input_file: "data.xlsx"
        encoding: "utf-8"
        input_sheet: null
        wafer_columns:
          - "wafer_1"
          - "wafer_2"
          - "wafer_3"
        target_column: "target"
        fit_intercept: true
        normalize_weights: false

    Args:
        config_path: 配置文件路径

    Returns:
        拟合结果字典
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    input_file = config.get('input_file')
    encoding = config.get('encoding', 'utf-8')
    sheet_name = config.get('input_sheet')
    wafer_columns = config.get('wafer_columns', [])
    target_column = config.get('target_column')
    fit_intercept = config.get('fit_intercept', True)
    normalize_weights = config.get('normalize_weights', False)

    if not input_file:
        raise ValueError("配置中缺少 input_file")
    if not wafer_columns:
        raise ValueError("配置中缺少 wafer_columns")
    if not target_column:
        raise ValueError("配置中缺少 target_column")

    print(f"片数: {len(wafer_columns)}, 目标列: {target_column}")
    print(f"截距: {'是' if fit_intercept else '否'}, 归一化: {'是' if normalize_weights else '否'}")

    df = load_data(input_file, encoding=encoding, sheet_name=sheet_name)
    result = fit_wafer_weights(df, wafer_columns, target_column,
                               fit_intercept=fit_intercept,
                               normalize_weights=normalize_weights)
    print_result(result)
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='晶圆加权系数拟合')
    parser.add_argument('-c', '--config', default='config_wafer_weight.yaml',
                        help='YAML配置文件路径')
    args = parser.parse_args()

    run_from_config(config_path=args.config)
