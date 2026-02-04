"""
XLSX文件特征提取工具
从多个xlsx文件中提取参数特征，生成汇总表
"""

import json
import re
import numpy as np
import pandas as pd
from pathlib import Path
from scipy.optimize import curve_fit


def load_config(config_path: str = "config_feature.json") -> dict:
    """加载配置文件"""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_step_config(steps_config: list[str]) -> list[str]:
    """
    解析STEP配置，支持单个STEP和范围
    例如：["STEP18", "STEP20-23"] -> ["STEP18", "STEP20", "STEP21", "STEP22", "STEP23"]
    """
    result = []
    for item in steps_config:
        if "-" in item:
            match = re.match(r"(\D*)(\d+)-(\d+)", item)
            if match:
                prefix = match.group(1)
                start = int(match.group(2))
                end = int(match.group(3))
                for i in range(start, end + 1):
                    result.append(f"{prefix}{i}")
        else:
            result.append(item)
    return result


def get_step_groups(steps_config: list[str]) -> dict[str, list[str]]:
    """
    获取STEP分组，用于合并分析
    返回 {组名: [STEP列表]}
    例如：["STEP18", "STEP20-23"] -> {"STEP18": ["STEP18"], "STEP20-23": ["STEP20", "STEP21", "STEP22", "STEP23"]}
    """
    groups = {}
    for item in steps_config:
        if "-" in item:
            match = re.match(r"(\D*)(\d+)-(\d+)", item)
            if match:
                prefix = match.group(1)
                start = int(match.group(2))
                end = int(match.group(3))
                steps = [f"{prefix}{i}" for i in range(start, end + 1)]
                groups[item] = steps
        else:
            groups[item] = [item]
    return groups


def read_meta_info(file_path: str, meta_sheet: str, meta_columns: list[str]) -> dict:
    """读取metaInfo sheet中的元数据"""
    try:
        df = pd.read_excel(file_path, sheet_name=meta_sheet)
        result = {}
        for col in meta_columns:
            if col in df.columns:
                value = df[col].iloc[0] if len(df) > 0 else ""
                result[col] = value if pd.notna(value) else ""
            else:
                result[col] = ""
        return result
    except Exception as e:
        print(f"读取元数据失败 {file_path}: {e}")
        return {col: "" for col in meta_columns}


def read_sheet_data(file_path: str, sheet_name: str, step_col: str, value_col: str, target_steps: list[str]) -> list[float]:
    """读取指定sheet中目标STEP的VALUE数据"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        if step_col not in df.columns or value_col not in df.columns:
            return []

        values = []
        for _, row in df.iterrows():
            step = str(row[step_col]).strip() if pd.notna(row[step_col]) else ""
            value = row[value_col]
            if step in target_steps and pd.notna(value):
                values.append(float(value))

        return values
    except Exception as e:
        return []


def is_stable(values: list[float], threshold: float) -> bool:
    """判断数据是否稳定（值相同或浮动在阈值内）"""
    if len(values) < 2:
        return True
    return (max(values) - min(values)) <= threshold


def exp_func(x, a, b, c):
    """指数函数 y = a(1 - e^(-bx)) + c"""
    return a * (1 - np.exp(-b * x)) + c


def fit_temperature_curve(values: list[float]) -> tuple[float | None, float | None]:
    """
    拟合温度曲线 y = a(1 - e^(-bx)) + c
    返回 (b值, 均值) - 如果拟合成功返回b，否则返回None
    """
    if len(values) < 4:
        return None, np.mean(values) if values else None

    x = np.arange(len(values))
    y = np.array(values)

    try:
        # 初始猜测
        a_init = max(y) - min(y)
        c_init = min(y)
        b_init = 0.1

        popt, _ = curve_fit(
            exp_func, x, y,
            p0=[a_init, b_init, c_init],
            bounds=([0, 0.001, -np.inf], [np.inf, 10, np.inf]),
            maxfev=5000
        )
        return popt[1], np.mean(values)  # 返回b值和均值
    except Exception:
        return None, np.mean(values)


def count_fluctuations(values: list[float]) -> int:
    """统计0/1数据的波动次数（状态切换次数）"""
    if len(values) < 2:
        return 0

    count = 0
    for i in range(1, len(values)):
        if values[i] != values[i-1]:
            count += 1
    return count


def find_stable_point(values: list[float], threshold: float) -> int:
    """
    找到数据开始稳定的位置
    返回稳定开始的索引，如果没找到返回-1
    """
    if len(values) < 5:
        return -1

    window_size = 5
    for i in range(len(values) - window_size + 1):
        window = values[i:i + window_size]
        if (max(window) - min(window)) <= threshold:
            return i

    return -1


def analyze_trend(values: list[float], threshold: float) -> dict:
    """
    分析数据趋势
    返回特征字典
    """
    if len(values) < 2:
        return {
            "slope": None,
            "fluctuations_before_stable": 0,
            "stable_max": values[0] if values else None,
            "overall_max": values[0] if values else None,
            "overall_min": values[0] if values else None,
            "trend_type": "insufficient_data"
        }

    overall_max = max(values)
    overall_min = min(values)

    # 找稳定点
    stable_idx = find_stable_point(values, threshold)

    if stable_idx == -1:
        # 没有稳定段，全部是波动
        fluctuations = count_fluctuations(values)
        return {
            "slope": None,
            "fluctuations_before_stable": fluctuations,
            "stable_max": None,
            "overall_max": overall_max,
            "overall_min": overall_min,
            "trend_type": "no_stable"
        }

    # 有稳定段
    before_stable = values[:stable_idx] if stable_idx > 0 else []
    stable_part = values[stable_idx:]
    stable_max = max(stable_part) if stable_part else None

    # 判断稳定前是上升、下降还是波动
    if len(before_stable) >= 2:
        first_val = before_stable[0]
        last_val = before_stable[-1]
        diff = last_val - first_val

        # 计算单调性
        increases = sum(1 for i in range(1, len(before_stable)) if before_stable[i] > before_stable[i-1])
        decreases = sum(1 for i in range(1, len(before_stable)) if before_stable[i] < before_stable[i-1])
        total_changes = len(before_stable) - 1

        # 判断是否单调
        monotonic_ratio = max(increases, decreases) / total_changes if total_changes > 0 else 0

        if monotonic_ratio >= 0.7:
            # 单调上升或下降
            x = np.arange(len(before_stable))
            slope = np.polyfit(x, before_stable, 1)[0]
            trend_type = "rising" if slope > 0 else "falling"
            return {
                "slope": slope,
                "fluctuations_before_stable": 0,
                "stable_max": stable_max,
                "overall_max": overall_max,
                "overall_min": overall_min,
                "trend_type": trend_type
            }
        else:
            # 波动
            fluctuations = count_fluctuations(before_stable)
            return {
                "slope": None,
                "fluctuations_before_stable": fluctuations,
                "stable_max": stable_max,
                "overall_max": overall_max,
                "overall_min": overall_min,
                "trend_type": "fluctuating"
            }
    else:
        # 几乎立即稳定
        return {
            "slope": None,
            "fluctuations_before_stable": 0,
            "stable_max": stable_max,
            "overall_max": overall_max,
            "overall_min": overall_min,
            "trend_type": "immediate_stable"
        }


def process_file(file_path: str, config: dict) -> dict:
    """处理单个文件，返回特征字典"""
    result = {}

    # 读取元数据
    meta = read_meta_info(file_path, config["meta_sheet"], config["meta_columns"])
    result.update(meta)

    step_groups = get_step_groups(config["steps_to_analyze"])
    threshold = config["stable_threshold"]
    step_col = config["step_column"]
    value_col = config["value_column"]

    # 处理温度类sheet
    for sheet in config.get("temperature_sheets", []):
        for group_name, steps in step_groups.items():
            col_prefix = f"{sheet}_{group_name}"
            values = read_sheet_data(file_path, sheet, step_col, value_col, steps)

            if not values:
                result[f"{col_prefix}_rate"] = None
                result[f"{col_prefix}_mean"] = None
                continue

            if is_stable(values, threshold):
                result[f"{col_prefix}_rate"] = None
                result[f"{col_prefix}_mean"] = np.mean(values)
            else:
                b_val, mean_val = fit_temperature_curve(values)
                result[f"{col_prefix}_rate"] = round(b_val, 6) if b_val else None
                result[f"{col_prefix}_mean"] = round(mean_val, 4) if mean_val else None

    # 处理0/1类sheet
    for sheet in config.get("binary_sheets", []):
        for group_name, steps in step_groups.items():
            col_prefix = f"{sheet}_{group_name}"
            values = read_sheet_data(file_path, sheet, step_col, value_col, steps)

            fluctuations = count_fluctuations(values) if values else 0
            result[f"{col_prefix}_fluctuations"] = fluctuations

    # 处理其他类sheet
    for sheet in config.get("other_sheets", []):
        for group_name, steps in step_groups.items():
            col_prefix = f"{sheet}_{group_name}"
            values = read_sheet_data(file_path, sheet, step_col, value_col, steps)

            if not values:
                result[f"{col_prefix}_slope"] = None
                result[f"{col_prefix}_fluctuations"] = None
                result[f"{col_prefix}_stable_max"] = None
                result[f"{col_prefix}_max"] = None
                result[f"{col_prefix}_min"] = None
                continue

            trend = analyze_trend(values, threshold)

            if trend["trend_type"] in ["rising", "falling"]:
                result[f"{col_prefix}_slope"] = round(trend["slope"], 6) if trend["slope"] else None
                result[f"{col_prefix}_fluctuations"] = None
            else:
                result[f"{col_prefix}_slope"] = None
                result[f"{col_prefix}_fluctuations"] = trend["fluctuations_before_stable"]

            result[f"{col_prefix}_stable_max"] = round(trend["stable_max"], 4) if trend["stable_max"] else None
            result[f"{col_prefix}_max"] = round(trend["overall_max"], 4) if trend["overall_max"] else None
            result[f"{col_prefix}_min"] = round(trend["overall_min"], 4) if trend["overall_min"] else None

    return result


def main():
    config = load_config()
    folder = Path(config["folder_path"])
    xlsx_files = list(folder.glob("*.xlsx"))

    print(f"找到 {len(xlsx_files)} 个xlsx文件")
    print(f"STEP配置: {config['steps_to_analyze']}")
    print(f"温度类sheet: {config.get('temperature_sheets', [])}")
    print(f"0/1类sheet: {config.get('binary_sheets', [])}")
    print(f"其他类sheet: {config.get('other_sheets', [])}")
    print("-" * 60)

    all_results = []
    for file_path in xlsx_files:
        print(f"处理: {file_path.name}")
        result = process_file(str(file_path), config)
        result["源文件"] = file_path.name
        all_results.append(result)

    if all_results:
        df = pd.DataFrame(all_results)
        # 调整列顺序，源文件和元数据列放前面
        meta_cols = ["源文件"] + config["meta_columns"]
        other_cols = [c for c in df.columns if c not in meta_cols]
        df = df[meta_cols + other_cols]

        df.to_excel(config["output_file"], index=False)
        print(f"\n结果已保存到: {config['output_file']}")
        print(f"共处理 {len(all_results)} 个文件，生成 {len(df.columns)} 列特征")
    else:
        print("\n未找到可处理的文件")


if __name__ == "__main__":
    main()
