"""
XLSX文件VALUE差异分析工具
比较多个xlsx文件中相同sheet、相同step的VALUE值差异、持续时长差异和趋势差异
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path


def load_config(config_path: str = "config.json") -> dict:
    """加载配置文件"""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_xlsx_files(folder_path: str, baseline_file: str) -> tuple[str, list[str]]:
    """获取基准文件和待比较文件列表"""
    folder = Path(folder_path)
    all_files = list(folder.glob("*.xlsx"))

    baseline_path = folder / baseline_file
    if not baseline_path.exists():
        raise FileNotFoundError(f"基准文件不存在: {baseline_path}")

    compare_files = [str(f) for f in all_files if f.name != baseline_file]
    return str(baseline_path), compare_files


def get_sheet_names(file_path: str, specified_sheets: list[str]) -> list[str]:
    """获取要分析的sheet名称列表"""
    xl = pd.ExcelFile(file_path)
    if specified_sheets:
        return [s for s in specified_sheets if s in xl.sheet_names]
    return xl.sheet_names


def normalize_step_value(step_val) -> str | None:
    """
    标准化STEP列的值，将数字或文本转为整数字符串
    例如：18.0 -> "18", 18 -> "18", "18" -> "18", "18.0" -> "18"
    """
    if pd.isna(step_val):
        return None
    # 如果是数字类型，转为整数再转字符串
    if isinstance(step_val, (int, float)):
        return str(int(step_val))
    # 如果是文本类型，尝试转为数字再转为整数字符串
    try:
        return str(int(float(str(step_val).strip())))
    except (ValueError, TypeError):
        return str(step_val).strip()


def parse_value(val) -> float | None:
    """
    将VALUE列的值转换为float类型
    支持数字类型和文本类型（如 "123.45"）
    """
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    # 文本类型，尝试转换
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def read_sheet_data(file_path: str, sheet_name: str, step_col: str, value_col: str) -> dict:
    """
    读取sheet数据，返回 {step: {'values': [values], 'count': 行数}} 字典
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        if step_col not in df.columns or value_col not in df.columns:
            return {}

        result = {}
        for _, row in df.iterrows():
            step_key = normalize_step_value(row[step_col])
            value = parse_value(row[value_col])
            if step_key is not None and value is not None:
                if step_key not in result:
                    result[step_key] = {'values': [], 'count': 0}
                result[step_key]['values'].append(value)
                result[step_key]['count'] += 1

        return result
    except Exception as e:
        print(f"读取 {file_path} - {sheet_name} 失败: {e}")
        return {}


def calculate_value_diff(baseline_values: list[float], compare_values: list[float]) -> tuple[float, float, float]:
    """计算VALUE差异百分比，返回 (基准均值, 比较均值, 差异百分比)"""
    baseline_avg = sum(baseline_values) / len(baseline_values)
    compare_avg = sum(compare_values) / len(compare_values)

    if baseline_avg == 0:
        diff_percent = 100.0 if compare_avg != 0 else 0.0
    else:
        diff_percent = abs(compare_avg - baseline_avg) / abs(baseline_avg) * 100

    return baseline_avg, compare_avg, diff_percent


def calculate_correlation(baseline_values: list[float], compare_values: list[float]) -> float:
    """
    计算Pearson相关系数
    如果长度不同，取较短长度进行对比
    返回相关系数 [-1, 1]，1表示完全正相关，-1表示完全负相关，0表示无相关
    """
    min_len = min(len(baseline_values), len(compare_values))
    if min_len < 3:
        return 1.0  # 数据点太少，无法判断

    arr1 = np.array(baseline_values[:min_len])
    arr2 = np.array(compare_values[:min_len])

    # 检查是否全部相同（标准差为0）
    if np.std(arr1) == 0 or np.std(arr2) == 0:
        return 1.0 if np.mean(arr1) == np.mean(arr2) else 0.0

    corr = np.corrcoef(arr1, arr2)[0, 1]
    return corr if not np.isnan(corr) else 1.0


def calculate_segment_trend(values: list[float], num_segments: int) -> list[int]:
    """
    计算分段趋势，返回每段的变化方向列表
    1: 上升, -1: 下降, 0: 平稳
    """
    if len(values) < 2:
        return [0]

    segment_size = max(1, len(values) // num_segments)
    trends = []

    for i in range(num_segments):
        start = i * segment_size
        end = start + segment_size if i < num_segments - 1 else len(values)

        if start >= len(values):
            break

        segment = values[start:end]
        if len(segment) < 2:
            trends.append(0)
            continue

        first_half_avg = np.mean(segment[:len(segment)//2]) if len(segment) >= 2 else segment[0]
        second_half_avg = np.mean(segment[len(segment)//2:]) if len(segment) >= 2 else segment[-1]
        diff = second_half_avg - first_half_avg

        if abs(diff) < 1e-6:
            trends.append(0)
        elif diff > 0:
            trends.append(1)
        else:
            trends.append(-1)

    return trends


def compare_segment_trends(baseline_values: list[float], compare_values: list[float], num_segments: int) -> tuple[float, list[int], list[int]]:
    """
    比较分段趋势，返回 (一致性百分比, 基准趋势, 比较趋势)
    """
    baseline_trends = calculate_segment_trend(baseline_values, num_segments)
    compare_trends = calculate_segment_trend(compare_values, num_segments)

    min_len = min(len(baseline_trends), len(compare_trends))
    if min_len == 0:
        return 100.0, baseline_trends, compare_trends

    matches = sum(1 for i in range(min_len) if baseline_trends[i] == compare_trends[i])
    consistency = (matches / min_len) * 100

    return consistency, baseline_trends, compare_trends


def calculate_diff_signs(values: list[float]) -> list[int]:
    """
    计算差分符号序列
    1: 上升, -1: 下降, 0: 不变
    """
    if len(values) < 2:
        return []

    signs = []
    for i in range(1, len(values)):
        diff = values[i] - values[i-1]
        if abs(diff) < 1e-6:
            signs.append(0)
        elif diff > 0:
            signs.append(1)
        else:
            signs.append(-1)

    return signs


def compare_diff_signs(baseline_values: list[float], compare_values: list[float]) -> tuple[float, int, int]:
    """
    比较差分符号序列，返回 (一致性百分比, 基准序列长度, 比较序列长度)
    """
    baseline_signs = calculate_diff_signs(baseline_values)
    compare_signs = calculate_diff_signs(compare_values)

    min_len = min(len(baseline_signs), len(compare_signs))
    if min_len == 0:
        return 100.0, len(baseline_signs), len(compare_signs)

    matches = sum(1 for i in range(min_len) if baseline_signs[i] == compare_signs[i])
    consistency = (matches / min_len) * 100

    return consistency, len(baseline_signs), len(compare_signs)


def trend_to_str(trends: list[int]) -> str:
    """将趋势列表转为可读字符串"""
    symbols = {1: "↑", -1: "↓", 0: "→"}
    return "".join(symbols.get(t, "?") for t in trends)


def analyze_files(config: dict) -> dict:
    """分析所有文件，返回各类差异结果"""
    baseline_file, compare_files = get_xlsx_files(config["folder_path"], config["baseline_file"])
    sheets = get_sheet_names(baseline_file, config.get("sheets", []))

    # 阈值配置
    value_threshold = config["threshold_percent"]
    duration_threshold = config.get("duration_threshold_rows", 1)
    correlation_threshold = config.get("correlation_threshold", 0.8)
    segment_threshold = config.get("segment_trend_threshold", 80)
    diff_sign_threshold = config.get("diff_sign_threshold", 70)
    num_segments = config.get("num_segments", 4)

    step_col = config["step_column"]
    value_col = config["value_column"]

    results = {
        "value": [],
        "duration": [],
        "correlation": [],
        "segment": [],
        "diff_sign": []
    }
    baseline_name = Path(baseline_file).name

    print(f"基准文件: {baseline_name}")
    print(f"待比较文件数: {len(compare_files)}")
    print(f"分析sheet: {sheets}")
    print(f"VALUE差异阈值: {value_threshold}%")
    print(f"时长差异阈值: {duration_threshold}行")
    print(f"相关系数阈值: {correlation_threshold}")
    print(f"分段趋势一致性阈值: {segment_threshold}%")
    print(f"差分符号一致性阈值: {diff_sign_threshold}%")
    print("-" * 60)

    for sheet in sheets:
        baseline_data = read_sheet_data(baseline_file, sheet, step_col, value_col)
        if not baseline_data:
            continue

        for compare_file in compare_files:
            compare_name = Path(compare_file).name
            compare_data = read_sheet_data(compare_file, sheet, step_col, value_col)
            if not compare_data:
                continue

            for step, baseline_info in baseline_data.items():
                if step not in compare_data:
                    continue

                compare_info = compare_data[step]
                baseline_values = baseline_info['values']
                compare_values = compare_info['values']
                baseline_count = baseline_info['count']
                compare_count = compare_info['count']

                # 1. 检查VALUE差异
                baseline_avg, compare_avg, diff_percent = calculate_value_diff(baseline_values, compare_values)
                if diff_percent > value_threshold:
                    results["value"].append({
                        "比较文件": compare_name, "Sheet名": sheet, "Step": step,
                        "基准均值": round(baseline_avg, 4), "比较均值": round(compare_avg, 4),
                        "差异百分比": round(diff_percent, 2)
                    })

                # 2. 检查时长(行数)差异
                duration_diff = abs(compare_count - baseline_count)
                if duration_diff > duration_threshold:
                    results["duration"].append({
                        "比较文件": compare_name, "Sheet名": sheet, "Step": step,
                        "基准行数": baseline_count, "比较行数": compare_count,
                        "差异行数": duration_diff
                    })

                # 3. 检查相关系数（趋势方向）
                corr = calculate_correlation(baseline_values, compare_values)
                if corr < correlation_threshold:
                    results["correlation"].append({
                        "比较文件": compare_name, "Sheet名": sheet, "Step": step,
                        "相关系数": round(corr, 4)
                    })

                # 4. 检查分段趋势
                seg_consistency, baseline_trends, compare_trends = compare_segment_trends(
                    baseline_values, compare_values, num_segments
                )
                if seg_consistency < segment_threshold:
                    results["segment"].append({
                        "比较文件": compare_name, "Sheet名": sheet, "Step": step,
                        "一致性": round(seg_consistency, 1),
                        "基准趋势": trend_to_str(baseline_trends),
                        "比较趋势": trend_to_str(compare_trends)
                    })

                # 5. 检查差分符号序列
                diff_consistency, _, _ = compare_diff_signs(baseline_values, compare_values)
                if diff_consistency < diff_sign_threshold:
                    results["diff_sign"].append({
                        "比较文件": compare_name, "Sheet名": sheet, "Step": step,
                        "一致性": round(diff_consistency, 1)
                    })

    return results


def save_results(results: dict, output_file: str):
    """保存结果到CSV文件"""
    all_rows = []

    for r in results["value"]:
        all_rows.append({
            "比较文件": r["比较文件"], "Sheet名": r["Sheet名"], "Step": r["Step"],
            "差异类型": "VALUE均值", "详情": f"基准:{r['基准均值']} 比较:{r['比较均值']} 差异:{r['差异百分比']}%"
        })

    for r in results["duration"]:
        all_rows.append({
            "比较文件": r["比较文件"], "Sheet名": r["Sheet名"], "Step": r["Step"],
            "差异类型": "时长", "详情": f"基准:{r['基准行数']}行 比较:{r['比较行数']}行 差异:{r['差异行数']}行"
        })

    for r in results["correlation"]:
        all_rows.append({
            "比较文件": r["比较文件"], "Sheet名": r["Sheet名"], "Step": r["Step"],
            "差异类型": "相关系数", "详情": f"相关系数:{r['相关系数']}"
        })

    for r in results["segment"]:
        all_rows.append({
            "比较文件": r["比较文件"], "Sheet名": r["Sheet名"], "Step": r["Step"],
            "差异类型": "分段趋势", "详情": f"一致性:{r['一致性']}% 基准:{r['基准趋势']} 比较:{r['比较趋势']}"
        })

    for r in results["diff_sign"]:
        all_rows.append({
            "比较文件": r["比较文件"], "Sheet名": r["Sheet名"], "Step": r["Step"],
            "差异类型": "差分符号", "详情": f"一致性:{r['一致性']}%"
        })

    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存到: {output_file}")
    else:
        print("\n未发现超出阈值的差异")


def main():
    config = load_config()
    results = analyze_files(config)

    print(f"\n===== 分析结果汇总 =====")
    print(f"VALUE均值差异: {len(results['value'])}条")
    print(f"时长差异: {len(results['duration'])}条")
    print(f"相关系数异常: {len(results['correlation'])}条")
    print(f"分段趋势差异: {len(results['segment'])}条")
    print(f"差分符号差异: {len(results['diff_sign'])}条")

    total = sum(len(v) for v in results.values())
    print(f"\n共发现 {total} 条差异记录")

    save_results(results, config["output_file"])


if __name__ == "__main__":
    main()
