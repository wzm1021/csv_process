"""
分析result.csv结果文件
按Recipe后缀、Sheet名、Step分组统计差异数量
"""

import pandas as pd
import argparse
from pathlib import Path


def analyze_result(input_file: str, output_file: str, recipe_suffix: str = "03P"):
    """
    分析结果文件，按Sheet和Step统计差异
    
    参数:
        input_file: 输入的result.csv文件
        output_file: 输出的汇总文件
        recipe_suffix: Recipe后缀筛选条件（如 "03P"）
    """
    df = pd.read_csv(input_file)

    # 筛选指定Recipe后缀的数据
    recipe_col = 'recipe'
    if recipe_col not in df.columns:
        print(f"错误: 文件中没有 {recipe_col} 列")
        return

    df['recipe'] = df['recipe'].astype(str)
    filtered = df[df['recipe'].str.endswith(recipe_suffix)]

    total_count = len(filtered)
    print(f"Recipe以 '{recipe_suffix}' 结尾的总差异数: {total_count}")
    print("-" * 60)

    if total_count == 0:
        print("没有匹配的数据")
        return

    # 按Sheet名、Step、差异类型分组统计
    summary = filtered.groupby(['Sheet名', 'Step', '差异类型']).size().reset_index(name='差异数量')

    # 按Sheet名、Step汇总，每种差异类型单独统计
    diff_types = ['VALUE均值', '时长', '相关系数', '分段趋势', '差分符号']

    sheet_step_pivot = filtered.groupby(['Sheet名', 'Step', '差异类型']).size().unstack(fill_value=0)
    # 确保所有差异类型列都存在
    for dt in diff_types:
        if dt not in sheet_step_pivot.columns:
            sheet_step_pivot[dt] = 0
    sheet_step_pivot = sheet_step_pivot[diff_types]  # 按顺序排列
    sheet_step_pivot['差异总数'] = sheet_step_pivot.sum(axis=1)
    sheet_step_pivot = sheet_step_pivot.reset_index()

    # 添加涉及文件数
    file_count = filtered.groupby(['Sheet名', 'Step'])['比较文件'].nunique().reset_index(name='涉及文件数')
    sheet_step_summary = sheet_step_pivot.merge(file_count, on=['Sheet名', 'Step'])

    # 按差异总数降序排序
    sheet_step_summary = sheet_step_summary.sort_values('差异总数', ascending=False)

    # 输出到控制台
    print(f"\n{'='*80}")
    print(f"按 Sheet名 + Step 汇总 (Recipe后缀: {recipe_suffix})")
    print(f"{'='*80}")
    print(f"{'Sheet名':<12} {'Step':<6} {'VALUE均值':<8} {'时长':<6} {'相关系数':<8} {'分段趋势':<8} {'差分符号':<8} {'文件数':<6}")
    print("-" * 80)

    for _, row in sheet_step_summary.head(20).iterrows():
        print(f"{row['Sheet名']:<12} {row['Step']:<6} {row['VALUE均值']:<8} {row['时长']:<6} "
              f"{row['相关系数']:<8} {row['分段趋势']:<8} {row['差分符号']:<8} {row['涉及文件数']:<6}")

    # 按Sheet名汇总
    sheet_summary = filtered.groupby('Sheet名').agg(
        差异总数=('差异类型', 'count'),
        涉及Step数=('Step', 'nunique'),
        涉及文件数=('比较文件', 'nunique')
    ).reset_index().sort_values('差异总数', ascending=False)

    print(f"\n{'='*60}")
    print(f"按 Sheet名 汇总")
    print(f"{'='*60}")

    for _, row in sheet_summary.iterrows():
        print(f"  {row['Sheet名']}: {row['差异总数']}条差异, "
              f"{row['涉及Step数']}个Step, {row['涉及文件数']}个文件")

    # 保存到Excel（多个sheet）
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 总览
        overview = pd.DataFrame({
            '指标': ['Recipe后缀', '总差异数', '涉及Sheet数', '涉及Step数', '涉及文件数'],
            '值': [
                recipe_suffix,
                total_count,
                filtered['Sheet名'].nunique(),
                filtered['Step'].nunique(),
                filtered['比较文件'].nunique()
            ]
        })
        overview.to_excel(writer, sheet_name='总览', index=False)

        # 按Sheet+Step汇总
        sheet_step_summary.to_excel(writer, sheet_name='Sheet_Step汇总', index=False)

        # 按Sheet汇总
        sheet_summary.to_excel(writer, sheet_name='Sheet汇总', index=False)

        # 详细差异类型统计
        summary.to_excel(writer, sheet_name='差异类型明细', index=False)

        # 原始筛选数据
        filtered.to_excel(writer, sheet_name='筛选原始数据', index=False)

    print(f"\n结果已保存到: {output_file}")


def main():
    parser = argparse.ArgumentParser(description='分析result.csv结果文件')
    parser.add_argument('input', nargs='?', default='result.csv', help='输入的result.csv文件')
    parser.add_argument('-o', '--output', default='result_summary.xlsx', help='输出文件')
    parser.add_argument('-s', '--suffix', default='03P', help='Recipe后缀筛选条件')
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"文件不存在: {args.input}")
        return

    analyze_result(args.input, args.output, args.suffix)


if __name__ == '__main__':
    main()
