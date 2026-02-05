"""
生成测试用的xlsx文件
"""

import pandas as pd
import numpy as np
from pathlib import Path


def generate_test_file(file_path: str, file_idx: int):
    """生成一个测试xlsx文件"""
    writer = pd.ExcelWriter(file_path, engine='openpyxl')

    # metaInfo sheet
    meta_df = pd.DataFrame({
        'Wafer': [f'W{file_idx:03d}'],
        'EQP': [f'EQP{file_idx % 3 + 1}'],
        'model': [f'Model{file_idx % 2 + 1}'],
        'recipe': [f'Recipe{file_idx % 4 + 1}']
    })
    meta_df.to_excel(writer, sheet_name='metaInfo', index=False)

    # 生成数据行（模拟每个STEP的数据）
    steps = []
    values_temp = []
    values_status = []
    values_pressure = []
    values_stair = []
    values_duration = []

    for step in [18, 19, 20, 21, 22, 23]:
        # 每个STEP随机行数
        num_rows = np.random.randint(15, 30)
        for i in range(num_rows):
            steps.append(step)

            # 温度类：渐进上升后稳定
            if i < num_rows // 2:
                temp = 25 + i * 2 + np.random.normal(0, 0.5) + file_idx * 0.1
            else:
                temp = 25 + (num_rows // 2) * 2 + np.random.normal(0, 0.2) + file_idx * 0.1
            values_temp.append(temp)

            # 状态类：0/1切换
            values_status.append(np.random.choice([0, 1]))

            # 压力类：有波动
            values_pressure.append(100 + np.sin(i * 0.5) * 10 + np.random.normal(0, 2) + file_idx)

            # 阶梯类：阶梯变化
            stair_level = (i // 5) * 10 + file_idx
            values_stair.append(stair_level + np.random.normal(0, 0.1))

            # 持续时间类：累计秒数
            values_duration.append(i + 1)

    # Temp1 sheet
    temp_df = pd.DataFrame({'STEP': steps, 'VALUE': values_temp})
    temp_df.to_excel(writer, sheet_name='Temp1', index=False)

    # Temp2 sheet
    temp_df2 = pd.DataFrame({'STEP': steps, 'VALUE': [v + 10 for v in values_temp]})
    temp_df2.to_excel(writer, sheet_name='Temp2', index=False)

    # Status1 sheet
    status_df = pd.DataFrame({'STEP': steps, 'VALUE': values_status})
    status_df.to_excel(writer, sheet_name='Status1', index=False)

    # Status2 sheet
    status_df2 = pd.DataFrame({'STEP': steps, 'VALUE': [1 - v for v in values_status]})
    status_df2.to_excel(writer, sheet_name='Status2', index=False)

    # Pressure1 sheet
    pressure_df = pd.DataFrame({'STEP': steps, 'VALUE': values_pressure})
    pressure_df.to_excel(writer, sheet_name='Pressure1', index=False)

    # Flow1 sheet
    flow_df = pd.DataFrame({'STEP': steps, 'VALUE': [v * 0.5 for v in values_pressure]})
    flow_df.to_excel(writer, sheet_name='Flow1', index=False)

    # Stair1 sheet
    stair_df = pd.DataFrame({'STEP': steps, 'VALUE': values_stair})
    stair_df.to_excel(writer, sheet_name='Stair1', index=False)

    # Stair2 sheet
    stair_df2 = pd.DataFrame({'STEP': steps, 'VALUE': [v * 2 for v in values_stair]})
    stair_df2.to_excel(writer, sheet_name='Stair2', index=False)

    # Duration1 sheet
    duration_df = pd.DataFrame({'STEP': steps, 'VALUE': values_duration})
    duration_df.to_excel(writer, sheet_name='Duration1', index=False)

    # Duration2 sheet
    duration_df2 = pd.DataFrame({'STEP': steps, 'VALUE': [v * 2 for v in values_duration]})
    duration_df2.to_excel(writer, sheet_name='Duration2', index=False)

    writer.close()


def main():
    # 创建data目录
    data_dir = Path('./data')
    data_dir.mkdir(exist_ok=True)

    # 生成5个测试文件
    num_files = 5
    print(f"生成 {num_files} 个测试文件...")

    for i in range(1, num_files + 1):
        file_path = data_dir / f'test_{i:03d}.xlsx'
        generate_test_file(str(file_path), i)
        print(f"  生成: {file_path.name}")

    print("测试文件生成完成!")


if __name__ == '__main__':
    main()
