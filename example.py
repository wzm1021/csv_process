"""
简单使用示例
演示如何使用CSV计算器处理您的数据
"""

from csv_calculator import CSVCalculator, calculate_csv_column
import pandas as pd


def example_1_basic():
    """示例1: 基本使用 - 您的具体需求"""
    print("=" * 70)
    print("示例1: 根据您的需求计算 PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ")
    print("=" * 70)
    
    # 创建示例数据（模拟您的CSV文件）
    data = {
        'PST_GLB_SS': [10.5, 20.3, 30.1, 40.8, 50.2],
        'P1_GLB_TQ': [2.0, 3.5, 4.2, 5.1, 6.3],
        'P2_GLB_TQ': [1.2, 2.1, 3.0, 4.5, 5.8],
    }
    df = pd.DataFrame(data)
    df.to_csv('my_data.csv', index=False, encoding='utf-8')
    print("\n创建的示例CSV数据:")
    print(df)
    
    # 方法1: 使用便捷函数（一行代码搞定）
    print("\n方法1: 使用便捷函数")
    result_df = calculate_csv_column(
        csv_path='my_data.csv',
        expression='PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ',
        new_column_name='PST_GLB_SS_TQ',
        output_path='result_method1.csv'
    )
    
    # 方法2: 使用CSVCalculator类（更灵活）
    print("\n\n方法2: 使用CSVCalculator类")
    calc = CSVCalculator('my_data.csv')
    calc.calculate_column('PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ', 'PST_GLB_SS_TQ')
    calc.save('result_method2.csv')
    
    print("\n✓ 完成！结果已保存")


def example_2_multiple_calculations():
    """示例2: 批量计算多个列"""
    print("\n\n" + "=" * 70)
    print("示例2: 批量计算多个新列")
    print("=" * 70)
    
    # 创建示例数据
    data = {
        'temperature': [20, 25, 30, 35, 40],
        'pressure': [1.0, 1.2, 1.5, 1.8, 2.0],
        'volume': [10, 12, 15, 18, 20],
    }
    df = pd.DataFrame(data)
    df.to_csv('physics_data.csv', index=False, encoding='utf-8')
    
    print("\n原始数据:")
    print(df)
    
    # 批量计算
    calc = CSVCalculator('physics_data.csv')
    
    rules = [
        # 理想气体定律: PV/T
        {'expression': 'pressure * volume / temperature', 'column_name': 'PV_over_T'},
        # 压强-体积乘积
        {'expression': 'pressure * volume', 'column_name': 'PV'},
        # 温度的平方
        {'expression': 'temperature ** 2', 'column_name': 'T_squared'},
    ]
    
    calc.calculate_multiple_columns(rules)
    calc.save('physics_result.csv')
    
    print("\n✓ 完成！")


def example_3_advanced():
    """示例3: 高级计算 - 使用数学函数"""
    print("\n\n" + "=" * 70)
    print("示例3: 高级计算 - 数学函数")
    print("=" * 70)
    
    # 创建示例数据
    data = {
        'x': [0, 30, 45, 60, 90],
        'y': [1, 2, 3, 4, 5],
        'z': [10, 20, 30, 40, 50],
    }
    df = pd.DataFrame(data)
    df.to_csv('math_data.csv', index=False, encoding='utf-8')
    
    print("\n原始数据:")
    print(df)
    
    calc = CSVCalculator('math_data.csv')
    
    # 三角函数（角度转弧度）
    calc.calculate_column('sin(x * pi / 180)', 'sin_x')
    
    # 平方根
    calc.calculate_column('sqrt(y**2 + z**2)', 'magnitude')
    
    # 对数
    calc.calculate_column('log10(z)', 'log_z')
    
    # 复合表达式
    calc.calculate_column('(y + z) / 2', 'average_yz')
    
    calc.save('math_result.csv')
    
    print("\n✓ 完成！")


def example_4_custom():
    """示例4: 自定义 - 根据您的实际CSV文件"""
    print("\n\n" + "=" * 70)
    print("示例4: 使用您自己的CSV文件")
    print("=" * 70)
    
    # TODO: 将下面的路径替换为您的实际CSV文件路径
    your_csv_file = 'your_data.csv'  # 修改这里
    
    # 检查文件是否存在
    import os
    if not os.path.exists(your_csv_file):
        print(f"\n文件 '{your_csv_file}' 不存在")
        print("请将 'your_data.csv' 替换为您的实际文件路径")
        return
    
    # 加载您的CSV
    calc = CSVCalculator(your_csv_file)
    
    # 定义您的计算规则
    # 格式: 表达式, 新列名
    my_rules = [
        {'expression': 'PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ', 'column_name': 'PST_GLB_SS_TQ'},
        # 添加更多规则...
    ]
    
    # 执行计算
    calc.calculate_multiple_columns(my_rules)
    
    # 保存结果
    calc.save('my_result.csv')
    
    print("\n✓ 完成！")


if __name__ == "__main__":
    print("CSV计算器 - 使用示例")
    print("=" * 70)
    
    # 运行所有示例
    example_1_basic()
    example_2_multiple_calculations()
    example_3_advanced()
    
    # 如果您有自己的CSV文件，取消下面的注释
    # example_4_custom()
    
    print("\n\n" + "=" * 70)
    print("所有示例运行完成！")
    print("=" * 70)
    print("\n生成的文件:")
    print("  - my_data.csv (示例数据)")
    print("  - result_method1.csv (方法1结果)")
    print("  - result_method2.csv (方法2结果)")
    print("  - physics_data.csv (物理数据示例)")
    print("  - physics_result.csv (物理计算结果)")
    print("  - math_data.csv (数学数据示例)")
    print("  - math_result.csv (数学计算结果)")

