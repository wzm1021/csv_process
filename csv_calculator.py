"""
CSV列计算器
支持根据自定义表达式对CSV文件的多列进行计算，并将结果保存到新列中
"""

import sys
import io
import pandas as pd
import numpy as np
import re
from typing import Union, List, Dict, Any

# 设置UTF-8输出编码（Windows兼容）
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass


class CSVCalculator:
    """
    CSV文件列计算器
    
    支持的运算符：
    - 基本运算：+, -, *, /, //, %, **
    - 括号：()
    - 数学函数：sin, cos, tan, sqrt, log, log10, exp, abs, ceil, floor, round等
    """
    
    def __init__(self, csv_path: str, encoding: str = 'utf-8'):
        """
        初始化CSV计算器
        
        Args:
            csv_path: CSV文件路径
            encoding: 文件编码，默认utf-8
        """
        self.csv_path = csv_path
        self.encoding = encoding
        self.df = None
        self._load_csv()
        
    def _load_csv(self):
        """加载CSV文件"""
        try:
            self.df = pd.read_csv(self.csv_path, encoding=self.encoding)
            print(f"成功加载CSV文件: {self.csv_path}")
            print(f"数据行数: {len(self.df)}, 列数: {len(self.df.columns)}")
            print(f"列名: {list(self.df.columns)}")
        except Exception as e:
            raise Exception(f"加载CSV文件失败: {str(e)}")
    
    def _extract_column_names(self, expression: str) -> List[str]:
        """
        从表达式中提取列名
        
        Args:
            expression: 计算表达式
            
        Returns:
            表达式中使用的列名列表
        """
        # 移除所有数学函数名，避免误识别
        math_functions = ['sin', 'cos', 'tan', 'sqrt', 'log', 'log10', 'log2', 
                         'exp', 'abs', 'ceil', 'floor', 'round', 'arcsin', 
                         'arccos', 'arctan', 'sinh', 'cosh', 'tanh', 'power',
                         'maximum', 'minimum', 'mean', 'sum', 'std', 'var']
        
        temp_expr = expression
        for func in math_functions:
            temp_expr = temp_expr.replace(func, '')
        
        # 使用正则提取可能的列名（字母、数字、下划线组成）
        potential_cols = re.findall(r'[A-Za-z_][A-Za-z0-9_]*', temp_expr)
        
        # 过滤出实际存在于CSV中的列名
        actual_cols = [col for col in potential_cols if col in self.df.columns]
        
        return actual_cols
    
    def _create_safe_namespace(self) -> Dict[str, Any]:
        """
        创建安全的命名空间用于表达式求值
        
        Returns:
            包含数学函数的安全命名空间
        """
        safe_namespace = {
            # NumPy数学函数
            'sin': np.sin,
            'cos': np.cos,
            'tan': np.tan,
            'arcsin': np.arcsin,
            'arccos': np.arccos,
            'arctan': np.arctan,
            'sinh': np.sinh,
            'cosh': np.cosh,
            'tanh': np.tanh,
            'sqrt': np.sqrt,
            'log': np.log,
            'log10': np.log10,
            'log2': np.log2,
            'exp': np.exp,
            'abs': np.abs,
            'ceil': np.ceil,
            'floor': np.floor,
            'round': np.round,
            'power': np.power,
            'maximum': np.maximum,
            'minimum': np.minimum,
            'mean': np.mean,
            'sum': np.sum,
            'std': np.std,
            'var': np.var,
            # 常量
            'pi': np.pi,
            'e': np.e,
            # NumPy本身
            'np': np,
        }
        return safe_namespace
    
    def calculate_column(self, expression: str, new_column_name: str, 
                        inplace: bool = False) -> pd.DataFrame:
        """
        根据表达式计算新列
        
        Args:
            expression: 计算表达式，如 "PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ"
            new_column_name: 新列的名称
            inplace: 是否在原数据框上修改，默认False（返回副本）
            
        Returns:
            包含新列的DataFrame
            
        Examples:
            >>> calc = CSVCalculator('data.csv')
            >>> # 简单四则运算
            >>> df = calc.calculate_column('PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ', 'PST_GLB_SS_TQ')
            >>> # 使用括号
            >>> df = calc.calculate_column('(A+B)/(C+D)', 'result')
            >>> # 使用数学函数
            >>> df = calc.calculate_column('sqrt(A**2 + B**2)', 'distance')
            >>> # 复杂表达式
            >>> df = calc.calculate_column('sin(A*pi/180) + cos(B*pi/180)', 'trig_result')
        """
        if self.df is None:
            raise Exception("CSV文件未加载")
        
        # 提取表达式中使用的列名
        used_columns = self._extract_column_names(expression)
        
        if not used_columns:
            raise ValueError(f"表达式 '{expression}' 中未找到有效的列名")
        
        # 验证所有列是否存在
        missing_cols = [col for col in used_columns if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"以下列在CSV文件中不存在: {missing_cols}")
        
        print(f"\n计算表达式: {expression}")
        print(f"使用的列: {used_columns}")
        print(f"目标列名: {new_column_name}")
        
        # 选择是否在原数据框上操作
        if inplace:
            df = self.df
        else:
            df = self.df.copy()
        
        try:
            # 创建安全的命名空间
            namespace = self._create_safe_namespace()
            
            # 将需要的列添加到命名空间
            for col in used_columns:
                namespace[col] = df[col]
            
            # 计算表达式
            df[new_column_name] = eval(expression, {"__builtins__": {}}, namespace)
            
            print(f"[OK] 成功创建新列 '{new_column_name}'")
            print(f"  前5行数据:")
            display_cols = used_columns + [new_column_name]
            print(df[display_cols].head())
            
            if inplace:
                self.df = df
            
            return df
            
        except Exception as e:
            raise Exception(f"计算表达式时出错: {str(e)}")
    
    def calculate_multiple_columns(self, rules: List[Dict[str, str]], 
                                   inplace: bool = False) -> pd.DataFrame:
        """
        批量计算多个新列
        
        Args:
            rules: 规则列表，每个规则是包含'expression'和'column_name'的字典
            inplace: 是否在原数据框上修改
            
        Returns:
            包含所有新列的DataFrame
            
        Example:
            >>> calc = CSVCalculator('data.csv')
            >>> rules = [
            ...     {'expression': 'A*B', 'column_name': 'A_times_B'},
            ...     {'expression': 'A+B', 'column_name': 'A_plus_B'},
            ...     {'expression': 'sqrt(A**2+B**2)', 'column_name': 'magnitude'}
            ... ]
            >>> df = calc.calculate_multiple_columns(rules)
        """
        df = self.df.copy() if not inplace else self.df
        
        for i, rule in enumerate(rules, 1):
            print(f"\n[{i}/{len(rules)}] 处理规则...")
            expression = rule.get('expression')
            column_name = rule.get('column_name')
            
            if not expression or not column_name:
                print(f"  [SKIP] 跳过无效规则: {rule}")
                continue
            
            df = self.calculate_column(expression, column_name, inplace=True)
        
        return df
    
    def save(self, output_path: str = None, encoding: str = None):
        """
        保存DataFrame到CSV文件
        
        Args:
            output_path: 输出文件路径，默认为原文件名_output.csv
            encoding: 输出文件编码，默认使用输入文件编码
        """
        if self.df is None:
            raise Exception("没有数据可保存")
        
        if output_path is None:
            base_name = self.csv_path.rsplit('.', 1)[0]
            output_path = f"{base_name}_output.csv"
        
        if encoding is None:
            encoding = self.encoding
        
        self.df.to_csv(output_path, index=False, encoding=encoding)
        print(f"\n[OK] 数据已保存到: {output_path}")
        return output_path
    
    def get_dataframe(self) -> pd.DataFrame:
        """获取当前的DataFrame"""
        return self.df
    
    def reset(self):
        """重置到初始加载的状态"""
        self._load_csv()
        print("[OK] 数据已重置到初始状态")


def calculate_csv_column(csv_path: str, expression: str, new_column_name: str,
                        output_path: str = None, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    便捷函数：直接对CSV文件执行单个计算规则
    
    Args:
        csv_path: CSV文件路径
        expression: 计算表达式
        new_column_name: 新列名称
        output_path: 输出文件路径（可选）
        encoding: 文件编码
        
    Returns:
        计算后的DataFrame
        
    Example:
        >>> df = calculate_csv_column('data.csv', 
        ...                           'PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ',
        ...                           'PST_GLB_SS_TQ',
        ...                           'output.csv')
    """
    calc = CSVCalculator(csv_path, encoding=encoding)
    df = calc.calculate_column(expression, new_column_name, inplace=True)
    
    if output_path:
        calc.save(output_path, encoding=encoding)
    
    return df


if __name__ == "__main__":
    # 使用示例
    print("=" * 70)
    print("CSV列计算器 - 使用示例")
    print("=" * 70)
    
    # 创建示例CSV文件
    sample_data = {
        'PST_GLB_SS': [10, 20, 30, 40, 50],
        'P1_GLB_TQ': [2, 3, 4, 5, 6],
        'P2_GLB_TQ': [1, 2, 3, 4, 5],
        'A': [1.5, 2.5, 3.5, 4.5, 5.5],
        'B': [0.5, 1.0, 1.5, 2.0, 2.5]
    }
    
    sample_df = pd.DataFrame(sample_data)
    sample_csv = 'sample_data.csv'
    sample_df.to_csv(sample_csv, index=False, encoding='utf-8')
    print(f"\n[OK] 已创建示例CSV文件: {sample_csv}")
    print("\n原始数据:")
    print(sample_df)
    
    # 示例1: 简单计算
    print("\n" + "=" * 70)
    print("示例1: 简单四则运算")
    print("=" * 70)
    calc = CSVCalculator(sample_csv)
    df = calc.calculate_column('PST_GLB_SS*P1_GLB_TQ-P2_GLB_TQ', 'PST_GLB_SS_TQ')
    
    # 示例2: 使用括号
    print("\n" + "=" * 70)
    print("示例2: 使用括号改变运算优先级")
    print("=" * 70)
    df = calc.calculate_column('(PST_GLB_SS+P1_GLB_TQ)*P2_GLB_TQ', 'combined_result')
    
    # 示例3: 使用数学函数
    print("\n" + "=" * 70)
    print("示例3: 使用数学函数")
    print("=" * 70)
    df = calc.calculate_column('sqrt(A**2 + B**2)', 'magnitude')
    
    # 示例4: 复杂表达式
    print("\n" + "=" * 70)
    print("示例4: 复杂表达式")
    print("=" * 70)
    df = calc.calculate_column('log10(PST_GLB_SS) + sqrt(P1_GLB_TQ)', 'complex_calc')
    
    # 示例5: 批量计算
    print("\n" + "=" * 70)
    print("示例5: 批量计算多个列")
    print("=" * 70)
    calc.reset()  # 重置到初始状态
    rules = [
        {'expression': 'PST_GLB_SS / P1_GLB_TQ', 'column_name': 'ratio1'},
        {'expression': 'A * B', 'column_name': 'product'},
        {'expression': 'power(A, 2) + power(B, 2)', 'column_name': 'sum_of_squares'}
    ]
    df = calc.calculate_multiple_columns(rules)
    
    # 保存结果
    output_file = calc.save('result.csv')
    
    print("\n" + "=" * 70)
    print("最终结果预览:")
    print("=" * 70)
    print(calc.get_dataframe())
    
    print("\n" + "=" * 70)
    print("所有示例执行完成！")
    print("=" * 70)

