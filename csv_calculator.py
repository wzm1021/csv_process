"""
CSV/XLSX特征计算器
支持通过YAML配置文件定义计算规则，包含全局基础特征和按工艺分组的特定特征配置
每种特定配置支持独立启用/禁用
"""

import sys
import io
import yaml
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Union, List, Dict, Any, Optional

# 设置UTF-8输出编码（Windows兼容）
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass


class CSVCalculator:
    """
    CSV/XLSX文件列计算器

    支持的运算符：
    - 基本运算：+, -, *, /, //, %, **
    - 括号：()
    - 数学函数：sin, cos, tan, sqrt, log, log10, exp, abs, ceil, floor, round等
    """

    def __init__(self, df: pd.DataFrame = None, file_path: str = None,
                 encoding: str = 'utf-8', sheet_name: Optional[str] = None):
        """
        初始化CSV计算器

        Args:
            df: 直接传入DataFrame
            file_path: 文件路径（csv或xlsx）
            encoding: 文件编码，默认utf-8
            sheet_name: xlsx文件的sheet名，None表示第一个sheet
        """
        self.file_path = file_path
        self.encoding = encoding
        self.sheet_name = sheet_name
        self.df = df

        if self.df is None and self.file_path:
            self._load_file()

    def _load_file(self):
        """加载CSV或XLSX文件"""
        try:
            path = Path(self.file_path)
            if path.suffix.lower() in ['.xlsx', '.xls']:
                self.df = pd.read_excel(
                    self.file_path,
                    sheet_name=self.sheet_name if self.sheet_name else 0,
                    engine='openpyxl'
                )
            else:
                self.df = pd.read_csv(self.file_path, encoding=self.encoding)

            print(f"成功加载文件: {self.file_path}")
            print(f"数据行数: {len(self.df)}, 列数: {len(self.df.columns)}")
        except Exception as e:
            raise Exception(f"加载文件失败: {str(e)}")

    def _extract_column_names(self, expression: str) -> List[str]:
        """
        从表达式中提取列名

        Args:
            expression: 计算表达式

        Returns:
            表达式中使用的列名列表
        """
        math_functions = [
            'sin', 'cos', 'tan', 'sqrt', 'log', 'log10', 'log2',
            'exp', 'abs', 'ceil', 'floor', 'round', 'arcsin',
            'arccos', 'arctan', 'sinh', 'cosh', 'tanh', 'power',
            'maximum', 'minimum', 'mean', 'sum', 'std', 'var'
        ]

        temp_expr = expression
        for func in math_functions:
            temp_expr = temp_expr.replace(func, '')

        potential_cols = re.findall(r'[A-Za-z_][A-Za-z0-9_]*', temp_expr)
        actual_cols = [col for col in potential_cols if col in self.df.columns]
        return actual_cols

    def _create_safe_namespace(self) -> Dict[str, Any]:
        """创建安全的命名空间用于表达式求值"""
        safe_namespace = {
            'sin': np.sin, 'cos': np.cos, 'tan': np.tan,
            'arcsin': np.arcsin, 'arccos': np.arccos, 'arctan': np.arctan,
            'sinh': np.sinh, 'cosh': np.cosh, 'tanh': np.tanh,
            'sqrt': np.sqrt, 'log': np.log, 'log10': np.log10, 'log2': np.log2,
            'exp': np.exp, 'abs': np.abs, 'ceil': np.ceil, 'floor': np.floor,
            'round': np.round, 'power': np.power,
            'maximum': np.maximum, 'minimum': np.minimum,
            'mean': np.mean, 'sum': np.sum, 'std': np.std, 'var': np.var,
            'pi': np.pi, 'e': np.e, 'np': np,
        }
        return safe_namespace

    def calculate_column(self, expression: str, new_column_name: str,
                         inplace: bool = False) -> pd.DataFrame:
        """
        根据表达式计算新列

        Args:
            expression: 计算表达式
            new_column_name: 新列的名称
            inplace: 是否在原数据框上修改

        Returns:
            包含新列的DataFrame
        """
        if self.df is None:
            raise Exception("数据未加载")

        used_columns = self._extract_column_names(expression)
        if not used_columns:
            raise ValueError(f"表达式 '{expression}' 中未找到有效的列名")

        missing_cols = [col for col in used_columns if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"以下列不存在: {missing_cols}")

        print(f"  计算: {expression} -> {new_column_name}")

        if inplace:
            df = self.df
        else:
            df = self.df.copy()

        try:
            namespace = self._create_safe_namespace()
            for col in used_columns:
                namespace[col] = df[col]

            df[new_column_name] = eval(expression, {"__builtins__": {}}, namespace)

            if inplace:
                self.df = df

            return df

        except Exception as e:
            raise Exception(f"计算表达式 '{expression}' 时出错: {str(e)}")

    def calculate_multiple_columns(self, rules: List[Dict[str, str]],
                                   inplace: bool = False) -> pd.DataFrame:
        """
        批量计算多个新列

        Args:
            rules: 规则列表，每个规则包含'expression'和'name'(或'column_name')
            inplace: 是否在原数据框上修改

        Returns:
            包含所有新列的DataFrame
        """
        if not inplace:
            self.df = self.df.copy()

        for i, rule in enumerate(rules, 1):
            expression = rule.get('expression')
            column_name = rule.get('name') or rule.get('column_name')

            if not expression or not column_name:
                print(f"  [跳过] 无效规则: {rule}")
                continue

            try:
                self.calculate_column(expression, column_name, inplace=True)
            except Exception as e:
                print(f"  [失败] {column_name}: {e}")

        return self.df

    def save(self, output_path: str = None, encoding: str = None):
        """
        保存DataFrame到文件（自动根据后缀选择csv/xlsx格式）

        Args:
            output_path: 输出文件路径
            encoding: 输出文件编码
        """
        if self.df is None:
            raise Exception("没有数据可保存")

        if output_path is None:
            base_name = self.file_path.rsplit('.', 1)[0] if self.file_path else 'output'
            output_path = f"{base_name}_output.csv"

        if encoding is None:
            encoding = self.encoding

        path = Path(output_path)
        if path.suffix.lower() in ['.xlsx', '.xls']:
            self.df.to_excel(output_path, index=False, engine='openpyxl')
        else:
            self.df.to_csv(output_path, index=False, encoding=encoding)

        print(f"数据已保存到: {output_path}")
        return output_path

    def get_dataframe(self) -> pd.DataFrame:
        """获取当前的DataFrame"""
        return self.df

    def reset(self):
        """重置到初始加载的状态"""
        self._load_file()
        print("数据已重置到初始状态")


class YAMLConfigCalculator:
    """
    基于YAML配置文件的特征计算器
    支持全局基础特征和按工艺分组的特定特征配置
    """

    def __init__(self, config_path: str = "config_calculator.yaml"):
        """
        初始化YAML配置计算器

        Args:
            config_path: YAML配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.calculator = None

    def _load_config(self) -> dict:
        """加载YAML配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            print(f"成功加载配置文件: {self.config_path}")
            return config
        except Exception as e:
            raise Exception(f"加载配置文件失败: {str(e)}")

    def _get_global_config(self) -> dict:
        """获取全局配置"""
        return self.config.get('global', {})

    def _get_base_features(self) -> List[dict]:
        """获取基础特征规则列表"""
        return self.config.get('base_features', [])

    def _get_specific_features(self) -> dict:
        """获取特定工艺特征配置"""
        return self.config.get('specific_features', {})

    def _get_enabled_specific_groups(self) -> Dict[str, dict]:
        """获取所有已启用的特定工艺配置"""
        specific = self._get_specific_features()
        enabled = {}

        for group_name, group_config in specific.items():
            if group_config.get('enabled', False):
                enabled[group_name] = group_config

        return enabled

    def show_config_summary(self):
        """打印配置摘要"""
        global_cfg = self._get_global_config()
        base_features = self._get_base_features()
        specific = self._get_specific_features()

        print("=" * 60)
        print("配置摘要")
        print("=" * 60)
        print(f"输入文件: {global_cfg.get('input_file', '未配置')}")
        print(f"输出文件: {global_cfg.get('output_file', '未配置')}")
        print(f"编码: {global_cfg.get('encoding', 'utf-8')}")
        print(f"基础特征数量: {len(base_features)}")

        if base_features:
            for f in base_features:
                print(f"  - {f.get('name')}: {f.get('expression')}")

        print(f"\n特定工艺配置:")
        for group_name, group_config in specific.items():
            status = "启用" if group_config.get('enabled', False) else "禁用"
            desc = group_config.get('description', '')
            features = group_config.get('features', [])
            print(f"  [{status}] {group_name} ({desc}) - {len(features)}个特征")

            if group_config.get('enabled', False):
                for f in features:
                    print(f"    - {f.get('name')}: {f.get('expression')}")

        print("=" * 60)

    def run(self) -> pd.DataFrame:
        """
        执行配置中定义的所有特征计算

        Returns:
            计算完成后的DataFrame
        """
        global_cfg = self._get_global_config()
        input_file = global_cfg.get('input_file')
        output_file = global_cfg.get('output_file')
        encoding = global_cfg.get('encoding', 'utf-8')
        sheet_name = global_cfg.get('input_sheet')

        if not input_file:
            raise ValueError("配置中缺少 global.input_file")

        # 加载数据
        self.calculator = CSVCalculator(
            file_path=input_file,
            encoding=encoding,
            sheet_name=sheet_name
        )

        self.show_config_summary()

        # 1. 执行基础特征计算
        base_features = self._get_base_features()
        if base_features:
            print(f"\n-- 执行基础特征计算 ({len(base_features)}个) --")
            self.calculator.calculate_multiple_columns(base_features, inplace=True)

        # 2. 执行已启用的特定工艺特征计算
        enabled_groups = self._get_enabled_specific_groups()
        if enabled_groups:
            for group_name, group_config in enabled_groups.items():
                features = group_config.get('features', [])
                desc = group_config.get('description', group_name)
                print(f"\n-- 执行特定特征: {desc} ({len(features)}个) --")
                self.calculator.calculate_multiple_columns(features, inplace=True)
        else:
            print("\n-- 无已启用的特定工艺特征 --")

        # 3. 保存结果
        if output_file:
            self.calculator.save(output_file, encoding=encoding)

        df = self.calculator.get_dataframe()
        print(f"\n计算完成，共 {len(df.columns)} 列, {len(df)} 行")
        return df

    def run_with_overrides(self, enable: List[str] = None,
                           disable: List[str] = None) -> pd.DataFrame:
        """
        执行计算，支持运行时覆盖特定工艺的启用/禁用状态

        Args:
            enable: 需要临时启用的工艺组名列表
            disable: 需要临时禁用的工艺组名列表

        Returns:
            计算完成后的DataFrame
        """
        specific = self._get_specific_features()

        # 临时覆盖启用状态
        if enable:
            for name in enable:
                if name in specific:
                    specific[name]['enabled'] = True
                else:
                    print(f"[警告] 工艺组 '{name}' 不存在于配置中")

        if disable:
            for name in disable:
                if name in specific:
                    specific[name]['enabled'] = False

        return self.run()


# ==============================================================================
# 便捷函数
# ==============================================================================

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
    """
    calc = CSVCalculator(file_path=csv_path, encoding=encoding)
    df = calc.calculate_column(expression, new_column_name, inplace=True)

    if output_path:
        calc.save(output_path, encoding=encoding)

    return df


def run_from_yaml(config_path: str = "config_calculator.yaml",
                  enable: List[str] = None,
                  disable: List[str] = None) -> pd.DataFrame:
    """
    便捷函数：直接从YAML配置文件执行特征计算

    Args:
        config_path: YAML配置文件路径
        enable: 临时启用的工艺组名列表
        disable: 临时禁用的工艺组名列表

    Returns:
        计算完成后的DataFrame
    """
    runner = YAMLConfigCalculator(config_path)

    if enable or disable:
        return runner.run_with_overrides(enable=enable, disable=disable)

    return runner.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='CSV/XLSX特征计算器')
    parser.add_argument('-c', '--config', default='config_calculator.yaml', help='YAML配置文件路径')
    parser.add_argument('--enable', nargs='*', default=None, help='临时启用的工艺组')
    parser.add_argument('--disable', nargs='*', default=None, help='临时禁用的工艺组')
    args = parser.parse_args()

    run_from_yaml(config_path=args.config, enable=args.enable, disable=args.disable)
