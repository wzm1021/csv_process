# XLSX数据分析工具集

## 工具一：多文件差异分析 (analyze_xlsx.py)

比较多个xlsx文件中相同sheet、相同step的数据差异。

### 功能

| 检测类型 | 说明 | 判断标准 |
|---------|------|---------|
| VALUE均值差异 | 比较同一step的VALUE均值 | 差异超过阈值% |
| 时长差异 | 比较同一step的行数 | 差异超过N行 |
| 相关系数 | 整体趋势相似度 | 低于阈值 |
| 分段趋势 | 分N段比较升降方向 | 一致性低于阈值% |
| 差分符号 | 逐点升降方向对比 | 一致性低于阈值% |

### 配置文件 (config.json)

```json
{
    "folder_path": "./data",
    "baseline_file": "baseline.xlsx",
    "sheets": [],
    "step_column": "STEP",
    "value_column": "VALUE",
    "threshold_percent": 5,
    "duration_threshold_rows": 1,
    "correlation_threshold": 0.8,
    "segment_trend_threshold": 80,
    "diff_sign_threshold": 70,
    "num_segments": 4,
    "output_file": "result.csv"
}
```

| 参数 | 说明 |
|-----|------|
| folder_path | xlsx文件所在文件夹 |
| baseline_file | 基准文件名 |
| sheets | 要分析的sheet列表，空数组表示全部 |
| threshold_percent | VALUE差异阈值(%) |
| duration_threshold_rows | 时长差异阈值(行数) |
| correlation_threshold | 相关系数阈值 |
| segment_trend_threshold | 分段趋势一致性阈值(%) |
| diff_sign_threshold | 差分符号一致性阈值(%) |
| num_segments | 分段数量 |

### 运行

```bash
python analyze_xlsx.py
```

### 输出

生成 `result.csv`，包含所有超出阈值的差异记录。

---

## 工具二：特征提取 (extract_features.py)

从多个xlsx文件中提取参数特征，生成汇总表。

### 功能

支持三类参数的特征提取：

**1. 温度类参数**
- 拟合曲线 y = a(1 - e^(-bx)) + c
- 提取b值（上升速率）
- 如果数据稳定，输出均值

**2. 0/1二值参数**
- 统计状态切换次数（波动次数）

**3. 其他参数**
- 单调变化：输出斜率
- 波动变化：输出稳定前波动次数
- 通用：稳定后最大值、整体最大/最小值

### 配置文件 (config_feature.json)

```json
{
    "folder_path": "./data",
    "output_file": "feature_result.xlsx",
    "meta_sheet": "metaInfo",
    "meta_columns": ["Wafer", "EQP", "model", "recipe"],
    "step_column": "STEP",
    "value_column": "VALUE",
    "steps_to_analyze": ["STEP18", "STEP19", "STEP20-23"],
    "temperature_sheets": ["Temp1", "Temp2"],
    "binary_sheets": ["Status1", "Status2"],
    "other_sheets": ["Pressure1", "Flow1"],
    "stable_threshold": 0.5
}
```

| 参数 | 说明 |
|-----|------|
| folder_path | xlsx文件所在文件夹 |
| output_file | 输出文件名 |
| meta_sheet | 元数据sheet名 |
| meta_columns | 要提取的元数据列 |
| steps_to_analyze | 要分析的STEP，支持范围如"STEP20-23" |
| temperature_sheets | 温度类参数sheet列表 |
| binary_sheets | 0/1类参数sheet列表 |
| other_sheets | 其他类参数sheet列表 |
| stable_threshold | 稳定判断阈值 |

### 输出列命名规则

| 参数类型 | 列名格式 | 说明 |
|---------|---------|------|
| 温度类 | {sheet}_{step}_rate | 拟合b值 |
| 温度类 | {sheet}_{step}_mean | 均值 |
| 0/1类 | {sheet}_{step}_fluctuations | 波动次数 |
| 其他类 | {sheet}_{step}_slope | 斜率 |
| 其他类 | {sheet}_{step}_fluctuations | 稳定前波动次数 |
| 其他类 | {sheet}_{step}_stable_max | 稳定后最大值 |
| 其他类 | {sheet}_{step}_max | 整体最大值 |
| 其他类 | {sheet}_{step}_min | 整体最小值 |

### 运行

```bash
python extract_features.py
```

### 输出

生成 `feature_result.xlsx`，每个源文件一行，包含元数据和所有特征列。

---

## 依赖安装

```bash
pip install -r requirements.txt
```

依赖包：
- pandas >= 2.0.0
- openpyxl >= 3.1.0
- numpy >= 1.24.0
- scipy >= 1.10.0
