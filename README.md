# SennPAI-OverView-V0.2
抓取网页信息关键

---

# 🎓 OverView 

**OverView** 是一款专门针对大学及研究生院（Graduate School）官网设计的**智能深度爬虫与数据过滤平台**。它通过 Selenium 进行深度优先/广度优先遍历，并结合 **大语言模型 (LLM)** 实现自动化网络节点剪枝，旨在从成千上万个网页中精准定位“募集要项（Guidelines）”与“过去问（Past Papers）”。

![University Network Visualization](https://img.shields.io/badge/Data_Analysis-AI_Powered-blue)
![Python](https://img.shields.io/badge/Python-3.9+-green)
![LLM](https://img.shields.io/badge/LLM-Doubao_1.5_Pro-orange)

## 🌟 核心特性

-   **层级化节点追踪**：不仅采集 URL，还通过 `Index | FatherIndex` 建立完整的父子级联拓扑结构。
-   **AI 驱动的智能剪枝 (Recursive Pruning)**：
    -   利用 LLM 对网页分支进行定性分析。
    -   **硬限抽样算法 (`Easied`)**：强制限制父节点抽样数，大幅削减 90% 以上的 API Token 消耗。
    -   **全路径溯源**：确保剪枝后剩余节点均能逻辑闭环，链接回根节点，杜绝“孤岛数据”。
-   **自动化对抗策略**：
    -   **Anti-Download**: 内置 `ChromeOptions` 优化，阻止 PDF 自动下载。
    -   **Captcha Detection**: 自动识别并跳过人机验证页面。
-   **可视化交互图谱**：自动生成基于 `vis-network` 的交互式 HTML 关系图，支持双击跳转。
-   **沉浸式 Debug 体验**：自定义动画日志系统并配合 8-bit 合成音效反馈任务状态。

---

## 🛠️ 技术架构

### 1. 爬虫引擎 (`OverView` 类)
采用 Selenium 驱动，支持 `Eager` 加载策略提升速度。通过 `BeautifulSoup` 与 `Markdownify` 将复杂的 HTML 转化为 AI 易读的精简 Markdown 摘要。

### 2. 采样与切片算法
*   **`Easied` 函数**：解决长上下文溢出问题。对拥有大量子节点的父级进行物理截断，每组仅保留前 $N$ 个代表性子节点。
*   **`packChunks` 函数**：智能数据分包。根据字节大小自动计算批次，确保 120KB+ 的原始数据能完美适配 32k 甚至更小上下文的 AI 接口。

### 3. AI 交互流
项目集成 **火山引擎（豆包 Doubao-1.5-pro）** API：
-   **第一阶段 (Category)**：初步判断节点类型（文件、页面、无关噪音）。
-   **第二阶段 (Pruning)**：根据 AI 决策，物理移除整块无关的网页分支（如新闻、就职、校友活动）。

---

## 📂 项目结构

```text
├── main.py                # 程序入口
├── OverView.py            # 核心类定义与算法实现
├── Sdata.py               # 配置文件 (API Key, 关键词, 黑名单)
├── AIPmt/                 # 存放 AI Prompt 模板 (Cutf.txt, Get.txt)
├── _debug/                # 生成日志会存放于此
└── MemMD/                 # 结果输出文件夹
    ├── University_Name/
    │   ├── CollegeWeb.html              # Final.csv的交互式拓扑图
    │   ├── EasiedWeb.html              # cleaned.csv的交互式拓扑图
    │   ├── Final.csv                    # Seek查找得到的图结构数据
    │   ├── cleaned.csv                  # 剪枝后的核心数据
    │   ├── categoryA.csv                # 分类A组数据，为高价值数据
    │   ├── categoryB.csv                # 分类B组数据，为不确定价值数据
    │   └── INDEX_XXX.txt                # 网页快照 MD 文件
```

---

## 🚀 快速开始

### 环境配置
```bash
pip install selenium webdriver-manager beautifulsoup4 markdownify openai json_repair
```

### 运行说明
1. 在 `Sdata.py` 中配置你的 `Dou_Bao_Key` 和推理接入点 ID。
2. 初始化并启动：
```python
from OverView import OverView, overViewInit
from OverView import DEBPrint,DEBAnimaPrint,_DP,OPAexists

# 初始化浏览器设置
driver = overViewInit()

# 定义爬取任务 (目标URL, 最大深度, 项目标识)
ov = OverView("https://www.xxx-u.ac.jp/", depth=3, sign="XXX_University")

ov.start(driver)
ov.Seek()      # 执行广度优先爬取
ov.Pruning()   # 执行 AI 智能剪枝
ov.Category()  # 执行 AI 分类与数据沉淀
ov.end()       # 释放资源
```

---

## 📊 数据报告展示

系统会自动生成 MD 格式的结构化报告，如下所示：

```markdown
# [CLUSTER_GROUP_START: FatherNode 18]
FatherNode -> 18 | 0 | /admission/past-exams | 入试情报 > 过去问 | 过去の入试問題
-------------------------
273 | 18 | /admission/2025-zenki | 入试情报 > 学部入试 | 一般选拔（前期日程）
274 | 18 | /admission/2025-kouki | 入试情报 > 学部入试 | 一般选拔（后期日程）
# [CLUSTER_GROUP_END]
```

---


**License:** MIT  
**Author:** SennPAI-UkawaJUn
**Contact:** 
