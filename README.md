# 🛠️ Data Quality Index - An Open Source Spark Framework

## Big Data - Final Project (CS-GY 6513)

## 🚀 Project Overview
This project introduces **Data Quality Index (DQI)**, an **open-source Spark-based framework** for **efficient data quality assessment** in the **Big Data era**. The framework evaluates datasets based on **dimensional metrics** such as **Uniqueness, Completeness, Range Adherence, and Format Adherence**, providing a **quantitative Data Quality Score**. 

---

## 🔍 Problem Statement
With the **growing volume of unfiltered datasets**, ensuring **data quality** is **time-consuming and resource-intensive**. This project aims to:
- **Develop a scalable Data Quality Assessment (DQA) framework** using **Apache Spark (PySpark)**.
- **Compute a Data Quality Index (DQI)** to quantify dataset usability.
- **Provide an open-source tool** for **semi-automated data quality scoring**.
- **Support user-defined quality metrics** to allow **domain-specific customization**.

---

## 📊 Key Features
✅ **Intrinsic & Extrinsic Data Quality Metrics**  
✅ **Parallel Processing with PySpark** for large datasets  
✅ **User-defined YAML-based validation rules**  
✅ **Support for Master Data Adherence** from AWS S3  
✅ **Open-Source & Scalable Design**  

---

## 📂 Datasets Used
We evaluate **4 real-world datasets** from **NYC Open Data**:
1. **🛠️ Electrical Permits Dataset** - Building permits & inspection records.
2. **🚖 Hire Vehicles Dataset** - NYC vehicle for hire service details.
3. **📞 311 Sampled Dataset** - NYC citizen complaints.
4. **🚗 Parking Violations Dataset** - Parking tickets & violations.

📌 **Data Sources:**
- [NYC Open Data](https://opendata.cityofnewyork.us/)
- AWS S3 (Master Data Storage)

---

## 🏗 Methodology

### **🔹 Data Quality Dimensions**
We define multiple **Data Quality Metrics**:
1. **Uniqueness**: Checks if a dataset has unique identifiers.
2. **Completeness**: Measures missing/null values.
3. **Duplicate Records**: Identifies redundancy in data.
4. **Range Adherence**: Ensures data falls within a valid range.
5. **Format Adherence**: Validates data format using **regex patterns**.
6. **Functional Dependency Adherence**: Detects invalid column relationships.
7. **Master Data Adherence**: Verifies data against **reference datasets**.
8. **Outlier Detection**: Identifies extreme values using **IQR method**.

### **🔹 Data Quality Score (DQI) Calculation**
The **Data Quality Index (DQI)** is computed as:
```yaml
DQI = (w1 * Dim1 + w2 * Dim2 + ... + wn * DimN) / N
```

Where:
- **w** = Weight of each data quality metric
- **Dim** = Individual metric score
- **N** = Total number of dimensions

---

## 📈 Results & Analysis

| **Metric**          | **Parking Violations** | **Hire Vehicles** | **311 Samples** | **Electrical Permits** |
|----------------------|----------------------|-------------------|----------------|-------------------|
| **Uniqueness**       | ✅ 1.00 | ✅ 1.00 | ❌ 0.00 | ✅ 1.00 |
| **Completeness**     | ✅ 0.89 | ❌ 0.71 | ✅ 1.00 | ✅ 0.77 |
| **Format Adherence** | ✅ 0.93 | ❌ 0.52 | ✅ 0.50 | ✅ 1.00 |
| **Range Adherence**  | ❌ 0.42 | ❌ - | ❌ 0.67 | ✅ 0.93 |
| **Master Data Adherence** | ✅ 0.89 | ❌ - | ✅ 0.87 | ❌ - |
| **Duplicate Records** | ✅ 0.99 | ✅ 1.00 | ✅ 1.00 | ✅ 1.00 |
| **Outlier Detection** | ✅ 0.89 | ❌ - | ✅ 0.98 | ✅ 0.90 |
| **Final DQI Score**  | **0.86** | **0.75** | **0.78** | **0.87** |

✅ **Best Dataset:** **Electrical Permits** (DQI = 0.87)  
⚠️ **Worst Dataset:** **Hire Vehicles** (DQI = 0.75, poor format adherence)  

---

## 🔧 Framework Usage

### **🔹 Installation**
```bash
pip install pyspark
git clone https://github.com/sumedhsp/Data-Quality-Assessment.git
cd Data-Quality-Assessment
```

---

## 🔹 Running Data Quality Checks

```python
from dq_framework import DataQuality
dq = DataQuality("your_dataset.csv", "config.yaml")
dq.run_checks()
```

---

## 🔹 Sample YAML Configuration

```yaml
format_rules:
  Work_ID: '([aA-zZ0-9]+)-([aA-zZ0-9]+)-([aA-zZ0-9]+)'
  Vehicle_Year: '[0-9]{4}'
  Website: 'www.[a-zA-Z0-9]+.com'
```

---

## 🔮 Future Enhancements

🚀 Enhancements planned for future versions:

📏 Functional Dependency Detection: Automatically identify relationships between columns.
🔍 Misspelling Detection: Using pyspellchecker to correct textual inconsistencies.
📊 Time-Series Data Quality Assessment: Tracking changes in data quality over time.

---

## 🤝 Contributors
👨‍💻 Team Members:

Sriphani Bellamkonda (`sb9179@nyu.edu`)
Kaartikeya Panjwani (`kp3291@nyu.edu@nyu.edu`)
Sumedh Parvatikar (`sp7479@nyu.edu`)

---

## 📜 License
🔓 MIT License - Free to use, modify, and distribute.

---

### 🔥 Want to contribute? Fork this repo, open an issue, or submit a pull request!
### 📢 If you find this project useful, give it a ⭐!
