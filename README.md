# Sales Analysis Project

A PySpark-based solution for analyzing sales and product data. This project performs data transformations, calculations, and insights using PySpark.

---

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Folder Structure](#folder-structure)
- [Getting Started](#getting-started)
- [Analysis Details](#analysis-details)
- [Expected Output](#expected-output)
- [License](#license)

---

## Project Overview
This project leverages PySpark to:
- Load and clean sales and product data.
- Apply transformation rules as defined in the provided instructions.
- Calculate key metrics such as:
  - **LeadTimeInBusinessDays** (excluding weekends).
  - **TotalLineExtendedPrice**.
- Generate insightful reports on:
  - **Top-selling colors by year**.
  - **Average lead time by product category**.

---

## Folder Structure
```
sales-analysis/ 
│ ├── publish/ 
│ ├── publish_product.csv 
│ └── publish_sales.csv 
│ ├── raw/ 
│ ├── raw_products.csv 
│ ├── raw_sales_order_detail.csv 
│ ├── raw_sales_order_header.csv 
│ ├── store/ 
│ ├── store_sales_order_detail.csv 
│ ├── store_sales_order_header.csv 
│ └── sales_analysis.py
```
---

## Getting Started

### Prerequisites

Ensure you have the following installed in your system:
- Python 3.9 or higher
- PySpark
- Git

### Installation

#### 1. Clone the Repository
```bash
git clone https://github.com/<your-username>/sales-analysis.git
cd sales-analysis
```

#### 2. Install PySpark:
```bash
pip install pyspark
```

### Running the project
To execute the project, run the following command:
```bash
python sales_analysis.py
```

Upon successful execution, the following folders will be generated
- `raw/` - Contains the original data with a `raw_` prefix for each file
- `store/` - Contains transformed data with a `store_` prefix for each file
- `publish/` - Contains finalized output data with a `publish_` prefix for each file

All data will be saved in `.csv` format

---

## Analysis Details

The project performs the following key transformations and analysis:

### Product Master Transformation

- Replaces `NULL` values in `Color` column with `N/A`
- Enhances `ProductCategoryName` based on `ProductSubCategoryName` values to classify data into:
  - Clothing (e.g., Gloves, Socks, Vests)
  - Accessories(e.g., Locks, Helmets)
  - Components (e.g., Frames, Wheels)

### Sales Data Transformation
- Calculates `LeadTimeInBusinessDays` by calculating the difference between `OrderDate` and `ShipDate`, **excluding weekends**
- Calculates `TotalLineExtendedPrice` using:
```python
OrderQty * (UnitPrice - UnitPriceDiscount)
```
- Renames `Freight` to `TotalOrderFreight`
### Analysis Results
- Identifies the **Top Revenue-Generating Color** per year
- Computes the **Average Lead Time by product** category

---

## Expected Output

```sql
+----+--------+
|Year|TopColor |
+----+--------+
|2021|     Red |
|2022|   Black |
|2023|   Black |
|2024|  Yellow |
+----+--------+

+-------------------+-----------------+
|ProductCategoryName | AvgLeadTime     |
+-------------------+-----------------+
|Bikes                | 5.82            |
|Clothing             | 5.83            |
|Accessories          | 5.83            |
|Components           | 5.81            |
+-------------------+-----------------+
```

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

