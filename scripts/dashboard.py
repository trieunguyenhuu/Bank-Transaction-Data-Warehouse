from matplotlib import ticker
import pandas as pd
import sqlalchemy
import pyodbc
import urllib.parse
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ==============================================================================
# 1. KẾT NỐI SQL SERVER (Copy y hệt từ etl.py sang)
# ==============================================================================
DRIVER = '{ODBC Driver 18 for SQL Server}'
SERVER_NAME = 'localhost'
DATABASE_NAME = 'DW_Bank'
USERNAME = 'sa'
PASSWORD = 'Trieu123'  # <--- Kiểm tra lại mật khẩu của bạn

connection_string = f"DRIVER={DRIVER};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};UID={USERNAME};PWD={PASSWORD};Encrypt=no;"
quoted_conn_str = urllib.parse.quote_plus(connection_string)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}")

# Tạo thư mục để chứa ảnh (nếu chưa có)
if not os.path.exists('charts'):
    os.makedirs('charts')

print("--- Đang kết nối DW và lấy dữ liệu... ---")

# ==============================================================================
# 2. TRUY VẤN DỮ LIỆU (Data Query)
# ==============================================================================

# Query 1: Xu hướng chi tiêu theo Thời gian (Tháng)
sql_trend = """
SELECT 
    d.Year, d.Month, 
    CAST(d.Year AS VARCHAR) + '-' + RIGHT('0' + CAST(d.Month AS VARCHAR), 2) as YearMonth,
    SUM(f.Amount_Spent) as Total_Spent
FROM Fact_Spending f
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
GROUP BY d.Year, d.Month
ORDER BY d.Year, d.Month
"""
df_trend = pd.read_sql(sql_trend, engine)

# Query 2: Top Hạng mục chi tiêu
sql_category = """
SELECT TOP 5
    m.Category,
    SUM(f.Amount_Spent) as Total_Spent
FROM Fact_Spending f
JOIN Dim_Merchant m ON f.Merchant_Key = m.Merchant_Key
GROUP BY m.Category
ORDER BY Total_Spent DESC
"""
df_category = pd.read_sql(sql_category, engine)

# Query 3: Chi tiêu theo Nhóm tuổi
sql_age = """
SELECT 
    c.Age_Group,
    SUM(f.Amount_Spent) as Total_Spent
FROM Fact_Spending f
JOIN Dim_Customer c ON f.Customer_Key = c.Customer_Key
GROUP BY c.Age_Group
ORDER BY Total_Spent DESC
"""
df_age = pd.read_sql(sql_age, engine)

print("--- Đã lấy dữ liệu xong. Bắt đầu vẽ biểu đồ... ---")

# ==============================================================================
# 3. VẼ BIỂU ĐỒ (Visualization)
# ==============================================================================

# Hàm định dạng số tiền (Ví dụ: 1.500.000.000 -> "1.5 Tỷ")
def currency_formatter(x, pos):
    if x >= 1e9:
        return f'{x*1e-9:.1f} Tỷ'
    elif x >= 1e6:
        return f'{x*1e-6:.1f} Tr'
    else:
        return f'{x:.0f}'

# Cấu hình giao diện chung
sns.set_theme(style="whitegrid")

# --- BIỂU ĐỒ 1: XU HƯỚNG CHI TIÊU ---
plt.figure(figsize=(12, 6))
ax1 = sns.lineplot(data=df_trend, x='YearMonth', y='Total_Spent', marker='o', linewidth=2.5, color='#2ecc71')
plt.title('Xu Hướng Chi Tiêu Theo Tháng', fontsize=16, fontweight='bold', color='#2c3e50')
plt.xlabel('Thời gian', fontsize=12)
plt.ylabel('Tổng Tiền (VNĐ)', fontsize=12)
plt.xticks(rotation=45)

# Áp dụng format cho trục Y
ax1.yaxis.set_major_formatter(ticker.FuncFormatter(currency_formatter))

plt.tight_layout()
plt.savefig('charts/1_XuHuongChiTieu.png')
print("-> Đã lưu: charts/1_XuHuongChiTieu.png")

# --- BIỂU ĐỒ 2: TOP HẠNG MỤC ---
plt.figure(figsize=(10, 6))
ax2 = sns.barplot(data=df_category, x='Total_Spent', y='Category', palette='viridis')
plt.title('Top 5 Hạng Mục Chi Tiêu Lớn Nhất', fontsize=16, fontweight='bold', color='#2c3e50')
plt.xlabel('Tổng Tiền (VNĐ)', fontsize=12)
plt.ylabel('Hạng Mục', fontsize=12)

# Áp dụng format cho trục X (vì là biểu đồ ngang)
ax2.xaxis.set_major_formatter(ticker.FuncFormatter(currency_formatter))

plt.tight_layout()
plt.savefig('charts/2_TopHangMuc.png')
print("-> Đã lưu: charts/2_TopHangMuc.png")

# --- BIỂU ĐỒ 3: PHÂN KHÚC ĐỘ TUỔI ---
plt.figure(figsize=(10, 6))
ax3 = sns.barplot(data=df_age, x='Age_Group', y='Total_Spent', palette='magma')
plt.title('Mức Chi Tiêu Theo Nhóm Tuổi Khách Hàng', fontsize=16, fontweight='bold', color='#2c3e50')
plt.xlabel('Nhóm Tuổi', fontsize=12)
plt.ylabel('Tổng Tiền (VNĐ)', fontsize=12)

# Áp dụng format cho trục Y
ax3.yaxis.set_major_formatter(ticker.FuncFormatter(currency_formatter))

plt.tight_layout()
plt.savefig('charts/3_NhomTuoi.png')
print("-> Đã lưu: charts/3_NhomTuoi.png")
print("\n=== HOÀN TẤT! HÃY KIỂM TRA THƯ MỤC 'charts' ===")