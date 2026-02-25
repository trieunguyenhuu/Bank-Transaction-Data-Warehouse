import pandas as pd
import sqlalchemy
import pyodbc
import sys
import urllib.parse

# ==============================================================================
# Thông tin kết nối
# ==============================================================================
DRIVER = '{ODBC Driver 18 for SQL Server}'
SERVER_NAME = 'localhost'
DATABASE_NAME = 'DW_Bank'
USERNAME = 'sa'
PASSWORD = 'yourpass' 

print("Đang bắt đầu quá trình ETL...")

try:
    # 1. Tạo chuỗi kết nối (Connection String) chuẩn của pyodbc
    connection_string = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"Encrypt=no;"
    )
    
    # 2. Mã hóa (quote) chuỗi này để SQLAlchemy hiểu
    quoted_connection_string = urllib.parse.quote_plus(connection_string)
    
    # 3. Tạo 'engine' của SQLAlchemy bằng cách sử dụng chuỗi đã mã hóa
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc:///?odbc_connect={quoted_connection_string}",
        fast_executemany=True
    )

    # ==============================================================================
    # BƯỚC 1: EXTRACT (Trích xuất)
    # ==============================================================================
    
    # Kết nối thử
    with engine.connect() as conn:
        print("Kết nối SQL Server (qua SQLAlchemy) thành công!")

        # --- 1. Đọc 3 bảng từ SQL Server ---
        print("Đang trích xuất (Extract) dữ liệu từ SQL Server...")
        df_customers = pd.read_sql("SELECT * FROM tbl_Customers", conn)
        df_accounts = pd.read_sql("SELECT * FROM tbl_Accounts", conn)
        df_transactions = pd.read_sql("SELECT * FROM tbl_Transactions", conn)
    
    # --- 2. Đọc 1 file từ CSV ---
    print("Đang trích xuất (Extract) dữ liệu từ file CSV...")
    # (Hãy đảm bảo file 'tbl_MCC_Mapping.csv' nằm CÙNG THƯ MỤC với file 'etl.py' này)
    csv_path = 'tbl_MCC_Mapping.csv'
    df_mcc_mapping = pd.read_csv(csv_path)

    # --- In kết quả để kiểm tra ---
    print("\n--- Trích xuất thành công! ---")
    print(f"Đã tải {len(df_customers)} khách hàng.")
    print(f"Đã tải {len(df_accounts)} tài khoản.")
    print(f"Đã tải {len(df_transactions)} giao dịch.")
    print(f"Đã tải {len(df_mcc_mapping)} quy tắc mapping.")

    print("\n--- 5 dòng đầu tbl_Customers: ---")
    print(df_customers.head())

except pyodbc.Error as ex:
    print("\n*** ĐÃ XẢY RA LỖI PYODBC ***")
    print("Lỗi này thường là do sai thông tin đăng nhập hoặc driver.")
    sqlstate = ex.args[0]
    if sqlstate == '28000':
        print("Lỗi [28000]: Login failed! (SAI USERNAME HOẶC PASSWORD)")
    else:
        print(ex)

except FileNotFoundError:
    print(f"\n*** LỖI: Không tìm thấy file '{csv_path}' ***")
    print("Hãy đảm bảo file 'tbl_MCC_Mapping.csv' nằm CÙNG THƯ MỤC với file 'etl.py'.")
    
except Exception as e:
    print("\n*** ĐÃ XẢY RA LỖI KHÁC ***")
    print(e)
    sys.exit(1)


# ==============================================================================
# BƯỚC 2: TRANSFORM
# ==============================================================================
print("\nBắt đầu Bước 2: Transform...")
print("\n--- Đang xử lý dữ liệu (Transform)... ---")

# 1. CHUẨN HÓA KIỂU DỮ LIỆU (Để so sánh ngày tháng được)
# ------------------------------------------------------------------------------
print("1. Chuẩn hóa định dạng ngày tháng...")
df_customers['BirthDate'] = pd.to_datetime(df_customers['BirthDate'])
df_accounts['OpenDate'] = pd.to_datetime(df_accounts['OpenDate'])
df_transactions['TransactionTimestamp'] = pd.to_datetime(df_transactions['TransactionTimestamp'])

# 2. LÀM SẠCH DỮ LIỆU (Data Cleaning) - Xử lý các lỗi logic
# ------------------------------------------------------------------------------
print("2. Lọc bỏ dữ liệu lỗi logic (Cleaning)...")

# A. Kiểm tra Logic: Ngày mở TK phải > Ngày sinh
df_acc_cust = pd.merge(df_accounts, df_customers[['CustomerID', 'BirthDate']], on='CustomerID', how='inner')
valid_accounts_mask = df_acc_cust['OpenDate'] > df_acc_cust['BirthDate']

# Chỉ giữ lại các tài khoản hợp lệ
df_accounts_clean = df_acc_cust[valid_accounts_mask].copy()
df_accounts_clean = df_accounts_clean[['AccountID', 'CustomerID', 'AccountType', 'OpenDate']]

print(f"   - Đã loại bỏ {len(df_accounts) - len(df_accounts_clean)} tài khoản lỗi (Mở trước khi sinh).")
print(f"   - Số tài khoản hợp lệ còn lại: {len(df_accounts_clean)}")

# B. Lọc bỏ Khách hàng không có tài khoản
# Chỉ giữ lại những CustomerID nào CÓ XUẤT HIỆN trong danh sách tài khoản sạch
sl_khach_truoc = len(df_customers)
df_customers = df_customers[df_customers['CustomerID'].isin(df_accounts_clean['CustomerID'])]

print(f"   - Đã loại bỏ {sl_khach_truoc - len(df_customers)} khách hàng 'vô chủ' (Không có tài khoản hợp lệ).")
print(f"   - Số khách hàng còn lại để nạp vào kho: {len(df_customers)}")

# C. Kiểm tra Logic: Giao dịch phải xảy ra SAU ngày mở TK
df_trans_acc = pd.merge(df_transactions, df_accounts_clean[['AccountID', 'OpenDate']], on='AccountID', how='inner')
valid_trans_mask = df_trans_acc['TransactionTimestamp'] >= df_trans_acc['OpenDate']

# Chỉ giữ lại các giao dịch hợp lệ
df_transactions_clean = df_trans_acc[valid_trans_mask].copy()
# Giữ lại đủ cột (bao gồm cả P2P)
df_transactions_clean = df_transactions_clean[[
    'TransactionID', 'AccountID', 'MerchantName', 'Amount', 
    'TransactionTimestamp', 'TransactionCountry',
    'BeneficiaryName', 'TransactionDescription'
]]

print(f"   - Đã loại bỏ {len(df_transactions) - len(df_transactions_clean)} giao dịch lỗi logic.")


# 3. CHUẨN BỊ DỮ LIỆU CHO CÁC BẢNG DIMENSION (Enrichment)
# ------------------------------------------------------------------------------
print("3. Tính toán và chuẩn bị dữ liệu cho Star Schema...")

# --- A. Chuẩn bị Dim_Customer ---
# Logic: Tính Tuổi và Nhóm Tuổi
now = pd.Timestamp.now()
df_customers['Age'] = (now - df_customers['BirthDate']).dt.days // 365

def get_age_group(age):
    if age < 18: return '< 18'
    elif 18 <= age <= 24: return '18-24 (Sinh viên)'
    elif 25 <= age <= 34: return '25-34 (Người đi làm)'
    elif 35 <= age <= 50: return '35-50 (Trung niên)'
    else: return '> 50 (Cao tuổi)'

df_customers['Age_Group'] = df_customers['Age'].apply(get_age_group)

# Tạo DataFrame cho Dim_Customer

df_dim_customer_upload = df_customers[['CustomerID', 'FirstName', 'LastName', 'Age_Group', 'Gender', 'City', 'Country', 'BirthDate']].copy()

df_dim_customer_upload.columns = ['CustomerID_Source', 'FirstName', 'LastName', 'Age_Group', 'Gender', 'City', 'Country', 'BirthDate']

df_dim_customer_upload['CustomerName'] = df_dim_customer_upload['FirstName'] + ' ' + df_dim_customer_upload['LastName']

df_dim_customer_upload = df_dim_customer_upload[['CustomerID_Source', 'CustomerName', 'Age_Group', 'Gender', 'City', 'Country', 'BirthDate']]

# --- B. Chuẩn bị Dim_Account ---
df_dim_account_upload = df_accounts_clean[['AccountID', 'AccountType', 'OpenDate']].copy()
df_dim_account_upload.columns = ['AccountID_Source', 'Account_Type', 'Account_Open_Date']


# --- C. Chuẩn bị Dim_Location ---
# Lấy danh sách duy nhất các quốc gia từ giao dịch
unique_locations = df_transactions_clean[['TransactionCountry']].drop_duplicates().copy()
unique_locations.columns = ['Transaction_Country']

# Xác định Trong nước / Nước ngoài
def get_region(country):
    if country in ['Việt Nam', 'Vietnam', 'Viet Nam']:
        return 'Trong nước'
    return 'Nước ngoài'

unique_locations['Transaction_Region'] = unique_locations['Transaction_Country'].apply(get_region)
df_dim_location_upload = unique_locations


# D. Chuẩn bị Dim_Merchant (Nâng cấp xử lý P2P)
print("   -> Đang xử lý Dim_Merchant (Gộp Merchant và P2P)...")

# 1. Tạo bản sao để xử lý
df_merch = df_transactions_clean.copy()

# 2. Gộp tên: Nếu MerchantName rỗng (P2P) thì lấy tên Người nhận (BeneficiaryName)
df_merch['Final_Name'] = df_merch['MerchantName'].fillna(df_merch['BeneficiaryName'])

# 3. Hàm đoán Category từ nội dung
def get_category(row):
    # A. Ưu tiên 1: Nếu có MerchantName -> Tra từ điển CSV
    if pd.notna(row['MerchantName']):
        # Tìm trong df_mcc_mapping
        match = df_mcc_mapping[df_mcc_mapping['MerchantName'] == row['MerchantName']]
        if not match.empty:
            return match.iloc[0]['Category']
    
    # B. Ưu tiên 2: Nếu là P2P -> Phân tích nội dung Description
    desc = str(row['TransactionDescription']).lower()
    
    if any(x in desc for x in ['an', 'com', 'pho', 'bun', 'cafe', 'nuoc', 'tra sua', 'nhau']):
        return 'Ăn uống & Cà phê'
    elif any(x in desc for x in ['mua', 'shop', 'quan', 'ao', 'giay', 'tui', 'my pham', 'son', 'vay']):
        return 'Mua sắm'
    elif any(x in desc for x in ['xe', 'grab', 'xang', 'ship', 'taxi', 'di lai']):
        return 'Di chuyển & Vận tải'
    elif any(x in desc for x in ['dien', 'mang', 'wifi', 'nha', 'hoc', 'nuoc', 'phi', 'internet']):
        return 'Hóa đơn & Dịch vụ'
    elif any(x in desc for x in ['phim', 'du lich', 've', 'spa', 'game', 'homestay', 'karaoke']):
        return 'Giải trí & Du lịch'
    elif any(x in desc for x in ['tap hoa', 'rau', 'qua', 'gao', 'sieu thi', 'banh', 'keo']):
        return 'Tạp hóa & Siêu thị'
    else:
        return 'Chuyển khoản khác'

# Áp dụng hàm vào từng dòng
df_merch['Category_Final'] = df_merch.apply(get_category, axis=1)

# 4. Tạo DataFrame cho Dim_Merchant
# Lấy danh sách duy nhất các cặp (Tên, Category)
df_dim_merchant_upload = df_merch[['Final_Name', 'Category_Final']].drop_duplicates(subset=['Final_Name'])
df_dim_merchant_upload.columns = ['MerchantName_Source', 'Category']


# --- E. Chuẩn bị Dim_Date ---
# Lấy tất cả các mốc thời gian duy nhất (theo giờ)
temp_dates = df_transactions_clean[['TransactionTimestamp']].copy()
# Làm tròn về giờ
temp_dates['Full_Date'] = temp_dates['TransactionTimestamp'].dt.date
temp_dates['Hour_Of_Day'] = temp_dates['TransactionTimestamp'].dt.hour

# Lấy danh sách ngày+giờ duy nhất
unique_dates = temp_dates[['TransactionTimestamp', 'Full_Date', 'Hour_Of_Day']].drop_duplicates()

# Tính toán các thuộc tính ngày tháng
unique_dates['Day_Of_Week'] = unique_dates['TransactionTimestamp'].dt.dayofweek + 2
# Lưu ý: Nếu CN thì dayofweek=6 -> +2 = 8. Cần xử lý nếu muốn CN=1. 
# Thường VN: T2=2, ..., T7=7, CN=8. Code trên ra T2=2, CN=8. OK.

# Tên thứ
days = {2: 'Thứ Hai', 3: 'Thứ Ba', 4: 'Thứ Tư', 5: 'Thứ Năm', 6: 'Thứ Sáu', 7: 'Thứ Bảy', 8: 'Chủ Nhật'}
unique_dates['Day_Name'] = unique_dates['Day_Of_Week'].map(days)

# Cuối tuần (T7, CN)
unique_dates['Is_Weekend'] = unique_dates['Day_Of_Week'].isin([7, 8])

unique_dates['Month'] = unique_dates['TransactionTimestamp'].dt.month
unique_dates['Month_Name'] = 'Tháng ' + unique_dates['Month'].astype(str)
unique_dates['Quarter'] = unique_dates['TransactionTimestamp'].dt.quarter
unique_dates['Year'] = unique_dates['TransactionTimestamp'].dt.year

# TẠO DATE_KEY
# Quy tắc: yyyyMMddHH (ví dụ: 2025110614) -> INT
unique_dates['Date_Key'] = (
    unique_dates['Year'].astype(str) + 
    unique_dates['Month'].astype(str).str.zfill(2) + 
    unique_dates['TransactionTimestamp'].dt.day.astype(str).str.zfill(2) + 
    unique_dates['Hour_Of_Day'].astype(str).str.zfill(2)
).astype(int)

df_dim_date_upload = unique_dates.drop(columns=['TransactionTimestamp'])
# Loại bỏ trùng lặp
df_dim_date_upload = df_dim_date_upload.drop_duplicates(subset=['Date_Key'])


print("\n--- Transform hoàn tất! Đã chuẩn bị xong 5 DataFrames cho Dim ---")
print(f"1. Dim_Customer: {len(df_dim_customer_upload)} dòng")
print(f"2. Dim_Account:  {len(df_dim_account_upload)} dòng")
print(f"3. Dim_Merchant: {len(df_dim_merchant_upload)} dòng")
print(f"4. Dim_Location: {len(df_dim_location_upload)} dòng")
print(f"5. Dim_Date:     {len(df_dim_date_upload)} dòng")


# ==============================================================================
# BƯỚC 3: LOAD (Tải)
# ==============================================================================
print("\nBắt đầu Bước 3: Load...")

# Hàm nạp dữ liệu
def load_to_sql(df, table_name, engine):
    try:
        print(f"   + Đang nạp {len(df)} dòng vào bảng '{table_name}'...")
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"     -> Thành công!")
    except Exception as e:
        print(f"     -> LỖI khi nạp {table_name}: {e}")
        sys.exit(1)

# 1. NẠP CÁC BẢNG DIMENSION
# ------------------------------------------------------------------------------
print("1. Nạp dữ liệu vào các bảng Dimension...")

load_to_sql(df_dim_date_upload, 'Dim_Date', engine)
load_to_sql(df_dim_customer_upload, 'Dim_Customer', engine)
load_to_sql(df_dim_account_upload, 'Dim_Account', engine)
load_to_sql(df_dim_merchant_upload, 'Dim_Merchant', engine)
load_to_sql(df_dim_location_upload, 'Dim_Location', engine)


# 2. TẠO DỮ LIỆU CHO BẢNG FACT
# ------------------------------------------------------------------------------
print("2. Xử lý bảng Fact (Lookup Keys)...")

# Để tạo bảng Fact, cần thay thế các ID gốc (CustomerID, AccountID...) 
# bằng các KEY tự tăng (Customer_Key, Account_Key...) vừa được sinh ra trong SQL Server.

# A. Đọc lại các bảng Dim từ SQL để lấy Key mới nhất
with engine.connect() as conn:
    dim_cust_db = pd.read_sql("SELECT Customer_Key, CustomerID_Source FROM Dim_Customer", conn)

    # 1. Lấy Account_Key từ kho (Dim_Account)
    dim_acc_key = pd.read_sql("SELECT Account_Key, AccountID_Source FROM Dim_Account", conn)
    # 2. Lấy mối quan hệ Account -> Customer từ nguồn (tbl_Accounts) 
    source_acc_cust = pd.read_sql("SELECT AccountID, CustomerID FROM tbl_Accounts", conn)
    # 3. Ghép lại trong Python để có: Account_Key + CustomerID
    dim_acc_db = pd.merge(dim_acc_key, source_acc_cust, left_on='AccountID_Source', right_on='AccountID', how='inner')
    
    dim_merch_db = pd.read_sql("SELECT Merchant_Key, MerchantName_Source FROM Dim_Merchant", conn)
    dim_loc_db = pd.read_sql("SELECT Location_Key, Transaction_Country FROM Dim_Location", conn)

# B. Bắt đầu ghép nối (Mapping) vào bảng giao dịch sạch (df_transactions_clean)
fact_table = df_transactions_clean.copy()

# 2.1. Map Account_Key và lấy CustomerID
# Join bảng Fact với Dim_Account (qua AccountID)
fact_table = pd.merge(fact_table, dim_acc_key, left_on='AccountID', right_on='AccountID_Source', how='inner')

# 2.2. Map Customer_Key
# Bảng Transaction không có CustomerID, lấy nó từ thông tin Account
# Lấy mapping Account -> Customer từ dữ liệu gốc
acc_to_cust = df_accounts_clean[['AccountID', 'CustomerID']]
fact_table = pd.merge(fact_table, acc_to_cust, on='AccountID', how='inner')

# Giờ mới map với Dim_Customer để lấy Customer_Key
fact_table = pd.merge(fact_table, dim_cust_db, left_on='CustomerID', right_on='CustomerID_Source', how='inner')

# 2.3. Map Merchant_Key
# Bước A: Tái tạo lại cột 'Final_Name' cho bảng Fact
fact_table['Final_Name'] = fact_table['MerchantName'].fillna(fact_table['BeneficiaryName'])

# Bước B: Map dựa trên cột Final_Name này
fact_table = pd.merge(fact_table, dim_merch_db, left_on='Final_Name', right_on='MerchantName_Source', how='inner')

# 2.4. Map Location_Key
fact_table = pd.merge(fact_table, dim_loc_db, left_on='TransactionCountry', right_on='Transaction_Country', how='inner')

# 2.5. Map Date_Key
fact_table['Date_Key'] = (
    fact_table['TransactionTimestamp'].dt.year.astype(str) + 
    fact_table['TransactionTimestamp'].dt.month.astype(str).str.zfill(2) + 
    fact_table['TransactionTimestamp'].dt.day.astype(str).str.zfill(2) + 
    fact_table['TransactionTimestamp'].dt.hour.astype(str).str.zfill(2)
).astype(int)

# 2.6. Tạo cột Transaction_Count
fact_table['Transaction_Count'] = 1

# 2.7. Đổi tên cột Amount
fact_table.rename(columns={'Amount': 'Amount_Spent'}, inplace=True)

# 3. CHUẨN BỊ DATAFRAME CHO FACT
# ------------------------------------------------------------------------------
# Chỉ giữ lại các cột Key và Measure
df_fact_upload = fact_table[[
    'Date_Key', 
    'Customer_Key', 
    'Account_Key', 
    'Merchant_Key', 
    'Location_Key', 
    'Amount_Spent', 
    'Transaction_Count'
]]

print(f"   -> Đã tạo bảng Fact với {len(df_fact_upload)} dòng.")


# 4. NẠP BẢNG FACT
# ------------------------------------------------------------------------------
load_to_sql(df_fact_upload, 'Fact_Spending', engine)


print("\n=======================================================")
print("   CHÚC MỪNG! QUÁ TRÌNH ETL ĐÃ HOÀN TẤT 100%   ")
print("   DỮ LIỆU ĐÃ SẴN SÀNG TRONG KHO 'DW_Bank'     ")
print("=======================================================")