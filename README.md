# Hệ thống Kho Dữ Liệu Giao Dịch Ngân Hàng (Bank Transaction Data Warehouse)

## Tổng quan
Kho dữ liệu Giao dịch Ngân hàng là một nền tảng kỹ thuật dữ liệu (data engineering) mạnh mẽ được thiết kế để tổng hợp, làm sạch và phân tích dữ liệu giao dịch. Được xây dựng nhằm phục vụ các hoạt động Quản lý Quan hệ Khách hàng (CRM), hệ thống sở hữu một quy trình ETL (Extract, Transform, Load) hoàn chỉnh giúp xử lý dữ liệu giao dịch thô thành kho dữ liệu mô hình sao (star-schema) được tối ưu hóa cao. Từ đó, cung cấp những thông tin chi tiết và sâu sắc về hành vi chi tiêu cũng như xu hướng tài chính của khách hàng.

## Công nghệ sử dụng
- **Python 3.10+**
- **SQL Server 2022**
- **Docker**
- **Pandas**
- **SQLAlchemy**
- **PyODBC**
- **Matplotlib & Seaborn**
- **Azure Data Studio / SSMS**

## Các tính năng cốt lõi

### Trích xuất dữ liệu (Data Extraction)
- Tự động trích xuất dữ liệu từ nhiều bảng cơ sở dữ liệu quan hệ (relational tables).
- Hỗ trợ xử lý khối lượng lớn dữ liệu giao dịch hỗn hợp (POS, P2P).
- Tích hợp tính năng ánh xạ Mã danh mục người bán (MCC - Merchant Category Code).

### Chuyển đổi dữ liệu (Data Transformation)
- Làm sạch và chuẩn hóa dữ liệu toàn diện.
- Xử lý các giá trị khuyết thiếu và dữ liệu bất thường.
- Kỹ nghệ đặc trưng (Feature engineering) phục vụ phân tích CRM (ví dụ: thói quen chi tiêu, phân khúc khách hàng).

### Nạp dữ liệu (Data Loading)
- Thiết kế cơ sở dữ liệu theo mô hình sao (Star-schema gồm các bảng Fact và Dimension).
- Cơ chế nạp dữ liệu hàng loạt (bulk loading) tối ưu hiệu suất.
- Tự động tạo và kiểm tra cấu trúc (schema) cơ sở dữ liệu.

### Hỗ trợ Phân tích (Analytics Support)
- Tính toán sẵn các chỉ số quan trọng cho quản trị quan hệ khách hàng.
- Cấu trúc dữ liệu sẵn sàng tích hợp với các công cụ BI (Dashboard) và các mô hình Học máy (Machine Learning).

## Cấu trúc Dự án
```text
├── data_source/                                 
│   ├── sql_mixed_data/        
│   ├── tbl_Accounts/
│   ├── tbl_Customers/        
│   ├── tbl_MCC_Mapping/                      
│   ├── tbl_Transactions/                                
│   └── schema.sql                         
├── etl_pipeline/                                   
│   ├── etl.py        
│   └── tbl_MCC_Mapping.csv                       
├── scripts/
├── venv/                                                       
└── README.md                              
```

## Hướng dẫn Cài đặt

### Yêu cầu hệ thống
- **Docker** (để chạy SQL Server)
- **Python 3.10** trở lên
- **Azure Data Studio** hoặc **SQL Server Management Studio (SSMS)**
- **Visual Studio Code** (hoặc IDE bất kỳ)

### 1. Thiết lập Database (SQL Server qua Docker)
Khởi chạy một container SQL Server 2022. Mở Terminal và chạy lệnh tương ứng với hệ điều hành của bạn:

**Trên Linux/Mac:**
```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=yourpass" \
   -p 1433:1433 --name sql_server --hostname sql_server \
   -d [mcr.microsoft.com/mssql/server:2022-latest](https://mcr.microsoft.com/mssql/server:2022-latest)
```

**Trên Windows (PowerShell):**
```powershell
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=yourpass" `
   -p 1433:1433 --name sql_server --hostname sql_server `
   -d [mcr.microsoft.com/mssql/server:2022-latest](https://mcr.microsoft.com/mssql/server:2022-latest)
```
> **Thông tin kết nối:** Server: `localhost` (Port 1433) | User: `sa` | Password: `yourpass`

### 2. Khởi tạo Database & Nạp dữ liệu thô
1. Mở Azure Data Studio hoặc SSMS, kết nối vào server `localhost`.
2. Chạy file `schema.sql` để tạo Database `DW_Bank` và cấu trúc các bảng.
3. Chạy lần lượt các script dữ liệu nguồn để nạp dữ liệu vào các bảng thô (raw tables):
   - `tbl_Customers.sql`
   - `tbl_Accounts_part1.sql`, `tbl_Accounts_part2.sql`
   - `tbl_Transactions_POS_New.sql`, `tbl_Transactions_P2P.sql`
   - `tbl_Transactions_POS_New.sql_p2`, `tbl_Transactions_P2P_p2.sql`
   - `mixed_transactions_part_1.sql` đến `mixed_transactions_part_32.sql`

> **Lưu ý:** Nếu gặp lỗi tràn bộ nhớ (Memory Overflow) khi chạy các file SQL quá lớn, hãy khởi động lại container SQL Server và tiếp tục nạp.

### 3. Thiết lập Môi trường Python

**Cài đặt ODBC Driver 18:**
- **Trên Windows:** Tải và cài đặt ODBC Driver 18 for SQL Server từ trang chủ Microsoft.
- **Trên Linux (Ubuntu):** Chạy các lệnh sau:
```bash
curl [https://packages.microsoft.com/keys/microsoft.asc](https://packages.microsoft.com/keys/microsoft.asc) | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
curl [https://packages.microsoft.com/config/ubuntu/$(lsb_release](https://packages.microsoft.com/config/ubuntu/$(lsb_release) -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo apt-get install -y msodbcsql18 unixodbc-dev
```

**Tạo môi trường ảo (Virtual Environment):**
```bash
# Tạo môi trường ảo
python -m venv venv

# Kích hoạt môi trường (Linux/Mac)
source venv/bin/activate
# Kích hoạt môi trường (Windows)
.\venv\Scripts\activate

# Cài đặt thư viện
pip install pandas sqlalchemy pyodbc matplotlib seaborn
```

### 4. Chạy Quy trình ETL
Đảm bảo file `tbl_MCC_Mapping.csv` nằm cùng thư mục với file code ETL của bạn. Chạy lệnh sau để khởi động quy trình:
```bash
python etl.py
```
> Đợi đến khi màn hình hiển thị thông báo: **"CHÚC MỪNG! QUÁ TRÌNH ETL ĐÃ HOÀN TẤT 100%"**. 
> Sau đó, bạn có thể kiểm tra lại trong Azure Data Studio để xem các bảng `Dim_` và `Fact_` đã được nạp đầy đủ dữ liệu.

## Cấu trúc Kho Dữ Liệu (Data Warehouse Schema)

### Bảng Chiều (Dimensions - Dim)
- **Dim_Customer:** Chi tiết và thông tin nhân khẩu học của khách hàng.
- **Dim_Account:** Thông tin và trạng thái tài khoản.
- **Dim_Date:** Bảng thời gian (Calendar) phục vụ phân tích theo ngày/tháng/năm.
- **Dim_Merchant:** Thông tin người bán và phân loại nhóm ngành (MCC).

### Bảng Sự Kiện (Facts - Fact)
- **Fact_Spending:** Bảng lưu trữ các chỉ số giao dịch chi tiết và tổng hợp (tổng chi tiêu, tần suất giao dịch, và phân bổ chi tiêu theo từng danh mục).

---

## Liên hệ
- **Nguyễn Hữu Triệu** - [nguyenhuutrieu2004@gmail.com](mailto:nguyenhuutrieu2004@gmail.com)
- **Link github:** [https://github.com/trieunguyenhuu](https://github.com/trieunguyenhuu)