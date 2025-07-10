import pandas as pd

# Đọc file CSV gốc
df = pd.read_csv("hospital_emails_result_parallel.csv")

# Lấy 2 cột cần thiết
df_filtered = df[["Email", "Tên bệnh viện"]]

# Lưu thành file Excel
df_filtered.to_excel("Ireland_filtered.xlsx", index=False)

print("✅ Đã tạo file Ireland_filtered.xlsx thành công.")
