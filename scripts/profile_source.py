# from pathlib import Path
# import pandas as pd

# ROOT_DIR = Path(__file__).resolve().parent.parent
# REPORTS = ROOT_DIR/"reports"
# REPORTS.mkdir(exist_ok=True)
# df = pd.read_parquet(
#     ROOT_DIR/"data/raw/yellow_tripdata_2025-01.parquet"
# )
# zones = pd.read_csv(ROOT_DIR / "data/raw/taxi_zone_lookup.csv")
# # 1. Số dòng, số cột và một vài dòng để quan sát
# print(f"So dong: {len(df):,}")
# print(f"So cot: {len(df.columns)}")
# print(df.head(5).to_string(index=False))

# # 2. Kiểu dữ liệu và tỷ lệ thiếu của từng cột
# schema = pd.DataFrame({
#     "dtype": df.dtypes.astype(str),
#     "null_count": df.isna().sum(),
#     "null_percent": (df.isna().mean() * 100).round(2),
# })

# print(schema.to_string())


# # 4. Các dấu hiệu cần điều tra; chưa kết luận là dữ liệu sai
# pickup = df["tpep_pickup_datetime"]
# dropoff = df["tpep_dropoff_datetime"]

# has_times = pickup.notna() & dropoff.notna()
# outside_month = pickup.notna() & (
#     (pickup < pd.Timestamp("2025-01-01"))
#     | (pickup >= pd.Timestamp("2025-02-01"))
# )

# checks = {
#     "missing_pickup_time": pickup.isna(),
#     "missing_dropoff_time": dropoff.isna(),
#     "dropoff_before_pickup": has_times & (dropoff < pickup),
#     "zero_duration": has_times & (dropoff == pickup),
#     "pickup_outside_file_month": outside_month,
#     "non_positive_distance": df["trip_distance"] <= 0,
#     "negative_fare": df["fare_amount"] < 0,
#     "negative_total": df["total_amount"] < 0,
# }

# for column in ["PULocationID", "DOLocationID"]:
#     checks[f"{column}_missing"] = df[column].isna()
#     checks[f"{column}_not_in_lookup"] = (
#         df[column].notna()
#         & ~df[column].isin(zones["LocationID"])
#     )

# # Đếm những dòng giống hệt một dòng đã xuất hiện trước đó
# checks["exact_duplicate_extra_rows"] = df.duplicated()

# results = []
# for name, mask in checks.items():
#     count = int(mask.sum())
#     results.append({
#         "check": name,
#         "count": count,
#         "percent": round(count / len(df) * 100, 4) if len(df) else 0,
#     })

#     # Lưu tối đa 10 ví dụ cho mỗi dấu hiệu
#     # df.loc[mask].head(10).to_csv(
#     #     REPORTS / f"examples_{name}.csv", index=False
#     # )

# # pd.DataFrame(results).to_csv(
# #     REPORTS / "quality_checks.csv", index=False
# # )

# print("\nKhoang thoi gian don khach:")
# print(pickup.min(), "->", pickup.max())
# # print(f"\nDa luu bao cao tai: {REPORTS}")

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

df = pd.read_parquet(
    ROOT / "data/raw/yellow_tripdata_2025-01.parquet"
)
zones = pd.read_csv(ROOT / "data/raw/taxi_zone_lookup.csv")

# 1. Số dòng, số cột và một vài dòng để quan sát
print(f"So dong: {len(df):,}")
print(f"So cot: {len(df.columns)}")
print(df.head(5).to_string(index=False))

# 2. Kiểu dữ liệu và tỷ lệ thiếu của từng cột
schema = pd.DataFrame({
    "dtype": df.dtypes.astype(str),
    "null_count": df.isna().sum(),
    "null_percent": (df.isna().mean() * 100).round(2),
})
schema.to_csv(REPORTS / "schema_and_nulls.csv")

# 3. Phân bố giá trị các cột số
df.describe(
    percentiles=[0.01, 0.5, 0.95, 0.99]
).to_csv(REPORTS / "value_summary.csv")

# 4. Các dấu hiệu cần điều tra; chưa kết luận là dữ liệu sai
pickup = df["tpep_pickup_datetime"]
dropoff = df["tpep_dropoff_datetime"]

has_times = pickup.notna() & dropoff.notna()
outside_month = pickup.notna() & (
    (pickup < pd.Timestamp("2025-01-01"))
    | (pickup >= pd.Timestamp("2025-02-01"))
)

checks = {
    "missing_pickup_time": pickup.isna(),
    "missing_dropoff_time": dropoff.isna(),
    "dropoff_before_pickup": has_times & (dropoff < pickup),
    "zero_duration": has_times & (dropoff == pickup),
    "pickup_outside_file_month": outside_month,
    "non_positive_distance": df["trip_distance"] <= 0,
    "negative_fare": df["fare_amount"] < 0,
    "negative_total": df["total_amount"] < 0,
}

for column in ["PULocationID", "DOLocationID"]:
    checks[f"{column}_missing"] = df[column].isna()
    checks[f"{column}_not_in_lookup"] = (
        df[column].notna()
        & ~df[column].isin(zones["LocationID"])
    )

# Đếm những dòng giống hệt một dòng đã xuất hiện trước đó
checks["exact_duplicate_extra_rows"] = df.duplicated()

results = []
for name, mask in checks.items():
    count = int(mask.sum())
    results.append({
        "check": name,
        "count": count,
        "percent": round(count / len(df) * 100, 4) if len(df) else 0,
    })

    # Lưu tối đa 10 ví dụ cho mỗi dấu hiệu
    df.loc[mask].head(10).to_csv(
        REPORTS / f"examples_{name}.csv", index=False
    )

pd.DataFrame(results).to_csv(
    REPORTS / "quality_checks.csv", index=False
)

print("\nKhoang thoi gian don khach:")
print(pickup.min(), "->", pickup.max())
print(f"\nDa luu bao cao tai: {REPORTS}")
