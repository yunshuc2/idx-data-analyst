import pandas as pd
import numpy as np


sold_path = "/Users/yunshucao/Desktop/yunshu/IDXIntern/sold_clean_202602_202603.csv"
listing_path = "/Users/yunshucao/Desktop/yunshu/IDXIntern/listing_clean_202602_202603.csv"

sold = pd.read_csv(sold_path)
listing = pd.read_csv(listing_path)

print("=== ORIGINAL SHAPES ===")
print("Sold shape before cleaning:", sold.shape)
print("Listing shape before cleaning:", listing.shape)


# 2. Helper functions

def convert_datetime_columns(df, cols, df_name):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            print(f"[{df_name}] Converted {col} to datetime")
    return df


def convert_numeric_columns(df, cols, df_name):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            print(f"[{df_name}] Converted {col} to numeric")
    return df


def drop_all_null_columns(df, df_name):
    before_cols = df.shape[1]
    df = df.dropna(axis=1, how="all")
    after_cols = df.shape[1]
    print(f"[{df_name}] Dropped {before_cols - after_cols} all-null columns")
    return df


def add_coordinate_flags(df, df_name):
    if "Latitude" in df.columns and "Longitude" in df.columns:
        df["missing_coord_flag"] = df["Latitude"].isna() | df["Longitude"].isna()
        df["zero_coord_flag"] = (df["Latitude"] == 0) | (df["Longitude"] == 0)
        df["positive_longitude_flag"] = df["Longitude"] > 0

        # Approximate California bounds check
        df["implausible_coord_flag"] = (
            (df["Latitude"] < 32) | (df["Latitude"] > 43) |
            (df["Longitude"] < -125) | (df["Longitude"] > -114)
        )

        print(f"[{df_name}] Added geographic flags")
    else:
        print(f"[{df_name}] Latitude/Longitude columns not found; skipped geographic flags")
    return df


def print_flag_summary(df, flag_cols, df_name):
    print(f"\n=== {df_name} FLAG SUMMARY ===")
    for col in flag_cols:
        if col in df.columns:
            print(f"{col}: {int(df[col].sum())}")


def create_missing_summary(df, df_name):
    missing_summary = pd.DataFrame({
        "column": df.columns,
        "missing_count": df.isna().sum().values,
        "missing_pct": (df.isna().mean().values * 100).round(2),
        "dtype": df.dtypes.astype(str).values
    }).sort_values(by=["missing_count", "column"], ascending=[False, True])

    print(f"\n=== {df_name} TOP 20 MISSING VALUE SUMMARY ===")
    print(missing_summary.head(20).to_string(index=False))

    return missing_summary


def print_shape_change(before_rows, before_cols, after_rows, after_cols, df_name):
    print(f"\n=== {df_name} SHAPE CHANGE ===")
    print(f"Before cleaning: {before_rows} rows, {before_cols} columns")
    print(f"After cleaning:  {after_rows} rows, {after_cols} columns")
    print(f"Rows removed:    {before_rows - after_rows}")
    print(f"Columns removed: {before_cols - after_cols}")


sold_before_rows, sold_before_cols = sold.shape

sold_date_cols = [
    "CloseDate",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate"
]
sold = convert_datetime_columns(sold, sold_date_cols, "SOLD")

sold_numeric_cols = [
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "LivingArea",
    "LotSizeAcres",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "DaysOnMarket",
    "Latitude",
    "Longitude"
]
sold = convert_numeric_columns(sold, sold_numeric_cols, "SOLD")

sold = drop_all_null_columns(sold, "SOLD")

if "ClosePrice" in sold.columns:
    sold["invalid_closeprice_flag"] = sold["ClosePrice"] <= 0
if "LivingArea" in sold.columns:
    sold["invalid_livingarea_flag"] = sold["LivingArea"] <= 0
if "DaysOnMarket" in sold.columns:
    sold["invalid_dom_flag"] = sold["DaysOnMarket"] < 0
if "BedroomsTotal" in sold.columns:
    sold["invalid_bedrooms_flag"] = sold["BedroomsTotal"] < 0
if "BathroomsTotalInteger" in sold.columns:
    sold["invalid_bathrooms_flag"] = sold["BathroomsTotalInteger"] < 0

# Date consistency flags
if "ListingContractDate" in sold.columns and "CloseDate" in sold.columns:
    sold["listing_after_close_flag"] = sold["ListingContractDate"] > sold["CloseDate"]

if "PurchaseContractDate" in sold.columns and "CloseDate" in sold.columns:
    sold["purchase_after_close_flag"] = sold["PurchaseContractDate"] > sold["CloseDate"]

if (
    "ListingContractDate" in sold.columns and
    "PurchaseContractDate" in sold.columns and
    "CloseDate" in sold.columns
):
    sold["negative_timeline_flag"] = (
        (sold["ListingContractDate"] > sold["PurchaseContractDate"]) |
        (sold["PurchaseContractDate"] > sold["CloseDate"])
    )

sold = add_coordinate_flags(sold, "SOLD")

sold_missing_summary = create_missing_summary(sold, "SOLD")

sold_cleaned = sold.copy()

if "ClosePrice" in sold_cleaned.columns:
    sold_cleaned = sold_cleaned[
        sold_cleaned["ClosePrice"].isna() | (sold_cleaned["ClosePrice"] > 0)
    ]

if "LivingArea" in sold_cleaned.columns:
    sold_cleaned = sold_cleaned[
        sold_cleaned["LivingArea"].isna() | (sold_cleaned["LivingArea"] > 0)
    ]

if "DaysOnMarket" in sold_cleaned.columns:
    sold_cleaned = sold_cleaned[
        sold_cleaned["DaysOnMarket"].isna() | (sold_cleaned["DaysOnMarket"] >= 0)
    ]

if "BedroomsTotal" in sold_cleaned.columns:
    sold_cleaned = sold_cleaned[
        sold_cleaned["BedroomsTotal"].isna() | (sold_cleaned["BedroomsTotal"] >= 0)
    ]

if "BathroomsTotalInteger" in sold_cleaned.columns:
    sold_cleaned = sold_cleaned[
        sold_cleaned["BathroomsTotalInteger"].isna() | (sold_cleaned["BathroomsTotalInteger"] >= 0)
    ]

sold_after_rows, sold_after_cols = sold_cleaned.shape
print_shape_change(sold_before_rows, sold_before_cols, sold_after_rows, sold_after_cols, "SOLD")


listing_before_rows, listing_before_cols = listing.shape

listing_date_cols = [
    "ListingContractDate",
    "PurchaseContractDate",
    "CloseDate",
    "ContractStatusChangeDate"
]
listing = convert_datetime_columns(listing, listing_date_cols, "LISTING")

listing_numeric_cols = [
    "ListPrice",
    "OriginalListPrice",
    "ClosePrice",
    "LivingArea",
    "LotSizeAcres",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "DaysOnMarket",
    "Latitude",
    "Longitude"
]
listing = convert_numeric_columns(listing, listing_numeric_cols, "LISTING")

listing = drop_all_null_columns(listing, "LISTING")

if "ListPrice" in listing.columns:
    listing["invalid_listprice_flag"] = listing["ListPrice"] <= 0
if "OriginalListPrice" in listing.columns:
    listing["invalid_original_listprice_flag"] = listing["OriginalListPrice"] <= 0
if "ClosePrice" in listing.columns:
    listing["invalid_closeprice_flag"] = listing["ClosePrice"] <= 0
if "LivingArea" in listing.columns:
    listing["invalid_livingarea_flag"] = listing["LivingArea"] <= 0
if "DaysOnMarket" in listing.columns:
    listing["invalid_dom_flag"] = listing["DaysOnMarket"] < 0
if "BedroomsTotal" in listing.columns:
    listing["invalid_bedrooms_flag"] = listing["BedroomsTotal"] < 0
if "BathroomsTotalInteger" in listing.columns:
    listing["invalid_bathrooms_flag"] = listing["BathroomsTotalInteger"] < 0
if "ListingContractDate" in listing.columns and "CloseDate" in listing.columns:
    listing["listing_after_close_flag"] = listing["ListingContractDate"] > listing["CloseDate"]

if "PurchaseContractDate" in listing.columns and "CloseDate" in listing.columns:
    listing["purchase_after_close_flag"] = listing["PurchaseContractDate"] > listing["CloseDate"]

if (
    "ListingContractDate" in listing.columns and
    "PurchaseContractDate" in listing.columns and
    "CloseDate" in listing.columns
):
    listing["negative_timeline_flag"] = (
        (listing["ListingContractDate"] > listing["PurchaseContractDate"]) |
        (listing["PurchaseContractDate"] > listing["CloseDate"])
    )

listing = add_coordinate_flags(listing, "LISTING")

listing_missing_summary = create_missing_summary(listing, "LISTING")

listing_cleaned = listing.copy()

if "ListPrice" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["ListPrice"].isna() | (listing_cleaned["ListPrice"] > 0)
    ]

if "OriginalListPrice" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["OriginalListPrice"].isna() | (listing_cleaned["OriginalListPrice"] > 0)
    ]

if "ClosePrice" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["ClosePrice"].isna() | (listing_cleaned["ClosePrice"] > 0)
    ]

if "LivingArea" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["LivingArea"].isna() | (listing_cleaned["LivingArea"] > 0)
    ]

if "DaysOnMarket" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["DaysOnMarket"].isna() | (listing_cleaned["DaysOnMarket"] >= 0)
    ]

if "BedroomsTotal" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["BedroomsTotal"].isna() | (listing_cleaned["BedroomsTotal"] >= 0)
    ]

if "BathroomsTotalInteger" in listing_cleaned.columns:
    listing_cleaned = listing_cleaned[
        listing_cleaned["BathroomsTotalInteger"].isna() | (listing_cleaned["BathroomsTotalInteger"] >= 0)
    ]

listing_after_rows, listing_after_cols = listing_cleaned.shape
print_shape_change(
    listing_before_rows,
    listing_before_cols,
    listing_after_rows,
    listing_after_cols,
    "LISTING"
)


sold_flag_cols = [
    "invalid_closeprice_flag",
    "invalid_livingarea_flag",
    "invalid_dom_flag",
    "invalid_bedrooms_flag",
    "invalid_bathrooms_flag",
    "listing_after_close_flag",
    "purchase_after_close_flag",
    "negative_timeline_flag",
    "missing_coord_flag",
    "zero_coord_flag",
    "positive_longitude_flag",
    "implausible_coord_flag"
]

listing_flag_cols = [
    "invalid_listprice_flag",
    "invalid_original_listprice_flag",
    "invalid_closeprice_flag",
    "invalid_livingarea_flag",
    "invalid_dom_flag",
    "invalid_bedrooms_flag",
    "invalid_bathrooms_flag",
    "listing_after_close_flag",
    "purchase_after_close_flag",
    "negative_timeline_flag",
    "missing_coord_flag",
    "zero_coord_flag",
    "positive_longitude_flag",
    "implausible_coord_flag"
]

print_flag_summary(sold, sold_flag_cols, "SOLD")
print_flag_summary(listing, listing_flag_cols, "LISTING")

print("\n=== SOLD DTYPES SAMPLE ===")
print(sold_cleaned.dtypes.head(20))

print("\n=== LISTING DTYPES SAMPLE ===")
print(listing_cleaned.dtypes.head(20))


sold_flagged_output = "/Users/yunshucao/Desktop/yunshu/IDXIntern/sold_week4_flagged.csv"
sold_cleaned_output = "/Users/yunshucao/Desktop/yunshu/IDXIntern/sold_week4_cleaned.csv"
sold_missing_output = "/Users/yunshucao/Desktop/yunshu/IDXIntern/sold_week4_missing_summary.csv"

listing_flagged_output = "/Users/yunshucao/Desktop/yunshu/IDXIntern/listing_week4_flagged.csv"
listing_cleaned_output = "/Users/yunshucao/Desktop/yunshu/IDXIntern/listing_week4_cleaned.csv"
listing_missing_output = "/Users/yunshucao/Desktop/yunshu/IDXIntern/listing_week4_missing_summary.csv"

sold.to_csv(sold_flagged_output, index=False)
sold_cleaned.to_csv(sold_cleaned_output, index=False)
sold_missing_summary.to_csv(sold_missing_output, index=False)

listing.to_csv(listing_flagged_output, index=False)
listing_cleaned.to_csv(listing_cleaned_output, index=False)
listing_missing_summary.to_csv(listing_missing_output, index=False)

print("\n=== FILES SAVED ===")
print("Sold flagged: ", sold_flagged_output)
print("Sold cleaned: ", sold_cleaned_output)
print("Sold missing summary: ", sold_missing_output)
print("Listing flagged: ", listing_flagged_output)
print("Listing cleaned: ", listing_cleaned_output)
print("Listing missing summary: ", listing_missing_output)