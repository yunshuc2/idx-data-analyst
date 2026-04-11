import pandas as pd

sold_202603 = pd.read_csv(r"/Users/yunshucao/Downloads/CRMLSSold202603.csv")
sold_202602 = pd.read_csv(r"/Users/yunshucao/Downloads/CRMLSSold202602 (1).csv")
listing_202603 = pd.read_csv(r"/Users/yunshucao/Downloads/CRMLSListing202603.csv")
listing_202602 = pd.read_csv(r"/Users/yunshucao/Downloads/CRMLSListing202602 (1).csv")

print("sold_202603 rows:", len(sold_202603))
print("sold_202602 rows:", len(sold_202602))
print("listing_202603 rows:", len(listing_202603))
print("listing_202602 rows:", len(listing_202602))

sold_202603["SourceMonth"] = "2026-03"
sold_202602["SourceMonth"] = "2026-02"
listing_202603["SourceMonth"] = "2026-03"
listing_202602["SourceMonth"] = "2026-02"

sold_all = pd.concat([sold_202602, sold_202603], ignore_index=True)
listing_all = pd.concat([listing_202602, listing_202603], ignore_index=True)

sold_keep = [
    "SourceMonth",
    "ListingId",
    "ListingKey",
    "ListingKeyNumeric",
    "CloseDate",
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate",
    "MlsStatus",
    "PropertyType",
    "PropertySubType",
    "UnparsedAddress",
    "City",
    "StateOrProvince",
    "PostalCode",
    "CountyOrParish",
    "Latitude",
    "Longitude",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "LivingArea",
    "BuildingAreaTotal",
    "LotSizeSquareFeet",
    "LotSizeAcres",
    "LotSizeArea",
    "YearBuilt",
    "DaysOnMarket",
    "GarageSpaces",
    "ParkingTotal",
    "Stories",
    "Levels",
    "FireplaceYN",
    "FireplacesTotal",
    "PoolPrivateYN",
    "WaterfrontYN",
    "ViewYN",
    "NewConstructionYN",
    "SubdivisionName",
    "MLSAreaMajor",
    "TaxAnnualAmount",
    "TaxYear",
    "ListOfficeName",
    "BuyerOfficeName",
    "BuyerAgentFirstName",
    "BuyerAgentLastName",
    "ListAgentFirstName",
    "ListAgentLastName",
    "ListAgentFullName"
]

listing_keep = [
    "SourceMonth",
    "ListingId",
    "ListingKey",
    "ListingKeyNumeric",
    "ListingContractDate",
    "ContractStatusChangeDate",
    "CloseDate",
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "MlsStatus",
    "PropertyType",
    "PropertySubType",
    "UnparsedAddress",
    "City",
    "StateOrProvince",
    "PostalCode",
    "CountyOrParish",
    "Latitude",
    "Longitude",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "LivingArea",
    "BuildingAreaTotal",
    "LotSizeSquareFeet",
    "LotSizeAcres",
    "LotSizeArea",
    "YearBuilt",
    "DaysOnMarket",
    "GarageSpaces",
    "ParkingTotal",
    "Stories",
    "Levels",
    "FireplaceYN",
    "FireplacesTotal",
    "PoolPrivateYN",
    "WaterfrontYN",
    "ViewYN",
    "NewConstructionYN",
    "SubdivisionName",
    "MLSAreaMajor",
    "TaxAnnualAmount",
    "TaxYear",
    "ListOfficeName",
    "BuyerOfficeName",
    "BuyerAgentFirstName",
    "BuyerAgentLastName",
    "ListAgentFirstName",
    "ListAgentLastName",
    "ListAgentFullName"
]

sold_clean = sold_all[[col for col in sold_keep if col in sold_all.columns]].copy()
listing_clean = listing_all[[col for col in listing_keep if col in listing_all.columns]].copy()

sold_clean = sold_clean.dropna(how="all")
listing_clean = listing_clean.dropna(how="all")

sold_numeric_cols = [
    "ClosePrice", "ListPrice", "OriginalListPrice",
    "BedroomsTotal", "BathroomsTotalInteger",
    "LivingArea", "BuildingAreaTotal",
    "LotSizeSquareFeet", "LotSizeAcres", "LotSizeArea",
    "YearBuilt", "DaysOnMarket",
    "GarageSpaces", "ParkingTotal",
    "Stories", "FireplacesTotal",
    "Latitude", "Longitude",
    "TaxAnnualAmount", "TaxYear"
]

listing_numeric_cols = [
    "ClosePrice", "ListPrice", "OriginalListPrice",
    "BedroomsTotal", "BathroomsTotalInteger",
    "LivingArea", "BuildingAreaTotal",
    "LotSizeSquareFeet", "LotSizeAcres", "LotSizeArea",
    "YearBuilt", "DaysOnMarket",
    "GarageSpaces", "ParkingTotal",
    "Stories", "FireplacesTotal",
    "Latitude", "Longitude",
    "TaxAnnualAmount", "TaxYear"
]

for col in sold_numeric_cols:
    if col in sold_clean.columns:
        sold_clean[col] = pd.to_numeric(sold_clean[col], errors="coerce")

for col in listing_numeric_cols:
    if col in listing_clean.columns:
        listing_clean[col] = pd.to_numeric(listing_clean[col], errors="coerce")

sold_date_cols = [
    "CloseDate",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate"
]

listing_date_cols = [
    "CloseDate",
    "ListingContractDate",
    "ContractStatusChangeDate"
]

for col in sold_date_cols:
    if col in sold_clean.columns:
        sold_clean[col] = pd.to_datetime(sold_clean[col], errors="coerce")

for col in listing_date_cols:
    if col in listing_clean.columns:
        listing_clean[col] = pd.to_datetime(listing_clean[col], errors="coerce")

if "ListingId" in sold_clean.columns:
    sold_clean = sold_clean.drop_duplicates(subset=["ListingId", "SourceMonth"])
else:
    sold_clean = sold_clean.drop_duplicates()

if "ListingId" in listing_clean.columns:
    listing_clean = listing_clean.drop_duplicates(subset=["ListingId", "SourceMonth"])
else:
    listing_clean = listing_clean.drop_duplicates()

sold_clean.to_csv("sold_clean_202602_202603.csv", index=False)
listing_clean.to_csv("listing_clean_202602_202603.csv", index=False)

print("\nDone!")
print("sold_clean rows:", len(sold_clean))
print("listing_clean rows:", len(listing_clean))

print("\nSold clean columns:")
print(sold_clean.columns.tolist())

print("\nListing clean columns:")
print(listing_clean.columns.tolist())

print("\nSaved files:")
print("sold_clean_202602_202603.csv")
print("listing_clean_202602_202603.csv")