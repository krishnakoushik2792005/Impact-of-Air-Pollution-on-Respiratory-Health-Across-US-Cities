import pandas as pd

# ---------- STEP 1: Load EPA datasets (2015–2020) ----------
epa_2015 = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\epa\annual_conc_by_monitor_2015.csv")
epa_2016 = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\epa\annual_conc_by_monitor_2016.csv")
epa_2017 = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\epa\annual_conc_by_monitor_2017.csv")
epa_2018 = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\epa\annual_conc_by_monitor_2018.csv")
epa_2019 = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\epa\annual_conc_by_monitor_2019.csv")
epa_2020 = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\epa\annual_conc_by_monitor_2020.csv")

# Combine all EPA datasets
epa_data = pd.concat([epa_2015, epa_2016, epa_2017, epa_2018, epa_2019, epa_2020], ignore_index=True)
print(f"✅ EPA data loaded and combined. Shape: {epa_data.shape}")

# Rename EPA columns
epa_data.rename(columns={
    'Year': 'Year',
    'State Name': 'State',
    'County Name': 'County',
    'Arithmetic Mean': 'EPA_PM25_Mean',
    '1st Max Value': 'EPA_PM25_Max',
    '99th Percentile': 'EPA_PM25_99th_Percentile'
}, inplace=True)

# Strip spaces and fix datatypes
epa_data['State'] = epa_data['State'].str.strip()
epa_data['County'] = epa_data['County'].str.strip()
epa_data['Year'] = epa_data['Year'].astype(int)

# ---------- STEP 2: Load health datasets ----------
asthma = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\asthma.csv")
pollution = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\pollution.csv")
copd = pd.read_csv(r"C:\Users\amare\OneDrive\Desktop\final\final ds\copd.csv")

# Rename and clean health datasets
asthma.rename(columns={'Value': 'Asthma_Rate'}, inplace=True)
pollution.rename(columns={'Value': 'Pollution_Index'}, inplace=True)
copd.rename(columns={'Value': 'COPD_Rate'}, inplace=True)

for df in [asthma, pollution, copd]:
    df['State'] = df['State'].str.strip()
    df['County'] = df['County'].str.strip()
    df['Year'] = df['Year'].astype(int)

# ---------- STEP 3: Reduce EPA data to only counties/years present in health datasets ----------
# First filter by asthma data
epa_reduced = epa_data.merge(asthma[['State', 'County', 'Year']], on=['State', 'County', 'Year'], how='inner')
# Further filter by COPD data
epa_reduced = epa_reduced.merge(copd[['State', 'County', 'Year']], on=['State', 'County', 'Year'], how='inner')
# Further filter by Pollution data
epa_reduced = epa_reduced.merge(pollution[['State', 'County', 'Year']], on=['State', 'County', 'Year'], how='inner')

print(f"✅ EPA data reduced to matching rows. Shape: {epa_reduced.shape}")

# ---------- STEP 4: Final merge with health values ----------
merged = epa_reduced.merge(
    asthma[['State', 'County', 'Year', 'Asthma_Rate']],
    on=['State', 'County', 'Year'],
    how='left'
)

merged = merged.merge(
    pollution[['State', 'County', 'Year', 'Pollution_Index']],
    on=['State', 'County', 'Year'],
    how='left'
)

merged = merged.merge(
    copd[['State', 'County', 'Year', 'COPD_Rate']],
    on=['State', 'County', 'Year'],
    how='left'
)

# ---------- STEP 5: Check for missing values (should be minimal or none) ----------
missing_values = merged[['Asthma_Rate', 'Pollution_Index', 'COPD_Rate']].isna().sum()
print(f"\n🔍 Missing values after filtering and merging:\n{missing_values}")

# ---------- STEP 6: Save the final merged dataset ----------
output_path = r"C:\Users\amare\OneDrive\Desktop\final\final ds\merged_dataset_clean.csv"
merged.to_csv(output_path, index=False)
print(f"\n✅ Final cleaned merged dataset saved successfully at {output_path}")

# ---------- STEP 7: Preview the saved dataset ----------
df = pd.read_csv(output_path, low_memory=False)
print("\n🔍 First 5 rows of the merged dataset:\n")
print(df.head())
