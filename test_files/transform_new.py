# Import Packages
import pandas as pd
from sklearn import preprocessing
from pathlib import Path
import numpy as np

# TRANSFORM DATA
def transform_data(neonatal_csv_path, skilled_health_csv_path, antenatal_csv_path, maternal_mortality_csv_path, target_dir, to_parquet=False, to_csv=True):
    print('Transforming Data...')
    neonatal_df = pd.read_csv(neonatal_csv_path)
    skilled_health_df = pd.read_csv(skilled_health_csv_path)
    antenatal_df = pd.read_csv(antenatal_csv_path)
    maternal_mortality_df = pd.read_csv(maternal_mortality_csv_path)

    # -------------------------------------------------------------------------------------#
    ## FILL nan VALUES
    neonatal_df.fillna('N/A', inplace=True)
    skilled_health_df.fillna('N/A', inplace=True)
    antenatal_df.fillna('N/A', inplace=True)
    maternal_mortality_df.fillna('N/A', inplace=True)

    # -------------------------------------------------------------------------------------#
    ## YEAR TABLE
    all_years = []
    all_years += list(neonatal_df['Year'].unique())
    all_years += list(skilled_health_df['Year'].unique())
    all_years += list(antenatal_df['Year'].unique())
    all_years += list(maternal_mortality_df['Year'].unique())
    all_years = list(set(all_years))

    year_dim_df = pd.DataFrame(all_years, columns=['Year'])

    # create year_id
    year_dim_df['Year_Id'] = year_dim_df.index

    year_dim_df = year_dim_df[['Year_Id', 'Year']]


    # -------------------------------------------------------------------------------------#
    ## COUNTRY TABLE
    all_countries = []
    all_countries += list(neonatal_df['Country'].unique())
    all_countries += list(skilled_health_df['Country'].unique())
    all_countries += list(antenatal_df['Country'].unique())
    all_countries += list(maternal_mortality_df['Country'].unique())
    all_countries = list(set(all_countries))

    country_dim_df = pd.DataFrame(all_countries, columns=['Country'])

    # sort by alphabetical
    country_dim_df.sort_values(by=['Country'], inplace=True)

    # create country_id
    country_dim_df['Country_Id'] = country_dim_df.index

    country_dim_df = country_dim_df[['Country_Id', 'Country']]

    # -------------------------------------------------------------------------------------#
    ## WHO REGION TABLE
    all_regions = []
    all_regions += list(neonatal_df['WHO region'].unique())
    all_regions += list(skilled_health_df['WHO region'].unique())
    all_regions += list(antenatal_df['WHO region'].unique())
    all_regions += list(maternal_mortality_df['WHO region'].unique())
    all_regions = list(set(all_regions))

    region_dim_df = pd.DataFrame(all_regions, columns=['WHO_Region'])

    # sort by alphabetical
    region_dim_df.sort_values(by=['WHO_Region'], inplace=True)

    # create region_id
    region_dim_df['WHO_Region_Id'] = region_dim_df.index

    region_dim_df = region_dim_df[['WHO_Region_Id', 'WHO_Region']]

    # -------------------------------------------------------------------------------------#
    ## WORLD BANK INCOME GROUP TABLE
    all_income_groups = []
    all_income_groups += list(neonatal_df['World bank income group'].unique())
    all_income_groups += list(skilled_health_df['World bank income group'].unique())
    all_income_groups += list(antenatal_df['World bank income group'].unique())
    all_income_groups += list(maternal_mortality_df['World bank income group'].unique())
    all_income_groups = list(set(all_income_groups))

    income_dim_df = pd.DataFrame(all_income_groups, columns=['WB_Income_Group'])

    # sort by alphabetical
    income_dim_df.sort_values(by=['WB_Income_Group'], inplace=True)

    # create income_id
    income_dim_df['WB_Income_Group_Id'] = income_dim_df.index

    income_dim_df = income_dim_df[['WB_Income_Group_Id', 'WB_Income_Group']]


    # -------------------------------------------------------------------------------------#
    ## NEONATAL TABLE
    neonatal_dim_df = neonatal_df[['Indicator', 'Year', 'Country', 'WHO region', 'World bank income group', 'Value Numeric', 'Value low', 'Value high']]
    neonatal_dim_df = neonatal_dim_df.rename(columns={'Year': 'Year_Id', 'Country': 'Country_Id', 'WHO region': 'WHO_Region_Id', 'World bank income group':'WB_Income_Group_Id', 'Value Numeric':'Value', 'Value low':'Value_Low', 'Value high': 'Value_High'})

    neonatal_dim_df['NMR_Id'] = neonatal_dim_df.index

    neonatal_dim_df = neonatal_dim_df[['NMR_Id', 'Indicator', 'Year_Id', 'Country_Id', 'WHO_Region_Id', 'WB_Income_Group_Id', 'Value', 'Value_Low', 'Value_High']]

    # -------------------------------------------------------------------------------------#
    ## ANTENATAL TABLE
    antenatal_dim_df = antenatal_df[['Indicator', 'Year', 'Country', 'WHO region', 'World bank income group', 'Value Numeric']]
    antenatal_dim_df = antenatal_dim_df.rename(columns={'Year': 'Year_Id', 'Country': 'Country_Id', 'WHO region': 'WHO_Region_Id', 'World bank income group':'WB_Income_Group_Id', 'Value Numeric':'Value'})

    antenatal_dim_df['ACC_Id'] = antenatal_dim_df.index

    antenatal_dim_df = antenatal_dim_df[['ACC_Id', 'Indicator', 'Year_Id', 'Country_Id', 'WHO_Region_Id', 'WB_Income_Group_Id', 'Value']]

    # -------------------------------------------------------------------------------------#
    ## MATERNAL MORTALITY TABLE
    maternal_mortality_dim_df = maternal_mortality_df[['Indicator', 'Year', 'Country', 'WHO region', 'World bank income group', 'Value Numeric', 'Value low', 'Value high']]
    maternal_mortality_dim_df = maternal_mortality_dim_df.rename(columns={'Year': 'Year_Id', 'Country': 'Country_Id', 'WHO region': 'WHO_Region_Id', 'World bank income group':'WB_Income_Group_Id', 'Value Numeric':'Value', 'Value low':'Value_Low', 'Value high': 'Value_High'})

    maternal_mortality_dim_df['MMR_Id'] = maternal_mortality_dim_df.index

    maternal_mortality_dim_df = maternal_mortality_dim_df[['MMR_Id', 'Indicator', 'Year_Id', 'Country_Id', 'WHO_Region_Id', 'WB_Income_Group_Id', 'Value', 'Value_Low', 'Value_High']]


    # -------------------------------------------------------------------------------------#
    ## SKILLED HEALTH TABLE
    skilled_health_dim_df = skilled_health_df[['Indicator', 'Year', 'Country', 'WHO region', 'World bank income group', 'Value Numeric']]
    skilled_health_dim_df = skilled_health_dim_df.rename(columns={'Year': 'Year_Id', 'Country': 'Country_Id', 'WHO region': 'WHO_Region_Id', 'World bank income group':'WB_Income_Group_Id', 'Value Numeric':'Value'})

    skilled_health_dim_df['SHP_Id'] = skilled_health_dim_df.index

    skilled_health_dim_df = skilled_health_dim_df[['SHP_Id', 'Indicator', 'Year_Id', 'Country_Id', 'WHO_Region_Id', 'WB_Income_Group_Id', 'Value']]

    # -------------------------------------------------------------------------------------#
    ## HEALTHCARE DISPARITIES FACT TABLE
    healthcare_disparities_fct_df = pd.concat([neonatal_df, maternal_mortality_df, antenatal_df, skilled_health_df], ignore_index=True)
    healthcare_disparities_fct_df = healthcare_disparities_fct_df[['Year', 'Country', 'WHO region', 'World bank income group']]
    
    healthcare_disparities_fct_df = healthcare_disparities_fct_df.replace({'Year': dict(zip(year_dim_df.Year, year_dim_df.Year_Id))})
    healthcare_disparities_fct_df = healthcare_disparities_fct_df.replace({'Country': dict(zip(country_dim_df.Country, country_dim_df.Country_Id))})
    healthcare_disparities_fct_df = healthcare_disparities_fct_df.replace({'WHO region': dict(zip(region_dim_df.WHO_Region, region_dim_df.WHO_Region_Id))})
    healthcare_disparities_fct_df = healthcare_disparities_fct_df.replace({'World bank income group': dict(zip(income_dim_df.WB_Income_Group, income_dim_df.WB_Income_Group_Id))})
    
    healthcare_disparities_fct_df['NMR_Id']  = np.nan
    healthcare_disparities_fct_df['MMR_Id']  = np.nan
    healthcare_disparities_fct_df['ACC_Id']  = np.nan
    healthcare_disparities_fct_df['SHP_Id']  = np.nan
    
    healthcare_disparities_fct_df.fillna('N/A', inplace=True)

    
    nmr_index = len(neonatal_dim_df)
    mmr_index = len(maternal_mortality_df) + nmr_index
    acc_index = len(antenatal_dim_df) + mmr_index
    shp_index = len(skilled_health_dim_df) + acc_index
    
    healthcare_disparities_fct_df.loc[healthcare_disparities_fct_df.index[0:nmr_index], 'NMR_Id'] = neonatal_dim_df['NMR_Id']
    healthcare_disparities_fct_df.loc[healthcare_disparities_fct_df.index[nmr_index:mmr_index], 'MMR_Id'] = list(maternal_mortality_dim_df['MMR_Id'])
    healthcare_disparities_fct_df.loc[healthcare_disparities_fct_df.index[mmr_index:acc_index], 'ACC_Id'] = list(antenatal_dim_df['ACC_Id'])
    healthcare_disparities_fct_df.loc[healthcare_disparities_fct_df.index[acc_index:shp_index], 'SHP_Id'] = list(skilled_health_dim_df['SHP_Id'])

    
    healthcare_disparities_fct_df = healthcare_disparities_fct_df.rename(columns={'Year': 'Year_Id', 'Country': 'Country_Id', 'WHO region': 'WHO_Region_Id', 'World bank income group':'WB_Income_Group_Id'})
    
    healthcare_disparities_fct_df = healthcare_disparities_fct_df[['SHP_Id', 'MMR_Id', 'NMR_Id', 'ACC_Id', 'Year_Id', 'Country_Id', 'WHO_Region_Id', 'WB_Income_Group_Id']]

    # -------------------------------------------------------------------------------------#
    # SAVE TRANSFORMED DATAFRAMES
    Path(target_dir).mkdir(parents=True, exist_ok=True)

    if to_parquet == True:
        year_dim_df.to_parquet(target_dir+'/year_dim_df.parquet')
        country_dim_df.to_parquet(target_dir+'/country_dim_df.parquet')
        region_dim_df.to_parquet(target_dir+'/region_dim_df.parquet')
        income_dim_df.to_parquet(target_dir+'/income_dim_df.parquet')
        neonatal_dim_df.to_parquet(target_dir+'/neonatal_dim_df.parquet')
        antenatal_dim_df.to_parquet(target_dir+'/antenatal_dim_df.parquet')
        maternal_mortality_dim_df.to_parquet(target_dir+'/maternal_mortality_dim_df.parquet')
        skilled_health_dim_df.to_parquet(target_dir+'/skilled_health_dim_df.parquet')
        healthcare_disparities_fct_df.to_parquet(target_dir+'/healthcare_disparities_fct_df.parquet')

    if to_csv == True:
        year_dim_df.to_csv(target_dir+'/year_dim_df.csv', index=False)
        country_dim_df.to_csv(target_dir+'/country_dim_df.csv', index=False)
        region_dim_df.to_csv(target_dir+'/region_dim_df.csv', index=False)
        income_dim_df.to_csv(target_dir+'/income_dim_df.csv', index=False)
        neonatal_dim_df.to_csv(target_dir+'/neonatal_dim_df.csv', index=False)
        antenatal_dim_df.to_csv(target_dir+'/antenatal_dim_df.csv', index=False)
        maternal_mortality_dim_df.to_csv(target_dir+'/maternal_mortality_dim_df.csv', index=False)
        skilled_health_dim_df.to_csv(target_dir+'/skilled_health_dim_df.csv', index=False)
        healthcare_disparities_fct_df.to_csv(target_dir+'/healthcare_disparities_fct_df.csv', index=False)


    print('Transformation Complete')


## TEST
# neonatal_path = 'C:/Users/casey/OneDrive/Documents/MSDS_Courses/Fall_2023/CSCI-5283/Project/data/neonatal.csv'
# skilled_health_path = 'C:/Users/casey/OneDrive/Documents/MSDS_Courses/Fall_2023/CSCI-5283/Project/data/skilled_health.csv'
# antenatal_path = 'C:/Users/casey/OneDrive/Documents/MSDS_Courses/Fall_2023/CSCI-5283/Project/data/antenatal.csv'
# maternal_mortality_path = 'C:/Users/casey/OneDrive/Documents/MSDS_Courses/Fall_2023/CSCI-5283/Project/data/maternal_mortality.csv'
# transform_data(neonatal_path, skilled_health_path, antenatal_path, maternal_mortality_path, 'test/', to_csv=True)