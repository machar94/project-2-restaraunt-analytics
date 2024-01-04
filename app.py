# This app is used to format the data from the csv files into a format that can be used by the database

import pandas as pd
import format
import random
import string
from tqdm import tqdm


primaryKeyLength = 16


#############
# Functions #
#############


def generateRandomString(length=primaryKeyLength) -> str:
    '''

    Generate a random string of length characters
    '''
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def assignBranchID(df: pd.DataFrame, verbose=False) -> pd.DataFrame:
    '''
    Assign a unique 16 character BranchID for each unique branch where branch is
    determined based on location.
    '''

    # Create a BranchID for each row
    df['BranchID'] = None

    if verbose:
        print('Assigning BranchIDs to each row')
        grouped = tqdm(df.groupby(
            ['FormattedLegalBusinessName', 'FormattedBusinessAddress'], dropna=True))
    else:
        grouped = df.groupby(
            ['FormattedLegalBusinessName', 'FormattedBusinessAddress'], dropna=True)

    # Iterate through location address and assign a BranchID
    # TODO: Update the groupby function
    for _, df_address in grouped:
        branch_id = generateRandomString()

        for index, _ in df_address.iterrows():
            df.loc[index, 'BranchID'] = branch_id

    return df


def formatOpenRestaurantApplications(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Clean, transform and normalize the data from the
    OpenRestaurantApplications.csv file
    '''

    # Rename LegalBusinessName to RawLegalBusinessName
    df = df.rename(columns={'Legal Business Name': 'RawLegalBusinessName'})

    # Standardize the LegalBusinessName
    df = format.standardizeString(
        df, {'RawLegalBusinessName': 'FormattedLegalBusinessName'})

    return df


def formatOpenRestaurantInspections(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Clean, transform and normalize the data from the
    OpenRestaurantInspections.csv file
    '''

    # Rename LegalBusinessName to RawLegalBusinessName
    df = df.rename(columns={'LegalBusinessName': 'RawLegalBusinessName'})

    # Standardize the LegalBusinessName
    df = format.standardizeString(
        df, {'RawLegalBusinessName': 'FormattedLegalBusinessName'})

    # Drop all rows where FormattedLegalBusinessName is null
    df = df.dropna(subset=['FormattedLegalBusinessName'])

    # Rename BusinessAddress to RawBusinessAddress
    df = df.rename(columns={'BusinessAddress': 'RawBusinessAddress'})

    # Standardize the BusinessAddress
    df = format.standardizeString(
        df, {'RawBusinessAddress': 'FormattedBusinessAddress'})

    # Assign a BranchID to each row
    df = assignBranchID(df, verbose=True)

    return df


def formatRestaurantInspections(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Clean, transform and normalize the data from the RestaurantInspections.csv
    file
    '''
    return df


def assembleTables(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    '''
    Assemble the tables from the dataframes
    '''
    tables = {}

    # Create businesses table
    df_businesses = pd.DataFrame()

    df = datasets['OpenRestaurantInspections']

    df_businesses['FormattedLegalBusinessName'] = df['FormattedLegalBusinessName']
    df_businesses['FormattedBusinessAddress'] = df['FormattedBusinessAddress']
    df_businesses['RawLegalBusinessName'] = df['RawLegalBusinessName']
    df_businesses['RawBusinessAddress'] = df['RawBusinessAddress']
    df_businesses['BranchID'] = df['BranchID']

    df_businesses.drop_duplicates(subset=['FormattedLegalBusinessName', 'FormattedBusinessAddress'], inplace=True)

    tables['businesses'] = df_businesses

    return tables


##############
# User Input #
##############


print('Select a dataset to format\n')
print('1. 1_OpenRestaurantApplications.csv')
print('2. 2_OpenRestaurantInspections.csv')
print('3. 3_RestaurantInspections.csv')
print('4. All datasets')

selection = 0
while selection < 1 or selection > 4:
    selection = input('\nPlease select a database to format (1-4): ')

    try:
        selection = int(selection)
        break
    except:
        pass


datasets = {}
if selection == 1 or selection == 4:
    datasets['OpenRestaurantApplications'] = pd.read_csv(
        'data/raw/1_OpenRestaurantApplications.csv')
elif selection == 2 or selection == 4:
    datasets['OpenRestaurantInspections'] = pd.read_csv(
        'data/raw/2_OpenRestaurantInspections.csv')
elif selection == 3 or selection == 4:
    datasets['RestaurantInspections'] = pd.read_csv(
        'data/raw/3_RestaurantInspections.csv')
else:
    print('Invalid format option selected')
    exit()


###############
# Format Rows #
###############


if 'OpenRestaurantApplications' in datasets:
    datasets['OpenRestaurantApplications'] = formatOpenRestaurantApplications(
        datasets['OpenRestaurantApplications'])
if 'OpenRestaurantInspections' in datasets:
    datasets['OpenRestaurantInspections'] = formatOpenRestaurantInspections(
        datasets['OpenRestaurantInspections'])
if 'RestaurantInspections' in datasets:
    datasets['RestaurantInspections'] = formatRestaurantInspections(
        datasets['RestaurantInspections'])


###################
# Assemble Tables #
###################

tables = assembleTables(datasets)

#################
# Write To Disk #
#################

print("\nWriting to disk in data/formatted ...")

for table_name, data in tables.items():
    data.to_csv(f'data/formatted/{table_name}.csv', index=False)
