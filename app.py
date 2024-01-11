# This app is used to format the data from the csv files into a format that can be used by the database


import format
import pandas as pd
import pytz
import random
import string
import yaml

from tqdm import tqdm
from typing import Union

primaryKeyLength = 16


#############
# Functions #
#############


def matchRestaraunt(row: pd.Series, df: pd.DataFrame) -> Union[pd.Series, None]:
    '''
    Check if a restaurant already exists in the Restaurant table
    '''

    return None


def fillRestaurantTable(tables: dict[str, pd.DataFrame], datasets: dict[str, pd.DataFrame]) -> None:
    '''
    Fill the Restaurant table
    '''

    if 'OpenRestaurantInspections' in datasets:
        df = datasets['OpenRestaurantInspections']

        to_add, restaurant_ids = [], []

        # Iterate through rows and add to table if restarant does not exist
        for _, row in tqdm(df.iterrows()):

            if matchRestaraunt(row, tables['Restaurant']):
                continue
            else:
                id = str(generateRandomBits(64))
                restaurant_ids.append(id)

                to_add.append({
                    'ID': id,
                    'Name': row['RestaurantName'],
                    'LegalBusinessName': row['FormattedLegalBusinessName'],
                    'StreetAddress': row['FormattedBusinessAddress'],
                    'Borough': row['Borough'],
                    'Zipcode': row['Postcode'],
                    'Latitude': row['Latitude'],
                    'Longitude': row['Longitude'],
                    'CommunityBoard': row['CommunityBoard'],
                    'CouncilDistrict': row['CouncilDistrict'],
                    'CensusTract': row['CensusTract'],
                    'BIN': row['BIN'],
                    'BBL': row['BBL'],
                    'NTA': row['NTA']
                })

        tables['Restaurant'] = pd.concat(
            [tables['Restaurant'], pd.DataFrame(to_add)], ignore_index=True)
        datasets['OpenRestaurantInspections']['RestaurantID'] = restaurant_ids


def fillSidewalkInspectionTable(tables: dict[str, pd.DataFrame], df: pd.DataFrame) -> None:
    '''
    Fill the Restaurant table
    '''

    if 'OpenRestaurantInspections' in datasets:
        df = datasets['OpenRestaurantInspections']

        to_add = []

        for _, row in tqdm(df.iterrows()):

            to_add.append({
                'ID': str(generateRandomBits(64)),
                'RestaurantID': row['RestaurantID'],
                'InspectedOn': row['InspectedOn'],
                'SidewayCompliant': row['SidewayCompliant'],
                'SkippedReason': row['SkippedReason'],
                'AgencyCode': row['AgencyCode'],
            })

        tables['SidewalkInspection'] = pd.concat(
        [tables['SidewalkInspection'], pd.DataFrame(to_add)], ignore_index=True)


def fillTables(datasets: dict[str, pd.DataFrame], tables: dict[str, pd.DataFrame]) -> None:

    fillRestaurantTable(tables, datasets)
    fillSidewalkInspectionTable(tables, datasets)


def editData(filename: str, dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    '''
    Make edits to the raw data
    '''

    # Open the file
    with open(filename) as file:
        raw_edits = yaml.load(file, Loader=yaml.FullLoader)

    # Iterate through each edit
    for edit in raw_edits:

        # Convert X_name.csv to name
        key = edit['file'].split('_')[1].split('.')[0]

        if key in dfs:
            for row in edit['row']:
                dfs[key].loc[row, edit['column']] = edit['value']

    return dfs


def generateRandomString(length=primaryKeyLength) -> str:
    '''
    Generate a random string of length characters
    '''
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generateRandomBits(length: int) -> int:
    '''
    Generate a random number
    '''

    return random.randint(0, 2**length-1)


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

    ###################
    # Restaurant Name #
    ###################

    # Standardize the RestaurantName
    df = format.standardizeString(
        df, {'RestaurantName': 'FormattedRestaurantName'})

    ###############
    # InspectedOn #
    ###############

    # Validate that all values in InspectedOn are convertable to a datetime and
    # convert the date to UTC

    # Timezone in NYC
    eastern = pytz.timezone('US/Eastern')

    try:
        inspected_on = pd.to_datetime(
            df['InspectedOn'], errors='raise').dt.tz_localize(eastern)
        df['InspectedOn'] = inspected_on.dt.tz_convert(pytz.utc)
    except:
        print('Cannot convert all values in InspectedOn to a date')

    #########################################
    # IsRoadwayCompliant/IsSidewayCompliant #
    #########################################

    # Drop IsSideWalkCompliant and rename IsRoadwayCompliant to IsSidewayCompliant
    df = df.drop(columns=['IsSidewayCompliant'])
    df = df.rename(columns={'IsRoadwayCompliant': 'SidewayCompliant'})

    #################
    # SkippedReason #
    #################

    # Replaced null values with 'N/A'
    df['SkippedReason'] = df['SkippedReason'].fillna('NA')

    ###############
    # Agency Code #
    ###############

    # Do nothing. Don't want to fill in null values with 'NA' because it will be
    # confusing when 3 and 4 letter codes

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
    # df = assignBranchID(df, verbose=True)

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

    # Create a DataFrame from a list of column names and types
    tables['SidewalkInspection'] = pd.DataFrame({
        'ID': pd.Series(dtype='string'),
        'RestaurantID': pd.Series(dtype='string'),
        'InspectedOn': pd.Series(dtype='datetime64[ns, UTC]'),
        'SidewayCompliant': pd.Series(dtype='string'),
        'SkippedReason': pd.Series(dtype='string'),
        'AgencyCode': pd.Series(dtype='string')})

    tables['Restaurant'] = pd.DataFrame({
        'ID': pd.Series(dtype='string'),
        'DBA': pd.Series(dtype='string'),
        'Name': pd.Series(dtype='string'),
        'LegalBusinessName': pd.Series(dtype='string'),
        'StreetAddress': pd.Series(dtype='string'),
        'Borough': pd.Series(dtype='string'),
        'Zipcode': pd.Series(dtype='int32'),
        'FoodServicePermit': pd.Series(dtype='int32'),
        'IsPermittedToSellAlcohol': pd.Series(dtype='boolean'),
        'SLASerialNumber': pd.Series(dtype='string'),
        'SLALicenseType': pd.Series(dtype='string'),
        'IsLandmark': pd.Series(dtype='boolean'),
        'HasAgreedToLandmarkTerms': pd.Series(dtype='boolean'),
        'Latitude': pd.Series(dtype='float64'),
        'Longitude': pd.Series(dtype='float64'),
        'CommunityBoard': pd.Series(dtype='int32'),
        'CouncilDistrict': pd.Series(dtype='int32'),
        'CensusTract': pd.Series(dtype='int32'),
        'BIN': pd.Series(dtype='int32'),
        'BBL': pd.Series(dtype='int32'),
        'NTA': pd.Series(dtype='string'),
        'CAMIS': pd.Series(dtype='int32'),
        'Phone': pd.Series(dtype='string'),
        'Cuisine': pd.Series(dtype='string')})

    fillTables(datasets, tables)

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

editData('data/raw/edits.yaml', datasets)

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
