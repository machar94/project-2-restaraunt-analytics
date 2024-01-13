# This app is used to format the data from the csv files into a format that can be used by the database


import argparse
import format
import os
import pandas as pd
import pytz
import random
import string
import yaml

from tqdm import tqdm
from typing import Union

###########
# GLOBALS #
###########

# Contains the configuration for the app
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config/config.yaml')

# Contains specific edits to the raw data
EDITS_PATH = os.path.join(os.path.dirname(__file__), 'data/raw/edits.yaml')

# Folder to write debug artifacts
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'debug')

# Name of the file to write the Restaurant table debug artifacts to
RESTAURANT_MATCH_PATH = os.path.join(OUTPUT_DIR, 'Restaurant_match.csv')

# Statistics about the data conversion
G_stats = {}


primaryKeyLength = 16


#############
# Functions #
#############


def matchRestaraunt(row: pd.Series, restaurants: list[dict]) -> Union[dict, None]:
    '''
    Check if a restaurant already exists in the list of restaurants. Match by street address.
    '''

    for restaurant in restaurants:
        if restaurant['StreetAddress'] == row['StreetAddress']:
            return restaurant

    return None


def mergeRestaurants(match: dict, row: pd.Series) -> None:
    '''
    Merge strategy is that values should be exactly the same. If they are not
    then raise an exception.
    '''

    unmatched_keys = []

    for key, value in match.items():

        # No need to check ID wiht Restaurant ID since it is the same
        if key == 'ID':
            continue
        elif key == 'Zipcode':
            if value != row['Postcode']:
                unmatched_keys.append(key)
                continue
        else:
            if value != row[key]:
                unmatched_keys.append(key)
    
    if len(unmatched_keys) > 0:
        raise Exception(f'Unmatched keys: {unmatched_keys}')


def fillRestaurantTable(tables: dict[str, pd.DataFrame], datasets: dict[str, pd.DataFrame], debug=False) -> None:
    '''
    Fill the Restaurant table
    '''

    if 'OpenRestaurantInspections' in datasets:

        if debug:
            # Remove Restaurant_match.csv if it exists using try and except
            try:
                os.remove(RESTAURANT_MATCH_PATH)
            except:
                pass

        df = datasets['OpenRestaurantInspections']

        # Allocate a column for RestaurantID
        df['RestaurantID'] = ''

        # Organize restaurants by zip code for quicker matching
        by_zip = {}

        # Iterate through rows and add to table if restarant does not exist
        for index, row in tqdm(df.iterrows()):

            if row['Postcode'] not in by_zip:
                by_zip[row['Postcode']] = []

            match = matchRestaraunt(row, by_zip[row['Postcode']])
            if match is not None:

                df.loc[index, 'RestaurantID'] = match['ID']

                try:
                    mergeRestaurants(match, row)
                except Exception as e:

                    print(e)

                    if debug:
                        with open('debug/Restaurant_match.csv', 'a') as file:

                            formatted_restaurant = {
                                'ID': match['ID'],
                                'Name': row['Name'],
                                'LegalBusinessName': row['LegalBusinessName'],
                                'StreetAddress': row['StreetAddress'],
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
                            }
                                                
                            file.write(e.args[0] + '\n')
                            pd.DataFrame([match, formatted_restaurant]).to_csv(
                                file, mode='a', index=False)
                            file.write('\n')
                    else:
                        print('Restaurant does not match')
                        exit()

                continue
            else:
                id = str(generateRandomBits(64))

                by_zip[row['Postcode']].append({
                    'ID': id,
                    'Name': row['Name'],
                    'LegalBusinessName': row['LegalBusinessName'],
                    'StreetAddress': row['StreetAddress'],
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

                df.loc[index, 'RestaurantID'] = id

        restaurants = [place for places in by_zip.values() for place in places]

        tables['Restaurant'] = pd.concat(
            [tables['Restaurant'], pd.DataFrame(restaurants)], ignore_index=True)


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


def fillTables(datasets: dict[str, pd.DataFrame], tables: dict[str, pd.DataFrame], debug=False) -> None:

    fillRestaurantTable(tables, datasets, debug)
    fillSidewalkInspectionTable(tables, datasets)


def editData(dfs: dict[str, pd.DataFrame], verbose=False, debug=False) -> dict[str, pd.DataFrame]:
    '''
    Make edits to the raw data
    '''

    # Open the file
    with open(EDITS_PATH) as file:
        raw_edits = yaml.load(file, Loader=yaml.FullLoader)

    num_edits = 0

    # Iterate through each edit
    for edit in raw_edits:
        if edit['key'] in dfs:
            for row in edit['row']:
                if row in dfs[edit['key']].index:
                    dfs[edit['key']].loc[row, edit['column']] = edit['value']
                    num_edits += 1

    # Test files can be reindexed after edits have been made
    for df in dfs.values():
        df.reset_index(inplace=True, drop=True)

    if verbose:
        print(f'\nMade {num_edits} edits to the raw data')

    if debug:
        for name, df in dfs.items():
            df.to_csv(os.path.join(OUTPUT_DIR, f'{name}_edit.csv'))

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


def formatOpenRestaurantInspections(df: pd.DataFrame, debug=False) -> pd.DataFrame:
    '''
    Clean, transform and normalize the data from the
    OpenRestaurantInspections.csv file
    '''

    ###########
    # Borough #
    ###########

    df['Borough'] = df['Borough'].str.upper()

    ###################
    # Restaurant Name #
    ###################

    df['Name'] = format.normalizeStrings(df['RestaurantName'])

    #######################
    # Legal Business Name #
    #######################

    df['LegalBusinessName'] = format.normalizeStrings(df['LegalBusinessName'])

    ####################
    # Business Address #
    ####################

    df['StreetAddress'] = format.normalizeAddress(
        df['BusinessAddress'], G_stats)

    ###############
    # InspectedOn #
    ###############

    # Validate that all values in InspectedOn are convertable to a datetime and
    # convert the date to UTC

    # Timezone in NYC
    eastern = pytz.timezone('US/Eastern')

    try:
        inspected_on = pd.to_datetime(
            df['InspectedOn'], format='%m/%d/%Y %I:%M:%S %p', exact=True, errors='raise').dt.tz_localize(eastern)
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

    #######
    # NTA #
    #######

    df['NTA'] = df['NTA'].str.upper()

    if debug:
        df.to_csv(os.path.join(
            OUTPUT_DIR, 'OpenRestaurantInspections_formatted.csv'))

    return df


def formatRestaurantInspections(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Clean, transform and normalize the data from the RestaurantInspections.csv
    file
    '''
    return df


def assembleTables(datasets: dict[str, pd.DataFrame], debug=False) -> dict[str, pd.DataFrame]:
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

    fillTables(datasets, tables, debug)

    return tables


if __name__ == '__main__':

    argparser = argparse.ArgumentParser(
        description='Format the data from the csv files into a format that can be used by the database')
    argparser.add_argument('dataset', metavar='dataset', type=str)
    argparser.add_argument('--debug', '-d', action='store_true', default=False,
                           help='Generate debug artifacts in debug/')
    argparser.add_argument('--verbose', '-v', action='store_true',
                           default=False, help='Print verbose output')
    args = argparser.parse_args()

    # Read the dataset argument and check if it is one of the available datasets
    # in the config file
    with open(CONFIG_PATH) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

        datasets = [dataset['name'] for dataset in config['dataset']]

        if args.dataset not in datasets:
            print(f'\nInvalid dataset option selected: {args.dataset}\n')

            print('Available datasets:')
            for dataset in datasets:
                print(f'\t- {dataset}')
            exit()
        else:
            files = config['dataset'][datasets.index(args.dataset)]['files']

    if args.debug:
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

    #################
    # Load datasets #
    #################

    # Test datasets contain an Index column

    datasets = {}

    for file in files:
        if 'OpenRestaurantApplications' in file:
            datasets['OpenRestaurantApplications'] = pd.read_csv(file)
            if 'Index' in datasets['OpenRestaurantApplications'].columns:
                datasets['OpenRestaurantApplications'].set_index(
                    'Index', inplace=True)
        elif 'OpenRestaurantInspections' in file:
            datasets['OpenRestaurantInspections'] = pd.read_csv(file)
            if 'Index' in datasets['OpenRestaurantInspections'].columns:
                datasets['OpenRestaurantInspections'].set_index(
                    'Index', inplace=True)
        elif 'RestaurantInspections' in file:
            datasets['RestaurantInspections'] = pd.read_csv(file)
            if 'Index' in datasets['RestaurantInspections'].columns:
                datasets['RestaurantInspections'].set_index(
                    'Index', inplace=True)

    ###############
    # Format Rows #
    ###############

    editData(datasets, verbose=args.verbose, debug=args.debug)

    if 'OpenRestaurantApplications' in datasets:
        datasets['OpenRestaurantApplications'] = formatOpenRestaurantApplications(
            datasets['OpenRestaurantApplications'])
    if 'OpenRestaurantInspections' in datasets:
        datasets['OpenRestaurantInspections'] = formatOpenRestaurantInspections(
            datasets['OpenRestaurantInspections'], args.debug)
    if 'RestaurantInspections' in datasets:
        datasets['RestaurantInspections'] = formatRestaurantInspections(
            datasets['RestaurantInspections'])

    ###################
    # Assemble Tables #
    ###################

    tables = assembleTables(datasets, args.debug)

    #################
    # Write To Disk #
    #################

    print("\nWriting to disk in data/formatted ...")

    for table_name, data in tables.items():
        data.to_csv(f'data/formatted/{table_name}.csv', index=False)

    # Pretty print the statistics
    print('\nStatistics:')
    for key, value in G_stats.items():
        print(f'\t{key}: {value}')
