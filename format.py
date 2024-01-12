import pandas as pd
import string
import scourgify

from tqdm import tqdm
from typing import Dict

keep_punctuation = ['&', '(', ')', '/']


def standardizeString(df: pd.DataFrame, subset: Dict[str, str]) -> pd.DataFrame:
    '''
    For each column specified by name in subset:

    1. All letters are lowercased
    2. Convert dangling encodings (é, ó, amp&, etc.) to (e, o, &, etc.)
    3. All white space is removed from rows
    4. All puncuation except '(', ')', '&', '/' is removed from rows

    Args:
        df: The dataframe to be formatted
        subset: {current_column_name: new_column_name}
    '''

    # Remove keep punctuation from string.punctuation
    remove_punctuation = string.punctuation
    for p in keep_punctuation:
        remove_punctuation = remove_punctuation.replace(p, '')

    for c1, c2 in subset.items():

        df[c2] = df[c1].str.lower()

        # Replace instances of é with e
        df[c2] = df[c2].str.replace('+?', 'e')

        # Replace instances of ó with o
        df[c2] = df[c2].str.replace('+ae', 'o')

        # Replace instances of â with a
        df[c2] = df[c2].str.replace('+e', 'a')

        # Replace instances of á with a
        df[c2] = df[c2].str.replace('+u', 'a')

        # Example v+isquez to vasquez
        df[c2] = df[c2].str.replace('+i', 'a')

        # Example Frank-?s to Frank's
        df[c2] = df[c2].str.replace('-?s', "'s")

        # Hurley;s to Hurley's
        df[c2] = df[c2].str.replace('y;s', "'s")

        df[c2] = df[c2].str.replace('&amp;', '&')

        # Remove all white space from value
        df[c2] = df[c2].str.replace(' ', '')

        df[c2] = df[c2].str.replace('-', '')
        df[c2] = df[c2].str.replace('.', '')
        df[c2] = df[c2].str.replace(',', '')
        df[c2] = df[c2].str.replace("'", '')
        df[c2] = df[c2].str.replace('#', '')
        df[c2] = df[c2].str.replace(':', '')
        df[c2] = df[c2].str.replace('*', '')
        df[c2] = df[c2].str.replace(';', '')
        df[c2] = df[c2].str.replace('+', '')
        df[c2] = df[c2].str.replace('@', '')
        df[c2] = df[c2].str.replace('!', '')
        df[c2] = df[c2].str.replace('~', '')
        df[c2] = df[c2].str.replace('[', '')
        df[c2] = df[c2].str.replace(']', '')
        df[c2] = df[c2].str.replace('_', '')
        df[c2] = df[c2].str.replace('%', '')
        df[c2] = df[c2].str.replace('?', '')

        # # Remove remaining punctuation from value
        # for p in remove_punctuation:
        #     df[c2] = df[c2].str.replace(p, '')

    # Open a file to write to
    # f = open('output.txt', 'w')

    # for index, v in df[c2].items():

    #     try:
    #         # if not pd.isnull(v) and any(c in [''] for c in v):
    #         if not pd.isnull(v) and any(c in remove_punctuation for c in v):
    #             f.write(f'{index}, {v}\n')
    #     except Exception as e:
    #         print(e)
    #         print(v)
    #         print(type(v))
    #         exit()

    # f.close()
    # exit()

    return df


def normalizeAddress(column: pd.Series, statistics: dict) -> pd.Series:
    '''
    Normalize address according to usps standards
    '''

    count = 0
    addresses = []

    for address in column:
        try:
            normalized_address = scourgify.normalize_address_record(address)

            if normalized_address['address_line_2'] is not None:
                street_address = ' '.join(
                    [normalized_address['address_line_1'], normalized_address['address_line_2']])
            else:
                street_address = normalized_address['address_line_1']

            addresses.append(street_address)
        except Exception as e:
            # Fallback to string normalization
            address = normalizeStrings(pd.Series([address])).iloc[0]
            
            # Unnormalizable address
            count += 1

            addresses.append(address)

    statistics['unnormalizable_addresses'] = f'{count} / {len(column)}: {count / len(column) * 100}%'

    return pd.Series(addresses, name=column.name)


def normalizeStrings(column: pd.Series) -> pd.Series:
    '''
    Normalize string according to:

    1. All letters are uppercased
    3. White space is minimized (only one space between words)
    2. All html errors are removed
    '''

    column = column.str.upper()

    # Replace instances of é with e
    column = column.str.replace('+?', 'E')

    # Replace instances of ó with o
    column = column.str.replace('+AE', 'O')

    # Replace instances of â with a
    column = column.str.replace('+E', 'A')

    # Replace instances of á with a
    column = column.str.replace('+U', 'A')

    # Example v+isquez to vasquez
    column = column.str.replace('+I', 'A')

    # Example Frank-?s to Frank's
    column = column.str.replace('-?S', "'S")

    # Hurley;s to Hurley's
    column = column.str.replace('Y;S', "'S")

    column = column.str.replace('&AMP;', '&')

    # Convert all white space to a single space
    column = column.str.replace(r'\s+', ' ', regex=True)

    # TODO: Add debug option to review unnormalizable strings

    return column
