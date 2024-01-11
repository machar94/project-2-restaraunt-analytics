import pandas as pd
from tqdm import tqdm
from typing import Dict
import string


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