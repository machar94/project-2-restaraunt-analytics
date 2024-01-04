import pandas as pd
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

    for c1, c2 in subset.items():

        df[c2] = df[c1].str.lower()

        # Replace instances of é with e
        df[c2] = df[c2].str.replace('+?', 'e')

        # Replace instances of ó with o
        df[c2] = df[c2].str.replace('+ae', 'o')

        # Example v+isquez to vasquez
        df[c2] = df[c2].str.replace('+i', 'a')

        df[c2] = df[c2].str.replace('&amp;', '&')

        # Remove all white space from value
        df[c2] = df[c2].str.replace(' ', '')

        # Remove keep punctuation from string.punctuation
        remove_punctuation = string.punctuation
        for p in keep_punctuation:
            remove_punctuation = remove_punctuation.replace(p, '')

        # Remove remaining punctuation from value
        for p in remove_punctuation:
            df[c2] = df[c2].str.replace(p, '')

    # Open a file to write to
    f = open('output.txt', 'w')

    for v in df[c2]:

        try:
            if not pd.isnull(v) and any(c in string.punctuation.replace('&', '').replace('(', '').replace(')', '').replace('/', '') for c in v):
                f.write(v + '\n')
        except:
            print(v)
            print(type(v))
            exit()

    f.close()

    return df
