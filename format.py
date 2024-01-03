import pandas as pd
from typing import Dict


def standardizeString(df: pd.DataFrame, subset: Dict[str, str]) -> pd.DataFrame:
    '''
    For each column specified by name in subset:

    1. All white space is removed from rows
    2. All puncuation (',-) is removed from rows
    3. All letters are lowercased
    
    Args:
        df: The dataframe to be formatted
        subset: {current_column_name: new_column_name}
    '''

    for c1, c2 in subset.items():

        df[c2] = df[c1].str.replace(' ', '')
        df[c2] = df[c2].str.replace(',', '')
        df[c2] = df[c2].str.replace("'", '')
        df[c2] = df[c2].str.replace('-', '')
        df[c2] = df[c2].str.lower()

    return df