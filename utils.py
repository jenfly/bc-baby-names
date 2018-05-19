import pandas as pd

def process_raw_df(df_raw, name='Value', copy=False):
    data = df_raw.drop('Total', axis=1).stack()
    data = data.rename_axis(['Name', 'Year']).to_frame(name=name)
    return data

def printif(msg, verbose=True):
    if verbose:
        print(msg)

def merge_data(girls_file, boys_file, verbose=True):

    printif(f'Loading {girls_file}', verbose)
    girl_names = pd.read_csv(girls_file, index_col=0)
    printif(f'Loading {boys_file}')
    boy_names = pd.read_csv(boys_file, index_col=0)

    printif('Processing raw data')
    girl_data = process_raw_df(girl_names, name='Girls Count')
    boy_data = process_raw_df(boy_names, name='Boys Count')

    printif('Merging data')
    data = girl_data.join(boy_data, how='outer').fillna(0)
    data = data.reset_index(level='Year')
    data['Year'] = data['Year'].astype(int)

    return data


def get_names_data(girls_file='data/bc-popular-girls-names.csv',
                   boys_file='data/bc-popular-boys-names.csv',
                   verbose=True):
    data = merge_data(girls_file, boys_file, verbose=verbose)

    printif('Performing calculations', verbose)
    data['Both Count'] = data[['Girls Count', 'Boys Count']].sum(axis=1)
    data = data[data['Both Count'] > 0]
    nms = ['Girls', 'Boys', 'Both']
    cols_map = {f'{nm} Count' : f'{nm} Yearly Total (All Names)' for nm in nms}
    yrly_totals = data.groupby('Year').sum().rename(columns=cols_map)

    data = data.merge(yrly_totals, how='left', left_on='Year',
                      right_index=True)

    columns = list(data.columns)
    data['First Letter'] = data.index.str[0]
    data['Last Letter'] = data.index.str[-1]
    data['Last 3 Letters'] = data.index.str[-3:]
    data = data[['First Letter', 'Last Letter', 'Last 3 Letters'] + columns]

    for nm in nms:
        col_numerator = f'{nm} Count'
        col_denominator = f'{nm} Yearly Total (All Names)'
        data[f'% of {nm}'] = 100 * data[col_numerator] / data[col_denominator]
    for nm in ['Girls', 'Boys']:
        data[f'{nm} Fraction'] = data[f'{nm} Count'] / data['Both Count']

    data = data.drop(cols_map.values(), axis=1)
    return data
