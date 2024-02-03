if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # print('Rows with zero passengers: ', data['passenger_count'].isin([0]).sum())
    # print('Rows with trip distance graeter than zero: ', data['trip_distance'].gt(0).sum())

    # Question 2
    df = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    print(f'The shape of the transformed dataframe is: {df.shape[0]} rows and {df.shape[1]} columns')

    # Question 3
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    print(df[['lpep_pickup_datetime', 'lpep_pickup_date']].dtypes)

    # Question 4
    print('Existing values for VendorID :', df['VendorID'].unique())

    # Question 5
    def is_camel_case(s):
        return s != s.lower() and s != s.upper() and "_" not in s

    cols = df.columns.to_list()
    n = 0
    for col in cols:
        if is_camel_case(col):
            n += 1
    
    print('Number of columns that need to be renamed to snake case: ', n)

    # transform cols CamelCase to snake_case
    df.columns = (df.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )

    return df


@test
def test_1_output(output, *args):
    # vendor_id (snake_case) is in the columns of df
    assert 'vendor_id' in output.columns, '"vendor_id" column is not in df'

@test
def test_2_output(output, *args):
    # passenger_count is greater than 0
    assert output['passenger_count'].gt(0).any(), 'There are rides with zero passengers'

@test
def test_3_output(output, *args):
    # trip_distance is greater than 0
    assert output['trip_distance'].gt(0).any(), 'There are rides with zero trip distances'