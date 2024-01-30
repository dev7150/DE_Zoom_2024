if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(data.passenger_count.count())
    print(data.lpep_dropoff_datetime.count())
    # print(data[(data['passenger_count'] == 0) & (data['trip_distance']==0)].count())
    zero_passengers_df = data[data['passenger_count'].isin([0])]
    zero_passengers_count = zero_passengers_df['passenger_count'].count()
    print(f'Zero passenger count={zero_passengers_count}')

    zero_distance_df = data[data['trip_distance'].isin([0])]
    zero_distance_count = zero_passengers_df['trip_distance'].count()
    print(f'Zero passenger count={zero_distance_count}')
    # Assuming final_data is your DataFrame
    zero_passenger_distance_df = data[(data['passenger_count'] == 0) | (data['trip_distance'] == 0)]
    zero_passenger_distance_count = zero_passenger_distance_df.passenger_count.count()
    print(f'zero_passenger_distance_count={zero_passenger_distance_count}')

    non_zero_passenger_distance_df = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]
    non_zero_passenger_distance_count = non_zero_passenger_distance_df.passenger_count.count()
    print(f'non_zero_passenger_distance_count={non_zero_passenger_distance_count}')
    # print(non_zero_passenger_distance_df.count())
    non_zero_passenger_distance_df['lpep_pickup_date'] = non_zero_passenger_distance_df['lpep_pickup_datetime'].dt.date

    # final_data = non_zero_passenger_distance_df.rename(columns=lambda x: ''.join(['_' + i.lower() if i.isupper() else i for i in x]).lstrip('_'))
    final_data = non_zero_passenger_distance_df.rename(columns=lambda x: ''.join(['_' + i.lower() if i.isupper() and (x[index - 1] if index > 0 else '').islower() else i.lower() for index, i in enumerate(x)]).lstrip('_'))

    return final_data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendor_id'].isin(output['vendor_id'].unique()).all(), "Assertion Error: vendor_id has invalid values."
    assert output['passenger_count'].isin([0]).sum() == 0, "Assertion Error: passenger_count = 0"
    assert output['trip_distance'].isin([0]).sum() == 0, "Assertion Error: trip_distance = 0"
    
