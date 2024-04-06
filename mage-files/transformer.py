import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df = df.drop_duplicates().reset_index()
    df.rename(columns={'index': 'trip_id'}, inplace=True)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    passenger_count_dim = df.loc[:, ['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim.insert(0, 'passenger_count_id', passenger_count_dim.index)

    trip_distance_dim = df.loc[:, ['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim.insert(0, 'trip_distance_id', trip_distance_dim.index)

    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }

    rate_code_dim = df.loc[:,['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim.loc[:,'rate_code_name'] = rate_code_dim.loc[:,'RatecodeID'].map(rate_code_type)
    rate_code_dim.insert(0, 'rate_code_id', rate_code_dim.index)

    pickup_location_dim = df.loc[:,['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim.insert(0, 'pickup_location_id', pickup_location_dim.index)

    dropoff_location_dim = df.loc[:,['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim.insert(0, 'dropoff_location_id', dropoff_location_dim.index)

    payment_type_type = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }

    payment_type_dim = df.loc[:,['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim.loc[:,'payment_type_name'] = payment_type_dim.loc[:,'payment_type'].map(payment_type_type)
    payment_type_dim.insert(0, 'payment_type_id', payment_type_dim.index)

    fact_table = df.merge(passenger_count_dim, on='passenger_count') \
                .merge(trip_distance_dim, on='trip_distance') \
                .merge(rate_code_dim, on='RatecodeID') \
                .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
                .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude']) \
                .merge(payment_type_dim, on='payment_type') \
                [['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count_id',
                'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag',
                'pickup_location_id', 'dropoff_location_id', 'payment_type_id',
                'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount']]

    return {
        "passenger_count_dim": passenger_count_dim.to_dict(orient="dict"),
        "trip_distance_dim": trip_distance_dim.to_dict(orient="dict"),
        "rate_code_dim": rate_code_dim.to_dict(orient="dict"),
        "pickup_location_dim": pickup_location_dim.to_dict(orient="dict"),
        "dropoff_location_dim": dropoff_location_dim.to_dict(orient="dict"),
        "payment_type_dim": payment_type_dim.to_dict(orient="dict"),
        "fact_table": fact_table.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'