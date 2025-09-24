import sys

from google.cloud import bigquery
import pandas as pd

def rewards_received(start_date, end_date):

    client = bigquery.Client(project="bigdata-staging-cm")
    query = "SELECT * FROM `bigdata-staging-cm.treasury_ml.coinmerce_incoming_outgoing_rewards_daily`"
    df = client.query(query).to_dataframe()
    # print(df.columns)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df[
        (df['date'] >= start_date) &
        (df['date'] <= end_date)
        ].copy()


    df_in = df.rename(columns={
        'manual_input_finance_earn_in_native_cm': "earn_in",
        'manual_input_finance_staking_in_native_cm': "stake_in",
    })

    df_in['rewards_in_native'] = df_in['earn_in'] + df_in['stake_in']

    keep_columns = ["date", "asset", "rewards_in_native"]

    df_in = df_in[keep_columns]
    df = (
        df_in.groupby(['date', 'asset'], as_index=False)[["rewards_in_native"]]
        .sum()
        .sort_values(["date", "rewards_in_native"], ascending=[True, False])
    )
    return df

def get_loan_interests(df):
    df = (df.groupby(["period", "asset"], as_index=False)
      .agg(revenue_native=("revenue_native", "sum")))

    df = df.sort_values("asset", ascending=True)

    return df

