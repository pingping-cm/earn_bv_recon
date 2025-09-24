import sys
from datetime import datetime

from database_conn import *
from earn_payouts import *
from earn_buybacks import *
from earn_loan_interest import *
# Ignore warnings
warnings.filterwarnings("ignore")
load_dotenv('.env')

#GLOBAL VARIABLES
stable = ['EUR', 'FDUSD', 'USD', 'USDC', 'USDT']
epoch_t = 45707
epoch_t_1 =45563
start_time_rfqs = '2025-08-10 00:00:00'
start_time_earn_bv = '2025-07-01 00:00:00'

end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

assets_deployed  = [
    "ADA",
    "ALGO",
    "ATOM",
    "AVAX",
    "BCH",
    "BNB",
    "BONK",
    "BTT",
    "BTC",
    "CHZ",
    "CRV",
    "DENT",
    "DOT",
    "ETH",
    "ETC",
    "FET",
    "FLOKI",
    "GALA",
    "HYPE",
    "INJ",
    "LINK",
    "LTC",
    "MANA",
    "MORPHO",
    "NEAR",
    "ONDO",
    "PEPE",
    "POL",
    "RENDER",
    "S",
    "SAND",
    "SEI",
    "SHIB",
    "SNX",
    "SOL",
    "SUI",
    "TAO",
    "TIA",
    "TRUMP",
    "UNI",
    "WIF",
    "XLM",
    "XRP",
    "YFI"
]


def printdf(df: pd.DataFrame) -> None:
    # Get terminal width dynamically
    max_width = shutil.get_terminal_size().columns

    # Adjust display settings temporarily
    with pd.option_context('display.width', max_width,       # Set max width to terminal size
                           'display.max_columns', None,      # Display all columns
                           'display.expand_frame_repr', True):  # Avoid splitting columns
        # df = pd.DataFrame(df, columns=df.feature_names)
        print(df)


def plot(df, asset_name, n_data):
    asset = df[df['asset'] == asset_name]

    native_values = asset.drop(columns='asset').squeeze()

    dates = [col.replace('diff_native_', '') for col in native_values.index]
    dates = pd.to_datetime(dates, format='%Y%m%d')

    plt.figure(figsize=(10, 5))
    # sort by date
    sorted_pairs = sorted(zip(dates, native_values.values))
    sorted_pairs = sorted_pairs[-n_data:]
    sorted_dates, sorted_values = zip(*sorted_pairs)
    plt.plot(sorted_dates, sorted_values, marker='o')
    plt.title(f'{asset_name} Native Difference Records')
    plt.xlabel('Date')
    plt.ylabel('Native Value')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def data_process(df):

    df = df.reset_index(drop=True)

    keep_columns = [
               "account", "asset", "price", "current_quantity", "desired_quantity", "global_desired_quantity",
               "expected_quantity","native_difference","nominal_difference","nominal_quantity","epoch","timestamp"
    ]

    df = df[keep_columns]

    float_columns = [
        "price", "current_quantity", "desired_quantity", "global_desired_quantity", "expected_quantity","native_difference","nominal_difference","nominal_quantity"
    ]

    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors='coerce')
    # df[float_columns] = df[float_columns].applymap(lambda x: f"{x:,.0f}" if pd.notna(x) else "0")
    return df

def overwrite_values(df, df_correction):

    # Iterate through df2 and update df1 where asset and epoch match
    for _, row in df_correction.iterrows():
        mask = (df['epoch'] == row['epoch']) & (df['asset'] == row['asset'])
        for col in df_correction.columns:
            if col not in ['epoch', 'asset'] and pd.notna(row[col]):
                df.loc[mask, col] = row[col]
    return df

def compute_diff_native(df):

    desired_quantity_sum = df.groupby(['asset', 'price', 'timestamp', 'epoch'])[
        'global_desired_quantity'].sum().reset_index()
    desired_quantity_sum.rename(columns={'global_desired_quantity': 'desired_quantity'}, inplace=True)

    current_quantity_sum = df.groupby(['asset', 'price', 'timestamp', 'epoch'])['current_quantity'].sum().reset_index()
    current_quantity_sum.rename(columns={'current_quantity': 'current_quantity'}, inplace=True)

    df_merged = current_quantity_sum.merge(desired_quantity_sum, on=['asset', 'price', 'timestamp', 'epoch'], how='left')

    df_merged['diff_native'] = df_merged['current_quantity'] - df_merged['desired_quantity'].fillna(0)
    df_merged['diff_nominal'] = df_merged['diff_native'] * df_merged['price']
    df_merged['current_nominal'] = df_merged['current_quantity'] * df_merged['price']

    keep_columns = [
        "asset", "price", "current_quantity",
        "current_nominal", "diff_native", "diff_nominal", "epoch", "timestamp"
    ]

    df = df_merged[keep_columns]
    # df_merged['diff_native'] = df_merged['diff_native'].apply(lambda x: f"{x:,.0f}")
    return df



def df_split(df, stable):
    #  df_crypto: asset not in stable
    df_crypto = df[~df['asset'].isin(stable)]

    #  df_stable: asset in stable
    df_stable = df[df['asset'].isin(stable)]

    #  df_stable_usd: asset in stable excluding 'EUR'
    df_stable_usd = df[df['asset'].isin(stable[1:])]

    #  df_stable_eur: asset == 'EUR'
    df_stable_eur = df[df['asset'] == stable[0]]

    return df_crypto, df_stable, df_stable_usd, df_stable_eur

def delta_overview(df, eur_usd_t,eur_usd_t_1):

    aum_t = df['current_nominal_t'].sum() / eur_usd_t
    equity_t = (df['diff_native_t'] * df['price_t']).sum()/ eur_usd_t

    df["diff_nominal_t"] = df['diff_native_t'] * df['price_t']
    long_equity = df.loc[df["diff_nominal_t"] > 0, "diff_nominal_t"].sum() / eur_usd_t
    short_equity = df.loc[df["diff_nominal_t"] < 0, "diff_nominal_t"].sum() / eur_usd_t
    equity_lag = (df['diff_native_t_1'] * df['price_t']).sum() / eur_usd_t

    aum_t_1 = df['current_nominal_t_1'].sum() / eur_usd_t_1
    equity_t_1 = (df['diff_native_t_1'] * df['price_t_1']).sum() / eur_usd_t_1
    delta_equity = (equity_t - equity_t_1)

    delta_aum = (aum_t - aum_t_1)
    delta_price_diff = ((df['price_t'] - df['price_t_1']) * df['diff_native_t_1']).sum() / eur_usd_t
    delta_pos_diff = ((df['diff_native_t'] - df['diff_native_t_1']) * df['price_t']).sum() / eur_usd_t
    delta_fx_diff = (df['diff_native_t_1'] * df['price_t_1']).sum() / eur_usd_t - (df['diff_native_t_1'] * df['price_t_1']).sum() / eur_usd_t_1
    delta_market_diff = delta_price_diff + delta_fx_diff
    delta_dict = {
        # "aum_t": int(aum_t),
        # "aum_t_1": int(aum_t_1),
        # "delta_aum": int(delta_aum),

        "equity_t": int(equity_t),
        "equity_t_1": int(equity_t_1),

        "long_equity": int(long_equity),
        "short_equity": int(short_equity),
        "equity_lag": int(equity_lag),
        "delta_equity": int(delta_equity),
        "delta_price_diff": int(delta_price_diff),
        "delta_pos_diff": int(delta_pos_diff),
        "delta_fx_diff": int(delta_fx_diff),
        "delta_market_diff": int(delta_market_diff)

    }

    for key, value in delta_dict.items():
        print(f"{key}: {value:,}")
    return delta_dict

def process_dataframe(df,df_correction):
    df = data_process(df)

    df = compute_diff_native(df)

    df = overwrite_values(df, df_correction)
    eur_usd = df.loc[df['asset'] == 'EUR', 'price'].values
    return df, eur_usd

def recon_breaks(df, threshold):

    df['break_native'] = df['diff_native_t'] - df['diff_native_t_1']
    df['break_nominal'] = df['break_native'] * df['price_t']

    df['breaks']  = abs(df['break_nominal']) > threshold

    df_breaks = df[df['breaks']][['asset', 'diff_native_t', 'diff_native_t_1', 'breaks', 'break_native', 'break_nominal', 'timestamp_t']].copy()
    df_breaks = df_breaks.reindex(df_breaks['break_nominal'].abs().sort_values(ascending=False).index)

    return df_breaks

def historical_diff(df, df_new):
    new_col_df = extract_diff_native_column(df_new)
    df = extract_diff_native_column(df)

    if new_col_df.empty:
        return df  # skip if nothing to merge

    # Get the name of the new column (besides 'asset')
    new_col_name = [col for col in new_col_df.columns if col != 'asset'][0]

    if new_col_name in df.columns:
        # Replace values in that column for matching assets
        df = df.set_index('asset')
        new_col_df = new_col_df.set_index('asset')

        df.update(new_col_df)  # only updates the existing column
        df = df.reset_index()
    else:
        # New date — merge as new column
        df = df.merge(new_col_df, on='asset', how='outer')

    return df

def extract_diff_native_column(df):
    try:
        date_str = pd.to_datetime(df['timestamp'].iloc[0]).strftime('%Y%m%d')
        column_name = f'diff_native_{date_str}'
        return df[['asset', 'diff_native']].rename(columns={'diff_native': column_name})
    except Exception:
        return df

def get_processed_df(df,df_correction):

    timestamp = df['timestamp'].iloc[0]
    df, fx = process_dataframe(df, df_correction)
    return df, fx, timestamp




def rewards_payouts(start_date, end_date):

    client = bigquery.Client(project="bigdata-staging-cm")
    query = "SELECT * FROM `bigdata-staging-cm.treasury_ml.coinmerce_group_outgoing_rewards_daily`"
    df = client.query(query).to_dataframe()
    df = df.rename(columns={"coin": "asset"})
    # df_cm = df[df['entity'] == 'CM'].copy()
    # df_blox = df[df['entity'] == 'BLOX'].copy()

    return df

def summarize_payouts_buybacks(
    df_payouts: pd.DataFrame,      # cols: period, entity, asset, total_amount_crypto, total_amount_fiat
    df_buyback: pd.DataFrame,      # cols: period, entity, baseAsset, total_quantity, total_quoted_nominal
    df_revenue: pd.DataFrame,      # cols: Asset, Revenue NATIVE
    df_sold_rewards: pd.DataFrame, # cols: Revenue Month, Base Asset, Order Amount, Total
    period: str                    # e.g. "2025-09"
) -> pd.DataFrame:
    # --- Filter by period ---
    df_payouts = df_payouts[df_payouts["period"].astype(str) == period]
    df_buyback = df_buyback[df_buyback["period"].astype(str) == period]
    df_sold_rewards = df_sold_rewards[df_sold_rewards["Revenue Month"].astype(str) == period]
    df_revenue = df_revenue[df_revenue["period"].astype(str) == period]

    # --- Payouts ---
    p = df_payouts.copy()
    p["total_amount_crypto"] = pd.to_numeric(p["total_amount_crypto"], errors="coerce").fillna(0.0)
    p["total_amount_fiat"]   = pd.to_numeric(p["total_amount_fiat"],   errors="coerce").fillna(0.0)
    p_agg = (p.groupby("asset", as_index=False)
               .agg(total_payout=("total_amount_crypto", "sum"),
                    total_payout_nominal=("total_amount_fiat", "sum")))

    # --- Buybacks ---
    b = df_buyback.rename(columns={"baseAsset": "asset"}).copy()
    b["total_quantity"]       = pd.to_numeric(b["total_quantity"],       errors="coerce").fillna(0.0)
    b["total_quoted_nominal"] = pd.to_numeric(b["total_quoted_nominal"], errors="coerce").fillna(0.0)
    b_agg = (b.groupby("asset", as_index=False)
               .agg(total_buyback=("total_quantity", "sum"),
                    total_buyback_nominal=("total_quoted_nominal", "sum")))

    # --- Merge & net_flow ---
    out = p_agg.merge(b_agg, on="asset", how="outer").fillna(0.0)
    out["net_flow"] = out["total_buyback"] - out["total_payout"]

    # --- Revenue ---

    r = df_revenue.copy()
    r["revenue_native"] = pd.to_numeric(r["revenue_native"], errors="coerce").fillna(0.0)
    out = out.merge(r, on=["asset"], how="left").fillna({"revenue_native": 0.0})
    out["net_position"] = out["revenue_native"] + out["net_flow"]
    # --- Sold rewards ---
    s = df_sold_rewards.rename(columns={
        "Revenue Month": "period",
        "Base Asset": "asset",
        "Order Amount": "sold_quantity",
        "Order Price": "sold_price",
        "Total": "sold_nominal",
    }).copy()
    s["sold_quantity"] = pd.to_numeric(s["sold_quantity"], errors="coerce").fillna(0.0)
    s["sold_nominal"]    = pd.to_numeric(s["sold_nominal"],  errors="coerce").fillna(0.0)
    s_agg = (s.groupby("asset", as_index=False)
               .agg(sold_quantity=("sold_quantity", "sum"),
                    sold_nominal=("sold_nominal", "sum")))

    out = out.merge(s_agg, on="asset", how="left").fillna({"sold_quantity": 0.0, "sold_nominal": 0.0})

    # Add period column at the end
    out["period"] = period

    return out.loc[:, [
        "asset",
        "period",
        "total_payout",
        "total_payout_nominal",
        "total_buyback",
        "total_buyback_nominal",
        "net_flow",
        "revenue_native",
        "net_position",
        "sold_quantity",
        "sold_nominal",

    ]]

def main():

################### REWARDS PAYOUTS ###########################################################

    df_payouts = rewards_payouts(pd.to_datetime(start_time_earn_bv), pd.to_datetime(end_time))
    df_payouts_weekly = payouts_weekly(df_payouts)  # all weeks
    df_payouts_monthly = payouts_monthly(df_payouts)  # all months
    df_payouts_current_week = payouts_current_week(df_payouts)  # only current week
    df_payouts_current_month = payouts_current_month(df_payouts)  # only current month
    asset_to_check_and_buy_blox = df_payouts_current_week[
        (df_payouts_current_week['entity'] == 'BLOX')
        & (~df_payouts_current_week['asset'].isin(assets_deployed))
    ]

    asset_to_check_and_buy_cm = df_payouts_current_week[
            (df_payouts_current_week['entity'] == 'CM')
        & (~df_payouts_current_week['asset'].isin(assets_deployed))
    ]

    asset_to_check_and_buy_blox.to_csv("blox_asset_to_check_and_buyback_current_week.csv", index=False)

    asset_to_check_and_buy_cm.to_csv("cm_asset_to_check_and_buyback_current_week.csv", index=False)
    # printdf(df_payouts_current_month.head(2))

################### REWARDS BUYBACKS ###########################################################
    print('The rewards buyback overview:---\n')

    df_buybacks = get_rfqs_otc(start_time_rfqs,end_time)
    df_buybacks_blox_total, df_buybacks_cm_total = aggregate_buybacks (df_buybacks)
    df_buybacks_week = aggregate_buybacks_by_period(df_buybacks, period="W")
    df_buybacks_month = aggregate_buybacks_by_period(df_buybacks, period="M")

    df_buybacks_current_week = aggregate_buybacks_current(df_buybacks, "W")
    df_buybacks_current_month = aggregate_buybacks_current(df_buybacks, "M")


    # printdf(df_buybacks_month.head(2))
    # printdf(df_buybacks_current_month.head(2))
    # df_payouts_cm = rewards_in_out_cm(pd.to_datetime(start_time_earn_bv), pd.to_datetime(end_time) )


################### LOAN INTERESTS GENERATED ###########################################################
    print('The loan interest and staking rewards overview:---\n')

    df_loan_interest_received = rewards_received(start_time_earn_bv, end_time)

    # df_loan_interest_expected_july = get_loan_interests( df = pd.read_csv('Interest Loans + Staking JUL.csv'))
    # df_loan_interest_expected_aug = get_loan_interests(df=pd.read_csv('Interest Loans + Staking AUG.csv'))


    df_loan_interest = get_loan_interests(df=pd.read_csv('Interest Loans + Staking.csv'))

    df_sold_rewards = pd.read_csv('sold_reward_trades.csv')


    # printdf( df_loan_interest_expected_aug)

################### EARN ASSETS OVERVIEW ###########################################################

    df_summary_aug = summarize_payouts_buybacks(df_payouts_monthly, df_buybacks_month, df_loan_interest, df_sold_rewards, period="2025-08")


    df_summary_sep = summarize_payouts_buybacks(df_payouts_monthly, df_buybacks_month, df_loan_interest, df_sold_rewards,
                                                period="2025-09")

    print('The summary of earn bv activity overview:---\n')

    df_summary_aug.to_csv("summary_payouts_buybacks_aug.csv", index=False)
    df_summary_sep.to_csv("summary_payouts_buybacks_sep.csv", index=False)
    sys.exit(1)
    print(len(df_summarize_payouts_buybacks['asset'].unique()))


################### CHECKS ###########################################################


    assets_summary = set(df_summarize_payouts_buybacks["asset"].unique())
    assets_payouts = set(df_payouts_current_month["asset"].unique())

    # in payouts but missing in summary
    missing_in_summary = assets_payouts - assets_summary

    # in summary but not in payouts (could come from buybacks or revenue merge)
    extra_in_summary = assets_summary - assets_payouts

    print("Assets in payouts but missing in summary:", missing_in_summary)
    print("Assets in summary but not in payouts:", extra_in_summary)
####################### YIELD FARMING AND SIM PROFITS #################################################################

if __name__ == "__main__":
    main()