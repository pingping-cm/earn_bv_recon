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

start_time_rfqs = '2025-08-10 00:00:00'
start_time_earn_bv = '2025-07-01 00:00:00'
end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
period_to_check = '2025-10'

assets_deployed_sep = ["ADA","ALGO","ATOM","AVAX","BCH","BNB","BONK","BTTC","BTC","CHZ","CRV","DENT","DOT","ETH","ETC","FET","FLOKI","GALA","HYPE","INJ","LINK","LTC","MANA","MORPHO","NEAR","ONDO","PEPE","POL","RENDER","S","SAND","SEI","SHIB","SNX","SOL","SUI","TAO","TIA","TRUMP","UNI","WIF","XLM","XRP","YFI"]

assets_deployed_oct = ['1INCH','AAVE','ACH','ADA','ALGO','ANKR','AR','ATOM','AVAX','BCH','BNB','BTC','BTTC','CHZ','DOGE','DOT','ETH','FET','FLOKI','GLM','HBAR','HEI','HOT','HYPE','ICP','INJ','IOTA','IOTX','LINK','LTC','LUNA','NEAR','ONE','PEPE','POL','POWR','QNT','RAY','RENDER','ROSE','RSR','RUNE','S','SEI','SHIB','SOL','SUI','TAO','TFUEL','THETA','TIA','TRUMP','TRX', 'VANRY','VET','VTHO','WLD','XLM','XRP']
assets_deployed = assets_deployed_oct.copy()

def printdf(df: pd.DataFrame) -> None:
    # Get terminal width dynamically
    max_width = shutil.get_terminal_size().columns

    # Adjust display settings temporarily
    with pd.option_context('display.width', max_width,       # Set max width to terminal size
                           'display.max_columns', None,      # Display all columns
                           'display.expand_frame_repr', True):  # Avoid splitting columns
        # df = pd.DataFrame(df, columns=df.feature_names)
        print(df)


def rewards_payouts(start_date, end_date):

    client = bigquery.Client(project="bigdata-staging-cm")
    query = "SELECT * FROM `bigdata-staging-cm.treasury_ml.coinmerce_group_outgoing_rewards_daily`"
    df = client.query(query).to_dataframe()
    df = df.rename(columns={"coin": "asset"})
    # df_cm = df[df['entity'] == 'CM'].copy()
    # print(df_cm['day'].unique())
    # sys.exit(1)
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
    p["total_amount_crypto"] = -pd.to_numeric(p["total_amount_crypto"], errors="coerce").fillna(0.0)
    p["total_amount_fiat"]   = -pd.to_numeric(p["total_amount_fiat"],   errors="coerce").fillna(0.0)
    p_agg = (p.groupby("asset", as_index=False)
               .agg(total_payout_native=("total_amount_crypto", "sum"),
                    total_payout_nominal=("total_amount_fiat", "sum")))

    # --- Buybacks ---
    b = df_buyback.rename(columns={"baseAsset": "asset"}).copy()
    b["total_quantity"]       = pd.to_numeric(b["total_quantity"],       errors="coerce").fillna(0.0)
    b["total_quoted_nominal"] = pd.to_numeric(b["total_quoted_nominal"], errors="coerce").fillna(0.0)
    b_agg = (b.groupby("asset", as_index=False)
               .agg(total_buyback_native=("total_quantity", "sum"),
                    total_buyback_nominal=("total_quoted_nominal", "sum")))

    # --- Merge & net_flow ---
    out = p_agg.merge(b_agg, on="asset", how="outer").fillna(0.0)
    out["net_flow"] = out["total_buyback_native"] + out["total_payout_native"]

    # --- Revenue ---

    r = df_revenue.copy()

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
    s["sold_quantity"] = -pd.to_numeric(s["sold_quantity"], errors="coerce").fillna(0.0)
    s["sold_nominal"]    = -pd.to_numeric(s["sold_nominal"],  errors="coerce").fillna(0.0)
    s_agg = (s.groupby("asset", as_index=False)
               .agg(sold_quantity=("sold_quantity", "sum"),
                    sold_nominal=("sold_nominal", "sum")))

    out = out.merge(s_agg, on="asset", how="left").fillna({"sold_quantity": 0.0, "sold_nominal": 0.0})

    # Add period column at the end
    out["period"] = period
    out["position_remaining"] = out["sold_quantity"] + out["net_position"]
    out["position_remaining_nominal"] = out["position_remaining"] * out["total_payout_nominal"] / out['total_payout_native'].fillna(0.0)

    return out.loc[:, [
        "asset",
        "period",
        "total_payout_native",
        "total_payout_nominal",
        "total_buyback_native",
        "total_buyback_nominal",
        "net_flow",
        "revenue_native",
        "net_position",
        "sold_quantity",
        "sold_nominal",
        "position_remaining",
        "position_remaining_nominal"
    ]]

def check_send_to_blox(df_payouts_monthly, df_buybacks_month):
    df_payouts = df_payouts_monthly[(df_payouts_monthly['entity'] == 'BLOX') & (df_payouts_monthly['period'] == period_to_check)]
    df_buyback = df_buybacks_month[(df_buybacks_month['entity'] == 'BLOX') & (df_buybacks_month['period'] == period_to_check)]
    merged = pd.merge(
        df_payouts,
        df_buyback,
        left_on=["period", "entity", "asset"],
        right_on=["period", "entity", "baseAsset"],
        how="outer"
    )

    # compute total_to_send = total_amount_crypto - total_quantity
    merged["total_to_send"] = merged["total_amount_crypto"].fillna(0).astype(float) - merged["total_quantity"].fillna(0).astype(float)

    # keep only the desired columns
    result = merged[["period", "entity", "asset", "total_to_send"]]

    return result

def main():

################### REWARDS PAYOUTS ###########################################################

    df_payouts = rewards_payouts(pd.to_datetime(start_time_earn_bv), pd.to_datetime(end_time))
    df_payouts_weekly = payouts_weekly(df_payouts)  # all weeks

    df_payouts_monthly = payouts_monthly(df_payouts)  # all months
    df_payouts_current_week = payouts_current_week(df_payouts)  # only current week
    # df_payouts_current_month = payouts_current_month(df_payouts)  # only current month

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



################### REWARDS BUYBACKS ###########################################################
    print('The rewards buyback overview:---\n')

    df_buybacks = get_rfqs_otc(start_time_rfqs,end_time)

    df_buybacks_blox_total, df_buybacks_cm_total = aggregate_buybacks (df_buybacks)
    df_buybacks_week = aggregate_buybacks_by_period(df_buybacks, period="W")
    df_buybacks_month = aggregate_buybacks_by_period(df_buybacks, period="M")

    df_buybacks_current_week = aggregate_buybacks_current(df_buybacks, "W")
    df_buybacks_current_month = aggregate_buybacks_current(df_buybacks, "M")


    asset_to_send_blox_M = check_send_to_blox(df_payouts_monthly, df_buybacks_month)
    asset_to_send_blox_M.to_csv("asset_to_send_to_blox_current_month.csv", index=False)

################### LOAN INTERESTS GENERATED ###########################################################
    print('The loan interest and staking rewards overview:---\n')

    df_loan_interest_received = rewards_received(start_time_earn_bv, end_time)

    df_loan_interest = get_loan_interests(df=pd.read_csv('Interest Loans + Staking.csv'))

    df_sold_rewards = pd.read_csv('sold_reward_trades.csv')



################### EARN ASSETS OVERVIEW ###########################################################

    # df_summary_aug = summarize_payouts_buybacks(df_payouts_monthly, df_buybacks_month, df_loan_interest, df_sold_rewards, period="2025-08")
    df_summary_sep = summarize_payouts_buybacks(df_payouts_monthly, df_buybacks_month, df_loan_interest, df_sold_rewards,
                                                period=period_to_check)

    print('The summary of earn bv activity overview:---\n')

    # df_summary_aug.to_csv("summary_payouts_buybacks_aug.csv", index=False)
    df_summary_sep.to_csv(f"summary_payouts_buybacks_{period_to_check}.csv", index=False)

    # cols_to_check = ['ADA', 'ALGO', 'FLOKI', 'SOL', 'BCH', 'SUI', 'SEI', 'S', 'ETH', 'INJ', 'TRUMP', 'BNB', 'TIA',
    #                  'SHIB', 'LINK', 'YFI', 'PEPE', 'RENDER', 'UNI', 'SAND', 'SNX', 'LTC', 'ETC', 'XLM', 'TAO', 'GALA',
    #                  'BTTC', 'DENT', 'BONK', 'MORPHO', 'WIF', 'ONDO', 'CRV', 'MANA']
    #
    #
    # df_summary_sep[df_summary_sep['asset'].isin(cols_to_check)][['asset', 'net_position']].to_csv(
    #     "summary_payouts_buybacks_sep_check.csv", index=False
    # )
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