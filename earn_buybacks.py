import sys
import pandas as pd
# from recon_earn_bv import printdf


def aggregate_buybacks(df):
    # Filter BLOX (organizationId = 4)
    df_blox = df[df["organizationId"] == 4]
    df_blox = (
        df_blox.groupby("baseAsset", as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_quoted_nominal=("quoteQuantity", "sum"),
        )
        .sort_values("total_quoted_nominal", ascending=False)
        .reset_index(drop=True)
    )

    # Filter CM (organizationId = 7)
    df_cm = df[df["organizationId"] == 7]
    df_cm = (
        df_cm.groupby("baseAsset", as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_quoted_nominal=("quoteQuantity", "sum"),
        )
        .sort_values("total_quoted_nominal", ascending=False)
        .reset_index(drop=True)
    )

    return df_blox, df_cm

# def aggregate_buybacks_by_period(df, period="W"):
#
#     df["executedAt"] = pd.to_datetime(df["executedAt"])
#
#     # Add period column (week or month)
#     df["period"] = df["executedAt"].dt.to_period(period)
#
#     results = {}
#
#     for org, org_name in [(4, "BLOX"), (7, "CM")]:
#         df_org = df[(df["organizationId"] == org) & (df["side"] == "BUY")]
#         df_org = (
#             df_org.groupby(["period", "baseAsset"], as_index=False)
#             .agg(
#                 total_quantity=("quantity", "sum"),
#                 total_quoted_nominal=("quoteQuantity", "sum"),
#             )
#             .sort_values(["period", "total_quoted_nominal"], ascending=[True, False])
#             .reset_index(drop=True)
#         )
#         results[org_name] = df_org
#
#     return results


def aggregate_buybacks_by_period(df, period):
    """
    Aggregate BUY-side buybacks by period and entity (BLOX/CM),
    stacked into a single DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must include columns: executedAt, organizationId, side, baseAsset, quantity, quoteQuantity
    period : str, default "W"
        Any pandas to_period alias, e.g. "W" (weekly ISO-style), "M" (month), "D" (day), etc.

    Returns
    -------
    pd.DataFrame with columns:
        ['period', 'entity', 'baseAsset', 'total_quantity', 'total_quoted_nominal']
    """
    df = df.copy()
    df["executedAt"] = pd.to_datetime(df["executedAt"], errors="coerce")

    # Filter to the two orgs and BUY side
    org_map = {4: "BLOX", 7: "CM"}
    df = df[
        df["organizationId"].isin(org_map.keys()) &
        (df["side"] == "BUY")
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=["period", "entity", "baseAsset", "total_quantity", "total_quoted_nominal"])

    # Period + entity
    df["period"] = df["createdAt"].dt.to_period(period)
    df["entity"] = df["organizationId"].map(org_map)

    out = (
        df.groupby(["period", "entity", "baseAsset"], as_index=False)
          .agg(
              total_quantity=("quantity", "sum"),
              total_quoted_nominal=("quoteQuantity", "sum"),
          )
          .sort_values(["period", "entity", "total_quoted_nominal"], ascending=[True, True, False])
          .reset_index(drop=True)
    )

    return out

def aggregate_buybacks_current(df, period="W"):

    df["createdAt"] = pd.to_datetime(df["createdAt"])
    today = pd.Timestamp.today()

    if period == "W":
        # Current ISO year-week
        current_year, current_week = today.isocalendar().year, today.isocalendar().week
        mask = (
            (df["createdAt"].dt.isocalendar().year == current_year) &
            (df["createdAt"].dt.isocalendar().week == current_week)
        )
    elif period == "M":
        mask = (
            (df["createdAt"].dt.year == today.year) &
            (df["createdAt"].dt.month == today.month)
        )
    else:
        raise ValueError("period must be 'W' (week) or 'M' (month)")

    org_map = {4: "BLOX", 7: "CM"}
    df_f = df.loc[
        mask
        & df["organizationId"].isin(org_map.keys())
        & (df["side"] == "BUY"),
        ["organizationId", "baseAsset", "quantity", "quoteQuantity"]
    ].copy()

    if df_f.empty:
        return pd.DataFrame(columns=["entity", "baseAsset", "total_quantity", "total_quoted_nominal"])

        # Map to entity name
    df_f["entity"] = df_f["organizationId"].map(org_map)

    # Group both orgs together, stacked with entity column
    out = (
        df_f.groupby(["entity", "baseAsset"], as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_quoted_nominal=("quoteQuantity", "sum"),
        )
        .sort_values(["entity", "total_quoted_nominal"], ascending=[True, False])
        .reset_index(drop=True)
    )

    return out
