import altair as alt
import pandas as pd
import streamlit as st

def render_charts(df: pd.DataFrame, palette: list, section: str):
    if df.empty:
        st.info("No data to chart.")
        return

    if section == "entry_price":
        agg = df.groupby("provider", as_index=False)["monthly_price_gbp"].min().rename(
            columns={"monthly_price_gbp": "lowest_price_gbp"}
        )
        chart = (
            alt.Chart(agg, title="Entry Price by Provider (Lowest Monthly Price)")
              .mark_bar()
              .encode(
                  x=alt.X("provider:N", sort="-y", title="Provider"),
                  y=alt.Y("lowest_price_gbp:Q", title="Price (£/month)"),
                  color=alt.Color("provider:N", scale=alt.Scale(range=palette), legend=None),
                  tooltip=["provider", alt.Tooltip("lowest_price_gbp:Q", title="£/mo", format=".2f")]
              )
              .properties(height=280)
        )
        st.altair_chart(chart, use_container_width=True)

    elif section == "scatter":
        chart = (
            alt.Chart(df, title="Price vs. Speed by Provider")
              .mark_circle(size=80, opacity=0.85)
              .encode(
                  x=alt.X("speed_mbps:Q", title="Speed (Mb/s)"),
                  y=alt.Y("monthly_price_gbp:Q", title="Monthly Price (£)"),
                  color=alt.Color("provider:N", scale=alt.Scale(range=palette)),
                  tooltip=[
                      "provider","plan_name",
                      alt.Tooltip("speed_mbps:Q", title="Speed"),
                      alt.Tooltip("monthly_price_gbp:Q", title="£/mo", format=".2f"),
                      "url"
                  ]
              )
              .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

def render_comparison(df: pd.DataFrame):
    # Filters
    c1, c2, c3 = st.columns(3)
    with c1:
        min_price, max_price = float(df["monthly_price_gbp"].min()), float(df["monthly_price_gbp"].max())
        price_range = st.slider("Filter: Monthly Price Range (£/mo)", 0.0, max(5.0, max_price), (min_price, max_price), step=1.0)
    with c2:
        min_speed, max_speed = int(df["speed_mbps"].min()), int(df["speed_mbps"].max())
        speed_range = st.slider("Filter: Speed Range (Mb/s)", 0, max(10, max_speed), (min_speed, max_speed), step=10)
    with c3:
        by_provider = st.multiselect("Filter: Providers", options=sorted(df["provider"].unique().tolist()),
                                     default=sorted(df["provider"].unique().tolist()))

    fdf = df[
        (df["monthly_price_gbp"].between(price_range[0], price_range[1])) &
        (df["speed_mbps"].between(speed_range[0], speed_range[1])) &
        (df["provider"].isin(by_provider))
    ].copy()

    if "row_id" not in fdf.columns:
        fdf["row_id"] = fdf.index.astype(str)

    st.markdown("#### Sort & Select")
    sort_col = st.selectbox("Sort results by", options=["monthly_price_gbp", "speed_mbps", "provider", "plan_name"])
    sort_asc = st.toggle("Ascending order", value=True)
    fdf.sort_values([sort_col, "provider"], ascending=[sort_asc, True], inplace=True)

    # persist compare flag
    fdf["Compare"] = fdf["row_id"].isin(st.session_state.compare_ids)

    edited = st.data_editor(
        fdf[["row_id","Compare","provider","plan_name","speed_mbps","monthly_price_gbp","upfront_fee_gbp","contract_months","url","card_text_sample"]],
        key="compare_table",
        hide_index=True,
        column_config={
            "Compare": st.column_config.CheckboxColumn(help="Tick to compare"),
            "monthly_price_gbp": st.column_config.NumberColumn("Price (£/mo)", format="£%.2f"),
            "speed_mbps": st.column_config.NumberColumn("Speed (Mb/s)"),
            "upfront_fee_gbp": st.column_config.NumberColumn("Upfront (£)", format="£%.2f"),
            "contract_months": st.column_config.NumberColumn("Term (months)"),
        },
        use_container_width=True,
        height=380
    )
    st.session_state.compare_ids = set(edited.loc[edited["Compare"], "row_id"].tolist())

    picked = edited[edited["Compare"]]
    st.markdown("#### Side-by-side Cards")
    if picked.empty:
        st.info("Tick a few rows above to compare.")
    else:
        cards = picked.to_dict(orient="records")
        for i in range(0, len(cards), 3):
            cols = st.columns(min(3, len(cards) - i))
            for col, row in zip(cols, cards[i:i+3]):
                with col:
                    st.markdown(f"**{row['provider']}**")
                    st.caption(row["plan_name"] or "(Unnamed plan)")
                    st.metric("Speed", f"{int(row['speed_mbps'])} Mb/s")
                    st.metric("Price", f"£{float(row['monthly_price_gbp']):.2f}/mo")
                    if pd.notna(row.get("upfront_fee_gbp")):
                        st.metric("Upfront", f"£{float(row['upfront_fee_gbp']):.2f}")
                    if pd.notna(row.get("contract_months")):
                        st.metric("Contract", f"{int(row['contract_months'])} mo")
                    st.link_button("Open offer", row["url"])

def render_status_panel(rows: list):
    if not rows:
        st.info("No status yet. Run a check to see cache/robots/scrape decisions.")
        return
    sdf = pd.DataFrame(rows)
    # metrics
    total = len(sdf)
    robots_blocked = int(sdf["step"].isin(["robots_blocked_initial","robots_blocked_fallback","robots_blocked_base"]).sum())
    offers_events = int((sdf["step"] == "offers_found").sum())
    cache_used = int(sdf["step"].str.startswith("cache_used").sum()) if "step" in sdf else 0

    # NEW: nav counters (pages/steps)
    mean_goto = float(sdf["goto"].dropna().mean()) if "goto" in sdf else 0.0
    mean_steps = float(sdf["steps"].dropna().mean()) if "steps" in sdf else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("Log entries", total)
    with c2: st.metric("Robots blocked", robots_blocked)
    with c3: st.metric("Offers events", offers_events)
    with c4: st.metric("Avg pages navigated", f"{mean_goto:.1f}")
    with c5: st.metric("Avg wizard steps", f"{mean_steps:.1f}")

    # table
    show_cols = ["provider","url","step","detail","allowed","goto","steps"]
    for c in show_cols:
        if c not in sdf.columns: sdf[c] = None
    st.dataframe(
        sdf[show_cols].sort_values(["provider","step"]).reset_index(drop=True),
        use_container_width=True, height=320
    )
