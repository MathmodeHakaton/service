"""
Общий загрузчик данных для всех страниц дашборда.
Кэшируется на 30 минут через st.cache_data.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import streamlit as st


@st.cache_data(ttl=1800)
def load_all():
    from src.application.pipeline import Pipeline
    from src.domain.modules.m1_reserves import M1Reserves
    from src.domain.modules.m2_repo     import M2Repo
    from src.domain.modules.m3_ofz      import M3OFZ
    from src.domain.modules.m4_tax      import M4Tax
    from src.domain.modules.m5_treasury import M5Treasury
    from src.domain.aggregation.lsi_engine import LSIEngine
    from datetime import datetime

    p      = Pipeline()
    result = p.execute_full()
    data   = result.raw_data

    # Fallback OFZ из кэша
    if not data.get("ofz") is not None and not data.get("ofz", pd.DataFrame()).empty:
        import json, os
        cache_path = os.path.normpath(os.path.join(
            os.path.dirname(__file__),
            "../../../liquidity_sentinel/data/m3/m3_data.json"
        ))
        if os.path.exists(cache_path):
            with open(cache_path, encoding="utf-8") as f:
                raw = json.load(f)
            ofz_cached = pd.DataFrame(raw["auctions"])
            ofz_cached["date"] = pd.to_datetime(ofz_cached["date"], errors="coerce")
            for col in ["offer_volume","demand_volume","placement_volume","avg_yield","cover_ratio"]:
                if col in ofz_cached.columns:
                    ofz_cached[col] = pd.to_numeric(ofz_cached[col], errors="coerce")
            data["ofz"] = ofz_cached.dropna(subset=["date"]).reset_index(drop=True)

    m1 = M1Reserves(); m2 = M2Repo(); m3 = M3OFZ()
    m4 = M4Tax();      m5 = M5Treasury()
    engine = LSIEngine()

    df1 = m1._calculate(data.get("reserves", pd.DataFrame()),
                        data.get("ruonia",   pd.DataFrame()),
                        data.get("keyrate",  pd.DataFrame()))
    df2 = m2._calculate(data.get("repo",        pd.DataFrame()),
                        data.get("keyrate",     pd.DataFrame()),
                        data.get("repo_params", pd.DataFrame()))
    ofz_raw = data.get("ofz", pd.DataFrame())
    df3 = m3._calculate(ofz_raw) if ofz_raw is not None and not ofz_raw.empty and "date" in ofz_raw.columns else pd.DataFrame()
    df5 = m5._calculate(data.get("bliquidity", pd.DataFrame()))

    tax_df      = data.get("tax_calendar", pd.DataFrame())
    target_date = datetime.now()
    dates_range = pd.date_range("2019-01-01", pd.Timestamp.today(), freq="D")
    df4 = m4.compute_series(pd.Series(dates_range), tax_df) if not tax_df.empty else pd.DataFrame()
    m4_today  = m4.compute({"tax_calendar": tax_df, "target_date": target_date})
    latest_m4 = m4_today.iloc[-1].to_dict() if not m4_today.empty else {}

    signal_dfs = result.signals
    lsi_result = result.lsi

    def _latest(df, col):
        if df is None or df.empty or col not in df.columns:
            return {}
        clean = df.dropna(subset=[col])
        return clean.iloc[-1].to_dict() if len(clean) else {}

    l1 = _latest(df1, "MAD_score_RUONIA")
    l2 = _latest(df2, "MAD_score_rate_spread")
    l3 = _latest(df3, "MAD_score_cover") if df3 is not None and not df3.empty else {}
    l5 = _latest(df5, "MAD_score_ЦБ")

    scores = {
        "M1": round(engine._score_m1(l1) * 100, 1),
        "M2": round(engine._score_m2(l2) * 100, 1),
        "M3": round((engine._score_m3(l3) or 0.5) * 100, 1),
        "M5": round(engine._score_m5(l5) * 100, 1),
    }
    sf = float(latest_m4.get("Seasonal_Factor", 1.0) or 1.0)

    lsi = {
        "lsi":           round(lsi_result.value * 100, 1),
        "status":        {"normal":"GREEN","warning":"YELLOW","critical":"RED"}.get(lsi_result.status,"GREEN"),
        "contributions": {k: round(v*100, 2) for k, v in lsi_result.contributions.items()},
        "scores":        scores,
        "seasonal_factor": sf,
        "computed_at":   pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
    }

    r1 = {"signals_df": df1, "latest": l1}
    r2 = {"signals_df": df2, "latest": l2}
    r3 = {"signals_df": df3 if df3 is not None else pd.DataFrame(), "latest": l3}
    r4 = {"signals_df": df4, "latest": latest_m4, "tax_df": tax_df}
    r5 = {"signals_df": df5, "latest": l5}

    return r1, r2, r3, r4, r5, lsi
