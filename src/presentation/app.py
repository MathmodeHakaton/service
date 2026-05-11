"""
RU Liquidity Sentinel — Streamlit Dashboard
Запуск: streamlit run src/presentation/app.py --server.port 8501
"""
import os
import json
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


st.set_page_config(
    page_title="RU Liquidity Sentinel",
    page_icon="📊",
    layout="wide",
)


# ЗАГРУЗКА ДАННЫХ

@st.cache_data(ttl=1800)
def load_all():
    from src.application.pipeline import Pipeline
    from src.domain.modules.m1_reserves import M1Reserves
    from src.domain.modules.m2_repo import M2Repo
    from src.domain.modules.m3_ofz import M3OFZ
    from src.domain.modules.m4_tax import M4Tax
    from src.domain.modules.m5_treasury import M5Treasury
    from src.domain.aggregation.lsi_engine import LSIEngine
    from src.infrastructure.storage.db.engine import get_session
    from datetime import datetime

    # Сбор данных через pipeline
    session = get_session()
    try:
        p = Pipeline(session=session)
        result = p.execute_full()
        data = result.raw_data

        if not data.get("ofz") is not None and not data.get("ofz", pd.DataFrame()).empty:
            cache_path = os.path.normpath(os.path.join(
                os.path.dirname(__file__),
                "../../../liquidity_sentinel/data/m3/m3_data.json"
            ))
            if os.path.exists(cache_path):
                with open(cache_path, encoding="utf-8") as f:
                    raw = json.load(f)
                ofz_cached = pd.DataFrame(raw["auctions"])
                ofz_cached["date"] = pd.to_datetime(
                    ofz_cached["date"], errors="coerce")
                for col in ["offer_volume", "demand_volume", "placement_volume",
                            "avg_yield", "cover_ratio"]:
                    if col in ofz_cached.columns:
                        ofz_cached[col] = pd.to_numeric(
                            ofz_cached[col], errors="coerce")
                data["ofz"] = ofz_cached.dropna(
                    subset=["date"]).reset_index(drop=True)

        m1 = M1Reserves()
        m2 = M2Repo()
        m3 = M3OFZ()
        m4 = M4Tax()
        m5 = M5Treasury()

        df1 = m1._calculate(data.get("reserves", pd.DataFrame()),
                            data.get("ruonia",   pd.DataFrame()),
                            data.get("keyrate",  pd.DataFrame()))
        df2 = m2._calculate(data.get("repo",        pd.DataFrame()),
                            data.get("keyrate",     pd.DataFrame()),
                            data.get("repo_params", pd.DataFrame()))
        ofz_raw = data.get("ofz", pd.DataFrame())
        df3 = m3._calculate(
            ofz_raw) if ofz_raw is not None and not ofz_raw.empty and "date" in ofz_raw.columns else pd.DataFrame()
        df5 = m5._calculate(data.get("bliquidity", pd.DataFrame()))

        tax_df = data.get("tax_calendar", pd.DataFrame())
        target_date = datetime.now()

        dates_range = pd.date_range(
            "2019-01-01", pd.Timestamp.today(), freq="D")
        df4 = m4.compute_series(pd.Series(dates_range),
                                tax_df) if not tax_df.empty else pd.DataFrame()
        m4_today = m4.compute(
            {"tax_calendar": tax_df, "target_date": target_date})
        latest_m4 = m4_today.iloc[-1].to_dict() if not m4_today.empty else {}

        engine = LSIEngine()
        signal_dfs = result.signals
        lsi_result = result.lsi

        def _latest(df, col):
            if df is None or df.empty or col not in df.columns:
                return {}
            clean = df.dropna(subset=[col])
            return clean.iloc[-1].to_dict() if len(clean) else {}

        l1 = _latest(df1, "MAD_score_RUONIA")
        l2 = _latest(df2, "MAD_score_rate_spread")
        l3 = _latest(
            df3, "MAD_score_cover") if df3 is not None and not df3.empty else {}
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
            "status":        {"normal": "GREEN", "warning": "YELLOW",
                              "critical": "RED"}.get(lsi_result.status, "GREEN"),
            "contributions": {k: round(v * 100, 2) for k, v in lsi_result.contributions.items()},
            "scores":        scores,
            "seasonal_factor": sf,
            "computed_at":   pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        }

        # Backtest
        try:
            bt = _build_backtest(data, engine)
        except Exception:
            bt = None

        return ({"signals_df": df1, "latest": l1},
                {"signals_df": df2, "latest": l2},
                {"signals_df": df3 if df3 is not None else pd.DataFrame(),
                 "latest": l3},
                {"signals_df": df4, "latest": latest_m4, "tax_df": tax_df},
                {"signals_df": df5, "latest": l5},
                lsi, bt)
    finally:
        session.close()


def _build_backtest(data: dict, engine=None) -> pd.DataFrame | None:
    """Строит исторический LSI на базе сигналов M1, M2, M5 + M4."""
    try:
        import numpy as np
        from src.domain.modules.m1_reserves import M1Reserves
        from src.domain.modules.m2_repo import M2Repo
        from src.domain.modules.m4_tax import M4Tax
        from src.domain.modules.m5_treasury import M5Treasury
        from src.domain.aggregation.lsi_engine import LSIEngine

        if engine is None:
            engine = LSIEngine()

        bliq = data.get("bliquidity", pd.DataFrame())
        if bliq is None or bliq.empty:
            return None

        m5 = M5Treasury()
        df5 = m5._calculate(bliq).sort_values("date").reset_index(drop=True)

        m2 = M2Repo()
        df2 = pd.DataFrame()
        if not data.get("repo", pd.DataFrame()).empty and not data.get("keyrate", pd.DataFrame()).empty:
            df2 = m2._calculate(data["repo"], data["keyrate"],
                                data.get("repo_params", pd.DataFrame()))
            df2["week"] = df2["date"].dt.to_period("W")
            df2 = df2.groupby("week").agg(
                date=("date", "last"),
                MAD_score_rate_spread=("MAD_score_rate_spread", "mean"),
                Flag_Demand=("Flag_Demand", "max"),
            ).reset_index(drop=True)

        m1 = M1Reserves()
        df1 = pd.DataFrame()
        if not data.get("reserves", pd.DataFrame()).empty and not data.get("ruonia", pd.DataFrame()).empty:
            df1 = m1._calculate(data["reserves"], data["ruonia"],
                                data.get("keyrate", pd.DataFrame()))
            df1["month"] = df1["date"].dt.to_period("M")

        m4 = M4Tax()
        tax_df = data.get("tax_calendar", pd.DataFrame())

        base = df5[["date", "MAD_score_ЦБ",
                    "MAD_score_Росказна", "Flag_Budget_Drain"]].copy()

        if not df1.empty:
            m1_cols = [c for c in ["month", "MAD_score_RUONIA", "MAD_score_спред",
                                   "Flag_AboveKey", "Flag_EndOfPeriod"] if c in df1.columns]
            df1_m = df1[m1_cols].copy()
            df1_m["month"] = df1["date"].dt.to_period("M")
            base["month"] = base["date"].dt.to_period("M")
            base = base.merge(df1_m, on="month", how="left").drop(
                columns="month")

        if not df2.empty:
            base = pd.merge_asof(
                base.sort_values("date"),
                df2.rename(columns={"MAD_score_rate_spread": "M2_MAD_rate",
                                    "Flag_Demand":           "M2_Flag"})
                .sort_values("date"),
                on="date", direction="backward",
            )

        # M4
        if tax_df is not None and not tax_df.empty:
            tax_df["date"] = pd.to_datetime(tax_df["date"])
            df4 = m4.compute_series(base["date"], tax_df)
            base = base.merge(
                df4[["date", "Seasonal_Factor"]], on="date", how="left")
        base["Seasonal_Factor"] = base.get(
            "Seasonal_Factor", pd.Series(1.0, index=base.index)
        ).fillna(1.0)

        def _row_lsi(row):
            r1 = {
                "MAD_score_RUONIA": row.get("MAD_score_RUONIA"),
                "MAD_score_спред":  row.get("MAD_score_спред"),
                "Flag_AboveKey":    row.get("Flag_AboveKey", 0),
                "Flag_EndOfPeriod": row.get("Flag_EndOfPeriod", 0),
            }
            r2 = {
                "MAD_score_rate_spread": row.get("M2_MAD_rate"),
                "Flag_Demand":           row.get("M2_Flag", 0),
            }
            r5 = {
                "MAD_score_ЦБ":       row.get("MAD_score_ЦБ"),
                "MAD_score_Росказна":  row.get("MAD_score_Росказна"),
                "Flag_Budget_Drain":   row.get("Flag_Budget_Drain", 0),
            }
            s1 = engine._score_m1(r1)
            s2 = engine._score_m2(r2)
            s5 = engine._score_m5(r5)
            sf = float(row.get("Seasonal_Factor", 1.0) or 1.0)

            w = {"M1_RESERVES": 0.387, "M2_REPO": 0.374, "M5_TREASURY": 0.088}
            wt = sum(w.values())
            base_val = (w["M1_RESERVES"] * s1 + w["M2_REPO"]
                        * s2 + w["M5_TREASURY"] * s5) / wt
            lsi_val = min(base_val * sf * 100, 100.0)
            status = "RED" if lsi_val > 70 else "YELLOW" if lsi_val > 40 else "GREEN"
            return pd.Series({"LSI": lsi_val, "status": status})

        scores = base.apply(_row_lsi, axis=1)
        result = pd.concat([base[["date"]].reset_index(drop=True),
                            scores.reset_index(drop=True)], axis=1)
        return result.sort_values("date").reset_index(drop=True)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(
            "Backtest error: %s", e, exc_info=True)
        return None


# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ

def status_color(status):
    return {"GREEN": "#27ae60", "YELLOW": "#f39c12", "RED": "#e74c3c"}.get(status, "#95a5a6")


def status_ru(status):
    return {"GREEN": "НОРМА", "YELLOW": "ВНИМАНИЕ", "RED": "СТРЕСС"}.get(status, status)


def gauge_fig(value):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={"text": "Индекс стресса ликвидности", "font": {"size": 14}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1,
                     "tickvals": [0, 20, 40, 70, 100],
                     "ticktext": ["0", "20", "40 — внимание", "70 — стресс", "100"]},
            "bar": {"color": "#2c3e50"},
            "steps": [
                {"range": [0,  40], "color": "#d5f5e3"},
                {"range": [40, 70], "color": "#fdebd0"},
                {"range": [70, 100], "color": "#fadbd8"},
            ],
            "threshold": {"line": {"color": "black", "width": 3}, "value": value},
        },
        number={"suffix": " / 100", "font": {"size": 28}},
    ))
    fig.update_layout(height=230, margin=dict(t=40, b=10, l=10, r=10))
    return fig


# ЗАГРУЗКА

with st.spinner("Загрузка данных с ЦБ РФ и Минфина..."):
    try:
        r1, r2, r3, r4, r5, lsi, bt = load_all()
        load_ok = True
    except Exception as e:
        st.error(f"Ошибка загрузки: {e}")
        import traceback
        st.code(traceback.format_exc())
        load_ok = False

if not load_ok:
    st.stop()

# ЗАГОЛОВОК
st.title("RU Liquidity Sentinel")
st.caption(
    f"Обновлено: {lsi['computed_at']}  |  Источники: ЦБ РФ · Минфин · ФНС")

col_lsi, col_sf, col_status = st.columns([2, 1, 1])

with col_lsi:
    st.metric("LSI", f"{lsi['lsi']:.1f} / 100")

with col_sf:
    sf = lsi["seasonal_factor"]
    sf_labels = {1.4: "Конец квартала",
                 1.2: "Конец месяца", 1.1: "Налоговая неделя"}
    sf_label = next((v for k, v in sf_labels.items() if sf >= k), "Норма")
    st.metric("Сезонный коэффициент", f"×{sf:.2f}")
    if sf >= 1.4:
        st.error(f"⚠ {sf_label} — максимальное налоговое давление")
    elif sf >= 1.2:
        st.warning(f"↑ {sf_label}")
    elif sf >= 1.1:
        st.info(f"↑ {sf_label}")
    else:
        st.success("✓ Норма — нет налогового давления")

with col_status:
    color = status_color(lsi["status"])
    icon = "🟢" if lsi["status"] == "GREEN" else "🟡" if lsi["status"] == "YELLOW" else "🔴"
    st.markdown(f"""
    <div style="background:{color};color:white;padding:24px 16px;
                border-radius:12px;text-align:center;margin-top:10px;">
        <div style="font-size:22px;font-weight:bold;">{icon} {status_ru(lsi['status'])}</div>
        <div style="font-size:36px;font-weight:bold;margin:6px 0;">{lsi['lsi']:.1f}</div>
        <div style="font-size:12px;opacity:0.85;">из 100 возможных</div>
    </div>
    """, unsafe_allow_html=True)

# Авто-комментарий
scores = lsi["scores"]
_top = max(scores.items(), key=lambda x: x[1])
_tax_s = f"Налоговое давление (SF=×{sf:.1f})." if sf > 1.0 else "Налогового давления нет."
_sts = {"GREEN": "в норме", "YELLOW": "в зоне внимания",
        "RED": "в стрессе"}[lsi["status"]]
st.info(
    f"**LSI = {lsi['lsi']:.1f}/100 — рынок ликвидности {_sts}.** "
    f"Главный драйвер: **{_top[0]}** ({_top[1]:.0f}/100). {_tax_s} "
    f"Данные обновлены: {lsi['computed_at']}."
)

st.divider()

# ВКЛАД МОДУЛЕЙ
st.subheader("Вклад модулей в индекс стресса")

MODULE_DESC = {
    "M1": "Корсчета банков + ставка RUONIA",
    "M2": "Аукционы репо Банка России",
    "M3": "Размещение гособлигаций ОФЗ",
    "M5": "Баланс ликвидности банковского сектора",
}
CONTRIB_KEY = {
    "M1": "M1_RESERVES", "M2": "M2_REPO",
    "M3": "M3_OFZ",      "M5": "M5_TREASURY",
}

cols = st.columns(4)
for col, mk in zip(cols, ["M1", "M2", "M3", "M5"]):
    with col:
        sv = scores.get(mk, 0) or 0
        cv = lsi["contributions"].get(CONTRIB_KEY[mk], 0) or 0
        color = "#27ae60" if sv < 40 else "#f39c12" if sv < 70 else "#e74c3c"
        label = "норма" if sv < 40 else "внимание" if sv < 70 else "стресс"
        st.markdown(f"""
        <div style="border:2px solid {color};border-radius:10px;
                    padding:14px;text-align:center;">
            <div style="font-size:11px;color:#666;margin-bottom:4px;">{MODULE_DESC[mk]}</div>
            <div style="font-size:34px;font-weight:bold;color:{color};">{sv:.0f}</div>
            <div style="font-size:11px;color:{color};font-weight:600;">{label}</div>
            <div style="font-size:11px;color:#aaa;margin-top:4px;">вклад: {cv:.1f} пт</div>
        </div>
        """, unsafe_allow_html=True)

contrib_vals = [lsi["contributions"].get(CONTRIB_KEY[mk], 0) or 0 for mk in [
    "M1", "M2", "M3", "M5"]]
fig_bar = go.Figure(go.Bar(
    x=[MODULE_DESC[k] for k in ["M1", "M2", "M3", "M5"]],
    y=contrib_vals,
    marker_color=[
        "#27ae60" if (scores.get(k, 0) or 0) < 40
        else "#f39c12" if (scores.get(k, 0) or 0) < 70
        else "#e74c3c"
        for k in ["M1", "M2", "M3", "M5"]
    ],
    text=[f"{v:.1f}" for v in contrib_vals],
    textposition="outside",
))
fig_bar.update_layout(height=260, margin=dict(t=10, b=10, l=10, r=10),
                      yaxis_title="Вклад в LSI (пунктов)", showlegend=False)
st.plotly_chart(fig_bar, use_container_width=True)

with st.expander("📐 Методология агрегации (формула LSI)"):
    m1v, m2v, m3v, m5v = (scores.get(k, 0) for k in ["M1", "M2", "M3", "M5"])
    base_ = m1v*0.387 + m2v*0.374 + m3v*0.152 + m5v*0.088
    st.markdown(f"""
**Метод:** взвешенная сумма (явная формула, интерпретируема по ТЗ).

```
LSI = (M1×0.387 + M2×0.374 + M3×0.152 + M5×0.088) × SF_M4
    = ({m1v:.1f}×0.387 + {m2v:.1f}×0.374 + {m3v:.1f}×0.152 + {m5v:.1f}×0.088) × {sf:.2f}
    = {base_:.1f} × {sf:.2f} = {min(base_*sf, 100):.1f} / 100
```

| Модуль | Признаки ТЗ | SNR | Вес |
|---|---|---|---|
| M1 | MAD_score_спред + MAD_score_RUONIA | 3.62 | 0.387 |
| M2 | MAD_score_cover + MAD_score_rate_spread | 3.50 | 0.374 |
| M3 | MAD_score_cover + MAD_score_yield_spread | 1.42 | 0.152 |
| M5 | MAD_score_ЦБ + MAD_score_Росказна | 0.82 | 0.088 |

**M4** — мультипликатор SF×{sf:.2f} (норма 1.0 · нал.неделя 1.1 · конец мес. 1.2 · конец кв. 1.4)
""")

st.divider()

# ВКЛАДКИ МОДУЛЕЙ
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "М1 · Резервы", "М2 · Репо ЦБ", "М3 · ОФЗ",
    "М4 · Налоги", "М5 · Казначейство", "История (Backtest)"
])

# М1
with tab1:
    df1 = r1["signals_df"].dropna(
        subset=["MAD_score_RUONIA"]) if not r1["signals_df"].empty else pd.DataFrame()
    st.metric("Стресс-оценка М1", f"{scores.get('M1', 0):.1f} / 100")
    st.caption(
        "Источник: Банк России — обязательные резервы (с 2004) и RUONIA (с 2014)")

    if not df1.empty and "spread" in df1.columns:
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(
            x=df1["date"], y=df1["spread"].fillna(0),
            name="Запас резервов (факт − норматив), млрд руб.",
            marker_color="steelblue", opacity=0.55,
        ))
        if "ruonia_avg" in df1.columns:
            fig1.add_trace(go.Scatter(
                x=df1["date"], y=df1["ruonia_avg"],
                name="Ставка RUONIA, % год.",
                yaxis="y2", line=dict(color="crimson", width=2),
            ))
        fig1.update_layout(
            title="М1: Запас резервов и ставка RUONIA",
            yaxis=dict(title="Запас, млрд руб."),
            yaxis2=dict(title="RUONIA, %", overlaying="y", side="right"),
            height=360, legend=dict(x=0, y=1.12, orientation="h"),
        )
        st.plotly_chart(fig1, use_container_width=True)

    if not df1.empty:
        fig1b = go.Figure()
        fig1b.add_trace(go.Scatter(
            x=df1["date"], y=df1["MAD_score_RUONIA"],
            name="MAD_score_RUONIA — аномалия ставки межбанка",
            line=dict(color="crimson", width=1.8),
        ))
        if "MAD_score_спред" in df1.columns:
            fig1b.add_trace(go.Scatter(
                x=df1["date"], y=df1["MAD_score_спред"],
                name="MAD_score_спред — аномалия запаса резервов",
                line=dict(color="steelblue", width=1.5),
            ))
        if "Flag_AboveKey" in df1.columns:
            above = df1[df1["Flag_AboveKey"] == 1]
            if len(above):
                fig1b.add_trace(go.Scatter(
                    x=above["date"], y=above["MAD_score_RUONIA"], mode="markers",
                    name="⚠ Flag_AboveKey — RUONIA выше ключевой",
                    marker=dict(color="orange", size=9, symbol="star"),
                ))
        fig1b.add_hline(y=0, line_color="gray",
                        line_dash="dot", line_width=0.8)
        fig1b.add_hrect(y0=2, y1=11, fillcolor="red", opacity=0.04)
        fig1b.update_layout(
            title="М1: MAD-сигналы",
            yaxis=dict(title="Отклонение от нормы (σ)", range=[-5, 11]),
            height=300, legend=dict(x=0, y=1.15, orientation="h"),
        )
        st.plotly_chart(fig1b, use_container_width=True)
        st.caption("MAD_score_RUONIA и MAD_score_спред. 0 = норма · +3σ = стресс")

# М2
with tab2:
    df2 = r2["signals_df"]
    st.metric("Стресс-оценка М2", f"{scores.get('M2', 0):.1f} / 100")
    st.caption("Источник: Банк России — итоги недельных аукционов репо (с 2010)")

    if not df2.empty and "rate_spread" in df2.columns:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df2["date"], y=df2["rate_spread"],
            name="MAD_score_rate_spread — переплата над ключевой, п.п.",
            line=dict(color="darkorange", width=1.5),
            fill="tozeroy", fillcolor="rgba(255,165,0,0.1)",
        ))
        if "MAD_score_rate_spread" in df2.columns:
            fig2.add_trace(go.Scatter(
                x=df2["date"], y=df2["MAD_score_rate_spread"],
                name="MAD_score_rate_spread (σ)",
                yaxis="y2", line=dict(color="crimson", width=1.5, dash="dot"),
            ))
        flags2 = df2[df2["Flag_Demand"] ==
                     1] if "Flag_Demand" in df2.columns else pd.DataFrame()
        if len(flags2):
            fig2.add_trace(go.Scatter(
                x=flags2["date"], y=flags2["rate_spread"], mode="markers",
                name="🔴 Flag_Demand — острый переспрос",
                marker=dict(color="red", size=9, symbol="triangle-up"),
            ))
        fig2.add_hline(y=0, line_color="gray", line_dash="dot", line_width=0.8)
        fig2.update_layout(
            title="М2: Переплата на аукционах репо ЦБ (7-дневные)",
            yaxis=dict(title="Переплата, п.п."),
            yaxis2=dict(title="MAD (σ)", overlaying="y", side="right"),
            height=380, legend=dict(x=0, y=1.18, orientation="h"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.caption(
        "Основной сигнал M2 — MAD_score_rate_spread (переплата над ключевой ставкой).")

# М3
with tab3:
    df3 = r3["signals_df"]
    st.metric("Стресс-оценка М3", f"{scores.get('M3', 0):.1f} / 100")
    st.caption("Источник: Министерство финансов РФ — результаты аукционов ОФЗ")

    if df3 is not None and not df3.empty and "cover_ratio" in df3.columns:
        auctions3 = df3.dropna(
            subset=["cover_ratio"]).copy().reset_index(drop=True)
        if len(auctions3):
            auctions3["rank"] = auctions3.groupby("date").cumcount()
            auctions3["n_per_day"] = auctions3.groupby(
                "date")["rank"].transform("count")
            auctions3["x_offset"] = auctions3.apply(
                lambda r: pd.Timedelta(days=-0.3) if r["n_per_day"] > 1 and r["rank"] == 0
                else (pd.Timedelta(days=0.3) if r["n_per_day"] > 1 and r["rank"] == 1
                      else pd.Timedelta(0)), axis=1
            )
            auctions3["x"] = auctions3["date"] + auctions3["x_offset"]

            colors3 = [
                "crimson" if c < 1.2 else
                "steelblue" if c > 2.0 else
                "#aaaaaa"
                for c in auctions3["cover_ratio"]
            ]
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=auctions3["x"],
                y=auctions3["cover_ratio"],
                marker_color=colors3,
                width=[1000 * 3600 * 24 * 0.55] *
                len(auctions3),
                showlegend=False,
            ))
            fig3.add_hline(y=1.2, line_dash="dash", line_color="orange", line_width=1.5,
                           annotation_text="< 1.2 недоспрос", annotation_position="right")
            fig3.add_hline(y=2.0, line_dash="dash", line_color="steelblue", line_width=1.5,
                           annotation_text="> 2.0 переспрос", annotation_position="right")
            fig3.update_layout(
                title="М3: Cover ratio аукционов ОФЗ (спрос / размещение)",
                yaxis_title="cover_ratio",
                xaxis=dict(type="date"),
                height=320,
            )
            st.plotly_chart(fig3, use_container_width=True)
            st.markdown(
                "🔴 Недоспрос (< 1.2) &nbsp;&nbsp; "
                "🔵 Переспрос (> 2.0) &nbsp;&nbsp; "
                "⚪ Норма (1.2 - 2.0)"
            )
            st.caption(
                "Два столбика на дату = два выпуска ОФЗ в один день. cover_ratio = спрос / размещение.")

        if "avg_yield" in df3.columns:
            fig3b = go.Figure()
            fig3b.add_trace(go.Scatter(
                x=df3["date"], y=df3["avg_yield"],
                line=dict(color="darkorange", width=2),
            ))
            fig3b.update_layout(
                title="М3: Доходность ОФЗ",
                yaxis_title="Доходность, % годовых",
                height=240, showlegend=False,
            )
            st.plotly_chart(fig3b, use_container_width=True)
            st.caption("MAD_score_cover и MAD_score_yield_spread.")
    else:
        st.info("Нет данных ОФЗ — нужны результаты аукционов Минфина")

# М4
with tab4:
    df4 = r4["signals_df"]
    tax_df = r4.get("tax_df", pd.DataFrame())
    m4_row = r4.get("latest", {})
    st.metric("Seasonal_Factor сегодня",
              f"×{m4_row.get('Seasonal_Factor', 1.0):.2f}")
    st.caption(
        "Источник: Налоговый кодекс РФ — даты ключевых платежей (2014–2027)")

    if tax_df is not None and not tax_df.empty:
        today_ts = pd.Timestamp.today()
        upcoming = tax_df[pd.to_datetime(
            tax_df["date"]) >= today_ts].head(8).copy()
        upcoming["date"] = pd.to_datetime(
            upcoming["date"]).dt.strftime("%d.%m.%Y")
        st.markdown("**Ближайшие налоговые даты:**")
        st.dataframe(upcoming[["date", "tax_type"]].rename(
            columns={"date": "Дата", "tax_type": "Налог"}
        ).reset_index(drop=True), use_container_width=True, hide_index=True)

    if df4 is not None and not df4.empty:
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(
            x=df4["date"], y=df4["Seasonal_Factor"],
            fill="tozeroy", fillcolor="rgba(128,0,128,0.07)",
            line=dict(color="purple", width=1.5),
        ))
        for val, label in [(1.1, "Tax_Week"), (1.2, "End_of_Month"), (1.4, "End_of_Quarter")]:
            fig4.add_hline(y=val, line_color="purple", line_dash="dash", line_width=0.6,
                           annotation_text=label, annotation_position="right",
                           annotation_font_size=10)
        fig4.add_hline(y=1.0, line_color="gray", line_dash="dot")
        fig4.update_layout(
            title="М4: Seasonal_Factor — налоговый мультипликатор LSI",
            yaxis=dict(title="SF", range=[0.95, 1.5]),
            height=300, showlegend=False,
        )
        st.plotly_chart(fig4, use_container_width=True)
        st.caption(
            "Tax_Week_Flag · End_of_Month_Flag · End_of_Quarter_Flag · Seasonal_Factor")

# М5
with tab5:
    df5 = r5["signals_df"]
    st.metric("Стресс-оценка М5", f"{scores.get('M5', 0):.1f} / 100")
    if not df5.empty and "balance" in df5.columns:
        bal_now = df5["balance"].iloc[-1]
        bal_sign = "дефицит" if bal_now > 0 else "профицит"
        st.caption(
            f"Источник: Банк России — структурный баланс (с 2019). Сейчас: {bal_sign} {abs(bal_now):.0f} млрд руб.")

        fig5 = go.Figure()
        fig5.add_trace(go.Scatter(
            x=df5["date"], y=df5["balance"],
            fill="tozeroy",
            fillcolor="rgba(231,76,60,0.12)" if bal_now > 0 else "rgba(39,174,96,0.12)",
            line=dict(color="steelblue", width=1.8),
            name="Структурный баланс, млрд руб.",
        ))
        if "Flag_Budget_Drain" in df5.columns:
            drains = df5[df5["Flag_Budget_Drain"] == 1]
            if len(drains):
                fig5.add_trace(go.Scatter(
                    x=drains["date"], y=drains["balance"], mode="markers",
                    name="⚠ Flag_Budget_Drain (отток > 500 млрд/нед)",
                    marker=dict(color="red", size=8, symbol="triangle-down"),
                ))
        fig5.add_hline(y=0, line_color="black", line_width=1,
                       annotation_text="граница профицит/дефицит")
        fig5.update_layout(
            title="М5: Структурный баланс ликвидности банков",
            yaxis_title="Баланс, млрд руб.",
            height=340, legend=dict(x=0, y=1.18, orientation="h"),
        )
        st.plotly_chart(fig5, use_container_width=True)

        fig5b = go.Figure()
        if "MAD_score_ЦБ" in df5.columns:
            fig5b.add_trace(go.Scatter(
                x=df5["date"], y=df5["MAD_score_ЦБ"],
                name="MAD_score_ЦБ — уровень баланса",
                line=dict(color="steelblue", width=1.8),
            ))
        if "MAD_score_Росказна" in df5.columns:
            fig5b.add_trace(go.Scatter(
                x=df5["date"], y=df5["MAD_score_Росказна"],
                name="MAD_score_Росказна — оттоки казначейства",
                line=dict(color="gray", width=1.2, dash="dot"),
            ))
        fig5b.add_hline(y=0, line_color="gray", line_dash="dot",
                        annotation_text="историческая норма", annotation_position="right")
        fig5b.update_layout(
            title="М5: MAD-сигналы",
            yaxis_title="Отклонение от нормы (σ)",
            height=260, legend=dict(x=0, y=1.18, orientation="h"),
        )
        st.plotly_chart(fig5b, use_container_width=True)
        st.caption("0 = норма · >0 дефицит (стресс) · <0 профицит (норма)")

# Backtest
with tab6:
    st.subheader("История LSI (2019 — сегодня)")
    st.caption("M5 доступен с 2019, поэтому backtest начинается с 2019.")

    if bt is None or bt.empty:
        st.warning("Не удалось построить backtest: недостаточно данных")
    else:
        bt_c = bt.dropna(subset=["LSI"]).sort_values("date").copy()
        bt_c["LSI_smooth"] = bt_c["LSI"].rolling(
            7, min_periods=1, center=True).mean()

        fig_bt = go.Figure()
        for status, color, label in [
            ("GREEN",  "#27ae60", "🟢 Норма (0-40)"),
            ("YELLOW", "#f39c12", "🟡 Внимание (40-70)"),
            ("RED",    "#e74c3c", "🔴 Стресс (70-100)"),
        ]:
            sub = bt_c[bt_c["status"] == status]
            fig_bt.add_trace(go.Scatter(
                x=sub["date"], y=sub["LSI"], mode="markers",
                name=label, marker=dict(color=color, size=3, opacity=0.45),
            ))
        fig_bt.add_trace(go.Scatter(
            x=bt_c["date"], y=bt_c["LSI_smooth"],
            mode="lines", name="LSI (7-дн. среднее)",
            line=dict(color="#e74c3c", width=2),
        ))
        for s, e, label in [
            ("2022-02-01", "2022-05-01", "Фев-май 2022<br>Геополитический шок"),
            ("2023-07-01", "2023-10-01", "Авг-окт 2023<br>Валютный стресс"),
        ]:
            fig_bt.add_vrect(x0=s, x1=e, fillcolor="red", opacity=0.07,
                             annotation_text=label, annotation_position="top left",
                             annotation_font_size=10)
        fig_bt.add_hline(y=40, line_dash="dash", line_color="#27ae60",
                         annotation_text="порог внимания (40)")
        fig_bt.add_hline(y=70, line_dash="dash", line_color="#e74c3c",
                         annotation_text="порог стресса (70)")
        fig_bt.update_layout(
            title="Индекс стресса ликвидности — история",
            yaxis=dict(range=[0, 105], title="LSI"),
            xaxis_title="Дата",
            height=440, legend=dict(x=0, y=1.12, orientation="h"),
        )
        st.plotly_chart(fig_bt, use_container_width=True)

        norm = bt_c[~(bt_c["date"].between("2022-02-01", "2022-05-01") |
                      bt_c["date"].between("2023-07-01", "2023-10-01"))]
        rows = [{"Период": "Норма", "Медиана LSI": norm["LSI"].median(),
                 "Макс LSI": norm["LSI"].max(), "Дней": len(norm)}]
        for s, e, lbl in [("2022-02-01", "2022-05-01", "Фев-май 2022"),
                          ("2023-07-01", "2023-10-01", "Авг-окт 2023")]:
            sub = bt_c[bt_c["date"].between(s, e)]
            if len(sub):
                rows.append({"Период": lbl, "Медиана LSI": sub["LSI"].median(),
                             "Макс LSI": sub["LSI"].max(), "Дней": len(sub)})
        st.dataframe(pd.DataFrame(rows).round(
            1), use_container_width=True, hide_index=True)

        st.markdown("**Последние 20 дней:**")
        recent = bt_c[["date", "LSI", "status"]].tail(20).copy()
        recent["status"] = recent["status"].map(
            {"GREEN": "🟢 Норма", "YELLOW": "🟡 Внимание", "RED": "🔴 Стресс"})
        st.dataframe(recent.round(1).reset_index(drop=True),
                     use_container_width=True, hide_index=True)
