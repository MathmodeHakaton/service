"""Страница 3: Backtest LSI 2019–сегодня."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def show(result):
    st.title("История LSI (Backtest)")
    st.caption("Рассчитано на исторических данных. Декабрь 2014 отсутствует — M5 с 2019.")

    data    = result.raw_data
    signals = {s.module_name: s for s in result.signals}

    with st.spinner("Расчёт исторического LSI..."):
        bt = _build_backtest(data)

    if bt is None or bt.empty:
        st.warning("Не удалось построить backtest: недостаточно данных")
        return

    bt_clean = bt.dropna(subset=["LSI"])

    # ── Основной график ────────────────────────────────────────────────────
    bt_clean = bt_clean.sort_values("date").copy()
    bt_clean["LSI_smooth"] = bt_clean["LSI"].rolling(window=7, min_periods=1, center=True).mean()

    fig = go.Figure()
    for status, color, label in [
        ("GREEN",  "#27ae60", "🟢 Норма (0–40)"),
        ("YELLOW", "#f39c12", "🟡 Внимание (40–70)"),
        ("RED",    "#e74c3c", "🔴 Стресс (70–100)"),
    ]:
        sub = bt_clean[bt_clean["status"] == status]
        fig.add_trace(go.Scatter(x=sub["date"], y=sub["LSI"], mode="markers",
                                 name=label, marker=dict(color=color, size=3, opacity=0.45)))
    fig.add_trace(go.Scatter(
        x=bt_clean["date"], y=bt_clean["LSI_smooth"],
        mode="lines", name="LSI (7-дн. среднее)",
        line=dict(color="#e74c3c", width=2), showlegend=True,
    ))

    for s, e, label in [("2022-02-01","2022-05-01","Фев–май 2022\nГеополитический шок"),
                         ("2023-07-01","2023-10-01","Авг–окт 2023\nВалютный стресс")]:
        fig.add_vrect(x0=s, x1=e, fillcolor="red", opacity=0.07,
                      annotation_text=label, annotation_position="top left",
                      annotation_font_size=10)

    fig.add_hline(y=40, line_dash="dash", line_color="#27ae60", line_width=1,
                  annotation_text="порог внимания (40)", annotation_position="right",
                  annotation_font_size=10)
    fig.add_hline(y=70, line_dash="dash", line_color="#e74c3c", line_width=1,
                  annotation_text="порог стресса (70)", annotation_position="right",
                  annotation_font_size=10)
    fig.update_layout(title="Индекс стресса ликвидности по дням",
                      yaxis=dict(range=[0, 105], title="LSI"),
                      xaxis_title="Дата", height=440,
                      legend=dict(x=0, y=1.12, orientation="h"))
    st.plotly_chart(fig, use_container_width=True)

    # ── Сводка ────────────────────────────────────────────────────────────
    st.markdown("**Сравнение стресс-эпизодов с нормой:**")
    episodes = [("2022-02-01","2022-05-01","Фев–май 2022 — геополитика"),
                ("2023-07-01","2023-10-01","Авг–окт 2023 — валюта/инфляция")]
    norm = bt_clean[~(bt_clean["date"].between("2022-02-01","2022-05-01") |
                      bt_clean["date"].between("2023-07-01","2023-10-01"))]
    rows = [{"Период": "Норма", "Медиана LSI": norm["LSI"].median(),
             "Макс LSI": norm["LSI"].max(), "Дней": len(norm)}]
    for s, e, label in episodes:
        sub = bt_clean[bt_clean["date"].between(s, e)]
        if len(sub):
            rows.append({"Период": label, "Медиана LSI": sub["LSI"].median(),
                         "Макс LSI": sub["LSI"].max(), "Дней": len(sub)})
    st.dataframe(pd.DataFrame(rows).round(1), use_container_width=True, hide_index=True)

    st.markdown("**Последние 20 дней:**")
    recent = bt_clean[["date","LSI","status"]].tail(20).copy()
    recent["status"] = recent["status"].map(
        {"GREEN":"🟢 Норма","YELLOW":"🟡 Внимание","RED":"🔴 Стресс"})
    st.dataframe(recent.round(1).reset_index(drop=True),
                 use_container_width=True, hide_index=True)

    # ── Валидация на отложенной выборке (по ТЗ) ───────────────────────────
    st.divider()
    st.subheader("Валидация на отложенных эпизодах")
    st.caption("Эпизоды НЕ использовались при калибровке весов (дек 2014, фев 2022, авг 2023).")

    holdout = [
        ("2020-03-01", "2020-05-01", "Мар–май 2020 — COVID-шок"),
        ("2024-06-01", "2024-12-31", "Июн–дек 2024 — пик ставки 21%"),
    ]
    h_rows = []
    for s, e, label in holdout:
        sub = bt_clean[bt_clean["date"].between(s, e)]
        if len(sub):
            h_rows.append({
                "Период (отложенный)": label,
                "Медиана LSI": sub["LSI"].median(),
                "Макс LSI":    sub["LSI"].max(),
                "% дней в стрессе (>70)": round(100 * (sub["LSI"] > 70).mean(), 1),
                "Дней": len(sub),
            })
    if h_rows:
        st.dataframe(pd.DataFrame(h_rows).round(1), use_container_width=True, hide_index=True)

    # Подсветка отложенных эпизодов на графике (отдельный мини-график)
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(
        x=bt_clean["date"], y=bt_clean["LSI_smooth"],
        mode="lines", line=dict(color="#e74c3c", width=1.5), showlegend=False,
    ))
    for s, e, label in holdout:
        fig_h.add_vrect(x0=s, x1=e, fillcolor="orange", opacity=0.15,
                        annotation_text=label, annotation_position="top left",
                        annotation_font_size=10)
    fig_h.add_hline(y=40, line_dash="dash", line_color="#27ae60", line_width=1)
    fig_h.add_hline(y=70, line_dash="dash", line_color="#e74c3c", line_width=1)
    fig_h.update_layout(title="LSI на отложенных эпизодах (оранжевые зоны)",
                        yaxis=dict(range=[0, 105], title="LSI"),
                        height=300, margin=dict(t=40, b=20))
    st.plotly_chart(fig_h, use_container_width=True)

    # ── Sensitivity analysis ±20% весов (по ТЗ) ───────────────────────────
    st.divider()
    st.subheader("Sensitivity Analysis — устойчивость LSI к изменению весов ±20%")
    st.caption("Как меняется текущий LSI при отклонении каждого веса на ±20%.")

    sig_list = result.signals if hasattr(result, "signals") else []
    _show_sensitivity(sig_list)


def _show_sensitivity(sig_list):
    import numpy as np
    from src.domain.aggregation.lsi_engine import LSIEngine

    WEIGHTS = {
        "M1_RESERVES": 0.387,
        "M2_REPO":     0.374,
        "M3_OFZ":      0.152,
        "M5_TREASURY": 0.088,
    }

    if not sig_list:
        st.info("Нет сигналов для sensitivity analysis")
        return

    base_engine = LSIEngine(weights=WEIGHTS)
    base_lsi    = base_engine.compute(sig_list).value * 100

    rows = []
    for mod, base_w in WEIGHTS.items():
        for delta_pct, label in [(-20, "-20%"), (+20, "+20%")]:
            new_weights = {**WEIGHTS, mod: base_w * (1 + delta_pct / 100)}
            total = sum(new_weights[k] for k in WEIGHTS)
            new_weights = {k: new_weights[k] / total * sum(WEIGHTS.values()) for k in WEIGHTS}
            engine  = LSIEngine(weights=new_weights)
            new_lsi = engine.compute(sig_list).value * 100
            rows.append({
                "Модуль":    mod,
                "Изменение": label,
                "Вес было":  round(base_w, 3),
                "Вес стало": round(new_weights[mod], 3),
                "LSI базовый": round(base_lsi, 1),
                "LSI новый":   round(new_lsi, 1),
                "Δ LSI":       round(new_lsi - base_lsi, 1),
            })

    df_s = pd.DataFrame(rows)
    st.dataframe(df_s, use_container_width=True, hide_index=True)

    max_delta = df_s["Δ LSI"].abs().max()
    st.caption(f"Максимальное отклонение LSI при ±20% любого веса: {max_delta:.1f} пунктов. "
               f"{'Система устойчива.' if max_delta < 5 else 'Значимая чувствительность — проверьте веса.'}")


def _build_backtest(data: dict) -> pd.DataFrame | None:
    """Строит исторический LSI через агрегатор service."""
    try:
        import numpy as np
        from src.domain.modules.m1_reserves import M1Reserves
        from src.domain.modules.m2_repo     import M2Repo
        from src.domain.modules.m4_tax      import M4Tax
        from src.domain.modules.m5_treasury import M5Treasury
        from src.domain.aggregation.lsi_engine import LSIEngine

        bliq = data.get("bliquidity", pd.DataFrame())
        if bliq is None or bliq.empty:
            return None

        m5 = M5Treasury()
        df5 = m5._calculate(bliq).sort_values("date").reset_index(drop=True)

        # M2 weekly → merge по дате
        m2 = M2Repo()
        repo_df = data.get("repo", pd.DataFrame())
        kr_df   = data.get("keyrate", pd.DataFrame())
        par_df  = data.get("repo_params", pd.DataFrame())
        df2 = pd.DataFrame()
        if not repo_df.empty and not kr_df.empty:
            df2 = m2._calculate(repo_df, kr_df, par_df)
            df2["week"] = df2["date"].dt.to_period("W")
            df2_w = df2.groupby("week").agg(
                date=("date","last"),
                MAD_rate=("MAD_score_rate_spread","mean"),
                Flag_Demand=("Flag_Demand","max"),
            ).reset_index(drop=True)
        else:
            df2_w = pd.DataFrame()

        # M1 monthly
        res_df = data.get("reserves", pd.DataFrame())
        ru_df  = data.get("ruonia", pd.DataFrame())
        m1 = M1Reserves()
        df1 = pd.DataFrame()
        if not res_df.empty and not ru_df.empty:
            df1 = m1._calculate(res_df, ru_df, kr_df)
            df1["month"] = df1["date"].dt.to_period("M")

        # M4
        m4 = M4Tax()
        tax_df = data.get("tax_calendar", pd.DataFrame())

        # Base grid: дни из M5
        base = df5[["date","balance","weekly_delta","MAD_score_ЦБ",
                    "MAD_score_delta","Flag_Budget_Drain"]].copy()

        # M1 → merge by month
        if not df1.empty:
            df1_m = df1[["month","MAD_score_RUONIA","MAD_score_rel_spread"]].copy()
            df1_m["month"] = df1["date"].dt.to_period("M")
            base["month"] = base["date"].dt.to_period("M")
            base = base.merge(df1_m, on="month", how="left").drop(columns="month")

        # M2 → merge asof
        if not df2_w.empty:
            base = pd.merge_asof(
                base.sort_values("date"),
                df2_w.rename(columns={"MAD_rate":"M2_MAD","Flag_Demand":"M2_Flag"})
                     .sort_values("date"),
                on="date", direction="backward",
            )

        # M4 flags
        if tax_df is not None and not tax_df.empty:
            tax_df["date"] = pd.to_datetime(tax_df["date"])
            df4 = m4.compute_for_series(base["date"], tax_df)
            base = base.merge(df4[["date","Seasonal_Factor"]], on="date", how="left")
        base["Seasonal_Factor"] = base.get("Seasonal_Factor", pd.Series(1.0, index=base.index)).fillna(1.0)

        engine = LSIEngine()

        def _row_lsi(row):
            from src.domain.models.module_signal import ModuleSignal
            sigs = []
            # M1
            m1v = m1._score({"MAD_score_RUONIA": row.get("MAD_score_RUONIA"),
                             "MAD_score_rel_spread": row.get("MAD_score_rel_spread"),
                             "Flag_AboveKey": 0, "Flag_EndOfPeriod": 0})
            sigs.append(ModuleSignal("M1_RESERVES", m1v, [], [], "normal", m1v * 0.25))
            # M2
            m2v = m2._score({"MAD_score_rate_spread": row.get("M2_MAD"),
                             "Flag_Demand": row.get("M2_Flag", 0)})
            sigs.append(ModuleSignal("M2_REPO", m2v, [], [], "normal", m2v * 0.333))
            # M5 (inverted)
            m5v = m5._score({"MAD_score_ЦБ": row.get("MAD_score_ЦБ"),
                             "MAD_score_delta": row.get("MAD_score_delta"),
                             "Flag_Budget_Drain": row.get("Flag_Budget_Drain", 0)})
            sigs.append(ModuleSignal("M5_TREASURY", m5v, [], [], "normal", m5v * 0.167))

            lsi_r = engine.compute(sigs)
            lsi_val = lsi_r.value * 100 * row.get("Seasonal_Factor", 1.0)
            lsi_val = min(lsi_val, 100.0)
            status = "RED" if lsi_val > 70 else "YELLOW" if lsi_val > 40 else "GREEN"
            return pd.Series({"LSI": lsi_val, "status": status})

        scores = base.apply(_row_lsi, axis=1)
        result = pd.concat([base[["date"]].reset_index(drop=True),
                            scores.reset_index(drop=True)], axis=1)
        return result.sort_values("date").reset_index(drop=True)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("Backtest error: %s", e)
        return None
