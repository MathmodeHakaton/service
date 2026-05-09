"""Страница 2: Детали модулей M1–M5."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def show(result):
    st.title("Детали модулей")

    signals = {s.module_name: s for s in result.signals}
    data    = result.raw_data

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "М1 · Резервы", "М2 · Репо ЦБ", "М3 · ОФЗ",
        "М4 · Налоги", "М5 · Казначейство"
    ])

    # ── М1 ────────────────────────────────────────────────────────────────
    with tab1:
        sig = signals.get("M1_RESERVES")
        _metric(sig, "М1 — Обязательные резервы + RUONIA",
                "0.60·RUONIA(w=1000) + 0.40·rel_spread. Выше = больше стресса.")
        st.caption("Источник: cbr.ru — резервы (с 2004), RUONIA (с 2014)")

        res_df = data.get("reserves", pd.DataFrame())
        ru_df  = data.get("ruonia",   pd.DataFrame())
        kr_df  = data.get("keyrate",  pd.DataFrame())

        if not res_df.empty and not ru_df.empty:
            # Вычисляем сигналы внутри страницы для графиков
            from src.domain.modules.m1_reserves import M1Reserves
            m1 = M1Reserves()
            df1 = m1._calculate(res_df, ru_df, kr_df if not kr_df.empty else pd.DataFrame())
            df1 = df1.dropna(subset=["MAD_score_RUONIA"]).sort_values("date")

            fig = go.Figure()
            fig.add_trace(go.Bar(x=df1["date"], y=df1["spread"].fillna(0),
                                 name="Запас над нормативом резервов, млрд руб.",
                                 marker_color="steelblue", opacity=0.5))
            fig.add_trace(go.Scatter(x=df1["date"], y=df1["ruonia_avg"],
                                     name="RUONIA, % годовых", yaxis="y2",
                                     line=dict(color="crimson", width=2)))
            fig.update_layout(title="М1: Запас резервов и ставка RUONIA",
                              yaxis=dict(title="Запас, млрд руб."),
                              yaxis2=dict(title="RUONIA, %", overlaying="y", side="right"),
                              height=350, legend=dict(x=0, y=1.12, orientation="h"))
            st.plotly_chart(fig, use_container_width=True)

            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=df1["date"], y=df1["MAD_score_RUONIA"],
                                      name="Аномалия RUONIA (основной, вес 60%)",
                                      line=dict(color="crimson", width=1.8)))
            if "MAD_score_rel_spread" in df1.columns:
                fig2.add_trace(go.Scatter(x=df1["date"], y=df1["MAD_score_rel_spread"],
                                          name="Аномалия запаса резервов (вес 40%)",
                                          line=dict(color="steelblue", width=1.5)))
            if "Flag_AboveKey" in df1.columns:
                above = df1[df1["Flag_AboveKey"] == 1]
                if len(above):
                    fig2.add_trace(go.Scatter(x=above["date"], y=above["MAD_score_RUONIA"],
                                              mode="markers",
                                              name="⚠ RUONIA выше ключевой — дефицит",
                                              marker=dict(color="orange", size=8, symbol="star")))
            fig2.add_hline(y=0, line_color="gray", line_dash="dot",
                           annotation_text="норма", annotation_position="right")
            fig2.add_hrect(y0=2, y1=10.5, fillcolor="red", opacity=0.04,
                           annotation_text="зона стресса")
            fig2.update_layout(title="М1: Отклонение от исторической нормы (σ)",
                               yaxis=dict(title="σ", range=[-5, 11]),
                               height=280, legend=dict(x=0, y=1.15, orientation="h"))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("Нет данных резервов/RUONIA для графиков")

    # ── М2 ────────────────────────────────────────────────────────────────
    with tab2:
        sig = signals.get("M2_REPO")
        _metric(sig, "М2 — Аукционы репо ЦБ (7-дневные)",
                "Переплата над ключевой ставкой. Выше = банки платят премию за ликвидность.")
        st.caption("Источник: cbr.ru — итоги аукционов репо (с 2010)")

        repo_df = data.get("repo", pd.DataFrame())
        kr_df   = data.get("keyrate", pd.DataFrame())
        par_df  = data.get("repo_params", pd.DataFrame())

        if not repo_df.empty and not kr_df.empty:
            from src.domain.modules.m2_repo import M2Repo
            m2 = M2Repo()
            df2 = m2._calculate(repo_df, kr_df, par_df)
            df2 = df2.sort_values("date")

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df2["date"], y=df2["rate_spread"],
                                     name="Переплата над ключевой ставкой, п.п.",
                                     line=dict(color="darkorange", width=1.5),
                                     fill="tozeroy", fillcolor="rgba(255,165,0,0.08)"))
            fig.add_trace(go.Scatter(x=df2["date"], y=df2["MAD_score_rate_spread"],
                                     name="Аномалия переплаты (правая ось)", yaxis="y2",
                                     line=dict(color="crimson", width=1.5, dash="dot")))
            flagged = df2[df2["Flag_Demand"] == 1]
            if len(flagged):
                fig.add_trace(go.Scatter(x=flagged["date"], y=flagged["rate_spread"],
                                         mode="markers",
                                         name="🔴 Острый стресс (аномалия > 3.5σ)",
                                         marker=dict(color="red", size=8, symbol="triangle-up")))
            fig.add_hline(y=0, line_color="gray", line_dash="dot")
            fig.update_layout(title="М2: Переплата на аукционах репо ЦБ",
                              yaxis=dict(title="Переплата, п.п."),
                              yaxis2=dict(title="σ", overlaying="y", side="right"),
                              height=380, legend=dict(x=0, y=1.15, orientation="h"))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных репо для графиков")

    # ── М3 ────────────────────────────────────────────────────────────────
    with tab3:
        sig = signals.get("M3_OFZ")
        _metric(sig, "М3 — Аукционы ОФЗ Минфина",
                "Низкий спрос (bid_cover < 0.25) = стресс. cover_ratio = demand/placement.")
        st.caption("Источник: minfin.gov.ru — результаты аукционов ОФЗ")

        ofz_df = data.get("ofz", pd.DataFrame())
        if ofz_df is not None and not ofz_df.empty:
            from src.domain.modules.m3_ofz import M3OFZ
            m3 = M3OFZ()
            df3 = m3._calculate(ofz_df)
            auctions = df3[df3.get("auction_format", pd.Series("")).str.contains(
                "Аукцион|AUCTION", case=False, na=False)] if "auction_format" in df3.columns \
                else df3.dropna(subset=["cover_ratio"])

            if len(auctions):
                colors = ["crimson" if c < 1.5 else "steelblue" if c > 2.5 else "#888"
                          for c in auctions["cover_ratio"]]
                fig = go.Figure(go.Bar(x=auctions["date"], y=auctions["cover_ratio"],
                                       marker_color=colors))
                fig.add_hline(y=1.5, line_dash="dash", line_color="orange",
                              annotation_text="Слабый спрос < 1.5×")
                fig.add_hline(y=2.5, line_dash="dash", line_color="steelblue",
                              annotation_text="Переспрос > 2.5×")
                fig.update_layout(title="М3: Коэффициент спроса (demand / placement)",
                                  height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            if "avg_yield" in df3.columns:
                fig2 = go.Figure(go.Scatter(x=df3["date"], y=df3["avg_yield"],
                                            line=dict(color="darkorange", width=2)))
                fig2.update_layout(title="М3: Средневзвешенная доходность ОФЗ, %",
                                   height=220, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Нет данных ОФЗ (загрузка с Минфина или кэш недоступны)")

    # ── М4 ────────────────────────────────────────────────────────────────
    with tab4:
        sig = signals.get("M4_TAX")
        _metric(sig, "М4 — Налоговый период",
                "Сезонный мультипликатор: ×1.4 конец квартала, ×1.2 конец месяца, ×1.1 налоговая неделя.")
        st.caption("Источник: НК РФ — генерируется программно (2014–2027)")

        tax_df = data.get("tax_calendar", pd.DataFrame())
        if tax_df is not None and not tax_df.empty:
            tax_df["date"] = pd.to_datetime(tax_df["date"])
            today = pd.Timestamp.today()

            upcoming = tax_df[tax_df["date"] >= today].head(8).copy()
            upcoming["date"] = upcoming["date"].dt.strftime("%d.%m.%Y")
            upcoming = upcoming.rename(columns={"date": "Дата", "tax_type": "Налог",
                                                "description": "Описание"})
            st.markdown("**Ближайшие налоговые даты:**")
            st.dataframe(upcoming[["Дата","Налог","Описание"]].reset_index(drop=True),
                         use_container_width=True, hide_index=True)

            # График Seasonal_Factor
            from src.domain.modules.m4_tax import M4Tax
            m4 = M4Tax()
            dates = pd.date_range("2019-01-01", today, freq="D")
            df4 = m4.compute_for_series(pd.Series(dates), tax_df)
            fig = go.Figure(go.Scatter(x=df4["date"], y=df4["Seasonal_Factor"],
                                       fill="tozeroy", fillcolor="rgba(128,0,128,0.07)",
                                       line=dict(color="purple", width=1.5)))
            fig.add_hline(y=1.0, line_color="gray", line_dash="dot",
                          annotation_text="норма", annotation_position="right")
            for val, label in [(1.1,"Налоговая неделя"), (1.2,"Конец месяца"),
                               (1.4,"Конец квартала")]:
                fig.add_hline(y=val, line_color="purple", line_dash="dash", line_width=0.7,
                              annotation_text=label, annotation_position="right",
                              annotation_font_size=9)
            fig.update_layout(title="М4: Налоговый коэффициент давления",
                              yaxis=dict(title="Коэффициент", range=[0.95, 1.5]),
                              height=280, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # ── М5 ────────────────────────────────────────────────────────────────
    with tab5:
        sig = signals.get("M5_TREASURY")
        _metric(sig, "М5 — Баланс ликвидности банковского сектора",
                "Инвертирован: дефицит (balance < 0) = стресс. Текущий профицит = нет стресса.")
        bliq_df = data.get("bliquidity", pd.DataFrame())
        if bliq_df is not None and not bliq_df.empty:
            from src.domain.modules.m5_treasury import M5Treasury
            m5 = M5Treasury()
            df5 = m5._calculate(bliq_df).sort_values("date")
            bal_now = df5["balance"].iloc[-1]
            st.caption(f"Источник: cbr.ru/hd_base/bliquidity — с 2019. "
                       f"Сейчас: {'профицит' if bal_now > 0 else 'дефицит'} "
                       f"{abs(bal_now):.0f} млрд руб.")

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df5["date"], y=df5["balance"],
                                     fill="tozeroy", fillcolor="rgba(70,130,180,0.1)",
                                     line=dict(color="steelblue", width=1.8),
                                     name="Структурный баланс (+ профицит / − дефицит), млрд руб."))
            drains = df5[df5["Flag_Budget_Drain"] == 1]
            if len(drains):
                fig.add_trace(go.Scatter(x=drains["date"], y=drains["balance"],
                                         mode="markers",
                                         name="⚠ Резкий отток казначейства (> 500 млрд/нед)",
                                         marker=dict(color="red", size=8, symbol="triangle-down")))
            fig.add_hline(y=0, line_color="black", line_width=1,
                          annotation_text="граница профицит/дефицит", annotation_position="right",
                          annotation_font_size=10)
            fig.update_layout(title="М5: Структурный баланс ликвидности",
                              height=340, legend=dict(x=0, y=1.15, orientation="h"))
            st.plotly_chart(fig, use_container_width=True)

            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=df5["date"], y=df5["MAD_score_ЦБ"],
                                      name="Аномалия уровня баланса (вес 88%)",
                                      line=dict(color="steelblue", width=1.8)))
            fig2.add_trace(go.Scatter(x=df5["date"], y=df5["MAD_score_delta"],
                                      name="Аномалия недельного изменения (вес 12%)",
                                      line=dict(color="gray", width=1.2, dash="dot")))
            fig2.add_hline(y=0, line_color="gray", line_dash="dot",
                           annotation_text="историческая норма", annotation_position="right")
            fig2.update_layout(title="М5: Отклонение от нормы (σ). Знак инвертирован: дефицит = стресс",
                               height=250, legend=dict(x=0, y=1.15, orientation="h"))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("Нет данных баланса ликвидности")


def _metric(sig, title: str, help_text: str):
    if sig:
        sv = sig.value * 100
        st.metric(f"Стресс-оценка", f"{sv:.1f} / 100", help=help_text)
    st.subheader(title)
