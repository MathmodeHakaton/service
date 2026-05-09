"""Страница 1: Обзор — LSI gauge, статус, вклад модулей."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def show(result):
    lsi      = result.lsi
    lsi_pct  = lsi.value * 100
    signals  = {s.module_name: s for s in result.signals}

    status_map = {
        "normal":   ("🟢", "НОРМА",    "#27ae60"),
        "warning":  ("🟡", "ВНИМАНИЕ", "#f39c12"),
        "critical": ("🔴", "СТРЕСС",   "#e74c3c"),
    }
    icon, status_ru, color = status_map.get(lsi.status, ("⚪", lsi.status, "#95a5a6"))

    st.title("RU Liquidity Sentinel")
    st.caption(f"Данные: ЦБ РФ · Минфин · ФНС  |  {result.computed_at}")

    # ── Главный экран ──────────────────────────────────────────────────────
    col_gauge, col_sf, col_status = st.columns([2, 1, 1])

    with col_gauge:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=lsi_pct,
            title={"text": "Индекс стресса ликвидности", "font": {"size": 13}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1,
                         "tickvals": [0, 40, 70, 100],
                         "ticktext": ["0", "40 — внимание", "70 — стресс", "100"]},
                "bar": {"color": "#2c3e50"},
                "steps": [
                    {"range": [0,  40], "color": "#d5f5e3"},
                    {"range": [40, 70], "color": "#fdebd0"},
                    {"range": [70, 100], "color": "#fadbd8"},
                ],
            },
            number={"suffix": " / 100", "font": {"size": 28}},
        ))
        fig.update_layout(height=230, margin=dict(t=40, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    # Seasonal Factor из M4
    m4_sig = signals.get("M4_TAX")
    sf = 1.0
    if m4_sig and m4_sig.mad_scores:
        sf = float(m4_sig.mad_scores[0])

    with col_sf:
        sf_label = ("Конец квартала" if sf >= 1.4 else
                    "Конец месяца"   if sf >= 1.2 else
                    "Налоговая неделя" if sf >= 1.1 else "Норма")
        st.metric("Сезонный коэффициент", f"×{sf:.2f}",
                  help="Налоговое давление: ×1.4 конец квартала, ×1.0 норма")
        if sf >= 1.4:
            st.error(f"⚠ {sf_label}")
        elif sf >= 1.2:
            st.warning(f"↑ {sf_label}")
        elif sf >= 1.1:
            st.info(f"↑ {sf_label}")
        else:
            st.success("✓ Норма")

    with col_status:
        st.markdown(f"""
        <div style="background:{color};color:white;padding:24px 16px;
                    border-radius:12px;text-align:center;margin-top:10px;">
            <div style="font-size:20px;font-weight:bold;">{icon} {status_ru}</div>
            <div style="font-size:36px;font-weight:bold;margin:4px 0;">{lsi_pct:.1f}</div>
            <div style="font-size:12px;opacity:0.85;">из 100</div>
        </div>""", unsafe_allow_html=True)

    st.divider()

    # ── Вклад модулей ──────────────────────────────────────────────────────
    st.subheader("Вклад модулей в индекс стресса")

    MODULE_LABELS = {
        "M1_RESERVES": "М1 · Резервы и RUONIA",
        "M2_REPO":     "М2 · Репо ЦБ",
        "M3_OFZ":      "М3 · Аукционы ОФЗ",
        "M4_TAX":      "М4 · Налоговый период",
        "M5_TREASURY": "М5 · Казначейство",
    }
    MODULE_DESC = {
        "M1_RESERVES": "Корсчета банков + ставка межбанка",
        "M2_REPO":     "Аукционы репо Банка России",
        "M3_OFZ":      "Размещение гособлигаций Минфина",
        "M4_TAX":      "Сезонный налоговый контекст",
        "M5_TREASURY": "Баланс ликвидности банковского сектора",
    }

    cols = st.columns(5)
    for col, sig in zip(cols, result.signals):
        sv = sig.value * 100
        cv = lsi.contributions.get(sig.module_name, 0) * 100
        c  = "#27ae60" if sv < 40 else "#f39c12" if sv < 70 else "#e74c3c"
        lbl = "норма" if sv < 40 else "внимание" if sv < 70 else "стресс"
        with col:
            st.markdown(f"""
            <div style="border:2px solid {c};border-radius:10px;padding:12px;text-align:center;">
                <div style="font-size:10px;color:#666;">{MODULE_DESC.get(sig.module_name,'')}</div>
                <div style="font-size:30px;font-weight:bold;color:{c};">{sv:.0f}</div>
                <div style="font-size:10px;color:{c};">{lbl}</div>
                <div style="font-size:10px;color:#aaa;">вклад: {cv:.1f}</div>
            </div>""", unsafe_allow_html=True)

    # Bar chart вклада
    active = [(sig.module_name, lsi.contributions.get(sig.module_name, 0) * 100)
              for sig in result.signals if sig.module_name != "M4_TAX"]
    if active:
        names, vals = zip(*active)
        colors = [
            "#27ae60" if (signals.get(n, None) and signals[n].value < 0.4) else
            "#f39c12" if (signals.get(n, None) and signals[n].value < 0.7) else
            "#e74c3c"
            for n in names
        ]
        fig2 = go.Figure(go.Bar(
            x=[MODULE_DESC.get(n, n) for n in names],
            y=vals,
            marker_color=colors,
            text=[f"{v:.1f} пт" for v in vals],
            textposition="outside",
        ))
        fig2.update_layout(height=260, margin=dict(t=10, b=10, l=10, r=10),
                           yaxis_title="Вклад в LSI (пунктов)", showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
