import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
<<<<<<< HEAD
=======
import numpy as np
>>>>>>> feature/merge

st.set_page_config(page_title="М2 · Репо ЦБ", page_icon="📊", layout="wide")
st.title("М2 — Аукционы репо ЦБ (7-дневные)")

from src.presentation.data_loader import load_all
<<<<<<< HEAD
=======
from src.domain.normalization.mad import mad_normalize

>>>>>>> feature/merge
with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

scores = lsi["scores"]
<<<<<<< HEAD
df2 = r2["signals_df"]

st.metric("Стресс-оценка М2", f"{scores.get('M2', 0):.1f} / 100")
st.caption("Источник: Банк России — итоги недельных аукционов репо (с 2010)")

if not df2.empty and "rate_spread" in df2.columns:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df2["date"], y=df2["rate_spread"],
        name="Переплата над ключевой ставкой, п.п.",
        line=dict(color="darkorange", width=1.5),
        fill="tozeroy", fillcolor="rgba(255,165,0,0.1)",
    ))
    if "MAD_score_rate_spread" in df2.columns:
        fig2.add_trace(go.Scatter(
            x=df2["date"], y=df2["MAD_score_rate_spread"],
            name="MAD_score_rate_spread (σ)",
            yaxis="y2", line=dict(color="crimson", width=1.5, dash="dot"),
        ))
    if "Flag_Demand" in df2.columns:
        flags2 = df2[df2["Flag_Demand"] == 1]
        if len(flags2):
            fig2.add_trace(go.Scatter(
                x=flags2["date"], y=flags2["rate_spread"], mode="markers",
                name="🔴 Flag_Demand — острый переспрос",
                marker=dict(color="red", size=9, symbol="triangle-up"),
            ))
    fig2.add_hline(y=0, line_color="gray", line_dash="dot", line_width=0.8)
    fig2.update_layout(
        title="М2: Переплата банков на аукционах репо ЦБ",
        yaxis=dict(title="Переплата, п.п."),
        yaxis2=dict(title="MAD (σ)", overlaying="y", side="right"),
        height=380, legend=dict(x=0, y=1.18, orientation="h"),
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("Переплата > 0 = банки готовы брать деньги дороже ключевой ставки.")
=======
df2    = r2["signals_df"]

st.caption("Источник: ЦБ РФ — RUONIA vs ключевая ставка")

ROOT = Path(__file__).parent.parent.parent.parent

# ── Загружаем данные ───────────────────────────────────────────────────────
try:
    ruonia  = pd.read_csv(ROOT / "cache/cbr/ruonia.csv",   parse_dates=["date"])
    keyrate = pd.read_csv(ROOT / "cache/cbr/keyrate.csv",  parse_dates=["date"])
    kr = keyrate.sort_values("date").set_index("date")["keyrate"]
    ruonia["rate_spread"] = ruonia["ruonia"] - ruonia["date"].map(
        lambda d: kr.asof(d) if d >= kr.index.min() else np.nan)
    ruonia = ruonia[ruonia["date"] >= "2013-09-13"].dropna(subset=["rate_spread"])
    has_data = True
except Exception:
    has_data = False
    st.error("Нет данных RUONIA")
    st.stop()

# ── Выбор периода ─────────────────────────────────────────────────────────
st.markdown("### Выберите период")

col_btns = st.columns(5)
presets = {"6 месяцев": 180, "1 год": 365, "3 года": 1095, "5 лет": 1825, "Всё": None}
selected_preset = st.session_state.get("m2_preset", "3 года")

for i, (label, days) in enumerate(presets.items()):
    with col_btns[i]:
        if st.button(label, use_container_width=True,
                     type="primary" if label == selected_preset else "secondary"):
            st.session_state["m2_preset"] = label
            selected_preset = label

# Вычисляем границы по кнопке
date_max = ruonia["date"].max()
date_min = ruonia["date"].min()
preset_days = presets[selected_preset]
if preset_days:
    date_start = max(date_min, date_max - pd.Timedelta(days=preset_days))
else:
    date_start = date_min

# Ручной выбор дат под кнопками
col_d1, col_d2 = st.columns(2)
with col_d1:
    d_from = st.date_input("От", value=date_start.date(),
                           min_value=date_min.date(), max_value=date_max.date())
with col_d2:
    d_to   = st.date_input("До", value=date_max.date(),
                           min_value=date_min.date(), max_value=date_max.date())

d_from = pd.Timestamp(d_from)
d_to   = pd.Timestamp(d_to)

# ── Фильтруем данные по периоду ───────────────────────────────────────────
ru_filtered = ruonia[(ruonia["date"] >= d_from) & (ruonia["date"] <= d_to)].copy()

# Пересчитываем флаги на выбранном периоде
# MAD_score на окне = min(260, len(ru_filtered)//2)
window = min(260, max(30, len(ru_filtered) // 4))
ru_filtered["mad_score"] = mad_normalize(ru_filtered["rate_spread"], window=window)
ru_filtered["Flag_Demand"] = (
    (ru_filtered["mad_score"] > 2.0) &
    (ru_filtered["mad_score"].notna().cumsum() >= window // 2)
).astype(int)

# ── График ────────────────────────────────────────────────────────────────
fig2 = go.Figure()

fig2.add_trace(go.Scatter(
    x=ru_filtered["date"], y=ru_filtered["rate_spread"],
    name="RUONIA − ключевая, п.п.",
    line=dict(color="gold", width=1.8),
    fill="tozeroy", fillcolor="rgba(255,215,0,0.1)",
))

flags = ru_filtered[ru_filtered["Flag_Demand"] == 1]
if len(flags):
    fig2.add_trace(go.Scatter(
        x=flags["date"], y=flags["rate_spread"],
        mode="markers", name=f"🔴 Flag_Demand (MAD > 2σ) — {len(flags)} дней",
        marker=dict(color="red", size=8, symbol="triangle-up"),
    ))

fig2.add_hline(y=0, line_color="gray", line_dash="dot", line_width=0.8)

n_days = (d_to - d_from).days
fig2.update_layout(
    title=f"М2: RUONIA − ключевая ставка  ({d_from.strftime('%d.%m.%Y')} — {d_to.strftime('%d.%m.%Y')}, {n_days} дней)",
    yaxis=dict(title="Переплата, п.п."),
    height=400,
    legend=dict(x=0, y=1.12, orientation="h"),
)
st.plotly_chart(fig2, use_container_width=True)

# Статистика по периоду
col_s1, col_s2, col_s3 = st.columns(3)
with col_s1:
    mean_v = ru_filtered["rate_spread"].mean()
    st.metric("Среднее отклонение", f"{mean_v:+.3f} п.п.",
              delta="дефицит" if mean_v > 0 else "профицит")
with col_s2:
    pct_above = (ru_filtered["rate_spread"] > 0).mean() * 100
    st.metric("Дней с дефицитом", f"{pct_above:.0f}%")
with col_s3:
    st.metric("Flag_Demand срабатываний", f"{len(flags)} дней")

st.info(
    f"**Флаги пересчитаны внутри выбранного периода.** "
    f"MAD-окно: **{window} дней** (≈ {window//30} мес.) — "
    f"каждый день сравнивается с предыдущими {window} днями этого же периода. "
    f"При смене периода флаги меняются — это норма для динамического режима."
)

# ── График 2: экстренное заимствование ────────────────────────────────────
if not df2.empty and "total_emergency_bln" in df2.columns:
    df2_f = df2[(df2["date"] >= d_from) & (df2["date"] <= d_to)]
    if not df2_f.empty:
        fig2b = go.Figure()
        fig2b.add_trace(go.Scatter(
            x=df2_f["date"], y=df2_f["total_emergency_bln"],
            name="Экстренное заимствование (col7+col8), млрд",
            fill="tozeroy", fillcolor="rgba(231,76,60,0.12)",
            line=dict(color="crimson", width=1.5),
        ))
        if "MAD_score_emergency" in df2_f.columns:
            fig2b.add_trace(go.Scatter(
                x=df2_f["date"], y=df2_f["MAD_score_emergency"],
                name="MAD_score (σ)", yaxis="y2",
                line=dict(color="darkred", width=1.5, dash="dot"),
            ))
        fig2b.add_hline(y=0, line_color="gray", line_dash="dot", line_width=0.8)
        fig2b.update_layout(
            title="М2: Экстренное заимствование у ЦБ (репо пост. + обесп. кредиты)",
            yaxis=dict(title="млрд руб."),
            yaxis2=dict(title="MAD (σ)", overlaying="y", side="right", showgrid=False),
            height=280, legend=dict(x=0, y=1.18, orientation="h"),
        )
        st.plotly_chart(fig2b, use_container_width=True)
        st.caption("Появление > 0 = банк взял деньги по штрафной ставке выше ключевой.")
>>>>>>> feature/merge
