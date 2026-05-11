"""
Knowledge base для RAG-чата. Чанки + теги (год, модуль, тип).

Источники:
    • Хардкод-описания модулей М1..М5 + методология (по ТЗ) — стабильны, не меняются.
    • Динамика из data/model_artifacts/*.csv: текущее состояние, годовые срезы,
      топ стрессовых дней, backtest. Перестраиваются при каждом refresh.
    • Налоговый календарь (если доступен через Pipeline).

Чанки — короткие самодостаточные тексты с числами. Никаких длинных простыней —
LLM получает 4–8 чанков в контексте.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Set, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
ART = ROOT / "data" / "model_artifacts"


@dataclass
class Chunk:
    id: str
    title: str
    text: str
    tags: Set[str] = field(default_factory=set)  # для буста: годы, модули, ключевые слова
    kind: str = "doc"                            # doc / fact / segment / event


# ── Статические описания (по ТЗ) ──────────────────────────────────────────

_STATIC_MODULE_DOCS = [
    Chunk(
        id="m1_doc",
        title="M1 · Усреднение обязательных резервов",
        text=(
            "M1 — мониторит, как банки усредняют обязательные резервы на корсчёте в ЦБ. "
            "Сигналы: спред (фактические остатки − обязательные) и его MAD-нормированная "
            "аномалия, RUONIA и её отклонение от ключевой ставки (m1_ruonia_spread_z), "
            "флаг конца периода усреднения. Большой положительный спред в конце периода "
            "+ RUONIA выше ключа = классический сигнал стресса межбанка. "
            "Источник: cbr.ru/hd_base/RReserves/ и cbr.ru/hd_base/ruonia/."
        ),
        tags={"M1", "RUONIA", "резервы", "корсчёт", "ключевая"},
    ),
    Chunk(
        id="m2_doc",
        title="M2 · Аукционы репо ЦБ",
        text=(
            "M2 — оперативное рефинансирование банков через репо ЦБ. Сигналы: cover ratio "
            "(спрос/размещение, флаг при >2.0), спред ставки отсечения к ключу, "
            "утилизация лимита (m2_repo_utilization_z) и переплата (m2_repo_rate_spread_z). "
            "Фокус — на 7-дневных аукционах. Высокий cover + ставка у верхней границы "
            "коридора = острый дефицит ликвидности. Источник: cbr.ru/hd_base/repo/."
        ),
        tags={"M2", "репо", "cover", "аукцион", "ключевая"},
    ),
    Chunk(
        id="m3_doc",
        title="M3 · Размещение ОФЗ",
        text=(
            "M3 — аукционы ОФЗ Минфина. Главный сигнал стресса — недоспрос "
            "(cover_ratio < 1.2, флаг m3_nedospros_flag): банки не могут или не хотят "
            "выкупать предложение. Также: средневзвешенная доходность и её отклонение, "
            "флаг переспроса (>2.0). По SHAP — m3_nedospros_flag даёт наибольший global "
            "importance среди всех фич модели. Источник: minfin.gov.ru."
        ),
        tags={"M3", "ОФЗ", "Минфин", "недоспрос", "cover", "доходность"},
    ),
    Chunk(
        id="m4_doc",
        title="M4 · Налоговая сезонность",
        text=(
            "M4 — сезонный контекстуализатор. НЕ входит в фичи CatBoost. Применяется как "
            "мультипликатор SF ∈ [1.0; 1.4] к итоговому LSI: обычные дни 1.0, налоговая "
            "неделя 1.1, конец месяца 1.2, конец квартала 1.4. Это позволяет отделить "
            "структурный стресс от сезонного. Флаги: m4_tax_week_flag, m4_end_of_month_flag, "
            "m4_end_of_quarter_flag, m4_seasonal_factor. Источник: налоговый календарь ФНС."
        ),
        tags={"M4", "налоги", "сезонность", "квартал", "месяц", "мультипликатор"},
    ),
    Chunk(
        id="m5_doc",
        title="M5 · Структурная ликвидность и казначейство",
        text=(
            "M5 — структурный баланс ликвидности (CBR bliquidity) и движение средств "
            "Федерального казначейства на счетах коммерческих банков. Дефицит "
            "(structural_balance < 0) и резкий 7-дневный отток (m5_structural_drain_z) — "
            "ведущие опережающие сигналы. Флаг Flag_Budget_Drain при оттоке >500 млрд "
            "за неделю. Эта же таблица — ground truth для weak-target обучения CatBoost. "
            "Источник: cbr.ru/hd_base/bliquidity/."
        ),
        tags={"M5", "казначейство", "бюджет", "структурный", "баланс", "корсчёт"},
    ),
    Chunk(
        id="lsi_method",
        title="Методология LSI — агрегационный слой",
        text=(
            "Архитектура LSI: 1) MAD-нормированные z-scores и флаги по 5 модулям (rolling "
            "1 год, winsorized 1% хвосты, past-only — честный backtest); 2) CatBoost "
            "regressor учится на ground truth из CBR bliquidity (weak target — composite "
            "percentile-rank по 4 каналам: дефицит, шок ставки, отрыв RUONIA, недоспрос "
            "ОФЗ); 3) SHAP per-day → contribution_M1..M5 + contribution_M4 как эффект "
            "мультипликатора; 4) Калман 1D + гистерезис на статус (зелёный <40, "
            "жёлтый 40-70, красный >70, переход через 3 дня подтверждения). "
            "Метрики: holdout MAE, time-series CV (5 фолдов), sensitivity ±20%."
        ),
        tags={"LSI", "CatBoost", "SHAP", "методология", "MAD", "Kalman", "веса"},
    ),
    Chunk(
        id="thresholds",
        title="Пороги статуса LSI",
        text=(
            "Зелёный (НОРМА): LSI < 40 — рынок ликвидности стабилен. "
            "Жёлтый (ВНИМАНИЕ): 40 ≤ LSI < 70 — нарастающее напряжение, нужно мониторить. "
            "Красный (СТРЕСС): LSI ≥ 70 — острый стресс, активируются репо ЦБ, риск "
            "недоспроса ОФЗ. Гистерезис: переход между уровнями подтверждается 3 днями "
            "подряд в новой полосе, чтобы избежать ложных алертов."
        ),
        tags={"статус", "порог", "красный", "жёлтый", "зелёный"},
    ),
]


# ── Динамические факты из артефактов ──────────────────────────────────────

def _year_segments(ts: pd.DataFrame) -> List[Chunk]:
    """Годовой срез: mean/max/share_red, пики, активные модули."""
    out: List[Chunk] = []
    if ts.empty or "date" not in ts.columns:
        return out
    df = ts.dropna(subset=["lsi_smoothed"]).copy()
    df["year"] = df["date"].dt.year
    for year, sub in df.groupby("year"):
        if len(sub) < 5:
            continue
        peak = sub.loc[sub["lsi_smoothed"].idxmax()]
        share_red = float((sub["lsi_smoothed"] >= 70).mean())
        share_yellow = float(((sub["lsi_smoothed"] >= 40) & (sub["lsi_smoothed"] < 70)).mean())
        text = (
            f"{year} год — средний LSI {sub['lsi_smoothed'].mean():.1f}, "
            f"максимум {peak['lsi_smoothed']:.1f} на дату {peak['date'].date()}. "
            f"Дни в красной зоне: {share_red:.0%}, в жёлтой: {share_yellow:.0%}. "
        )
        # Привязка к доминирующему модулю в пике
        contrib_cols = [c for c in sub.columns if c.startswith("contribution_M")]
        if contrib_cols and pd.notna(peak.get(contrib_cols[0])):
            peak_contribs = {c.replace("contribution_", ""): float(peak[c] or 0.0)
                             for c in contrib_cols}
            top = max(peak_contribs.items(), key=lambda x: x[1])
            text += f"В пике основной драйвер — {top[0]} (SHAP {top[1]:+.1f})."
        out.append(Chunk(
            id=f"yr_{year}",
            title=f"LSI за {year} год",
            text=text,
            tags={str(year), "год", "история"},
            kind="segment",
        ))
    return out


def _crisis_episodes(backtest: pd.DataFrame) -> List[Chunk]:
    out: List[Chunk] = []
    for _, row in backtest.iterrows():
        ep = str(row.get("episode", ""))
        verdict = str(row.get("verdict", ""))
        text = (
            f"Эпизод {ep} ({row.get('start')} — {row.get('end')}): "
            f"средний LSI {row.get('mean_lsi')}, максимум {row.get('max_lsi')}, "
            f"доля красных дней {row.get('share_red')}. Вердикт модели: {verdict}."
        )
        tags = {"backtest", "кризис", ep}
        # Подтянем годы из дат
        for key in ("start", "end"):
            d = str(row.get(key, ""))[:4]
            if d.isdigit():
                tags.add(d)
        out.append(Chunk(id=f"ep_{ep}", title=f"Кризис · {ep}",
                         text=text, tags=tags, kind="event"))
    return out


def _top_stress_days(ts: pd.DataFrame, n: int = 12) -> Chunk:
    if ts.empty:
        return Chunk(id="top_days", title="Топ стрессовых дней",
                     text="(история пуста)", tags={"топ"}, kind="fact")
    df = ts.dropna(subset=["lsi_smoothed"]).sort_values("lsi_smoothed",
                                                        ascending=False).head(n)
    rows = []
    for _, r in df.iterrows():
        rows.append(f"{r['date'].date()} LSI={r['lsi_smoothed']:.1f} "
                    f"({r.get('status', '?')})")
    return Chunk(
        id="top_days",
        title=f"Топ-{n} стрессовых дней (по lsi_smoothed)",
        text="; ".join(rows),
        tags={"топ", "пик", "максимум", "стресс"},
        kind="fact",
    )


def _latest_snapshot(extract: pd.DataFrame) -> Chunk:
    if extract.empty:
        return Chunk(id="snap", title="Снапшот", text="—", tags=set(), kind="fact")
    valid = extract[extract["full_model_valid"] == 1]
    row = valid.iloc[-1] if not valid.empty else extract.iloc[-1]
    contribs = ", ".join(
        f"{m}={float(row.get(f'contribution_{m}', 0.0) or 0.0):+.1f}"
        for m in ["M1", "M2", "M3", "M4", "M5"]
    )
    text = (
        f"Последний расчёт LSI на {pd.to_datetime(row['date']).date()}: "
        f"lsi={float(row.get('lsi', row['lsi_smoothed'])):.1f}, "
        f"lsi_raw={float(row['lsi_raw']):.1f}, "
        f"статус={row['status']}, m4_multiplier={float(row['m4_multiplier']):.2f}. "
        f"SHAP-вклады: {contribs}. "
        f"Активных модулей рынка: {int(row.get('active_market_modules_count', 0))} из 4."
    )
    year = str(pd.to_datetime(row["date"]).year)
    return Chunk(id="latest", title="Текущий снапшот LSI",
                 text=text, tags={"сейчас", "текущ", "последн", year},
                 kind="fact")


def _module_importance(mod_imp: pd.DataFrame) -> Chunk:
    if mod_imp.empty:
        return Chunk(id="imp", title="Важность модулей", text="—", tags=set())
    lines = []
    for _, r in mod_imp.iterrows():
        lines.append(f"{r['module']}: global_importance={r['global_importance']:.2f} "
                     f"({float(r['global_importance_share'])*100:.0f}%)")
    return Chunk(
        id="mod_imp",
        title="Глобальная важность модулей (mean |SHAP|)",
        text="; ".join(lines),
        tags={"важность", "SHAP", "importance"},
        kind="fact",
    )


def _feat_importance(feat_imp: pd.DataFrame, n: int = 8) -> Chunk:
    if feat_imp.empty:
        return Chunk(id="feat", title="Top-фичи", text="—", tags=set())
    top = feat_imp.head(n)
    lines = [f"{r['feature']} (модуль {r['module']}): {r['mean_abs_shap']:.2f}"
             for _, r in top.iterrows()]
    return Chunk(id="feat_imp", title=f"Top-{n} фич по mean|SHAP|",
                 text="; ".join(lines), tags={"фичи", "feature", "SHAP"},
                 kind="fact")


def _tax_calendar(tax_df: Optional[pd.DataFrame]) -> Optional[Chunk]:
    if tax_df is None or tax_df.empty:
        return None
    tax_df = tax_df.copy()
    tax_df["date"] = pd.to_datetime(tax_df["date"], errors="coerce")
    upcoming = tax_df[tax_df["date"] >= pd.Timestamp.today()].head(8)
    if upcoming.empty:
        return None
    lines = [f"{r['date'].date()} — {r.get('tax_type', '')}"
             for _, r in upcoming.iterrows()]
    return Chunk(id="tax_upcoming", title="Ближайшие налоговые даты",
                 text="; ".join(lines), tags={"налог", "календарь", "ближайш"},
                 kind="fact")


def build_knowledge_base(tax_df: Optional[pd.DataFrame] = None) -> List[Chunk]:
    """Собирает полный KB. Грузит CSV из data/model_artifacts/.
    Падать на отсутствии файлов не должна — если артефактов нет, возвращает только статику."""
    chunks: List[Chunk] = list(_STATIC_MODULE_DOCS)

    try:
        extract = pd.read_csv(ART / "lsi_dashboard_extract.csv", parse_dates=["date"])
        chunks.append(_latest_snapshot(extract))
    except FileNotFoundError:
        extract = pd.DataFrame()

    try:
        ts = pd.read_csv(ART / "lsi_timeseries.csv", parse_dates=["date"])
        chunks.extend(_year_segments(ts))
        chunks.append(_top_stress_days(ts))
    except FileNotFoundError:
        pass

    try:
        bt = pd.read_csv(ART / "backtest_crisis_episodes.csv")
        chunks.extend(_crisis_episodes(bt))
    except FileNotFoundError:
        pass

    try:
        mod_imp = pd.read_csv(ART / "module_importance_catboost.csv")
        chunks.append(_module_importance(mod_imp))
    except FileNotFoundError:
        pass

    try:
        feat_imp = pd.read_csv(ART / "feature_importance.csv")
        chunks.append(_feat_importance(feat_imp))
    except FileNotFoundError:
        pass

    tax_chunk = _tax_calendar(tax_df)
    if tax_chunk:
        chunks.append(tax_chunk)

    return chunks
