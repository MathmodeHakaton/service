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
SIGNAL_DATA = ROOT / "ml_model" / "data"   # сырые сигналы парсеров


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
        title="Методология LSI — агрегационный слой (CatBoost + SHAP)",
        text=(
            "Методология агрегации LSI и важность фич. Архитектура: "
            "1) MAD-нормированные z-scores и флаги по 5 модулям (rolling 1 год, "
            "winsorized 1% хвосты, past-only — честный backtest без утечки будущего); "
            "2) CatBoost regressor учится на ground truth из CBR bliquidity "
            "(weak target — composite percentile-rank по 4 каналам: структурный дефицит, "
            "шок ключевой ставки, отрыв RUONIA, недоспрос ОФЗ); "
            "3) SHAP per-day разложение прогноза → contribution_M1..M5 как вклад каждого "
            "модуля + contribution_M4 как эффект мультипликатора. SHAP — это интерпретация "
            "модели CatBoost: показывает, какая фича сколько пунктов LSI добавила; "
            "4) Калман 1D + гистерезис на статус (зелёный <40, жёлтый 40-70, красный >70, "
            "переход через 3 дня подтверждения). "
            "Метрики качества: holdout MAE, time-series CV (5 фолдов), backtest на "
            "стресс-эпизодах. Веса модулей не задаются вручную — они LEARNED CatBoost'ом."
        ),
        tags={"LSI", "CatBoost", "SHAP", "методология", "агрегация",
              "важность", "веса", "признаки", "фичи", "MAD", "Kalman", "гистерезис"},
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
    """Годовой срез: mean/max/share_red, пики, активные модули, разбивка по месяцам."""
    out: List[Chunk] = []
    if ts.empty or "date" not in ts.columns:
        return out
    df = ts.dropna(subset=["lsi_smoothed"]).copy()
    df["year"] = df["date"].dt.year
    _ru_months = ["январь", "февраль", "март", "апрель", "май", "июнь",
                  "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
    for year, sub in df.groupby("year"):
        if len(sub) < 5:
            continue
        peak = sub.loc[sub["lsi_smoothed"].idxmax()]
        share_red = float((sub["lsi_smoothed"] >= 70).mean())
        share_yellow = float(((sub["lsi_smoothed"] >= 40) & (sub["lsi_smoothed"] < 70)).mean())
        peak_month = _ru_months[int(peak["date"].month) - 1]
        text = (
            f"{year} год — средний LSI {sub['lsi_smoothed'].mean():.1f}, "
            f"максимум {peak['lsi_smoothed']:.1f} в {peak_month} ({peak['date'].date()}). "
            f"Дни в красной зоне: {share_red:.0%}, в жёлтой: {share_yellow:.0%}."
        )
        # Активные месяцы (где max LSI в этом месяце ≥ 40)
        sub_m = sub.copy()
        sub_m["month"] = sub_m["date"].dt.month
        hot = sub_m.groupby("month")["lsi_smoothed"].max()
        hot_months = [_ru_months[m - 1] for m in hot.index if hot[m] >= 40]
        if hot_months:
            text += f" Активные месяцы (max LSI ≥ 40): {', '.join(hot_months)}."

        # Доминирующий модуль в пике
        contrib_cols = [c for c in sub.columns if c.startswith("contribution_M")]
        if contrib_cols and pd.notna(peak.get(contrib_cols[0])):
            peak_contribs = {c.replace("contribution_", ""): float(peak[c] or 0.0)
                             for c in contrib_cols}
            top = max(peak_contribs.items(), key=lambda x: x[1])
            text += f" В пике основной драйвер — {top[0]} (SHAP {top[1]:+.1f})."

        # Теги: год + название месяца пика + сам месяц короткий код
        _m_codes = ["jan", "feb", "mar", "apr", "may", "jun",
                    "jul", "aug", "sep", "oct", "nov", "dec"]
        peak_code = _m_codes[int(peak["date"].month) - 1]
        out.append(Chunk(
            id=f"yr_{year}",
            title=f"LSI за {year} год",
            text=text,
            tags={str(year), "год", "история", peak_month, peak_code},
            kind="segment",
        ))
    return out


def _crisis_episodes(backtest: pd.DataFrame) -> List[Chunk]:
    out: List[Chunk] = []
    _ru_months_full = ["январь", "февраль", "март", "апрель", "май", "июнь",
                       "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
    _m_codes = ["jan", "feb", "mar", "apr", "may", "jun",
                "jul", "aug", "sep", "oct", "nov", "dec"]
    for _, row in backtest.iterrows():
        ep = str(row.get("episode", ""))
        verdict = str(row.get("verdict", ""))
        start = pd.to_datetime(row.get("start"), errors="coerce")
        end = pd.to_datetime(row.get("end"), errors="coerce")
        months_human, month_codes, years = [], set(), set()
        if pd.notna(start) and pd.notna(end):
            cur = start
            while cur <= end:
                months_human.append(_ru_months_full[cur.month - 1])
                month_codes.add(_m_codes[cur.month - 1])
                years.add(str(cur.year))
                # шаг на 1 месяц
                cur = (cur + pd.offsets.MonthBegin(1))
        period_human = (" – ".join(dict.fromkeys(months_human))
                        + (f" {sorted(years)[0]}" if years else ""))
        text = (
            f"Кризис-эпизод {ep} ({period_human}, {row.get('start')} — {row.get('end')}): "
            f"средний LSI {row.get('mean_lsi')}, максимум {row.get('max_lsi')}, "
            f"доля красных дней {row.get('share_red')}. Вердикт модели: {verdict}."
        )
        tags = {"backtest", "кризис", ep, *years, *month_codes,
                *dict.fromkeys(months_human)}
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


# ── Сигнальные чанки: сырые ряды по годам ─────────────────────────────────
#
# Зачем: KB по умолчанию знает только агрегаты LSI и описания модулей.
# Когда пользователь спрашивает «какая RUONIA была в 2022» — в чанках до этого
# не было НИ ОДНОГО значения RUONIA, и LLM честно отвечала «нет в данных».
# Эти функции читают сырые CSV из ml_model/data/ и кладут в KB по одному чанку
# на (источник × год) с mean/min/max + датами экстремумов.

def _year_stats(values: pd.Series, dates: pd.Series) -> dict:
    """Базовая статистика за год: mean/min/max + ISO-даты экстремумов."""
    s = pd.to_numeric(values, errors="coerce")
    mask = s.notna()
    if not mask.any():
        return {}
    s_v = s[mask]
    d_v = pd.to_datetime(dates[mask], errors="coerce")
    return {
        "mean": float(s_v.mean()),
        "min":  float(s_v.min()),
        "max":  float(s_v.max()),
        "min_date": d_v.loc[s_v.idxmin()].date().isoformat(),
        "max_date": d_v.loc[s_v.idxmax()].date().isoformat(),
        "n":   int(mask.sum()),
    }


def _signal_year_chunks(
    *,
    df: pd.DataFrame,
    value_col: str,
    name: str,
    module: str,
    unit: str,
    short_id: str,
    extra_tags: Set[str],
    direction_hint: str = "выше — стрессовее",
) -> List[Chunk]:
    """Создаёт по чанку на каждый год + сводный чанк за всю историю."""
    out: List[Chunk] = []
    if df.empty or value_col not in df.columns or "date" not in df.columns:
        return out
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    df["year"] = df["date"].dt.year

    # Лайфтайм-сводка
    lt = _year_stats(df[value_col], df["date"])
    if lt:
        out.append(Chunk(
            id=f"sig_{short_id}_lifetime",
            title=f"{name} · вся история ({df['year'].min()}–{df['year'].max()})",
            text=(
                f"{name} за всю историю наблюдений: "
                f"среднее {lt['mean']:.2f} {unit}, "
                f"минимум {lt['min']:.2f} {unit} ({lt['min_date']}), "
                f"максимум {lt['max']:.2f} {unit} ({lt['max_date']}). "
                f"Наблюдений: {lt['n']}. Направление стресса: {direction_hint}."
            ),
            tags={module, "история", short_id, *extra_tags},
            kind="signal",
        ))

    # Чанки по годам
    for year, sub in df.groupby("year"):
        st = _year_stats(sub[value_col], sub["date"])
        if not st or st["n"] < 5:
            continue
        out.append(Chunk(
            id=f"sig_{short_id}_{int(year)}",
            title=f"{name} · {int(year)}",
            text=(
                f"{name} в {int(year)} году: "
                f"среднее {st['mean']:.2f} {unit}, "
                f"минимум {st['min']:.2f} {unit} ({st['min_date']}), "
                f"максимум {st['max']:.2f} {unit} ({st['max_date']}). "
                f"Наблюдений за год: {st['n']}."
            ),
            tags={str(int(year)), module, short_id, *extra_tags},
            kind="signal",
        ))
    return out


def _build_signal_chunks() -> List[Chunk]:
    """Сводит все сырые сигналы в чанки. Каждый источник читается отдельно,
    падать на отсутствии файла не должно — просто пропускаем."""
    out: List[Chunk] = []

    def _safe_read(name: str) -> pd.DataFrame:
        p = SIGNAL_DATA / name
        try:
            return pd.read_csv(p) if p.exists() else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    # M1 · RUONIA
    out.extend(_signal_year_chunks(
        df=_safe_read("m1_ruonia.csv"),
        value_col="ruonia", name="RUONIA (ставка межбанка)",
        module="M1", unit="% год.", short_id="ruonia",
        extra_tags={"RUONIA", "ruonia", "ставка", "межбанк"},
        direction_hint="выше ключевой — стресс",
    ))

    # M1 · спред резервов (factual − required)
    res = _safe_read("m1_reserves.csv")
    if not res.empty and {"actual_avg_bln", "required_avg_bln"} <= set(res.columns):
        res["spread_bln"] = pd.to_numeric(res["actual_avg_bln"], errors="coerce") \
                          - pd.to_numeric(res["required_avg_bln"], errors="coerce")
        out.extend(_signal_year_chunks(
            df=res, value_col="spread_bln",
            name="Спред резервов (факт − норматив)",
            module="M1", unit="млрд руб.", short_id="reserve_spread",
            extra_tags={"резерв", "корсчёт", "спред", "усреднение"},
            direction_hint="большой плюс в конце периода — стресс",
        ))

    # M2 · ключевая ставка
    out.extend(_signal_year_chunks(
        df=_safe_read("m2_keyrate.csv"),
        value_col="keyrate", name="Ключевая ставка ЦБ",
        module="M2", unit="% год.", short_id="keyrate",
        extra_tags={"ключевая", "keyrate", "ставка", "ЦБ"},
        direction_hint="резкий рост — шок ликвидности",
    ))

    # M2 · ставка репо аукционов (rate_wavg, агрегируем по дню)
    repo = _safe_read("m2_repo_auctions.csv")
    if not repo.empty and "rate_wavg" in repo.columns:
        repo["date"] = pd.to_datetime(repo["date"], errors="coerce")
        # фокус — 7-дневные аукционы (основной инструмент по ТЗ)
        if "term_days" in repo.columns:
            repo = repo[pd.to_numeric(repo["term_days"], errors="coerce") == 7]
        out.extend(_signal_year_chunks(
            df=repo, value_col="rate_wavg",
            name="Средневзв. ставка репо ЦБ (7д)",
            module="M2", unit="% год.", short_id="repo_rate",
            extra_tags={"репо", "ставка", "аукцион"},
            direction_hint="выше верхней границы коридора — стресс",
        ))

    # M3 · ОФЗ cover ratio и доходность (кириллические колонки + BOM в первой)
    ofz = _safe_read("m3_ofz_full.csv")
    if not ofz.empty:
        def _norm(c: str) -> str:
            return c.replace("﻿", "").strip().lower()

        col_date = next((c for c in ofz.columns if _norm(c) in ("дата", "date")), None)
        col_cover = next((c for c in ofz.columns
                          if "cover ratio" in _norm(c)), None)
        col_yield = next((c for c in ofz.columns
                          if "доходность средневз" in _norm(c)
                          or "avg_yield" in _norm(c)), None)
        if col_date and col_cover:
            ofz = ofz.rename(columns={col_date: "date"})
            out.extend(_signal_year_chunks(
                df=ofz, value_col=col_cover,
                name="Cover ratio ОФЗ-аукционов",
                module="M3", unit="", short_id="ofz_cover",
                extra_tags={"ОФЗ", "ofz", "cover", "аукцион", "Минфин"},
                direction_hint="ниже 1.2 — недоспрос (стресс)",
            ))
        if col_date and col_yield:
            ofz = ofz.rename(columns={col_date: "date"})
            out.extend(_signal_year_chunks(
                df=ofz, value_col=col_yield,
                name="Средневзв. доходность ОФЗ",
                module="M3", unit="% год.", short_id="ofz_yield",
                extra_tags={"ОФЗ", "ofz", "доходность", "yield"},
                direction_hint="резкий рост — давление на размещение",
            ))

    # M5 · структурный баланс ликвидности
    out.extend(_signal_year_chunks(
        df=_safe_read("m5_bliquidity.csv"),
        value_col="structural_balance_bln",
        name="Структурный баланс ликвидности банков",
        module="M5", unit="млрд руб.", short_id="bliq",
        extra_tags={"баланс", "структурный", "ликвидность", "корсчёт"},
        direction_hint="отрицательный — дефицит (стресс)",
    ))

    # M5 · средства Федерального казначейства
    out.extend(_signal_year_chunks(
        df=_safe_read("m5_sors_federal_funds.csv"),
        value_col="federal_funds_on_banks_bln",
        name="Средства казначейства на счетах банков",
        module="M5", unit="млрд руб.", short_id="treasury",
        extra_tags={"казначейство", "ЕКС", "бюджет"},
        direction_hint="резкое падение — отток ликвидности",
    ))

    return out


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

    # Сырые сигналы по годам: RUONIA, ключевая, репо-ставка, ОФЗ cover/доходность,
    # структурный баланс, казначейство. Один источник = lifetime-чанк + по чанку
    # на каждый год с mean/min/max + датами экстремумов.
    chunks.extend(_build_signal_chunks())

    return chunks
