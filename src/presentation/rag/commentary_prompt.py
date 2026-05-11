"""
Сборка system+user промпта для авто-комментария LSI.

Чистые функции — никаких HTTP-вызовов. Вызывается со страницы LSI:
    ctx = build_context(scored_df)
    prompt = build_user_prompt(ctx)
    text = yandex_client.complete(SYSTEM_PROMPT, prompt, model=...)

Скопировано из ml_model/src/llm_commentator.py: эти функции стабильны,
не зависят от ml_model и не должны вынуждать дашборд тащить ml_model в sys.path
(там тоже пакет `src`, что конфликтует с нашим src/).
"""
from __future__ import annotations

from typing import Optional

import pandas as pd


SYSTEM_PROMPT = """Ты — старший аналитик казначейства банка ПСБ, эксперт по денежному рынку РФ.

Тебе передают числовые данные расчёта Liquidity Stress Index (LSI, шкала 0-100):
текущее значение, вклады 5 модулей, активные каналы стресса, динамика за неделю и месяц.

Твоя задача — написать аналитический комментарий 4-6 предложений на русском языке,
профессиональным деловым стилем, без эмодзи и без воды.

СТРУКТУРА КОММЕНТАРИЯ:
1. Текущее состояние: значение LSI, статус (зелёный/жёлтый/красный) и общая интерпретация.
2. Атрибуция: какие модули дают наибольший вклад, какие каналы стресса активны.
3. Динамика: куда LSI движется относительно недели и месяца назад.
4. Прогноз: чего ожидать в ближайшие 3-7 дней с учётом календарного контекста
   (налоговая неделя, конец квартала, конец месяца усиливают давление).

МОДУЛИ:
- M1 — усреднение обязательных резервов: спред корсчёта и RUONIA относительно ключа.
- M2 — аукционы РЕПО ЦБ: cover ratio, ставка, спред к ключевой ставке.
- M3 — размещение ОФЗ: bid cover, недоспрос, доходность.
- M4 — налоговая сезонность: мультипликатор 1.0-1.4 на остальные сигналы.
- M5 — структурная ликвидность и средства казначейства: дефицит/профицит на корсчетах.

КАНАЛЫ СТРЕССА (используются как ground truth обучения):
- ch_rate_shock — резкое изменение ключевой ставки ЦБ.
- ch_ruonia_spread — отрыв RUONIA от ключевой ставки.
- ch_ofz_nedospros — провал аукциона ОФЗ.
- ch_deficit — структурный дефицит ликвидности банковского сектора.

ТРЕБОВАНИЯ:
- Только факты и числа из переданных данных. Не выдумывай.
- Конкретные значения вкладов и каналов цитируй (например: «вклад M3 составил +12.3»).
- Не используй фразы «возможно», «вероятно» без числовой опоры.
- Не повторяй цифры дословно — интерпретируй их."""


def build_context(scored: pd.DataFrame, today_idx: Optional[int] = None) -> dict:
    df = scored.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    if today_idx is None:
        if "full_model_valid" in df.columns:
            valid_idx = df.index[df["full_model_valid"].fillna(0).astype(int).eq(1)]
            today_idx = int(valid_idx[-1]) if len(valid_idx) > 0 else len(df) - 1
        else:
            today_idx = len(df) - 1

    today = df.iloc[today_idx]
    week_ago = df.iloc[max(0, today_idx - 7)]
    month_ago = df.iloc[max(0, today_idx - 30)]

    def _f(row, col, default=0.0):
        v = row.get(col, default)
        try:
            return float(v) if pd.notna(v) else default
        except (TypeError, ValueError):
            return default

    contributions = {m: round(_f(today, f"contribution_{m}"), 1)
                     for m in ("M1", "M2", "M3", "M4", "M5")
                     if f"contribution_{m}" in today.index}

    channels = []
    for ch_key, ch_label in [
        ("ch_rate_shock", "шок ключевой ставки"),
        ("ch_ruonia_spread", "отрыв RUONIA"),
        ("ch_ofz_nedospros", "недоспрос ОФЗ"),
        ("ch_deficit", "структурный дефицит"),
    ]:
        v = _f(today, ch_key)
        if v >= 50.0:
            channels.append({"key": ch_key, "label": ch_label, "value": round(v, 1)})

    lsi_today = _f(today, "lsi_smoothed", _f(today, "lsi_raw"))
    lsi_week = _f(week_ago, "lsi_smoothed", _f(week_ago, "lsi_raw"))
    lsi_month = _f(month_ago, "lsi_smoothed", _f(month_ago, "lsi_raw"))
    recent_slice = df.iloc[max(0, today_idx - 14): today_idx + 1]
    max_recent = float(recent_slice["lsi_smoothed"].max()) if "lsi_smoothed" in recent_slice.columns else lsi_today
    min_recent = float(recent_slice["lsi_smoothed"].min()) if "lsi_smoothed" in recent_slice.columns else lsi_today

    return {
        "date": str(today["date"].date()),
        "lsi": round(lsi_today, 1),
        "status": str(today.get("status", "unknown")),
        "lsi_week_ago": round(lsi_week, 1),
        "lsi_month_ago": round(lsi_month, 1),
        "delta_week": round(lsi_today - lsi_week, 1),
        "delta_month": round(lsi_today - lsi_month, 1),
        "max_last_14d": round(max_recent, 1),
        "min_last_14d": round(min_recent, 1),
        "contributions": contributions,
        "m4_multiplier": round(_f(today, "m4_multiplier", 1.0), 2),
        "active_channels": channels,
        "tax_week": bool(int(_f(today, "m4_tax_week_flag", 0))),
        "end_of_month": bool(int(_f(today, "m4_end_of_month_flag", 0))),
        "end_of_quarter": bool(int(_f(today, "m4_end_of_quarter_flag", 0))),
        "active_market_modules_count": int(_f(today, "active_market_modules_count", 0)),
    }


def build_user_prompt(context: dict) -> str:
    contribs = context["contributions"]
    contribs_str = ", ".join(f"{m}={v:+.1f}" for m, v in contribs.items()) if contribs else "n/a"

    chans = context["active_channels"]
    chans_str = ("; ".join(f"{c['label']} ({c['key']}={c['value']})" for c in chans)
                 if chans else "ни один канал не превысил порог 50")

    cal_events = []
    if context["tax_week"]:
        cal_events.append("текущая неделя — налоговая")
    if context["end_of_quarter"]:
        cal_events.append("конец квартала")
    elif context["end_of_month"]:
        cal_events.append("конец месяца")
    cal_str = "; ".join(cal_events) if cal_events else "обычный период без сезонных факторов"

    return f"""Данные расчёта LSI на дату {context['date']}.

ТЕКУЩИЙ ИНДЕКС:
- LSI = {context['lsi']} (статус: {context['status']})
- Динамика к неделе назад: {context['lsi_week_ago']} -> {context['lsi']} (изменение {context['delta_week']:+.1f})
- Динамика к месяцу назад: {context['lsi_month_ago']} -> {context['lsi']} (изменение {context['delta_month']:+.1f})
- За последние 14 дней: max {context['max_last_14d']}, min {context['min_last_14d']}

ВКЛАДЫ МОДУЛЕЙ В LSI (SHAP, пункты):
{contribs_str}
M4 мультипликатор = ×{context['m4_multiplier']}

АКТИВНЫЕ КАНАЛЫ СТРЕССА:
{chans_str}

КАЛЕНДАРНЫЙ КОНТЕКСТ:
{cal_str}
Активных модулей рынка: {context['active_market_modules_count']} из 4 (M1, M2, M3, M5).

Напиши аналитический комментарий 4-6 предложений согласно структуре в system-промпте."""
