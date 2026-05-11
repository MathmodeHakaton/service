"""LLM commentator for LSI — auto-generated analytical commentary via Yandex AI Studio.

Implements the бонус LLM module from ТЗ: «Автоматический текстовый комментарий при каждом
пересчёте индекса». Takes the structured LSI output, builds a numeric context, and asks
YandexGPT to produce a 4-6 sentence analytical commentary in Russian.

Usage:
    from src.llm_commentator import generate_commentary
    result = generate_commentary(scored_df)   # reads YANDEX_API_KEY and YANDEX_FOLDER_ID env vars
    if result:
        text, context, prompt = result
        print(text)

Environment variables required:
    YANDEX_API_KEY   — Service-account API key from Yandex Cloud (https://yandex.cloud/ai-studio)
    YANDEX_FOLDER_ID — Yandex Cloud folder ID where the service account lives

Without these the function returns None (caller decides whether to fail or skip).
"""
from __future__ import annotations

import json
import os
from typing import Optional, Tuple

import pandas as pd
import requests


YANDEX_API_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

# Model choice:
#   yandexgpt-lite — faster and cheaper, good enough for templated analytical commentary
#   yandexgpt      — stronger reasoning, use for free-form analyst chat
DEFAULT_MODEL = "yandexgpt-lite"

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


def build_context(scored: pd.DataFrame, today_idx: Optional[int] = None,
                  lookback_days: int = 30) -> dict:
    """Extract numerical context for the LLM prompt from the scored timeseries.

    Returns a dictionary of facts: today's LSI, status, contributions, channel signals,
    week-ago and month-ago LSI for trend, calendar flags.
    """
    df = scored.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Default to the most recent valid row (full_model_valid == 1 if available).
    if today_idx is None:
        if "full_model_valid" in df.columns:
            valid_idx = df.index[df["full_model_valid"].fillna(0).astype(int).eq(1)]
            today_idx = int(valid_idx[-1]) if len(valid_idx) > 0 else len(df) - 1
        else:
            today_idx = len(df) - 1

    today = df.iloc[today_idx]
    week_ago_idx = max(0, today_idx - 7)
    month_ago_idx = max(0, today_idx - 30)
    week_ago = df.iloc[week_ago_idx]
    month_ago = df.iloc[month_ago_idx]

    def _f(row, col, default=0.0):
        v = row.get(col, default)
        try:
            return float(v) if pd.notna(v) else default
        except (TypeError, ValueError):
            return default

    contributions = {m: round(_f(today, f"contribution_{m}"), 1) for m in ("M1", "M2", "M3", "M4", "M5")
                     if f"contribution_{m}" in today.index}

    # Active channels: those above the "meaningful stress" threshold of 50.
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
    """Format the structured context as a numeric prompt for the LLM."""
    contribs = context["contributions"]
    contribs_str = ", ".join(f"{m}={v:+.1f}" for m, v in contribs.items()) if contribs else "n/a"

    chans = context["active_channels"]
    if chans:
        chans_str = "; ".join(f"{c['label']} ({c['key']}={c['value']})" for c in chans)
    else:
        chans_str = "ни один канал не превысил порог 50"

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


def call_yandex_gpt(system_prompt: str, user_prompt: str, api_key: str, folder_id: str,
                    model: str = DEFAULT_MODEL, temperature: float = 0.3,
                    max_tokens: int = 700, timeout: int = 60) -> str:
    """Single completion call to Yandex AI Studio. Raises on HTTP/JSON errors."""
    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json",
        "x-folder-id": folder_id,
    }
    payload = {
        "modelUri": f"gpt://{folder_id}/{model}/latest",
        "completionOptions": {
            "stream": False,
            "temperature": temperature,
            "maxTokens": str(max_tokens),
        },
        "messages": [
            {"role": "system", "text": system_prompt},
            {"role": "user", "text": user_prompt},
        ],
    }
    resp = requests.post(YANDEX_API_URL, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    return data["result"]["alternatives"][0]["message"]["text"].strip()


def generate_commentary(scored: pd.DataFrame, api_key: Optional[str] = None,
                        folder_id: Optional[str] = None, model: str = DEFAULT_MODEL,
                        today_idx: Optional[int] = None,
                        temperature: float = 0.3) -> Optional[Tuple[str, dict, str]]:
    """End-to-end: build context from scored df, call YandexGPT, return (text, context, prompt).

    Returns None (without raising) when credentials are not set, so the pipeline keeps running.
    """
    api_key = api_key or os.environ.get("YANDEX_API_KEY")
    folder_id = folder_id or os.environ.get("YANDEX_FOLDER_ID")
    if not api_key or not folder_id:
        return None

    context = build_context(scored, today_idx=today_idx)
    user_prompt = build_user_prompt(context)
    text = call_yandex_gpt(SYSTEM_PROMPT, user_prompt, api_key, folder_id,
                           model=model, temperature=temperature)
    return text, context, user_prompt


def write_commentary_markdown(text: str, context: dict, user_prompt: str, out_path) -> None:
    """Save commentary with full context for audit/debugging."""
    from pathlib import Path
    out_path = Path(out_path)
    md = []
    md.append(f"# Аналитический комментарий LSI")
    md.append("")
    md.append(f"**Дата расчёта:** {context['date']}")
    md.append(f"**LSI:** {context['lsi']} ({context['status']})")
    md.append(f"**Δ неделя:** {context['delta_week']:+.1f}  |  **Δ месяц:** {context['delta_month']:+.1f}")
    md.append("")
    md.append("## Комментарий аналитика")
    md.append("")
    md.append(text)
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Контекст для модели (audit trail)")
    md.append("")
    md.append("```json")
    md.append(json.dumps(context, ensure_ascii=False, indent=2))
    md.append("```")
    md.append("")
    md.append("## Промпт пользователя")
    md.append("")
    md.append("```")
    md.append(user_prompt)
    md.append("```")
    out_path.write_text("\n".join(md), encoding="utf-8")
