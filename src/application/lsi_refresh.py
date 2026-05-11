"""
Live-обновление LSI: парсеры сервиса → ml_model → артефакты дашборда.

Цепочка:
    1. Pipeline.execute_full()       — фетчеры (ЦБ/Минфин/ФНС/Росказна) с кэшем БД
    2. _upsert_ml_inputs(raw_data)   — апдейт CSV в ml_model/data/ (по дате, без перезаписи истории)
    3. _run_ml_pipeline()            — subprocess `python run_pipeline.py` в ml_model/
    4. _copy_artifacts()             — outputs/* → data/model_artifacts/*

Обновляются: M1 резервы, RUONIA, ключевая ставка, репо (auctions+params), bliquidity.
НЕ обновляются (используется снимок из ml_model/data/):
    • m3_ofz_full.csv — у парсера сервиса другая схема колонок (кириллица), не сводится 1:1
    • m4_tax_calendar.csv — меняется раз в год
    • m5_sors_federal_funds.csv — у сервиса нет парсера SORS (Росказна SSL недоступна)

Идея: history-preserving upsert по `date` — старые ряды остаются, новые накатываются сверху.
Если ml_model падает (нет CatBoost / нехватка истории), refresh пишет ошибку и оставляет
последний рабочий снапшот артефактов.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
ML_DIR = ROOT / "ml_model"
ML_DATA = ML_DIR / "data"
ML_OUT = ML_DIR / "outputs"
ARTIFACTS = ROOT / "data" / "model_artifacts"

# Соответствие service raw_data -> ml_model CSV.
# Каждая запись: (csv_name, service_key, рендерер DataFrame).
# Рендерер возвращает DataFrame в схеме ml_model или None — тогда апдейт пропускается.


def _render_reserves(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Service columns: date, actual_avg, required_avg, required_account
       ml_model:        date, actual_avg_bln, required_avg_bln, required_account_bln"""
    if df is None or df.empty or "date" not in df.columns:
        return None
    out = pd.DataFrame({
        "date": pd.to_datetime(df["date"], errors="coerce"),
        "actual_avg_bln": pd.to_numeric(df.get("actual_avg"), errors="coerce"),
        "required_avg_bln": pd.to_numeric(df.get("required_avg"), errors="coerce"),
        "required_account_bln": pd.to_numeric(df.get("required_account"), errors="coerce"),
    }).dropna(subset=["date"])
    return out


def _render_ruonia(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    return df[["date", "ruonia"]].dropna(subset=["date"]).copy()


def _render_keyrate(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    return df[["date", "keyrate"]].dropna(subset=["date"]).copy()


def _render_repo_auctions(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """ml_model: type, term_days, date, time, volume_mln, rate_wavg, settlement, volume_bln"""
    if df is None or df.empty:
        return None
    cols = ["type", "term_days", "date", "time",
            "volume_mln", "rate_wavg", "settlement", "volume_bln"]
    return df[[c for c in cols if c in df.columns]].dropna(subset=["date"]).copy()


def _render_repo_params(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """ml_model файл широкий, но features.py читает только date/term_days/limit_bln/min_rate.
    Заполняем минимально необходимые колонки + пустыми instrument_type/term_raw/settle*,
    чтобы не сломать снапшот."""
    if df is None or df.empty:
        return None
    out = pd.DataFrame({
        "date": pd.to_datetime(df["date"], errors="coerce"),
        "instrument_type": "",
        "term_raw": df.get("term_days", "").astype(str) + " д.",
        "settle1": "",
        "settle2": "",
        "limit_bln": pd.to_numeric(df.get("limit_bln"), errors="coerce"),
        "min_rate": pd.to_numeric(df.get("min_rate"), errors="coerce"),
        "term_days": pd.to_numeric(df.get("term_days"), errors="coerce"),
    }).dropna(subset=["date"])
    return out


def _render_bliquidity(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """ml_model: date, structural_balance_bln"""
    if df is None or df.empty or "structural_balance_bln" not in df.columns:
        return None
    return df[["date", "structural_balance_bln"]].dropna(subset=["date"]).copy()


RENDERERS: Dict[str, tuple] = {
    "m1_reserves.csv":      ("reserves",   _render_reserves),
    "m1_ruonia.csv":        ("ruonia",     _render_ruonia),
    "m2_keyrate.csv":       ("keyrate",    _render_keyrate),
    "m2_repo_auctions.csv": ("repo",       _render_repo_auctions),
    "m2_repo_params.csv":   ("repo_params", _render_repo_params),
    "m5_bliquidity.csv":    ("bliquidity", _render_bliquidity),
}


def _upsert_csv(target: Path, fresh: pd.DataFrame) -> Dict[str, int]:
    """История + свежие строки → дедуп по date (свежие выигрывают).
    Возвращает {added, replaced, kept_history}."""
    fresh = fresh.copy()
    fresh["date"] = pd.to_datetime(fresh["date"], errors="coerce")
    fresh = fresh.dropna(subset=["date"])

    if target.exists():
        old = pd.read_csv(target)
        if "date" in old.columns:
            old["date"] = pd.to_datetime(old["date"], errors="coerce")
            old = old.dropna(subset=["date"])
        # Свежие имеют приоритет: drop_duplicates с keep='last' после concat (старые + новые)
        combined = pd.concat([old, fresh], ignore_index=True, sort=False)
        before = len(combined)
        combined = combined.drop_duplicates(subset=["date"], keep="last")
        combined = combined.sort_values("date").reset_index(drop=True)
        added = len(combined) - len(old)
        replaced = before - len(combined) - max(0, added)
        kept = len(old) - max(0, replaced)
    else:
        combined = fresh.sort_values("date").reset_index(drop=True)
        added, replaced, kept = len(combined), 0, 0

    target.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(target, index=False)
    return {"added": int(max(0, added)), "replaced": int(max(0, replaced)),
            "kept_history": int(max(0, kept)), "total": int(len(combined))}


def upsert_ml_inputs(raw_data: dict) -> Dict[str, Dict[str, int]]:
    """Накатывает данные парсеров на снапшоты ml_model/data/. Не трогает OFZ/налоги/SORS."""
    report: Dict[str, Dict[str, int]] = {}
    for fname, (key, render) in RENDERERS.items():
        df = raw_data.get(key)
        rendered = render(df) if df is not None else None
        if rendered is None or rendered.empty:
            report[fname] = {"status": "skipped (no data)"}
            continue
        try:
            stats = _upsert_csv(ML_DATA / fname, rendered)
            report[fname] = stats
        except Exception as e:
            logger.exception("Upsert %s failed: %s", fname, e)
            report[fname] = {"status": f"error: {e}"}
    return report


def _run_ml(mode: str, timeout: int = 600) -> tuple[bool, str]:
    """Запускает inference.py (быстро, без fit) или run_pipeline.py (полное переобучение).
    mode: "inference" | "retrain".

    Inference читает .cbm из ARTIFACTS (последний retrain), результаты пишет
    в ml_model/outputs/, дальше _copy_artifacts() переносит их в ARTIFACTS,
    .cbm и feature_importance оставлены нетронутыми."""
    if mode not in ("inference", "retrain"):
        return False, f"unknown mode={mode}"

    script = "inference.py" if mode == "inference" else "run_pipeline.py"
    cmd = [sys.executable, script, "--data-dir", "data", "--out-dir", "outputs"]
    if mode == "inference":
        # модель и метаданные — там, где их оставил последний retrain
        cmd += ["--model-dir", str(ARTIFACTS)]

    try:
        proc = subprocess.run(
            cmd, cwd=str(ML_DIR), capture_output=True, text=True,
            timeout=timeout, check=False,
        )
        ok = proc.returncode == 0
        log = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        return ok, log
    except subprocess.TimeoutExpired as e:
        return False, f"timeout: {e}"
    except Exception as e:
        return False, f"failed to spawn: {e}"


def _model_artifacts_exist() -> bool:
    return (ARTIFACTS / "lsi_ml_model.cbm").exists() and \
           (ARTIFACTS / "lsi_ml_metadata.joblib").exists()


def _copy_artifacts() -> int:
    """Копирует outputs/* в data/model_artifacts/ (плоский список + charts/)."""
    if not ML_OUT.exists():
        return 0
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    n = 0
    for p in ML_OUT.iterdir():
        dst = ARTIFACTS / p.name
        if p.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(p, dst)
        else:
            shutil.copy2(p, dst)
        n += 1
    return n


@dataclass
class RefreshReport:
    ok: bool
    mode: str                 # фактический режим: "inference" или "retrain"
    started_at: str
    finished_at: str
    upsert: Dict[str, Dict[str, int]]
    ml_log_tail: str
    artifacts_copied: int
    error: Optional[str] = None


def refresh_lsi(mode: str = "inference",
                force_refresh: bool = False) -> RefreshReport:
    """Полный live-цикл.

    mode="inference"  — быстрый ежедневный пересчёт. Грузит .cbm из ARTIFACTS,
                        делает predict + SHAP. Если модели нет — авто-фолбэк
                        в retrain.
    mode="retrain"    — полное переобучение CatBoost (раз в неделю или вручную).

    force_refresh=True заставляет фетчеры идти в источник мимо БД-кэша.
    """
    started = datetime.now()
    log_tail = ""
    upsert_rep: Dict[str, Dict[str, int]] = {}
    copied = 0
    err: Optional[str] = None
    ok = False
    actual_mode = mode

    try:
        if mode == "inference" and not _model_artifacts_exist():
            logger.warning("Inference запрошен, но .cbm/.joblib не найдены — "
                           "auto-fallback на retrain.")
            actual_mode = "retrain"

        from src.application.pipeline import Pipeline
        from src.infrastructure.storage.db.engine import get_session

        session = get_session()
        try:
            p = Pipeline(session=session, force_refresh=force_refresh)
            result = p.execute_full()
            raw = result.raw_data
        finally:
            session.close()

        upsert_rep = upsert_ml_inputs(raw)

        # Retrain — generous timeout (CatBoost + CV); inference — короткий.
        timeout = 900 if actual_mode == "retrain" else 180
        ml_ok, log = _run_ml(actual_mode, timeout=timeout)
        log_tail = "\n".join(log.splitlines()[-25:])
        if not ml_ok:
            raise RuntimeError(f"ml_model {actual_mode} failed.\n{log_tail}")

        copied = _copy_artifacts()
        ok = True
    except Exception as e:
        err = str(e)
        logger.exception("refresh_lsi failed: %s", e)

    return RefreshReport(
        ok=ok,
        mode=actual_mode,
        started_at=started.isoformat(timespec="seconds"),
        finished_at=datetime.now().isoformat(timespec="seconds"),
        upsert=upsert_rep,
        ml_log_tail=log_tail,
        artifacts_copied=copied,
        error=err,
    )
