"""
Быстрая проверка всех модулей service/.
Запуск из папки service/: python3 test_all.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd, numpy as np

print("\n" + "=" * 60)
print("  SERVICE — проверка всех модулей")
print("=" * 60)

# ── Загружаем данные из кэша основного проекта ─────────────────
DATA = os.path.join(os.path.dirname(__file__), "../liquidity_sentinel/data")

def load_cbr_data():
    reserves = pd.read_excel(f"{DATA}/m1/required_reserves.xlsx", header=2)
    reserves.columns = (["date","actual_avg","required_avg","required_account"]
                        + list(reserves.columns[4:]))
    reserves["date"] = pd.to_datetime(reserves["date"], errors="coerce")
    for c in ["actual_avg","required_avg"]:
        reserves[c] = pd.to_numeric(reserves[c], errors="coerce")
    reserves = reserves.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    ruonia  = pd.read_csv(f"{DATA}/m1/ruonia.csv",    parse_dates=["date"])
    keyrate = pd.read_csv(f"{DATA}/m2/keyrate.csv",   parse_dates=["date"])
    keyrate.columns = ["date","keyrate"]
    repo    = pd.read_csv(f"{DATA}/m2/repo_results.csv", parse_dates=["date"])
    params  = pd.read_csv(f"{DATA}/m2/repo_params.csv",  parse_dates=["date"])
    bliq    = pd.read_csv(f"{DATA}/m5/bliquidity.csv",    parse_dates=["date"])

    import json
    with open(f"{DATA}/m3/m3_data.json") as f:
        ofz_raw = json.load(f)
    ofz = pd.DataFrame(ofz_raw["auctions"])
    ofz["date"] = pd.to_datetime(ofz["date"])
    for col in ["offer_volume","demand_volume","placement_volume","avg_yield","cover_ratio"]:
        if col in ofz.columns:
            ofz[col] = pd.to_numeric(ofz[col], errors="coerce")

    return {
        "reserves": reserves, "ruonia": ruonia, "keyrate": keyrate,
        "repo": repo, "repo_params": params, "bliquidity": bliq,
        "ofz": ofz,
    }

print("\nЗагрузка данных из кэша...")
data = load_cbr_data()

# Добавляем налоговый календарь
from src.infrastructure.fetchers.fns import FNSFetcher
fns = FNSFetcher(cache_dir="/tmp/service_test_fns")
fns_result = fns.fetch()
data["tax_calendar"] = fns_result.data["tax_calendar"]
from datetime import datetime
data["target_date"] = datetime.now()

loaded = {k: len(v) for k,v in data.items() if hasattr(v,"__len__") and k != "target_date"}
print(f"  ✓ Данные загружены: {loaded}")

# ── MAD нормализация ──────────────────────────────────────────
print("\n--- MAD нормализация ---")
from src.domain.normalization.mad import mad_normalize, MADNormalizer

# MAD тест: разнообразный ряд с аномалией
import random; random.seed(42)
base = [random.gauss(5, 1) for _ in range(100)]
spike = base + [30.0] + [random.gauss(5, 1) for _ in range(20)]
s = pd.Series(spike)
mad = mad_normalize(s, window=50)
spike_mad = mad.iloc[100]
print(f"  ✓ mad_normalize: spike MAD={spike_mad:.2f}  норма_std={mad[50:100].std():.2f}")
assert not np.isnan(spike_mad), "MAD вернул NaN"
assert spike_mad > 3, f"MAD spike слабый: {spike_mad:.2f} (ожидание >3)"

norm = MADNormalizer(window_years=1)
scores = norm.compute([float(x) for x in s], window=50)
assert len(scores) == len(s)
print(f"  ✓ MADNormalizer: {len(scores)} значений, clip работает ({min(scores):.1f}..{max(scores):.1f})")

# ── Модули ────────────────────────────────────────────────────
print("\n--- Модули ---")

from src.domain.modules.m1_reserves import M1Reserves
from src.domain.modules.m2_repo     import M2Repo
from src.domain.modules.m3_ofz      import M3OFZ
from src.domain.modules.m4_tax      import M4Tax
from src.domain.modules.m5_treasury import M5Treasury

errors = []

for ModClass, name, check_range in [
    (M1Reserves, "M1_RESERVES",  (0.3, 0.7)),
    (M2Repo,     "M2_REPO",      (0.2, 0.6)),
    (M3OFZ,      "M3_OFZ",       (0.1, 0.9)),
    (M4Tax,      "M4_TAX",       (0.0, 1.0)),
    (M5Treasury, "M5_TREASURY",  (0.0, 0.5)),
]:
    try:
        mod = ModClass()
        sig = mod.compute(data)
        assert sig.module_name == name, f"Неверное имя: {sig.module_name}"
        assert 0.0 <= sig.value <= 1.0, f"value вне [0,1]: {sig.value}"
        score_pct = sig.value * 100
        ok = "✓" if sig.latest_flag != "error" else "⚠"
        print(f"  {ok} {name:<18} score={score_pct:5.1f}/100  flag={sig.latest_flag}")
    except Exception as e:
        errors.append(f"{name}: {e}")
        print(f"  ✗ {name}: {e}")
        import traceback; traceback.print_exc()

# ── LSI агрегатор ─────────────────────────────────────────────
print("\n--- LSI агрегатор ---")
from src.domain.aggregation.lsi_engine import LSIEngine

engine = LSIEngine()
signals = [ModClass().compute(data)
           for ModClass in [M1Reserves, M2Repo, M3OFZ, M4Tax, M5Treasury]]
lsi = engine.compute(signals)
print(f"  ✓ LSI value={lsi.value*100:.1f}/100  status={lsi.status}")
print(f"    Вклад: { {k: f'{v*100:.1f}' for k,v in lsi.contributions.items()} }")

# ── Pipeline ──────────────────────────────────────────────────
print("\n--- Pipeline ---")
from src.application.pipeline import Pipeline

p = Pipeline(cache_dir="/tmp/service_test_cache")
try:
    full = p.execute_full()
    print(f"  ✓ Pipeline.execute_full(): LSI={full.lsi.value*100:.1f} [{full.lsi.status}]")
    print(f"    Сигналов: {len(full.signals)}  Данных: {len(full.raw_data)} источников")
except Exception as e:
    errors.append(f"Pipeline: {e}")
    print(f"  ✗ Pipeline: {e}")

# ── FNS fetcher ────────────────────────────────────────────────
print("\n--- FNS Fetcher ---")
fns_df = fns_result.data["tax_calendar"]
print(f"  ✓ FNSFetcher: {len(fns_df)} событий  "
      f"{fns_df['date'].dt.year.min()}-{fns_df['date'].dt.year.max()}")

# ── Итог ──────────────────────────────────────────────────────
print("\n" + "=" * 60)
if errors:
    print(f"✗ Найдено {len(errors)} ошибок:")
    for e in errors: print(f"  - {e}")
else:
    print("✓ Все проверки пройдены")
    print(f"\nЗапуск дашборда:")
    print(f"  cd {os.path.dirname(__file__)}")
    print(f"  streamlit run src/presentation/app.py")
