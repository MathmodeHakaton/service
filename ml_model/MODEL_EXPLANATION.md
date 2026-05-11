# Объяснение модели

## Почему не статичные веса

Мы не используем формулу вида:

```text
LSI = 0.25*M1 + 0.30*M2 + 0.20*M3 + 0.25*M5
```

И не используем ручной score внутри модуля вида:

```text
M2_score = 0.35*repo_volume + 0.35*utilization + 0.30*rate_spread
```

Вместо этого каждый показатель превращается в отдельную `stress_component`, например:

```text
m2_repo_volume_stress
m2_repo_utilization_stress
m2_repo_rate_spread_stress
m3_ofz_low_cover_stress
m5_structural_drain_stress
```

CatBoost сам учится, какие компоненты важны в конкретном рыночном режиме.

## Как ставится target

Пока нет экспертных меток, используется weak target. Это не финальная формула LSI, а временная обучающая разметка рыночного состояния.

Weak target учитывает:

- сколько модулей активно;
- насколько сильны top-1/top-2/top-3 stress components;
- есть ли комбинации M2+M5, M2+M3, M2+M3+M5;
- налоговый контекст M4;
- persistence за 3 дня;
- future horizon для раннего предупреждения.

Target непрерывный. Поэтому модель не обязана предсказывать только 20/50/80.

## Как работает SHAP

Порядок работы:

```text
X_today -> CatBoost -> LSI_today
X_today + CatBoost -> SHAP explanation
```

SHAP раскладывает прогноз:

```text
LSI = baseline + shap_feature_1 + shap_feature_2 + ...
```

Потом признаки группируются:

```text
M1 contribution = sum(SHAP признаков M1)
M2 contribution = sum(SHAP признаков M2)
...
```

SHAP не влияет на прогноз. Он нужен для дашборда, объяснения, комментариев и контроля качества.
