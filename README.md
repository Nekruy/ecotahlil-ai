# EcotahlilAI v2.0-professional

Система макроэкономического анализа и прогнозирования для Республики Таджикистан.

## Модели

| Модель | Описание | Точность |
|--------|----------|----------|
| **AutoARIMA** | Авто-выбор p,d,q по AIC; ADF тест для d; walk-forward backtesting | MAPE ~5% |
| **Ensemble** | ARIMA + Prophet с весами по точности (MAPE holdout); CI 80%/95% | ARIMA вес 74% |
| **EGARCH(1,1)** | Волатильность курса; leverage effect (γ); авто-данные НБТ 124 точки | Annualized vol ~26% |
| **VAR(p)** | Авто-выбор лага по AIC; ADF тесты; F-тест Грейнджера; авто-данные МЭРиТ | Лаг 1 |
| **Prophet** | Аддитивная модель тренд+сезонность | Вес 26% в ансамбле |
| **CGE** | Вычислимое общее равновесие, 10 секторов; рекалибровка из прогноза МЭРиТ | — |
| **Stress Test** | 4 сценария (нефть, переводы, урожай, ГЭС); базисные значения МЭРиТ 2025 | — |

## Финальный отчёт v2.0 (2026-04-16)

```
1. AutoARIMA  p=2 d=2 q=3   AIC=115.0
   Прогноз ВВП 2025-2027:  175.9 / 197.0 / 220.8 млрд сомони

2. Ensemble   ARIMA=74%  Prophet=26%
   Прогноз:               159.3 / 175.6 / 194.1
   CI 95%:                [34–284] / [15–336] / [-6–394]

3. Backtest   MAPE=4.95%  RMSE=6.98  шагов=12

4. EGARCH     vol=25.9% годовых  риск=критический
   Leverage effect: есть — плохие новости опаснее
   Данных: 124 точки (2015–2026)

5. VAR        лаг=1  источник=МЭРиТ РТ (авто)
   ADF:  ВВП — нестационарен  |  Инфляция — нестационарна
         Курс USD/TJS — стационарен ✓  |  Переводы — нестационарны

6. МЭРиТ vs ARIMA: +5.9% / +5.1% / +4.5% — ХОРОШЕЕ согласие ✓
```

## Источники данных

| Источник | Данные | Период |
|----------|--------|--------|
| МЭРиТ РТ | ВВП, экспорт, импорт, электроэнергия, алюминий, пшеница | 1997–2027 |
| НБТ | Курсы USD, EUR, RUB, CNY (авто-обновление каждые 24 ч) | 2015–2026 |
| МВФ / Мировой банк | Инфляция, переводы мигрантов | 1997–2024 |
| Yahoo Finance | Цены на нефть, золото | исторические |

## API endpoints

### Прогнозирование

```
POST /forecast
  body: { data?, method: "auto-arima"|"ensemble"|"prophet", periods, indicator? }
  → { forecast[], weights?, ci80?, ci95?, meta }

POST /api/backtest
  body: { indicator?, data?, method: "arima" }
  → { validation: { mape, rmse, mae, steps }, meta }
```

### Волатильность и VAR

```
POST /api/garch
  body: { data?, periods }          ← авто-загрузка из НБТ если data не передан
  → { selectedModel, annualizedVol, riskLevel, egarch, leverage_effect, validation, meta }

POST /api/var
  body: { data?, periods }          ← авто-загрузка из МЭРиТ если data не передан
  → { optimalLag, aicByLag, adfTests, granger, forecasts, meta }
```

### МЭРиТ и сравнение

```
GET  /api/official-forecast
  → { official_forecast: { gdp_mln_somoni, export_mln_usd, ... }, arima_comparison_gdp }

GET  /api/model-vs-official?indicator=gdp
  → { model_arima[], model_ensemble[], official_mert[], deviation_arima_pct[], verdict }
```

### Стресс-тест и CGE

```
POST /api/stress-test
  body: { scenario: "oil_shock"|"remittances_drop"|"crop_failure"|"hydropower_crisis", severity }
  → { gdpChange, exportChange, inflationChange, baseline, quarterly }

POST /api/cge
  body: { shock: { sector, magnitude }, periods }
  → { equilibrium, welfare, calibration }
```

### Курсы НБТ

```
GET  /api/rates-history?currency=usd&days=365
  → { currency, days, data: [{date, usd}], count, source }
```

## Структура проекта

```
macro-analysis/
├── server.js              — HTTP сервер, все endpoints
├── forecasting.js         — AutoARIMA, EGARCH, VAR, Ensemble, Backtest
├── nbtParser.js           — Парсер НБТ + loadHistoricalRates()
├── historicalDB.js        — База данных МЭРиТ 1997–2024 + прогноз 2025–2027
├── cgeModel.js            — CGE модель (10 секторов, рекалибровка)
├── stressTest.js          — Стресс-тесты (4 сценария)
├── dataCollector.js       — Сбор данных МВФ/НБТ/Yahoo
├── importForecastData.js  — Импорт Excel прогноза МЭРиТ
├── dashboard.html         — Веб-панель с блоком профессиональных моделей
└── data/
    ├── rates_timeseries.json          — Курсы НБТ (реальные + синтетика 2015–2026)
    ├── ministry_forecast_2025_2027.json — Официальный прогноз МЭРиТ
    ├── exchange_rates_history.json    — Годовые курсы 2015–2024
    ├── gdp_history.json               — ВВП 1997–2024
    └── ...
```

## Установка и запуск

```bash
npm install
node server.js
# Сервер: http://localhost:3000
# Dashboard: http://localhost:3000/dashboard
```

## Технические характеристики

- **Язык**: Node.js (без внешних ML-библиотек)
- **Оптимизация**: Метод Нелдера–Мида для ARMA/EGARCH
- **Валидация**: Walk-forward (60/40 split), RMSE/MAE/MAPE
- **Детерминизм**: seededRandom (sin-based) для воспроизводимых синтетических данных
- **Версия**: 2.0-professional
