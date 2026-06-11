'use strict';

/**
 * model_diagnostics.js — Независимая диагностика моделей EcotahlilAI
 *
 * Тесты:
 *   1. ADF — стационарность входного ряда
 *   2. Льюнга–Бокса — автокорреляция остатков (H0: белый шум)
 *   3. Бэктест walk-forward vs наивный бенчмарк (random walk)
 *   4. Устойчивость VAR — спектральный радиус A1 < 1
 *   5. Ограничения GARCH — omega > 0, alpha ≥ 0, beta ≥ 0, alpha+beta < 1
 *   6. Покрытие ДИ — доля актуальных значений внутри интервала
 *   7. Длина ряда — минимум и рекомендация
 */

const hdb = require('./historicalDB');
const f   = require('./forecasting');

// ─── Утилиты ─────────────────────────────────────────────────────────────────

function mean(arr) { return arr.reduce((s, v) => s + v, 0) / arr.length; }
function variance(arr) {
  const m = mean(arr);
  return arr.reduce((s, v) => s + (v - m) ** 2, 0) / arr.length;
}
function round2(v) { return Math.round(v * 100) / 100; }
function round4(v) { return Math.round(v * 10000) / 10000; }

// Стандартная нормальная CDF (Abramowitz & Stegun, max err 7.5e-8)
function normCDF(x) {
  const sign = x < 0 ? -1 : 1;
  const z    = Math.abs(x);
  const t    = 1 / (1 + 0.2316419 * z);
  const phi  = Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI);
  const poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  const cdf  = 1 - phi * poly;
  return sign >= 0 ? cdf : 1 - cdf;
}

// p-value для хи-квадрат(df) через приближение Уилсона–Хилферти
function chi2pValue(Q, df) {
  if (df <= 0 || !isFinite(Q) || Q <= 0) return 1;
  const h = 2 / (9 * df);
  const z = (Math.pow(Q / df, 1 / 3) - (1 - h)) / Math.sqrt(h);
  return 1 - normCDF(z);
}

// ─── 1. ADF-тест (переиспользуем из forecasting.js) ─────────────────────────

function adfTest(series) {
  const result = f.adfTest(series);
  return {
    ...result,
    verdict: result.stationary
      ? `OK — стационарен (t=${result.tStat}, p≈${result.pValue})`
      : `WARN — нестационарен (t=${result.tStat}, p≈${result.pValue}) → нужно d≥1`,
  };
}

// ─── 2. Тест Льюнга–Бокса ────────────────────────────────────────────────────

function ljungBox(residuals, maxLag) {
  const n = residuals.length;
  const h = maxLag || Math.min(10, Math.max(4, Math.floor(n / 3)));
  if (n < h + 2) {
    return { qStat: null, pValue: null, whiteNoise: null,
             verdict: `н/д — мало остатков (${n} < ${h + 2})` };
  }

  const m   = mean(residuals);
  const c   = residuals.map(v => v - m);
  const c0  = c.reduce((s, v) => s + v * v, 0) / n;
  if (c0 < 1e-14) {
    return { qStat: 0, pValue: 1, whiteNoise: true, verdict: 'OK — нулевая дисперсия (константные остатки)' };
  }

  let Q = 0;
  const acf = [];
  for (let k = 1; k <= h; k++) {
    let ck = 0;
    for (let t = k; t < n; t++) ck += c[t] * c[t - k];
    ck /= n;
    const rk = ck / c0;
    acf.push(round4(rk));
    Q += rk * rk / (n - k);
  }
  Q = n * (n + 2) * Q;

  const pValue    = chi2pValue(Q, h);
  const whiteNoise = pValue > 0.05;

  return {
    qStat:    round4(Q),
    pValue:   round4(pValue),
    lags:     h,
    whiteNoise,
    acf,
    verdict: whiteNoise
      ? `OK — белый шум (Q=${round2(Q)}, df=${h}, p=${round4(pValue)})`
      : `FAIL — автокорреляция остатков (Q=${round2(Q)}, df=${h}, p=${round4(pValue)})`,
  };
}

// ─── 3. Ограничения GARCH ────────────────────────────────────────────────────

function checkGarch(omega, alpha, beta) {
  const issues = [];
  if (!isFinite(omega) || omega <= 0)
    issues.push(`omega=${round4(omega)} ≤ 0 (дисперсия должна быть положительной)`);
  if (!isFinite(alpha) || alpha < 0)
    issues.push(`alpha=${round4(alpha)} < 0`);
  if (!isFinite(beta) || beta < 0)
    issues.push(`beta=${round4(beta)} < 0`);
  if (alpha + beta >= 1)
    issues.push(`alpha+beta=${round4(alpha + beta)} ≥ 1 (нарушение ковариационной стационарности)`);
  if (alpha + beta > 0.999 && alpha + beta < 1)
    issues.push(`alpha+beta=${round4(alpha + beta)} ≈ 1 — IGARCH, нет конечного длинного среднего`);

  return {
    ok:          issues.length === 0,
    omega:       round4(omega),
    alpha:       round4(alpha),
    beta:        round4(beta),
    persistence: round4(alpha + beta),
    issues,
    verdict: issues.length === 0
      ? `OK — ограничения GARCH соблюдены (α+β=${round4(alpha + beta)})`
      : `FAIL — ${issues.join('; ')}`,
  };
}

// ─── 4. Устойчивость VAR (спектральный радиус) ───────────────────────────────

function varStability(A1) {
  if (!Array.isArray(A1) || A1.length === 0 || !Array.isArray(A1[0])) {
    return { spectralRadius: null, stable: null, verdict: 'н/д — VECM (устойчивость через коинтеграцию)' };
  }
  const k = A1.length;

  // Степенная итерация с несколькими стартами → доминирующее |λ|
  function powerIter(M, iters = 300) {
    let v = new Array(k).fill(1 / Math.sqrt(k));
    let lam = 0;
    for (let i = 0; i < iters; i++) {
      const Mv   = M.map(row => row.reduce((s, a, j) => s + a * v[j], 0));
      const norm = Math.sqrt(Mv.reduce((s, x) => s + x * x, 0));
      if (norm < 1e-14) return 0;
      lam = norm;
      v   = Mv.map(x => x / norm);
    }
    return lam;
  }

  // Проверяем A1, A1^T и A1^2 для охвата субдоминантных собственных значений
  const A1T = A1[0].map((_, j) => A1.map(row => row[j]));
  const A1sq = A1.map(row =>
    row.map((_, j) => A1.reduce((s, r2, l) => s + row[l] * A1[l][j], 0))
  );

  const rho = Math.max(
    powerIter(A1),
    powerIter(A1T),
    Math.sqrt(powerIter(A1sq))
  );

  return {
    spectralRadius: round4(rho),
    stable:         rho < 1.0,
    verdict: rho < 1.0
      ? `OK — устойчив (ρ=${round4(rho)} < 1)`
      : `FAIL — неустойчив (ρ=${round4(rho)} ≥ 1) → прогнозы расходятся`,
  };
}

// ─── 5. Покрытие доверительных интервалов ────────────────────────────────────

function coverageTest(lower, upper, actual, nominalLevel) {
  const lvl = nominalLevel || 0.95;
  if (!lower || !upper || !actual || lower.length === 0) {
    return { coverage: null, verdict: 'н/д — нет данных для проверки' };
  }
  const n = Math.min(lower.length, upper.length, actual.length);
  let covered = 0;
  for (let i = 0; i < n; i++) {
    if (actual[i] >= lower[i] && actual[i] <= upper[i]) covered++;
  }
  const cov = round4(covered / n);
  const tol = 0.10;
  const ok  = cov >= lvl - tol;

  return {
    coverage:     cov,
    nominalLevel: lvl,
    n,
    verdict: ok
      ? `OK — покрытие ${Math.round(cov * 100)}% (ном. ${lvl * 100}%, допуск ±10%)`
      : `FAIL — покрытие ${Math.round(cov * 100)}% < ${(lvl - tol) * 100}%`,
  };
}

// ─── 6. Минимальная длина ряда ────────────────────────────────────────────────

function checkSampleSize(n, modelType) {
  const MIN = { arima: 8, arimax: 10, garch: 20, var: 24, bvar: 10, ets: 6, ensemble: 8, kalman: 5 };
  const REC = { arima: 20, arimax: 20, garch: 60, var: 40, bvar: 15, ets: 20, ensemble: 20, kalman: 15 };

  const min = MIN[modelType] || 10;
  const rec = REC[modelType] || 30;

  return {
    n, min, recommended: rec,
    ok:   n >= min,
    verdict: n < min
      ? `FAIL — n=${n} < минимума ${min} для ${modelType}`
      : n < rec
        ? `WARN — n=${n} < рекомендуемых ${rec} для ${modelType}`
        : `OK — n=${n} ≥ ${rec} (${modelType})`,
  };
}

// ─── 7. Бэктест (walk-forward vs наивный) ────────────────────────────────────

/**
 * Walk-forward backtest.
 * Наивный бенчмарк: предсказывает последнее наблюдение (random walk).
 * Возвращает { modelMape, naiveMape, beats, steps, modelRmse, naiveRmse }
 *
 * @param {number[]} data
 * @param {function} forecastFn  — forecastFn(trainSlice) → array|number
 * @param {object}   opts        — { minTrain, maxSteps }
 */
function backtest(data, forecastFn, opts) {
  const minTrain = (opts && opts.minTrain) || 8;
  const maxSteps = (opts && opts.maxSteps) || 12;
  const n        = data.length;
  const steps    = Math.min(maxSteps, n - minTrain);

  if (steps < 3) {
    return { modelMape: null, naiveMape: null, beats: null, steps: 0,
             verdict: `н/д — мало точек (n=${n}, minTrain=${minTrain})` };
  }

  let mSape = 0, mSse = 0, mSae = 0;
  let nSape = 0, nSse = 0, nSae = 0;
  let cnt   = 0;

  for (let s = steps; s >= 1; s--) {
    const end    = n - s;
    const train  = data.slice(0, end);
    const actual = data[end];
    const naive  = train[train.length - 1];

    try {
      const raw  = forecastFn(train);
      const pred = Array.isArray(raw) ? raw[0] : raw;
      if (pred == null || !isFinite(pred)) continue;

      const me = pred - actual, ne = naive - actual;
      mSse += me * me;  mSae += Math.abs(me);
      nSse += ne * ne;  nSae += Math.abs(ne);
      if (actual !== 0) {
        mSape += Math.abs(me / actual) * 100;
        nSape += Math.abs(ne / actual) * 100;
      }
      cnt++;
    } catch (_) {}
  }

  if (cnt === 0) {
    return { modelMape: null, naiveMape: null, beats: null, steps: 0, verdict: 'ошибка в forecastFn' };
  }

  const modelMape = round2(mSape / cnt);
  const naiveMape = round2(nSape / cnt);
  const modelRmse = round2(Math.sqrt(mSse / cnt));
  const naiveRmse = round2(Math.sqrt(nSse / cnt));
  const beats     = modelMape < naiveMape;

  return {
    modelMape, naiveMape, modelRmse, naiveRmse,
    beats,  steps: cnt,
    verdict: beats
      ? `OK — побеждает наивный (MAPE: ${modelMape}% < ${naiveMape}%,  шагов: ${cnt})`
      : `FAIL — хуже наивного (MAPE: ${modelMape}% > ${naiveMape}%,  шагов: ${cnt})`,
  };
}

// ─── 8. Остатки (1-шаговые ошибки walk-forward) ──────────────────────────────

function computeResiduals(data, forecastFn, opts) {
  const n        = data.length;
  const minTrain = (opts && opts.minTrain) || Math.max(8, Math.floor(n * 0.55));
  const maxSteps = (opts && opts.maxSteps) || 15;
  const residuals = [];

  for (let t = minTrain; t < Math.min(n, minTrain + maxSteps); t++) {
    try {
      const train = data.slice(0, t);
      const raw   = forecastFn(train);
      const pred  = Array.isArray(raw) ? raw[0] : raw;
      if (pred != null && isFinite(pred)) residuals.push(data[t] - pred);
    } catch (_) {}
  }
  return residuals;
}

// ─── Вычисление условных дисперсий GARCH (для стандартизованных остатков) ─────

function garchStdResiduals(rets, omega, alpha, beta) {
  const n    = rets.length;
  const initV = rets.reduce((s, r) => s + r * r, 0) / n;
  const hArr  = [initV];
  for (let t = 1; t < n; t++) {
    hArr.push(omega + alpha * rets[t - 1] ** 2 + beta * hArr[t - 1]);
  }
  return rets.map((r, i) => (hArr[i] > 0 ? r / Math.sqrt(hArr[i]) : 0));
}

// ─── Форматирование строки вывода ─────────────────────────────────────────────

function tag(verdict) {
  if (!verdict) return '⬜';
  if (verdict.startsWith('OK'))   return '✅';
  if (verdict.startsWith('WARN')) return '⚠️ ';
  return '❌';
}

function pr(label, obj) {
  const v = obj && obj.verdict ? obj.verdict : String(obj);
  console.log(`  ${tag(v)} ${label}: ${v}`);
}

// ════════════════════════════════════════════════════════════════════════════════
//  diagnoseAll — главная функция диагностики
// ════════════════════════════════════════════════════════════════════════════════

function diagnoseAll() {
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('   ДИАГНОСТИКА МОДЕЛЕЙ EcotahlilAI v3.0');
  console.log('══════════════════════════════════════════════════════════════\n');

  // ── Загрузка данных ──────────────────────────────────────────────────────────
  const gdpGrowth   = hdb.getDataForForecasting('gdp_growth').filter(v => v != null && isFinite(v));
  const inflation   = hdb.getDataForForecasting('inflation').filter(v => v != null && isFinite(v));
  const usdTjs      = hdb.getDataForForecasting('usd_tjs').filter(v => v != null && isFinite(v));
  const remittances = hdb.getDataForForecasting('remittances').filter(v => v != null && isFinite(v));

  console.log('Данные:');
  console.log(`  ВВП-рост:   n=${gdpGrowth.length}   [${gdpGrowth.slice(0, 4).map(v => round2(v)).join(', ')}…]`);
  console.log(`  Инфляция:   n=${inflation.length}   [${inflation.slice(0, 4).map(v => round2(v)).join(', ')}…]`);
  console.log(`  USD/TJS:    n=${usdTjs.length}   [${usdTjs.slice(0, 4).map(v => round2(v)).join(', ')}…]`);
  console.log(`  Переводы:   n=${remittances.length}   [${remittances.slice(0, 4).map(v => round2(v)).join(', ')}…]`);
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // A. AUTO-ARIMA (ВВП-рост)
  // ════════════════════════════════════════════════════════════════
  console.log('── A. AUTO-ARIMA (ВВП-рост) ─────────────────────────────────');
  pr('Длина ряда',    checkSampleSize(gdpGrowth.length, 'arima'));
  pr('ADF входного',  adfTest(gdpGrowth));

  const arimaFn = train => f.autoArima(train, 1).forecast;
  pr('Бэктест',       backtest(gdpGrowth, arimaFn, { minTrain: 8, maxSteps: 12 }));

  const arimaRes = computeResiduals(gdpGrowth, arimaFn, { minTrain: 10 });
  pr('Льюнга–Бокса',  ljungBox(arimaRes));

  // Full-sample fit для p,d,q
  try {
    const full = f.autoArima(gdpGrowth, 1);
    console.log(`  ℹ️  Лучшая модель: ARIMA(${full.bestP},${full.bestD},${full.bestQ}), AIC=${full.aic}`);
    if (full.adfResults) {
      full.adfResults.forEach(r => {
        const st = r.stationary ? 'стационарен' : 'нестационарен';
        console.log(`  ℹ️  ADF d=${r.d}: t=${r.tStat} → ${st}`);
      });
    }
  } catch (e) { console.log(`  ⚠️  autoArima full-fit: ${e.message}`); }

  // ДИ нет в autoArima — явное замечание
  console.log('  ⚠️  ДИ: автоматические доверительные интервалы не реализованы в autoArima');
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // Б. ENSEMBLE (ВВП-рост)
  // ════════════════════════════════════════════════════════════════
  console.log('── Б. ENSEMBLE ARIMA+Prophet (ВВП-рост) ─────────────────────');
  pr('Длина ряда',   checkSampleSize(gdpGrowth.length, 'ensemble'));

  const ensFn = train => f.ensembleForecast(train, 1).ensemble;
  pr('Бэктест',      backtest(gdpGrowth, ensFn, { minTrain: 8, maxSteps: 12 }));

  const ensRes = computeResiduals(gdpGrowth, ensFn, { minTrain: 10 });
  pr('Льюнга–Бокса', ljungBox(ensRes));

  // Проверка покрытия CI95 через hold-out
  try {
    const ensHorizon = 3;
    const ensTrainEnd = gdpGrowth.length - ensHorizon;
    if (ensTrainEnd >= 8) {
      const ensResult = f.ensembleForecast(gdpGrowth.slice(0, ensTrainEnd), ensHorizon);
      const lowers = ensResult.ci95Raw ? ensResult.ci95Raw.lower : ensResult.ci95.map(p => p[0]);
      const uppers = ensResult.ci95Raw ? ensResult.ci95Raw.upper : ensResult.ci95.map(p => p[1]);
      const actuals = gdpGrowth.slice(ensTrainEnd, ensTrainEnd + ensHorizon);
      pr('Покрытие CI95', coverageTest(lowers, uppers, actuals, 0.95));
    }
  } catch (e) { console.log(`  ⚠️  CI95 покрытие: ${e.message}`); }

  try {
    const fullEns = f.ensembleForecast(gdpGrowth, 3);
    console.log(`  ℹ️  Веса: ARIMA=${round2(fullEns.weights.arima * 100)}%, Prophet=${round2(fullEns.weights.prophet * 100)}%`);
    console.log(`  ℹ️  MAPE (holdout): ARIMA=${fullEns.mape.arima}%, Prophet=${fullEns.mape.prophet}%`);
  } catch (e) { console.log(`  ⚠️  Ensemble full-fit: ${e.message}`); }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // В. GARCH / EGARCH (USD/TJS)
  // ════════════════════════════════════════════════════════════════
  console.log('── В. GARCH(1,1) + EGARCH(1,1) (USD/TJS) ───────────────────');
  pr('Длина ряда', checkSampleSize(usdTjs.length, 'garch'));
  pr('ADF USD/TJS', adfTest(usdTjs));

  try {
    const gr = f.garch(usdTjs, 5);
    pr('Ограничения GARCH(1,1)', checkGarch(gr.omega, gr.alpha, gr.beta));

    if (gr.egarch) {
      pr('Ограничения EGARCH', checkGarch(
        Math.exp(gr.egarch.omega),   // omega в лог-пространстве → нет прямого ограничения > 0
        Math.abs(gr.egarch.alpha),
        Math.abs(gr.egarch.beta)
      ));
    }

    // Стандартизованные остатки GARCH
    const rets = [];
    for (let i = 1; i < usdTjs.length; i++) {
      if (usdTjs[i - 1] > 0 && usdTjs[i] > 0)
        rets.push(Math.log(usdTjs[i] / usdTjs[i - 1]) * 100);
    }
    const stdRes = garchStdResiduals(rets, gr.omega, gr.alpha, gr.beta);
    pr('Льюнга–Бокса (стд. остатки)',  ljungBox(stdRes));
    pr('Льюнга–Бокса (стд. остатки²)', ljungBox(stdRes.map(r => r * r)));

    // Встроенный бэктест GARCH
    if (gr.validation && gr.validation.outOfSampleR2 != null) {
      const v  = gr.validation;
      const ok = v.outOfSampleR2 > 0;
      console.log(
        `  ${ok ? '✅' : '❌'} Бэктест GARCH: out-of-sample R²=${v.outOfSampleR2}, ` +
        `directional accuracy=${v.directionalAccuracy}%, RMSE=${v.rmse}`
      );
    }

    console.log(`  ℹ️  Выбранная модель: ${gr.selectedModel}, ρ (α+β)=${gr.persistence}`);
    console.log(`  ℹ️  Годовая волатильность: ${gr.annualizedVol}%, риск: ${gr.riskLevel}`);
    if (gr.egarchSignal) console.log(`  ℹ️  ${gr.egarchSignal}`);
  } catch (e) {
    console.log(`  ❌ GARCH ошибка: ${e.message}`);
  }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // Г. VAR(p) (4 переменных)
  // ════════════════════════════════════════════════════════════════
  console.log('── Г. VAR(p) (ВВП + Инфляция + Курс + Переводы) ────────────');
  const nVar = Math.min(gdpGrowth.length, inflation.length, usdTjs.length, remittances.length);
  pr('Длина ряда (мин. по 4 переменным)', checkSampleSize(nVar, 'var'));

  try {
    const varData = {
      gdp:           gdpGrowth,
      inflation:     inflation,
      exchange_rate: usdTjs,
      remittances:   remittances,
    };
    const vr = f.var_model(varData, 3);

    if (vr.modelType) console.log(`  ℹ️  Спецификация: ${vr.modelType.toUpperCase()}`);
    if (vr.cointegration && vr.cointegration.rank > 0)
      console.log(`  ℹ️  Коинтеграция (Йохансен): ранг=${vr.cointegration.rank}, λ_trace=${JSON.stringify(vr.cointegration.traceStats)}`);
    pr('Устойчивость VAR', varStability(vr.coefficients));
    console.log(`  ℹ️  Оптимальный лаг: p=${vr.lagOrder}, AIC по лагам: ${JSON.stringify(vr.aicByLag)}`);

    // R² каждого уравнения
    const varNames = ['ВВП', 'Инфляция', 'USD/TJS', 'Переводы'];
    vr.r2.forEach((r2, i) => {
      console.log(`  ${r2 > 0.5 ? '✅' : '⚠️ '} R²(${varNames[i]}) = ${r2}`);
    });

    // ADF по каждой переменной
    if (vr.adfTests) {
      Object.entries(vr.adfTests).forEach(([k, t]) => {
        const st = t.stationary ? 'стационарен' : 'нестационарен';
        console.log(`  ℹ️  ADF ${t.label}: ${st} (t=${t.tStat}, p≈${t.pValue})`);
      });
    }

    // Бэктест VAR на ВВП (компонент 0)
    const varFnGdp = train => {
      const T = train.length;
      if (T < 10) return [train[T - 1]];
      const len = Math.min(T, inflation.length, usdTjs.length, remittances.length);
      if (len < 7) return [train[T - 1]];
      try {
        const d = {
          gdp:           gdpGrowth.slice(0, len),
          inflation:     inflation.slice(0, len),
          exchange_rate: usdTjs.slice(0, len),
          remittances:   remittances.slice(0, len),
        };
        return [f.var_model(d, 1).forecasts[0][0]];
      } catch { return [train[T - 1]]; }
    };
    pr('Бэктест ВВП-рост', backtest(gdpGrowth, varFnGdp, { minTrain: 12, maxSteps: 8 }));

    // Причинность Грейнджера
    const sig = Object.entries(vr.granger).filter(([, v]) => v.significant);
    if (sig.length > 0) {
      console.log(`  ✅ Причинность Грейнджера (p<5%): ${sig.map(([k]) => k).join(', ')}`);
    } else {
      console.log('  ⚠️  Причинность Грейнджера: значимых связей нет (мало данных или слабые зависимости)');
    }
  } catch (e) {
    console.log(`  ❌ VAR ошибка: ${e.message}`);
  }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // Д. ETS / Holt-Winters (Инфляция)
  // ════════════════════════════════════════════════════════════════
  console.log('── Д. ETS / Holt-Winters (Инфляция) ────────────────────────');
  pr('Длина ряда',   checkSampleSize(inflation.length, 'ets'));
  pr('ADF Инфляция', adfTest(inflation));

  const etsFn = train => f.etsForecast(train, 1).forecast;
  pr('Бэктест',      backtest(inflation, etsFn, { minTrain: 6, maxSteps: 12 }));

  const etsRes = computeResiduals(inflation, etsFn, { minTrain: 8 });
  pr('Льюнга–Бокса', ljungBox(etsRes));

  // Покрытие — ETS не возвращает ДИ; замечание
  console.log('  ⚠️  ДИ: etsForecast не возвращает доверительных интервалов');

  try {
    const fullEts = f.etsForecast(inflation, 3);
    console.log(`  ℹ️  α=${fullEts.alpha}, β=${fullEts.beta}, breakPoint=${fullEts.breakPoint}`);
  } catch (e) { console.log(`  ⚠️  ETS full-fit: ${e.message}`); }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // Е. ARIMAX (ВВП-рост + переводы + курс)
  // ════════════════════════════════════════════════════════════════
  console.log('── Е. ARIMAX (ВВП-рост + переводы + USD/TJS) ───────────────');
  pr('Длина ряда', checkSampleSize(gdpGrowth.length, 'arimax'));

  const arimaxFn = train => {
    const T = train.length;
    const exog = {
      remittances: remittances.slice(0, T),
      usd_tjs:     usdTjs.slice(0, T),
    };
    try {
      return f.arimaxForecast(train, exog, 1).forecast;
    } catch { return [train[T - 1]]; }
  };
  pr('Бэктест', backtest(gdpGrowth, arimaxFn, { minTrain: 10, maxSteps: 10 }));

  const arimaxRes = computeResiduals(gdpGrowth, arimaxFn, { minTrain: 12 });
  pr('Льюнга–Бокса', ljungBox(arimaxRes));

  try {
    const axFull = f.arimaxForecast(gdpGrowth, { remittances, usd_tjs: usdTjs }, 3);
    console.log(`  ℹ️  ${axFull.model}, AIC=${axFull.aic}`);
    console.log(`  ℹ️  Регрессоры: ${axFull.exogenous_names.join(', ')}, использовано: ${axFull.exogenous_used}`);
    // Покрытие CI95 ARIMAX
    if (axFull.ci95) {
      const axEnd   = gdpGrowth.length - 3;
      const axResult = f.arimaxForecast(gdpGrowth.slice(0, axEnd), {
        remittances: remittances.slice(0, axEnd),
        usd_tjs:     usdTjs.slice(0, axEnd),
      }, 3);
      if (axResult.ci95) {
        const lowers  = axResult.ci95.map(p => p[0]);
        const uppers  = axResult.ci95.map(p => p[1]);
        const actuals = gdpGrowth.slice(axEnd, axEnd + 3);
        pr('Покрытие CI95', coverageTest(lowers, uppers, actuals, 0.95));
      }
    }
  } catch (e) { console.log(`  ⚠️  ARIMAX full-fit: ${e.message}`); }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // Ж. Kalman Filter (ВВП-рост)
  // ════════════════════════════════════════════════════════════════
  console.log('── Ж. Kalman Filter (ВВП-рост) ──────────────────────────────');
  pr('Длина ряда', checkSampleSize(gdpGrowth.length, 'kalman'));

  const kalmanFn = train => {
    try { return f.kalmanFilter(train, { periods: 1 }).forecast; }
    catch { return [train[train.length - 1]]; }
  };
  pr('Бэктест', backtest(gdpGrowth, kalmanFn, { minTrain: 6, maxSteps: 12 }));

  const kalmanRes = computeResiduals(gdpGrowth, kalmanFn, { minTrain: 8 });
  pr('Льюнга–Бокса инноваций', ljungBox(kalmanRes));

  try {
    const kFull = f.kalmanFilter(gdpGrowth, { periods: 3 });
    const lowers = kFull.ci95.map(p => p[0]);
    const uppers = kFull.ci95.map(p => p[1]);
    // hold-out: последние 3 точки
    const kEnd    = gdpGrowth.length - 3;
    const kResult = f.kalmanFilter(gdpGrowth.slice(0, kEnd), { periods: 3 });
    const kLow    = kResult.ci95.map(p => p[0]);
    const kUpp    = kResult.ci95.map(p => p[1]);
    const kAct    = gdpGrowth.slice(kEnd, kEnd + 3);
    pr('Покрытие CI95', coverageTest(kLow, kUpp, kAct, 0.95));
    console.log(`  ℹ️  Q=${kFull.Q}, R=${kFull.R}, тренд=${kFull.trend}, RMSE=${kFull.rmse}`);
  } catch (e) { console.log(`  ⚠️  Kalman CI: ${e.message}`); }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // З. B-VAR / Minnesota Prior (ВВП-рост)
  // ════════════════════════════════════════════════════════════════
  console.log('── З. B-VAR Minnesota Prior (ВВП-рост) ─────────────────────');
  pr('Длина ряда', checkSampleSize(gdpGrowth.length, 'bvar'));

  const bvarFn = train => {
    const T = train.length;
    if (T < 7) return [train[T - 1]];
    const d = {
      gdp:         train,
      inflation:   inflation.slice(0, T),
      usd_tjs:     usdTjs.slice(0, T),
      remittances: remittances.slice(0, T),
    };
    try {
      const r = f.bvarForecast(d, 1, 0.2);
      return [r.forecast.gdp ? r.forecast.gdp[0] : train[T - 1]];
    } catch { return [train[T - 1]]; }
  };
  pr('Бэктест', backtest(gdpGrowth, bvarFn, { minTrain: 8, maxSteps: 10 }));

  const bvarRes = computeResiduals(gdpGrowth, bvarFn, { minTrain: 10 });
  pr('Льюнга–Бокса', ljungBox(bvarRes));

  // λ и покрытие CI95
  try {
    const bhold = 4;
    const bEnd  = gdpGrowth.length - bhold;
    if (bEnd >= 10) {
      const bd = {
        gdp:         gdpGrowth.slice(0, bEnd),
        inflation:   inflation.slice(0, bEnd),
        usd_tjs:     usdTjs.slice(0, bEnd),
        remittances: remittances.slice(0, bEnd),
      };
      const bResult = f.bvarForecast(bd, bhold);
      console.log(`  ℹ️  λ авто=${bResult.lambdaSelected}, модель: ${bResult.model}`);
      if (bResult.ci95 && bResult.ci95.gdp) {
        const lowers  = bResult.ci95.gdp.map(ci => ci.lower);
        const uppers  = bResult.ci95.gdp.map(ci => ci.upper);
        const actuals = gdpGrowth.slice(bEnd, bEnd + bhold);
        pr('Покрытие CI95', coverageTest(lowers, uppers, actuals, 0.95));
      }
    }
  } catch (e) { console.log(`  ⚠️  B-VAR CI/λ: ${e.message}`); }
  console.log('');

  // ════════════════════════════════════════════════════════════════
  // ИТОГОВАЯ ТАБЛИЦА
  // ════════════════════════════════════════════════════════════════
  console.log('══════════════════════════════════════════════════════════════');
  console.log('  ЛЕГЕНДА: ✅ OK  |  ⚠️  WARN (не критично)  |  ❌ FAIL (нарушение)');
  console.log('  Запустите: node model_diagnostics.js');
  console.log('══════════════════════════════════════════════════════════════\n');
}

// ─── Экспорт ──────────────────────────────────────────────────────────────────

module.exports = {
  adfTest,
  ljungBox,
  checkGarch,
  varStability,
  coverageTest,
  checkSampleSize,
  backtest,
  computeResiduals,
  diagnoseAll,
};

// Авто-запуск при прямом вызове
if (require.main === module) {
  try {
    diagnoseAll();
  } catch (e) {
    console.error('\n❌ Критическая ошибка в diagnoseAll:', e.message);
    console.error(e.stack);
    process.exit(1);
  }
}
