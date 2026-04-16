'use strict';

/**
 * forecasting.js — Профессиональные эконометрические модели
 * Auto-ARIMA, EGARCH(1,1), VAR(p) с ADF-тестом, Backtest, Ensemble
 * Чистый JavaScript без внешних зависимостей
 */

const fs   = require('fs');
const path = require('path');

const MODEL_VERSION    = '2.0-professional';
const TIMESERIES_FILE  = path.join(__dirname, 'data', 'rates_timeseries.json');

// ─── Математические вспомогательные функции ───────────────────────────────────

function round2(v) { return Math.round(v * 100) / 100; }
function round4(v) { return Math.round(v * 10000) / 10000; }

function mean(arr) {
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

function variance(arr) {
  const m = mean(arr);
  return arr.reduce((s, v) => s + (v - m) ** 2, 0) / arr.length;
}

function stdDev(arr) { return Math.sqrt(variance(arr)); }

function linearRegression(xs, ys) {
  const n  = xs.length;
  const mx = mean(xs), my = mean(ys);
  const num = xs.reduce((s, x, i) => s + (x - mx) * (ys[i] - my), 0);
  const den = xs.reduce((s, x) => s + (x - mx) ** 2, 0);
  const slope     = den !== 0 ? num / den : 0;
  const intercept = my - slope * mx;
  return { slope, intercept, predict: t => slope * t + intercept };
}

function quantile(arr, p) {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = p * (sorted.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function validateData(data) {
  if (!Array.isArray(data) || data.length < 4)
    throw new Error('Необходимо минимум 4 точки данных');
  const nums = data.map(Number);
  if (nums.some(isNaN)) throw new Error('Все значения должны быть числами');
  return nums;
}

// ─── Метод Нелдера–Мида (минимизация без производных) ────────────────────────

function nelderMead(fn, x0, { maxIter = 2000, tol = 1e-10 } = {}) {
  const n = x0.length;
  const A = 1, G = 2, R = 0.5, S = 0.5;

  const simplex = [x0.slice()];
  for (let i = 0; i < n; i++) {
    const p = x0.slice();
    p[i] = p[i] !== 0 ? p[i] * 1.1 : 0.0005;
    simplex.push(p);
  }
  let fvals = simplex.map(fn);

  for (let iter = 0; iter < maxIter; iter++) {
    const idx = Array.from({ length: n + 1 }, (_, i) => i).sort((a, b) => fvals[a] - fvals[b]);
    const s = idx.map(i => simplex[i].slice());
    const f = idx.map(i => fvals[i]);

    if (f[n] - f[0] < tol) break;

    const c = new Array(n).fill(0);
    for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) c[j] += s[i][j] / n;

    const xr = c.map((v, j) => v + A * (v - s[n][j]));
    const fr = fn(xr);

    if (fr < f[0]) {
      const xe = c.map((v, j) => v + G * (xr[j] - v));
      const fe = fn(xe);
      s[n] = fe < fr ? xe : xr;
      f[n] = fe < fr ? fe : fr;
    } else if (fr < f[n - 1]) {
      s[n] = xr; f[n] = fr;
    } else {
      const xc = c.map((v, j) => v + R * (s[n][j] - v));
      const fc = fn(xc);
      if (fc < f[n]) {
        s[n] = xc; f[n] = fc;
      } else {
        for (let i = 1; i <= n; i++) {
          s[i] = s[0].map((v, j) => v + S * (s[i][j] - v));
          f[i] = fn(s[i]);
        }
      }
    }
    for (let i = 0; i <= n; i++) { simplex[i] = s[i]; fvals[i] = f[i]; }
  }

  let best = 0;
  for (let i = 1; i <= n; i++) if (fvals[i] < fvals[best]) best = i;
  return { params: simplex[best], value: fvals[best] };
}

// ─── ADF-тест (Augmented Dickey-Fuller) ──────────────────────────────────────

/**
 * Simplified ADF test: regression Δy_t = α + β·y_{t-1} + ε_t
 * H0: unit root (non-stationary). Critical value at 5%: -2.86.
 */
function adfTest(series) {
  const n = series.length;
  if (n < 5) return { stationary: false, tStat: 0, pValue: 0.99 };

  const yLag = series.slice(0, n - 1);
  const dy   = series.slice(1).map((v, i) => v - series[i]);
  const m    = dy.length;

  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (let i = 0; i < m; i++) { sx += yLag[i]; sy += dy[i]; sxx += yLag[i] ** 2; sxy += yLag[i] * dy[i]; }
  const mx = sx / m, my = sy / m;
  const Sxx = sxx - m * mx * mx;
  const Sxy = sxy - m * mx * my;

  const beta  = Math.abs(Sxx) > 1e-14 ? Sxy / Sxx : 0;
  const alpha = my - beta * mx;

  let rss = 0;
  for (let i = 0; i < m; i++) rss += (dy[i] - alpha - beta * yLag[i]) ** 2;

  const s2     = rss / Math.max(1, m - 2);
  const seBeta = Math.sqrt(Math.max(0, s2 / Math.max(1e-14, Sxx)));
  const tStat  = seBeta > 1e-10 ? beta / seBeta : 0;

  // ADF critical values (MacKinnon)
  const critValues = { '1%': -3.43, '5%': -2.86, '10%': -2.57 };
  const stationary = tStat < critValues['5%'];
  const pValue = tStat < -4.0 ? 0.001 : tStat < -3.5 ? 0.01 : tStat < critValues['5%'] ? 0.05 : tStat < critValues['10%'] ? 0.10 : 0.25;

  return { stationary, tStat: round4(tStat), pValue, critValues };
}

// ─── Разностное преобразование и обратное ─────────────────────────────────────

function diff(series, d = 1) {
  let s = series.slice();
  for (let i = 0; i < d; i++) {
    const nd = [];
    for (let j = 1; j < s.length; j++) nd.push(s[j] - s[j - 1]);
    s = nd;
  }
  return s;
}

/**
 * Обратное разностное преобразование.
 * originalSeries — исходный (недифференцированный) ряд для начальных значений.
 * diffForecast — прогноз на уровне d-й разности.
 */
function inverseDiff(originalSeries, diffForecast, d) {
  if (d === 0) return diffForecast.map(v => round2(v));

  // Строим уровни дифференцирования
  const levels = [originalSeries.slice()];
  for (let i = 0; i < d; i++) {
    const prev = levels[i];
    const nd = [];
    for (let j = 1; j < prev.length; j++) nd.push(prev[j] - prev[j - 1]);
    levels.push(nd);
  }

  // Восстанавливаем уровни обратно
  let fc = diffForecast.slice();
  for (let lv = d; lv >= 1; lv--) {
    const lastVal = levels[lv - 1][levels[lv - 1].length - 1];
    const undiffed = [];
    let prev = lastVal;
    for (const v of fc) { prev += v; undiffed.push(prev); }
    fc = undiffed;
  }

  return fc.map(v => round2(v));
}

// ─── ARMA(p,q) — оценка и прогноз ───────────────────────────────────────────

/**
 * Fit ARMA(p,q) via Conditional Sum of Squares (CSS) + Nelder-Mead.
 * Returns: { phi, theta, mu, rss, aic, logLik }
 */
function fitARMA(series, p, q) {
  const n  = series.length;
  const k  = p + q + 1;
  if (n < k + 2) return { phi: [], theta: [], mu: mean(series), rss: Infinity, aic: Infinity, logLik: -Infinity };

  const mu0 = mean(series);

  function css(params) {
    const phi_   = params.slice(0, p);
    const theta_ = params.slice(p, p + q);
    const mu_    = params[p + q];
    const resids = new Array(n).fill(0);
    let rss = 0;
    for (let t = 0; t < n; t++) {
      let pred = mu_;
      for (let i = 0; i < p; i++) if (t - i - 1 >= 0) pred += phi_[i] * (series[t - i - 1] - mu_);
      for (let j = 0; j < q; j++) if (t - j - 1 >= 0) pred -= theta_[j] * resids[t - j - 1];
      resids[t] = series[t] - pred;
      rss += resids[t] ** 2;
    }
    return isFinite(rss) ? rss : 1e15;
  }

  const x0 = [...new Array(p).fill(0.1), ...new Array(q).fill(0.05), mu0];
  const { params, value: rss } = nelderMead(css, x0, { maxIter: 600, tol: 1e-8 });

  const phi_   = params.slice(0, p);
  const theta_ = params.slice(p, p + q);
  const mu_    = params[p + q];

  const sigma2 = rss / Math.max(1, n);
  const logLik = sigma2 > 0 ? -n / 2 * Math.log(2 * Math.PI * sigma2) - n / 2 : -1e15;
  const aic    = 2 * k - 2 * logLik;

  return { phi: phi_, theta: theta_, mu: mu_, rss, aic, logLik };
}

/** Прогноз ARMA на periods шагов вперёд */
function forecastARMA(series, model, periods) {
  const { phi, theta, mu } = model;
  const p = phi.length, q = theta.length, n = series.length;

  // Вычисляем исторические остатки
  const resids = new Array(n).fill(0);
  for (let t = 0; t < n; t++) {
    let pred = mu;
    for (let i = 0; i < p; i++) if (t - i - 1 >= 0) pred += phi[i] * (series[t - i - 1] - mu);
    for (let j = 0; j < q; j++) if (t - j - 1 >= 0) pred -= theta[j] * resids[t - j - 1];
    resids[t] = series[t] - pred;
  }

  const extS = series.slice();
  const extR = resids.slice();
  const fc   = [];

  for (let h = 0; h < periods; h++) {
    let pred = mu;
    for (let i = 0; i < p; i++) {
      const idx = extS.length - 1 - i;
      if (idx >= 0) pred += phi[i] * (extS[idx] - mu);
    }
    for (let j = 0; j < q; j++) {
      const idx = extR.length - 1 - j;
      if (idx >= 0) pred -= theta[j] * extR[idx]; // будущие шоки = 0
    }
    fc.push(pred);
    extS.push(pred);
    extR.push(0);
  }

  return fc;
}

// ─── Сравнение с официальным прогнозом МЭРиТ ─────────────────────────────────

/**
 * Сравнивает модельный прогноз с официальными данными МЭРиТ.
 * @param {number[]} modelForecast — прогноз модели на periods шагов
 * @param {number[]} officialForecast — официальный прогноз (массив)
 * @returns {{ model_forecast, official_forecast, deviation_pct, agreement }}
 */
function compareWithOfficial(modelForecast, officialForecast) {
  if (!Array.isArray(officialForecast) || officialForecast.length === 0) return null;
  const n = Math.min(modelForecast.length, officialForecast.length);
  const model_fc  = modelForecast.slice(0, n);
  const official  = officialForecast.slice(0, n);
  const deviation_pct = model_fc.map((v, i) => {
    if (official[i] == null || official[i] === 0) return null;
    return round2((v - official[i]) / Math.abs(official[i]) * 100);
  });
  const maxDev = Math.max(...deviation_pct.filter(d => d != null).map(Math.abs));
  const agreement = maxDev < 5 ? 'хорошее' : maxDev < 15 ? 'умеренное' : 'расхождение';
  return { model_forecast: model_fc, official_forecast: official, deviation_pct, agreement };
}

// ─── Auto-ARIMA с перебором по AIC ──────────────────────────────────────────

/**
 * Auto-ARIMA: ADF-тест для d, перебор p=0..3, q=0..3, выбор по min AIC.
 * @param {number[]} officialForecast — опциональный официальный прогноз для сравнения
 * @returns {{ forecast, bestP, bestD, bestQ, aic, method, adfResults, comparison? }}
 */
function autoArima(data, periods, officialForecast) {
  const nums = validateData(data);

  // 1. Определяем d через ADF-тест (d = 0, 1 или 2)
  let d = 0;
  let diffSeries = nums.slice();
  const adfResults = [];

  const adf0 = adfTest(diffSeries);
  adfResults.push({ d: 0, ...adf0 });

  if (!adf0.stationary) {
    d = 1;
    diffSeries = diff(diffSeries, 1);
    if (diffSeries.length >= 5) {
      const adf1 = adfTest(diffSeries);
      adfResults.push({ d: 1, ...adf1 });
      if (!adf1.stationary && diffSeries.length >= 6) {
        d = 2;
        diffSeries = diff(diffSeries, 1);
      }
    }
  }

  // 2. Перебор p=0..3, q=0..3
  let bestAIC = Infinity, bestP = 0, bestQ = 0, bestModel = null;

  for (let p = 0; p <= 3; p++) {
    for (let q = 0; q <= 3; q++) {
      if (diffSeries.length < p + q + 3) continue;
      try {
        const model = fitARMA(diffSeries, p, q);
        if (isFinite(model.aic) && model.aic < bestAIC) {
          bestAIC = model.aic; bestP = p; bestQ = q; bestModel = model;
        }
      } catch (_) {}
    }
  }

  // 3. Прогноз + обратное разностное преобразование
  const diffFc  = bestModel ? forecastARMA(diffSeries, bestModel, periods) : new Array(periods).fill(0);
  const forecast = inverseDiff(nums, diffFc, d);

  const result = {
    forecast, bestP, bestD: d, bestQ, aic: round4(bestAIC), method: 'auto-arima', adfResults,
    meta: { dataPoints: nums.length, collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
  };
  if (officialForecast) result.comparison = compareWithOfficial(forecast, officialForecast);
  return result;
}

// ─── Backtesting ARIMA (Walk-Forward Validation) ─────────────────────────────

/**
 * Walk-forward validation для autoArima.
 * Начальное окно: 60% данных. Прогнозирует 1 шаг вперёд, сдвигает окно.
 * Ограничение: MAX_STEPS = 30 (для скорости).
 */
function backtestArima(data) {
  const nums = validateData(data);
  const n    = nums.length;
  const initW = Math.floor(n * 0.6);

  if (initW < 4 || n - initW < 2) {
    return { rmse: null, mae: null, mape: null, steps: 0, method: 'auto-arima' };
  }

  const MAX_STEPS = 30;
  const steps = Math.min(n - initW, MAX_STEPS);
  const errors = [];

  for (let step = 0; step < steps; step++) {
    const trainData = nums.slice(0, initW + step);
    const actual    = nums[initW + step];
    try {
      const result = autoArima(trainData, 1);
      errors.push({ actual, predicted: result.forecast[0] });
    } catch (_) {}
  }

  if (errors.length === 0) return { rmse: null, mae: null, mape: null, steps: 0, method: 'auto-arima' };

  let sse = 0, sae = 0, sape = 0;
  for (const { actual, predicted } of errors) {
    const err = predicted - actual;
    sse  += err ** 2;
    sae  += Math.abs(err);
    sape += actual !== 0 ? Math.abs(err / actual) * 100 : 0;
  }
  const m = errors.length;

  return { rmse: round4(Math.sqrt(sse / m)), mae: round4(sae / m), mape: round4(sape / m), steps: m, method: 'auto-arima' };
}

// ─── Prophet-подобная модель ──────────────────────────────────────────────────

function prophet(data, periods) {
  const nums = validateData(data);
  const n    = nums.length;

  const xs  = Array.from({ length: n }, (_, i) => i);
  const reg = linearRegression(xs, nums);

  const residuals = nums.map((v, i) => v - reg.predict(i));
  const period    = Math.min(12, Math.max(3, Math.floor(n / 2)));
  const sSum  = new Array(period).fill(0);
  const sCnt  = new Array(period).fill(0);
  residuals.forEach((r, i) => { sSum[i % period] += r; sCnt[i % period]++; });
  const seasonal = sSum.map((s, i) => sCnt[i] > 0 ? s / sCnt[i] : 0);
  const sMean = mean(seasonal);
  const sAdj  = seasonal.map(s => s - sMean);

  const forecast = [];
  for (let i = 0; i < periods; i++) {
    const t = n + i;
    forecast.push(round2(reg.predict(t) + sAdj[t % period]));
  }
  return forecast;
}

// ─── Обнаружение аномалий ────────────────────────────────────────────────────

function detectAnomalies(data) {
  const nums = validateData(data);
  const m    = mean(nums), s = stdDev(nums);
  const q1 = quantile(nums, 0.25), q3 = quantile(nums, 0.75);
  const iqr = q3 - q1;
  const lo  = q1 - 1.5 * iqr, hi = q3 + 1.5 * iqr;
  const anomalies = [];
  nums.forEach((v, i) => {
    const z = s !== 0 ? Math.abs(v - m) / s : 0;
    const isZ = z > 2, isI = v < lo || v > hi;
    if (isZ || isI) anomalies.push({
      index: i, value: v, zscore: round2(z),
      direction: v > m ? 'high' : 'low',
      method: isZ && isI ? 'Z-score + IQR' : isZ ? 'Z-score' : 'IQR',
    });
  });
  return anomalies;
}

// ─── GARCH/EGARCH — моделирование волатильности ───────────────────────────────

/** Оценка GARCH(1,1) через MLE + Nelder-Mead */
function estimateGARCH(retsPct) {
  const n = retsPct.length;
  const initVar = variance(retsPct);

  function negLogLik([omega, alpha, beta]) {
    if (omega <= 1e-8 || alpha <= 0 || beta <= 0) return 1e15;
    if (alpha + beta >= 0.9999) return 1e15;
    let h = initVar, ll = 0;
    for (let t = 0; t < n; t++) {
      if (t > 0) h = omega + alpha * retsPct[t - 1] ** 2 + beta * h;
      if (h <= 0) return 1e15;
      ll += Math.log(h) + retsPct[t] ** 2 / h;
    }
    return 0.5 * ll;
  }

  const starts = [
    [initVar * 0.05, 0.10, 0.85],
    [initVar * 0.02, 0.05, 0.90],
    [initVar * 0.10, 0.15, 0.80],
    [initVar * 0.20, 0.20, 0.70],
  ];

  let best = { value: Infinity, params: starts[0] };
  for (const init of starts) {
    const res = nelderMead(negLogLik, init);
    if (res.value < best.value) best = res;
  }

  let [omega, alpha, beta] = best.params;
  omega = Math.max(1e-8, omega);
  alpha = Math.max(0.001, Math.min(0.4999, alpha));
  beta  = Math.max(0.001, Math.min(0.9979 - alpha, beta));
  return { omega, alpha, beta, negLogLik: best.value };
}

/**
 * EGARCH(1,1): log(h_t) = ω + α(|z_{t-1}| − √(2/π)) + γ·z_{t-1} + β·log(h_{t-1})
 * γ < 0 означает, что плохие новости усиливают волатильность.
 */
function estimateEGARCH(retsPct) {
  const n        = retsPct.length;
  const initVar  = variance(retsPct);
  const initLogH = Math.log(Math.max(1e-8, initVar));
  const SQRT2PI  = Math.sqrt(2 / Math.PI);

  function negLogLik([omega, alpha, gamma, beta]) {
    let logH = initLogH, ll = 0;
    for (let t = 0; t < n; t++) {
      const h = Math.exp(logH);
      if (!isFinite(h) || h <= 0) return 1e15;
      ll += logH + retsPct[t] ** 2 / h;
      if (t < n - 1) {
        const z = retsPct[t] / Math.sqrt(Math.max(h, 1e-10));
        logH = omega + alpha * (Math.abs(z) - SQRT2PI) + gamma * z + beta * logH;
        if (!isFinite(logH) || logH > 50) return 1e15;
      }
    }
    return 0.5 * ll;
  }

  const starts = [
    [-0.10, 0.10, -0.05, 0.85],
    [-0.20, 0.15, -0.10, 0.80],
    [-0.05, 0.08, -0.03, 0.90],
    [-0.30, 0.20, -0.15, 0.75],
  ];

  let best = { value: Infinity, params: starts[0] };
  for (const init of starts) {
    try {
      const res = nelderMead(negLogLik, init, { maxIter: 1000, tol: 1e-8 });
      if (res.value < best.value) best = res;
    } catch (_) {}
  }

  const [omega, alpha, gamma, beta] = best.params;
  return {
    omega: round4(omega), alpha: round4(alpha),
    gamma: round4(gamma), beta: round4(beta),
    negLogLik: best.value,
  };
}

/** Backtesting GARCH: обучение на 80%, прогноз на 20% */
function backtestGARCH(nums) {
  const null_result = { rmse: null, directionalAccuracy: null, outOfSampleR2: null, dataPoints: nums.length };
  if (nums.length < 20) return null_result;

  const splitIdx  = Math.floor(nums.length * 0.8);
  const trainData = nums.slice(0, splitIdx);
  const testData  = nums.slice(splitIdx);

  // Доходности тренировочной выборки
  const trainPct = [];
  for (let i = 1; i < trainData.length; i++) {
    if (trainData[i - 1] > 0 && trainData[i] > 0)
      trainPct.push(Math.log(trainData[i] / trainData[i - 1]) * 100);
  }
  if (trainPct.length < 8) return null_result;

  const { omega, alpha, beta } = estimateGARCH(trainPct);
  const persistence = alpha + beta;
  const initVar  = variance(trainPct);
  const longRunV = persistence < 1 ? omega / (1 - persistence) : initVar;

  // Последняя условная дисперсия на трейне
  let lastH = initVar;
  for (let t = 1; t < trainPct.length; t++) {
    lastH = omega + alpha * trainPct[t - 1] ** 2 + beta * lastH;
    if (!isFinite(lastH) || lastH <= 0) lastH = initVar;
  }

  // Фактические |доходности| теста
  const actualAbs = [];
  for (let i = 1; i < testData.length; i++) {
    if (testData[i - 1] > 0 && testData[i] > 0)
      actualAbs.push(Math.abs(Math.log(testData[i] / testData[i - 1]) * 100));
  }
  if (actualAbs.length < 2) return null_result;

  // Прогноз волатильности
  const forecastVol = [];
  for (let k = 1; k <= actualAbs.length; k++) {
    const fv = longRunV + Math.pow(persistence, k) * (lastH - longRunV);
    forecastVol.push(Math.sqrt(Math.max(0, fv)));
  }

  const nn = Math.min(forecastVol.length, actualAbs.length);
  let sse = 0, dirOk = 0;
  for (let i = 0; i < nn; i++) {
    sse += (forecastVol[i] - actualAbs[i]) ** 2;
    if (i > 0) {
      const fd = forecastVol[i] > forecastVol[i - 1];
      const ad = actualAbs[i]   > actualAbs[i - 1];
      if (fd === ad) dirOk++;
    }
  }

  const rmse = round4(Math.sqrt(sse / nn));
  const directionalAccuracy = nn > 1 ? round2((dirOk / (nn - 1)) * 100) : null;
  const mA = mean(actualAbs.slice(0, nn));
  const tss = actualAbs.slice(0, nn).reduce((s, v) => s + (v - mA) ** 2, 0);
  const outOfSampleR2 = tss > 0 ? round4(1 - sse / tss) : null;

  return { rmse, directionalAccuracy, outOfSampleR2, dataPoints: nums.length };
}

/**
 * GARCH(1,1) + EGARCH(1,1) — прогноз волатильности курса валюты.
 * Включает backtesting и поле validation.
 * Если data не передан или длина < 30 — автозагрузка из data/rates_timeseries.json.
 */
function garch(data, periods) {
  let nums = Array.isArray(data) ? data.map(Number).filter(v => !isNaN(v)) : [];
  let dataSource = 'user-provided';

  // Автозагрузка: rates_timeseries.json → historicalDB (в порядке приоритета)
  if (nums.length < 30) {
    try {
      const ratesRaw = JSON.parse(fs.readFileSync(TIMESERIES_FILE, 'utf8'));
      const usdData  = ratesRaw.map(r => r.usd).filter(Boolean);
      if (usdData.length > nums.length) {
        nums = usdData;
        dataSource = 'НБТ РТ (авто)';
      }
    } catch (_) {}
  }
  // Второй фолбэк: годовые курсы из historicalDB (если NBT мало)
  if (nums.length < 10) {
    try {
      const hdb      = require('./historicalDB');
      const histData = hdb.getDataForForecasting('usd_tjs').filter(v => v != null);
      if (histData.length > nums.length) {
        nums = histData;
        dataSource = 'МЭРиТ/НБТ исторические (авто)';
      }
    } catch (_) {}
  }

  if (nums.length < 10) throw new Error('Для GARCH необходимо минимум 10 точек данных');

  // 1. Логарифмические доходности в %
  const rets = [], retsPct = [];
  for (let i = 1; i < nums.length; i++) {
    if (nums[i - 1] <= 0 || nums[i] <= 0) throw new Error('Все значения курса должны быть положительными');
    const r = Math.log(nums[i] / nums[i - 1]);
    rets.push(r);
    retsPct.push(r * 100);
  }

  // 2. Оценка GARCH(1,1)
  const garchParams = estimateGARCH(retsPct);
  const { omega, alpha, beta } = garchParams;
  const persistence = alpha + beta;

  // 3. Оценка EGARCH(1,1)
  let egarchParams = null;
  try { egarchParams = estimateEGARCH(retsPct); } catch (_) {}

  // Выбираем лучшую модель по log-likelihood
  const garchWins = !egarchParams || garchParams.negLogLik <= egarchParams.negLogLik;

  // 4. Исторические условные дисперсии (GARCH)
  const initVar = variance(retsPct);
  const condVar = new Array(retsPct.length);
  condVar[0] = initVar;
  for (let t = 1; t < retsPct.length; t++) {
    condVar[t] = omega + alpha * retsPct[t - 1] ** 2 + beta * condVar[t - 1];
  }
  const historicalVol = condVar.map(h => round4(Math.sqrt(Math.max(0, h))));

  // 5. Прогноз дисперсии
  const longRunVar = persistence < 1 ? omega / (1 - persistence) : initVar;
  const lastH      = condVar[condVar.length - 1];

  const fwdVar = [];
  for (let k = 1; k <= periods; k++) {
    const v = longRunVar + Math.pow(persistence, k) * (lastH - longRunVar);
    fwdVar.push(Math.max(0, v));
  }
  const forecastVol = fwdVar.map(v => round4(Math.sqrt(v)));

  // 5b. EGARCH прогноз (log-space)
  let egarchForecastVol = null;
  if (egarchParams) {
    const { omega: ow, alpha: aw, gamma: gw, beta: bw } = egarchParams;
    // Вычисляем последний log(h)
    const SQRT2PI = Math.sqrt(2 / Math.PI);
    let logH = Math.log(Math.max(1e-8, initVar));
    for (let t = 0; t < retsPct.length - 1; t++) {
      const h = Math.exp(logH);
      const z = retsPct[t] / Math.sqrt(Math.max(h, 1e-10));
      logH = ow + aw * (Math.abs(z) - SQRT2PI) + gw * z + bw * logH;
      if (!isFinite(logH) || logH > 50) logH = Math.log(Math.max(1e-8, initVar));
    }
    const egarchFv = [];
    const longLogH = Math.abs(bw) < 1 ? ow / (1 - bw) : logH;
    for (let k = 1; k <= periods; k++) {
      const eLH = longLogH + Math.pow(bw, k) * (logH - longLogH);
      egarchFv.push(round4(Math.sqrt(Math.exp(eLH))));
    }
    egarchForecastVol = egarchFv;
  }

  // 6. Доверительные интервалы
  const histVolArr = condVar.map(h => Math.sqrt(Math.max(0, h)));
  const histVolStd = stdDev(histVolArr);
  const ci1Lower = fwdVar.map((v, i) => round4(Math.max(0, Math.sqrt(v) - histVolStd * 0.5 * Math.sqrt(i + 1))));
  const ci1Upper = fwdVar.map((v, i) => round4(Math.sqrt(v) + histVolStd * 0.5 * Math.sqrt(i + 1)));
  const ci2Lower = fwdVar.map((v, i) => round4(Math.max(0, Math.sqrt(v) - histVolStd * 1.0 * Math.sqrt(i + 1))));
  const ci2Upper = fwdVar.map((v, i) => round4(Math.sqrt(v) + histVolStd * 1.0 * Math.sqrt(i + 1)));

  // 7. Уровень риска
  const currentDailyVol = round4(Math.sqrt(Math.max(0, lastH)));
  const annualizedVol   = round4(Math.sqrt(Math.max(0, lastH)) * Math.sqrt(252));

  let riskLevel, signal;
  if (annualizedVol < 5) {
    riskLevel = 'низкий';
    signal = `Волатильность курса низкая — ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). α+β = ${round4(persistence)}. Валютный риск минимален.`;
  } else if (annualizedVol < 15) {
    riskLevel = 'умеренный';
    signal = `Волатильность умеренная: ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). α+β = ${round4(persistence)}. Рекомендуется усилить мониторинг валютных рисков и рассмотреть частичное хеджирование.`;
  } else if (annualizedVol < 25) {
    riskLevel = 'высокий';
    signal = `ВНИМАНИЕ: Высокая волатильность — ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). α+β = ${round4(persistence)}. Активировать хеджирование ключевых позиций.`;
  } else {
    riskLevel = 'критический';
    signal = `КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ: Экстремальная волатильность — ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). Требуется немедленное вмешательство руководства.`;
  }

  // 8. Интерпретация асимметрии EGARCH
  let egarchSignal = null;
  if (egarchParams && egarchParams.gamma < -0.05) {
    egarchSignal = `EGARCH(γ=${egarchParams.gamma}): плохие новости усиливают волатильность на ${Math.abs(round2(egarchParams.gamma * 100))}% сильнее, чем хорошие (эффект левереджа).`;
  } else if (egarchParams && egarchParams.gamma > 0.05) {
    egarchSignal = `EGARCH(γ=${egarchParams.gamma}): хорошие новости усиливают волатильность (нестандартный эффект).`;
  }

  // 9. Backtesting
  const validation = backtestGARCH(nums);

  const selectedModel = garchWins ? 'GARCH' : 'EGARCH';
  const leverageEffect = egarchParams
    ? (egarchParams.gamma < 0
        ? 'есть — плохие новости опаснее'
        : egarchParams.gamma > 0.05
          ? 'нет — хорошие новости усиливают волатильность'
          : 'нейтральный')
    : 'не определён';

  return {
    // Выбранная модель
    selectedModel,
    leverage_effect: leverageEffect,
    // GARCH(1,1) параметры
    omega: round4(omega), alpha: round4(alpha), beta: round4(beta), persistence: round4(persistence),
    // EGARCH(1,1)
    egarch: egarchParams
      ? { omega: egarchParams.omega, alpha: egarchParams.alpha, gamma: egarchParams.gamma, beta: egarchParams.beta, persistence: round4(Math.abs(egarchParams.beta)) }
      : null,
    egarchForecastVol,
    egarchSignal,
    // Ряды
    returns:       rets.map(r => round4(r * 100)),
    historicalVol,
    forecastVol: garchWins ? forecastVol : (egarchForecastVol || forecastVol),
    ci1Lower, ci1Upper, ci2Lower, ci2Upper,
    // Итоговые метрики
    currentDailyVol, annualizedVol, riskLevel, signal,
    // Валидация (backtesting)
    validation: { rmse: validation.rmse, directionalAccuracy: validation.directionalAccuracy, outOfSampleR2: validation.outOfSampleR2, dataPoints: validation.dataPoints },
    // Мета
    meta: { dataSource, dataPoints: nums.length, collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
  };
}

// ─── Ensemble (ARIMA + Prophet) ───────────────────────────────────────────────

/**
 * Взвешенный ансамблевый прогноз.
 * Веса = обратная MAPE по последним 20% данных (мин. 5 точек).
 * @param {number[]} officialForecast — опциональный официальный прогноз для сравнения
 */
function ensembleForecast(data, periods, officialForecast) {
  const nums = validateData(data);
  const n    = nums.length;

  // Быстрый бэктест для весов
  const testSize  = Math.min(5, Math.max(1, Math.floor(n * 0.2)));
  const trainData = nums.slice(0, n - testSize);
  const testData  = nums.slice(n - testSize);

  let arimaMape = null, prophetMape = null;

  if (trainData.length >= 4) {
    // ARIMA MAPE
    try {
      const arimaTest = autoArima(trainData, testSize);
      let s = 0;
      for (let i = 0; i < testSize; i++) s += testData[i] !== 0 ? Math.abs((arimaTest.forecast[i] - testData[i]) / testData[i]) : 0;
      arimaMape = s / testSize;
    } catch (_) {}

    // Prophet MAPE
    try {
      const prophetTest = prophet(trainData, testSize);
      let s = 0;
      for (let i = 0; i < testSize; i++) s += testData[i] !== 0 ? Math.abs((prophetTest[i] - testData[i]) / testData[i]) : 0;
      prophetMape = s / testSize;
    } catch (_) {}
  }

  // Прогнозы на полных данных
  const arimaResult  = autoArima(nums, periods);
  const prophetResult = prophet(nums, periods);
  const arimaFc  = arimaResult.forecast;
  const propFc   = prophetResult;

  // Веса по обратной MAPE
  let wArima = 0.5, wProphet = 0.5;
  if (arimaMape != null && prophetMape != null) {
    const eps  = 1e-6;
    const invA = 1 / (arimaMape + eps);
    const invP = 1 / (prophetMape + eps);
    const tot  = invA + invP;
    wArima   = invA / tot;
    wProphet = invP / tot;
  }

  // Взвешенный прогноз
  const ensemble = arimaFc.map((a, i) => round2(wArima * a + wProphet * propFc[i]));

  // Доверительные интервалы через разброс моделей (z=1.28 → 80%, z=1.96 → 95%)
  const ci80 = { lower: [], upper: [] };
  const ci95 = { lower: [], upper: [] };
  for (let i = 0; i < periods; i++) {
    const spread = Math.abs(arimaFc[i] - propFc[i]);
    ci80.lower.push(round2(ensemble[i] - 1.28 * spread));
    ci80.upper.push(round2(ensemble[i] + 1.28 * spread));
    ci95.lower.push(round2(ensemble[i] - 1.96 * spread));
    ci95.upper.push(round2(ensemble[i] + 1.96 * spread));
  }

  const result = {
    ensemble,
    arima:   arimaFc,
    prophet: propFc,
    weights: { arima: round4(wArima), prophet: round4(wProphet) },
    mape: {
      arima:   arimaMape   != null ? round4(arimaMape * 100)   : null,
      prophet: prophetMape != null ? round4(prophetMape * 100) : null,
    },
    ci80,
    ci95,
    method: 'ensemble',
  };
  if (officialForecast) result.comparison = compareWithOfficial(ensemble, officialForecast);
  return result;
}

// ─── Матричные утилиты для VAR ────────────────────────────────────────────────

function matCreate(r, c, fill = 0) {
  return Array.from({ length: r }, () => new Array(c).fill(fill));
}

function matTranspose(A) {
  const r = A.length, c = A[0].length;
  const T = matCreate(c, r);
  for (let i = 0; i < r; i++) for (let j = 0; j < c; j++) T[j][i] = A[i][j];
  return T;
}

function matMul(A, B) {
  const rA = A.length, cA = A[0].length, cB = B[0].length;
  const C  = matCreate(rA, cB);
  for (let i = 0; i < rA; i++)
    for (let m = 0; m < cA; m++)
      if (A[i][m] !== 0)
        for (let j = 0; j < cB; j++) C[i][j] += A[i][m] * B[m][j];
  return C;
}

function matInverse(A) {
  const n = A.length;
  const aug = A.map((row, i) => {
    const id = new Array(n).fill(0); id[i] = 1;
    return [...row, ...id];
  });
  for (let col = 0; col < n; col++) {
    let pr = col;
    for (let r = col + 1; r < n; r++) if (Math.abs(aug[r][col]) > Math.abs(aug[pr][col])) pr = r;
    [aug[col], aug[pr]] = [aug[pr], aug[col]];
    const piv = aug[col][col];
    if (Math.abs(piv) < 1e-14) throw new Error('Матрица вырождена — проверьте данные на мультиколлинеарность');
    for (let j = 0; j < 2 * n; j++) aug[col][j] /= piv;
    for (let r = 0; r < n; r++) {
      if (r === col) continue;
      const f = aug[r][col];
      for (let j = 0; j < 2 * n; j++) aug[r][j] -= f * aug[col][j];
    }
  }
  return aug.map(row => row.slice(n));
}

function matVecMul(A, v) { return A.map(row => row.reduce((s, a, j) => s + a * v[j], 0)); }
function vecAdd(a, b) { return a.map((v, i) => v + b[i]); }

// ─── VAR(p) — обобщённая оценка ──────────────────────────────────────────────

/**
 * Оценка VAR(p) методом МНК.
 * Возвращает Afull (k × k*lag), A1 (lag-1, для IRF), constants, rss, seMatrix.
 */
function estimateVARp(series, lag) {
  const k = series.length;
  const T = series[0].length;
  const n = T - lag;
  const nPar = k * lag + 1; // столбцов в design matrix

  if (n < nPar + 1) throw new Error(`Недостаточно наблюдений для VAR(${lag}). Нужно минимум ${nPar + lag + 1}.`);

  // Design matrix X: n × (k*lag + 1)
  const X = [];
  for (let t = lag; t < T; t++) {
    const row = [1];
    for (let l = 1; l <= lag; l++)
      for (let j = 0; j < k; j++) row.push(series[j][t - l]);
    X.push(row);
  }

  const Ymat = [];
  for (let t = lag; t < T; t++) Ymat.push(series.map(s => s[t]));

  const Xt       = matTranspose(X);
  const XtX      = matMul(Xt, X);
  const XtXinv   = matInverse(XtX);
  const XtXinvXt = matMul(XtXinv, Xt);

  const constants = [];
  const Afull  = matCreate(k, k * lag);
  const rss    = new Array(k).fill(0);
  const residuals = Array.from({ length: k }, () => []);
  const seMatrix  = matCreate(k, nPar);
  const df = Math.max(1, n - nPar);

  for (let i = 0; i < k; i++) {
    const yi   = Ymat.map(row => row[i]);
    const beta = matVecMul(XtXinvXt, yi);
    constants[i] = beta[0];
    for (let j = 0; j < k * lag; j++) Afull[i][j] = beta[j + 1];

    for (let t = 0; t < n; t++) {
      let fit = beta[0];
      for (let j = 0; j < k * lag; j++) fit += Afull[i][j] * X[t][j + 1];
      const res = Ymat[t][i] - fit;
      residuals[i].push(res);
      rss[i] += res * res;
    }

    const s2 = rss[i] / df;
    for (let j = 0; j < nPar; j++) seMatrix[i][j] = Math.sqrt(Math.max(0, XtXinv[j][j] * s2));
  }

  // A1 — коэффициенты лага 1 (k×k) для IRF
  const A1 = matCreate(k, k);
  for (let i = 0; i < k; i++) for (let j = 0; j < k; j++) A1[i][j] = Afull[i][j];

  return { Afull, A1, constants, residuals, seMatrix, rss, df, n, lag, k, nPar };
}

/** Прогноз VAR(p) на periods шагов вперёд */
function forecastVARp(Afull, constants, zSeries, lag, periods) {
  const k = constants.length;
  // История последних lag наблюдений (oldest first)
  const history = [];
  for (let l = lag - 1; l >= 0; l--) history.push(zSeries.map(s => s[s.length - 1 - l]));

  const out = [];
  for (let h = 0; h < periods; h++) {
    const next = new Array(k).fill(0);
    for (let i = 0; i < k; i++) {
      next[i] = constants[i];
      for (let l = 0; l < lag; l++) {
        const lagVals = history[history.length - 1 - l];
        for (let j = 0; j < k; j++) next[i] += Afull[i][l * k + j] * lagVals[j];
      }
    }
    out.push(next.slice());
    history.push(next.slice());
  }
  return out;
}

/** Impulse Response Functions (использует только lag-1 матрицу A1) */
function computeIRF(A, maxH) {
  const k = A.length;
  const irf = Array.from({ length: k }, () => []);
  for (let j = 0; j < k; j++) {
    let resp = new Array(k).fill(0); resp[j] = 1;
    for (let h = 0; h <= maxH; h++) {
      irf[j].push(resp.map(v => round4(v)));
      if (h < maxH) resp = matVecMul(A, resp);
    }
  }
  return irf;
}

/**
 * Тест причинности Грейнджера (F-тест).
 * Для каждой пары (j→i): сравниваем ограниченную (без лагов j) и неограниченную модели.
 * result[j][i] = { fStat, pValue, significant }
 */
function grangerCausalityF(normalizedSeries, lag, rssUnrestricted, n) {
  const k = normalizedSeries.length;
  const T = normalizedSeries[0].length;
  const result = Array.from({ length: k }, () => new Array(k).fill(null));

  for (let i = 0; i < k; i++) {
    for (let j = 0; j < k; j++) {
      if (i === j) continue;

      // Ограниченная модель: убираем лаги переменной j
      const Xr = [];
      for (let t = lag; t < T; t++) {
        const row = [1];
        for (let l = 1; l <= lag; l++)
          for (let jj = 0; jj < k; jj++)
            if (jj !== j) row.push(normalizedSeries[jj][t - l]);
        Xr.push(row);
      }

      const yi = [];
      for (let t = lag; t < T; t++) yi.push(normalizedSeries[i][t]);

      try {
        const XrT    = matTranspose(Xr);
        const XrTXr  = matMul(XrT, Xr);
        const XrTXrI = matInverse(XrTXr);
        const betaR  = matVecMul(matMul(XrTXrI, XrT), yi);

        let rssR = 0;
        for (let t = 0; t < n; t++) {
          let fit = betaR[0];
          for (let c = 1; c < betaR.length; c++) fit += betaR[c] * Xr[t][c];
          rssR += (yi[t] - fit) ** 2;
        }

        const rssU = rssUnrestricted[i];
        const df1  = lag;
        const df2  = Math.max(1, n - 2 * lag * k - 1);
        const fStat = rssU > 1e-14
          ? Math.max(0, ((rssR - rssU) / df1) / (rssU / df2))
          : 0;

        const pValue = fStat > 10 ? 0.001 : fStat > 4.0 ? 0.01 : fStat > 2.5 ? 0.05 : fStat > 1.5 ? 0.15 : 0.30;
        result[j][i] = { fStat: round4(fStat), pValue, significant: pValue < 0.05 };
      } catch (_) {
        result[j][i] = { fStat: null, pValue: null, significant: false };
      }
    }
  }
  return result;
}

/** Выбор оптимального лага VAR по AIC. Возвращает { optimalLag, aicByLag }. */
function selectVARLags(normalizedSeries, maxLag = 4) {
  const k = normalizedSeries.length;
  const T = normalizedSeries[0].length;
  let bestAIC = Infinity, bestLag = 1;
  const aicByLag = {};

  for (let lag = 1; lag <= maxLag; lag++) {
    const n = T - lag;
    if (n < k * lag + 2) break;
    try {
      const { rss } = estimateVARp(normalizedSeries, lag);
      // AIC = T·ln(det(Σ)) + 2·p·k² , Σ приближается через сумму log(RSS_i/T)
      const logDetSigma = rss.reduce((s, r) => s + Math.log(Math.max(r / n, 1e-10)), 0);
      const aic = n * logDetSigma + 2 * k * (k * lag + 1);
      aicByLag[lag] = round4(aic);
      if (aic < bestAIC) { bestAIC = aic; bestLag = lag; }
    } catch (_) { break; }
  }

  return { optimalLag: bestLag, aicByLag };
}

// ─── Интерпретация VAR ────────────────────────────────────────────────────────

function buildVARInterpretation(keys, labels, A1, granger, forecasts, series, periods, adfResults, lagOrder) {
  const k       = keys.length;
  const lastObs = keys.map((_, i) => series[i][series[i].length - 1]);
  const lines   = [];

  lines.push(`Анализ VAR(${lagOrder}) макроэкономических показателей Таджикистана. Горизонт прогноза: ${periods} периодов.`);
  lines.push('');

  // ADF тесты
  if (adfResults && adfResults.length > 0) {
    lines.push('ТЕСТ ДИКИ-ФУЛЛЕРА (стационарность рядов):');
    for (const r of adfResults) {
      const status = r.stationary ? '✓ стационарен' : '✗ нестационарен';
      lines.push(`  • ${labels[keys[r.variable]] || keys[r.variable]}: ${status} (t=${r.tStat}, p≈${r.pValue})`);
    }
    lines.push('');
  }

  // Причинность Грейнджера (F-тест)
  const sigLinks = [];
  for (let j = 0; j < k; j++)
    for (let i = 0; i < k; i++)
      if (i !== j && granger[j][i]?.significant)
        sigLinks.push({ cause: j, effect: i, fStat: granger[j][i].fStat, pValue: granger[j][i].pValue });

  if (sigLinks.length > 0) {
    lines.push(`ПРИЧИННОСТЬ ГРЕЙНДЖЕРА — F-тест (p < 5%), лаг=${lagOrder}:`);
    for (const s of sigLinks) {
      lines.push(`  • ${labels[keys[s.cause]]} → ${labels[keys[s.effect]]} (F=${s.fStat}, p≈${s.pValue})`);
    }
  } else {
    lines.push('ПРИЧИННОСТЬ ГРЕЙНДЖЕРА: статистически значимых связей не выявлено. Рекомендуется расширить временной ряд.');
  }
  lines.push('');

  // Прогнозные изменения
  lines.push('ПРОГНОЗНЫЕ ИЗМЕНЕНИЯ (1-й период вперёд):');
  for (let i = 0; i < k; i++) {
    const last = lastObs[i], next = forecasts[0][i];
    const delta = next - last;
    const pct   = last !== 0 ? (delta / Math.abs(last) * 100).toFixed(1) : '—';
    const arrow = delta >= 0 ? '↑' : '↓';
    lines.push(`  • ${labels[keys[i]]}: ${arrow} ${Math.abs(pct)}% (${round4(last)} → ${round4(next)})`);
  }
  lines.push('');

  // Сильнейшие взаимодействия
  const cross = [];
  for (let i = 0; i < k; i++)
    for (let j = 0; j < k; j++)
      if (i !== j) cross.push({ from: j, to: i, coef: A1[i][j] });
  cross.sort((a, b) => Math.abs(b.coef) - Math.abs(a.coef));

  lines.push('СИЛЬНЕЙШИЕ ВЗАИМОДЕЙСТВИЯ (стандартизованные β):');
  for (const cc of cross.slice(0, 4)) {
    const eff = cc.coef > 0 ? 'усиливает' : 'сдерживает';
    lines.push(`  • ${labels[keys[cc.from]]} ${eff} ${labels[keys[cc.to]]} (A=${cc.coef > 0 ? '+' : ''}${round4(cc.coef)})`);
  }
  lines.push('');
  lines.push('РЕКОМЕНДАЦИИ: При формировании монетарной и фискальной политики учитывайте выявленные взаимозависимости. Динамика переводов мигрантов поддерживает внутренний спрос и может усиливать инфляционное давление. Курс USD/TJS влияет на импортную инфляцию. При структурных шоках рекомендуется переоценка модели.');

  return lines.join('\n');
}

// ─── VAR(p) — векторная авторегрессия ────────────────────────────────────────

/**
 * VAR(p) с автовыбором лага, ADF-тестами, F-тестом Грейнджера.
 */
function var_model(data, periods) {
  const KEYS   = ['gdp', 'inflation', 'exchange_rate', 'remittances'];
  const LABELS = {
    gdp:           'ВВП',
    inflation:     'Инфляция',
    exchange_rate: 'Курс USD/TJS',
    remittances:   'Переводы мигрантов',
  };

  // 1. Автозагрузка из historicalDB если data не передан или неполный
  const inp = (data && typeof data === 'object') ? data : {};
  let dataSource = inp._source || 'user-provided';
  const missing = KEYS.some(k => !Array.isArray(inp[k]) || inp[k].length < 6);
  if (missing) {
    try {
      const hdb = require('./historicalDB');
      const gdpRaw   = hdb.getDataForForecasting('gdp').filter(v => v != null);
      const infRaw   = hdb.getDataForForecasting('inflation').filter(v => v != null);
      const exRaw    = hdb.getDataForForecasting('usd_tjs').filter(v => v != null);
      const remHist  = hdb.getRemittancesHistory();
      const remRaw   = remHist.map(r => r.amount_mln_usd ?? r.total_mln_usd).filter(v => v != null);
      if (gdpRaw.length >= 6 && infRaw.length >= 6) {
        inp.gdp           = gdpRaw;
        inp.inflation     = infRaw;
        inp.exchange_rate = exRaw.length >= 6 ? exRaw : new Array(gdpRaw.length).fill(10.5);
        inp.remittances   = remRaw.length >= 6 ? remRaw : new Array(gdpRaw.length).fill(2000);
        dataSource        = 'МЭРиТ РТ (авто)';
      }
    } catch (_) {}
  }

  // 2. Валидация входных данных
  const raw = [];
  for (const key of KEYS) {
    const arr = (Array.isArray(inp[key]) ? inp[key] : []).map(Number);
    if (arr.length < 6)  throw new Error(`${LABELS[key]}: необходимо минимум 6 наблюдений`);
    if (arr.some(isNaN)) throw new Error(`${LABELS[key]}: все значения должны быть числами`);
    raw.push(arr);
  }

  const k = KEYS.length;
  const T = Math.min(...raw.map(s => s.length));
  if (T < k + 3) throw new Error(`Недостаточно наблюдений (нужно минимум ${k + 3}, есть ${T})`);

  const series = raw.map(s => s.slice(0, T));

  // 2. ADF-тест для каждого ряда
  const adfResults = [];
  for (let i = 0; i < k; i++) {
    const res = adfTest(series[i]);
    adfResults.push({ variable: i, ...res });
  }

  // 3. Z-нормализация
  const mu  = series.map(mean);
  const sig = series.map(s => { const sd = stdDev(s); return sd > 1e-10 ? sd : 1; });
  const zS  = series.map((s, i) => s.map(v => (v - mu[i]) / sig[i]));

  // 4. Выбор оптимального лага (1..4)
  const maxPossibleLag = Math.min(4, Math.floor((T - k - 1) / k));
  const { optimalLag, aicByLag } = maxPossibleLag >= 1
    ? selectVARLags(zS, maxPossibleLag)
    : { optimalLag: 1, aicByLag: {} };
  const lagOrder = optimalLag;

  // 5. Оценка VAR(lagOrder)
  const { Afull, A1, constants, rss, seMatrix, n: nObs } = estimateVARp(zS, lagOrder);

  // 6. Прогноз (денормализация)
  const zForecasts = forecastVARp(Afull, constants, zS, lagOrder, periods);
  const forecasts  = zForecasts.map(zv => zv.map((z, i) => round4(z * sig[i] + mu[i])));

  // 7. R² каждого уравнения
  const r2 = zS.map((s, i) => {
    const resp = s.slice(lagOrder);
    const mR   = mean(resp);
    const tss  = resp.reduce((a, v) => a + (v - mR) ** 2, 0);
    return tss > 0 ? round4(1 - rss[i] / tss) : 0;
  });

  // 8. IRF (использует A1 для совместимости)
  const irf = computeIRF(A1, Math.min(periods, 8));

  // 9. Тест Грейнджера (F-тест)
  const granger = grangerCausalityF(zS, lagOrder, rss, nObs);

  // 10. Интерпретация
  const interpretation = buildVARInterpretation(KEYS, LABELS, A1, granger, forecasts, series, periods, adfResults, lagOrder);

  // ADF tests keyed by variable name
  const adfTestsNamed = {};
  for (let i = 0; i < k; i++) adfTestsNamed[KEYS[i]] = { ...adfResults[i], label: LABELS[KEYS[i]] };

  return {
    keys:         KEYS,
    labels:       LABELS,
    historical:   series,
    forecasts,
    irf,
    granger,
    r2,
    coefficients: A1.map(row => row.map(round4)),
    constants:    constants.map(round4),
    interpretation,
    periods,
    lagOrder,
    optimalLag:   lagOrder,
    aicByLag,
    adfTests:     adfTestsNamed,
    dataSource,
    validation: { lagOrder, dataPoints: T, dataSource },
    meta: { dataSource, dataPoints: T, collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
  };
}

// ─── Экспорт ──────────────────────────────────────────────────────────────────

module.exports = {
  arima: autoArima,   // обратная совместимость — теперь autoArima
  autoArima,
  prophet,
  detectAnomalies,
  garch,
  var_model,
  backtestArima,
  ensembleForecast,
  adfTest,
  compareWithOfficial,
};
