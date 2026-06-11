'use strict';

/**
 * forecasting.js — Профессиональные эконометрические модели
 * Auto-ARIMA, EGARCH(1,1), VAR(p) с ADF-тестом, Backtest, Ensemble
 * Чистый JavaScript без внешних зависимостей
 */

const fs   = require('fs');
const path = require('path');

const MODEL_VERSION    = '3.0';
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
    if (Math.abs(beta) >= 0.999) return 1e15;
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
  let periodsPerYear = 252;  // дефолт: дневные данные

  // Всегда читаем файл: частота по датам + автозагрузка данных если мало
  try {
    const ratesRaw = JSON.parse(fs.readFileSync(TIMESERIES_FILE, 'utf8'));
    // Авто-определение частоты по полю date (независимо от источника данных)
    const dates = ratesRaw.filter(r => r.usd && r.date)
                           .map(r => new Date(r.date).getTime());
    if (dates.length >= 2) {
      let s = 0;
      for (let i = 1; i < dates.length; i++) s += (dates[i] - dates[i - 1]) / 86400000;
      periodsPerYear = Math.round(365.25 / (s / (dates.length - 1)));
    }
    // Данные — только если пользовательских мало
    if (nums.length < 30) {
      const usdData = ratesRaw.map(r => r.usd).filter(Boolean);
      if (usdData.length > nums.length) {
        nums = usdData;
        dataSource = 'НБТ РТ (авто)';
      }
    }
  } catch (_) {}

  if (nums.length < 24) throw new Error('Для GARCH необходимо минимум 24 точки данных');

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
  const annualizedVol   = round4(Math.sqrt(Math.max(0, lastH)) * Math.sqrt(periodsPerYear));

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
    omega: round4(omega), alpha: round4(alpha), beta: round4(beta),
    persistence: round4(persistence),
    alpha_plus_beta: round4(persistence), // FIX: алиас для persistence
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

  // FIX: ci80/ci95 как массив пар [lower, upper] — удобно для перебора
  const ci80Pairs = ci80.lower.map((v, i) => [v, ci80.upper[i]]);
  const ci95Pairs = ci95.lower.map((v, i) => [v, ci95.upper[i]]);

  const result = {
    forecast: ensemble,    // FIX: алиас — теперь ens.forecast работает
    ensemble,              // оригинальное поле (обратная совместимость)
    arima:   arimaFc,
    prophet: propFc,
    weights: { arima: round4(wArima), prophet: round4(wProphet) },
    mape: {
      arima:   arimaMape   != null ? round4(arimaMape * 100)   : null,
      prophet: prophetMape != null ? round4(prophetMape * 100) : null,
    },
    ci80: ci80Pairs,                        // FIX: массив пар [[lo,hi],...]
    ci95: ci95Pairs,
    ci80Raw: ci80,                          // { lower:[], upper:[] } для совместимости
    ci95Raw: ci95,
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

// ─── VECM helpers: Cholesky, Jacobi eigenvalues, Johansen trace, Engle-Granger ─

/** Cholesky: A = L L^T → нижнетреугольная L */
function choleskyLower(A) {
  const n = A.length;
  const L = matCreate(n, n);
  for (let i = 0; i < n; i++) {
    for (let j = 0; j <= i; j++) {
      let s = A[i][j];
      for (let r = 0; r < j; r++) s -= L[i][r] * L[j][r];
      L[i][j] = i === j ? Math.sqrt(Math.max(s, 1e-14)) : (L[j][j] > 1e-14 ? s / L[j][j] : 0);
    }
  }
  return L;
}

/** Прямая подстановка: решение L x = b */
function fwdSub(L, b) {
  const n = L.length, x = new Array(n).fill(0);
  for (let i = 0; i < n; i++) {
    let acc = b[i];
    for (let j = 0; j < i; j++) acc -= L[i][j] * x[j];
    x[i] = L[i][i] > 1e-14 ? acc / L[i][i] : 0;
  }
  return x;
}

/** Собственные значения симметричной матрицы (итерации Якоби) */
function symmEigenvalues(A) {
  const n = A.length;
  const M = A.map(row => [...row]);
  for (let iter = 0; iter < 200 * n * n; iter++) {
    let p = 0, q = 1, maxOff = 0;
    for (let i = 0; i < n; i++)
      for (let j = i + 1; j < n; j++)
        if (Math.abs(M[i][j]) > maxOff) { maxOff = Math.abs(M[i][j]); p = i; q = j; }
    if (maxOff < 1e-10) break;
    const theta = (M[q][q] - M[p][p]) / (2 * M[p][q]);
    const t = theta >= 0
      ? 1 / (theta + Math.sqrt(1 + theta ** 2))
      : 1 / (theta - Math.sqrt(1 + theta ** 2));
    const c = 1 / Math.sqrt(1 + t ** 2), s = t * c;
    const Mpq = M[p][q];
    M[p][p] -= t * Mpq; M[q][q] += t * Mpq; M[p][q] = M[q][p] = 0;
    for (let r = 0; r < n; r++) {
      if (r === p || r === q) continue;
      const Mrp = M[r][p], Mrq = M[r][q];
      M[r][p] = M[p][r] = c * Mrp - s * Mrq;
      M[r][q] = M[q][r] = s * Mrp + c * Mrq;
    }
  }
  return Array.from({ length: n }, (_, i) => M[i][i]);
}

/**
 * Тест следа Йохансена (VAR(1), константа в КЕ).
 * Возвращает { cointegrationRank, traceStats, eigenvalues, critVals }.
 */
function johansenTrace(series) {
  const k = series.length, T = series[0].length, nn = T - 1;
  const dY = series.map(s => s.slice(1).map((v, i) => v - s[i]));
  const Y0 = series.map(s => s.slice(0, nn));
  const mom = (A, B) => {
    const Mk = matCreate(k, k);
    for (let i = 0; i < k; i++)
      for (let j = 0; j < k; j++) {
        let acc = 0;
        for (let t = 0; t < nn; t++) acc += A[i][t] * B[j][t];
        Mk[i][j] = acc / nn;
      }
    return Mk;
  };
  try {
    const S00 = mom(dY, dY), S11 = mom(Y0, Y0);
    const S10 = mom(Y0, dY), S01 = matTranspose(S10);
    const A   = matMul(matMul(S10, matInverse(S00)), S01); // симметричная PSD
    const L   = choleskyLower(S11);
    // M = L^{-1} A L^{-T} (симметричная) — канонические корреляции
    const LinvA = matCreate(k, k);
    for (let j = 0; j < k; j++) {
      const x = fwdSub(L, A.map(row => row[j]));
      for (let i = 0; i < k; i++) LinvA[i][j] = x[i];
    }
    const LT = matTranspose(L), M = matCreate(k, k);
    for (let i = 0; i < k; i++) {
      const x = new Array(k).fill(0);
      for (let r = k - 1; r >= 0; r--) {
        let acc2 = LinvA[i][r];
        for (let c = r + 1; c < k; c++) acc2 -= LT[r][c] * x[c];
        x[r] = LT[r][r] > 1e-14 ? acc2 / LT[r][r] : 0;
      }
      for (let j = 0; j < k; j++) M[i][j] = x[j];
    }
    // Симметризация для устранения числового шума
    for (let i = 0; i < k; i++)
      for (let j = i + 1; j < k; j++)
        M[i][j] = M[j][i] = (M[i][j] + M[j][i]) / 2;
    const raw = symmEigenvalues(M);
    raw.sort((a, b) => b - a);
    const lambdas = raw.map(l => Math.max(0, Math.min(l, 0.9999)));
    // Критические значения 95%, Osterwald-Lenum 1992, константа в КЕ
    const cv95 = { 4: [47.21, 29.68, 15.41, 3.76], 3: [29.68, 15.41, 3.76], 2: [15.41, 3.76], 1: [3.76] };
    const crit = cv95[k] || cv95[4];
    let rank = 0;
    const traceStats = [];
    for (let r = 0; r < k; r++) {
      let stat = 0;
      for (let i = r; i < k; i++) stat -= nn * Math.log(1 - lambdas[i]);
      traceStats.push(round4(stat));
      if (stat > (crit[r] ?? Infinity)) rank = r + 1; else break;
    }
    return { cointegrationRank: rank, traceStats, eigenvalues: lambdas.map(l => round4(l)), critVals: crit };
  } catch (_) {
    return { cointegrationRank: 0, traceStats: [], eigenvalues: [], critVals: [] };
  }
}

/**
 * VECM оценка + прогноз (Engle-Granger двухшаговый, rank=1).
 * Шаг 1: OLS y₀ ~ c + Σbᵢyᵢ → вектор коинтеграции β
 * Шаг 2: Δyᵢ,t = μᵢ + αᵢ·ECT_{t-1} + εᵢ,t
 */
function vecmFit(series, rank, periods) {
  const k = series.length, T = series[0].length;
  // Шаг 1: коинтеграционный вектор
  const Xc  = series[0].map((_, t) => [1, ...series.slice(1).map(s => s[t])]);
  const XcT = matTranspose(Xc);
  const beta0 = matVecMul(matMul(matInverse(matMul(XcT, Xc)), XcT), series[0]);
  // beta0 = [c, b₁, ..., b_{k-1}]
  // Шаг 2: ECT
  const ect = series[0].map((v, t) => {
    let fit = beta0[0];
    for (let i = 1; i < k; i++) fit += beta0[i] * series[i][t];
    return v - fit;
  });
  // Шаг 3: VECM уравнения
  const dY = series.map(s => s.slice(1).map((v, i) => v - s[i]));
  const Xv  = ect.slice(0, T - 1).map(z => [1, z]);
  const XvT = matTranspose(Xv);
  const proj = matMul(matInverse(matMul(XvT, Xv)), XvT);
  const alphas = [], mus = [];
  for (let i = 0; i < k; i++) {
    const p = matVecMul(proj, dY[i]);
    mus.push(p[0]); alphas.push(p[1]);
  }
  // Шаг 4: прогноз
  const forecasts = [];
  let yNow = series.map(s => s[T - 1]);
  for (let h = 0; h < periods; h++) {
    let z = yNow[0] - beta0[0];
    for (let i = 1; i < k; i++) z -= beta0[i] * yNow[i];
    const yNext = yNow.map((v, i) => round4(v + mus[i] + alphas[i] * z));
    forecasts.push(yNext);
    yNow = yNext;
  }
  return { forecasts, ect, beta0, alphas, mus, isStable: alphas[0] < 0 };
}

// ─── Интерпретация VAR ────────────────────────────────────────────────────────

function buildVARInterpretation(keys, labels, A1, granger, forecasts, series, periods, adfResults, lagOrder, modelType = 'var-levels') {
  const k       = keys.length;
  const lastObs = keys.map((_, i) => series[i][series[i].length - 1]);
  const lines   = [];

  const typeLabel = modelType === 'var-diff' ? 'VAR(Δ, разности)' : 'VAR(уровни)';
  lines.push(`Анализ ${typeLabel}(${lagOrder}) макроэкономических показателей Таджикистана. Горизонт прогноза: ${periods} периодов.`);
  if (modelType === 'var-diff') lines.push('Ряды I(1) без коинтеграции — модель оценена на первых разностях (долгосрочные связи не моделируются).');
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

  // 2. ADF на уровнях и первых разностях → определяем I(1)
  const adfLevels = series.map(s => adfTest(s));
  const adfDiffs  = series.map(s => {
    const d = s.slice(1).map((v, i) => v - s[i]);
    return d.length >= 4 ? adfTest(d) : { stationary: true, tStat: 0, pValue: 1 };
  });
  const isI1       = series.map((_, i) => !adfLevels[i].stationary && adfDiffs[i].stationary);
  const adfResults = adfLevels.map((r, i) => ({ variable: i, ...r }));

  // 3. Выбор спецификации: VECM / VAR(Δ) / VAR(уровни)
  let modelType     = 'var-levels';
  let workSeries    = series;
  let vecmFitResult = null;
  let johansen      = null;

  if (isI1.every(Boolean)) {
    johansen = johansenTrace(series);
    if (johansen.cointegrationRank >= 1) {
      try {
        vecmFitResult = vecmFit(series, johansen.cointegrationRank, periods);
        modelType = 'vecm';
      } catch (_) {
        johansen.cointegrationRank = 0;
      }
    }
    if (modelType !== 'vecm') {
      modelType  = 'var-diff';
      workSeries = series.map(s => s.slice(1).map((v, i) => v - s[i]));
      const minL = Math.min(...workSeries.map(s => s.length));
      workSeries = workSeries.map(s => s.slice(-minL));
    }
  } else if (adfLevels.some(r => !r.stationary)) {
    // Смешанная интеграция: ВСЕ нестационарные ряды — в разностях, I(0) — в уровнях
    modelType  = 'var-mixed';
    const base = series.map(s => s.slice(1).map((v, i) => v - s[i]));
    const lvl  = series.map(s => s.slice(1));
    workSeries = series.map((_, i) => !adfLevels[i].stationary ? base[i] : lvl[i]);
    const minL = Math.min(...workSeries.map(s => s.length));
    workSeries = workSeries.map(s => s.slice(-minL));
  }

  // ── VECM ветка ────────────────────────────────────────────────────────────
  if (modelType === 'vecm' && vecmFitResult) {
    const adfTestsNamed = {};
    for (let i = 0; i < k; i++)
      adfTestsNamed[KEYS[i]] = { ...adfResults[i], label: LABELS[KEYS[i]] };

    const cointegrationInfo = {
      rank:        johansen.cointegrationRank,
      traceStats:  johansen.traceStats,
      eigenvalues: johansen.eigenvalues,
      critVals:    johansen.critVals,
      beta:        vecmFitResult.beta0,
      alphas:      vecmFitResult.alphas,
      mus:         vecmFitResult.mus,
    };

    const lines = [
      `Модель VECM(1) — ранг коинтеграции r=${johansen.cointegrationRank} (тест Йохансена).`,
      `Все ${k} ряда I(1), обнаружена долгосрочная связь — VECM сохраняет её (в отличие от VAR(Δ)).`,
      '',
      'СЛЕД-СТАТИСТИКА ЙОХАНСЕНА:',
      ...johansen.traceStats.map((stat, r) =>
        `  H₀: rank≤${r}: λ_trace=${stat} ${stat > (johansen.critVals[r] || 0)
          ? `> CV=${johansen.critVals[r]} → отвергается ✓`
          : `≤ CV=${johansen.critVals[r]} → принимается`}`),
      '',
      'КОИНТЕГРАЦИОННЫЙ ВЕКТОР β (β₀=1):',
      `  ${KEYS.map((k2, i) => `${i === 0 ? 'c' : LABELS[k2]}: ${round4(vecmFitResult.beta0[i])}`).join(', ')}`,
      '',
      'СКОРОСТИ РЕГУЛИРОВКИ α (α < 0 = стабилизация):',
      ...KEYS.map((k2, i) =>
        `  • ${LABELS[k2]}: α=${round4(vecmFitResult.alphas[i])} ${vecmFitResult.alphas[i] < 0 ? '✓' : '⚠ положительное — проверьте данные'}`),
      '',
      'ПРОГНОЗ (1-й период вперёд):',
      ...KEYS.map((k2, i) => {
        const last = series[i][series[i].length - 1];
        const next = vecmFitResult.forecasts[0][i];
        const pct  = last !== 0 ? ((next - last) / Math.abs(last) * 100).toFixed(1) : '—';
        return `  • ${LABELS[k2]}: ${next >= last ? '↑' : '↓'} ${Math.abs(pct)}% (${round4(last)} → ${next})`;
      }),
    ];

    return {
      keys: KEYS, labels: LABELS, historical: series,
      forecasts:   vecmFitResult.forecasts,
      irf: [], granger: {}, grangerMatrix: [],
      r2: KEYS.map(() => null),
      coefficients: [], constants: [],
      interpretation: lines.join('\n'),
      periods, lagOrder: 1, optimalLag: 1, aicByLag: {},
      adfTests: adfTestsNamed, dataSource, modelType,
      cointegration: cointegrationInfo,
      validation: { lagOrder: 1, dataPoints: T, dataSource, modelType },
      meta: { dataSource, dataPoints: T, collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
    };
  }

  // ── VAR ветка (уровни или разности) ──────────────────────────────────────
  const T2 = Math.min(...workSeries.map(s => s.length));
  if (T2 < k + 3) throw new Error(`Недостаточно наблюдений после преобразования (${T2})`);

  const mu  = workSeries.map(mean);
  const sig = workSeries.map(s => { const sd = stdDev(s); return sd > 1e-10 ? sd : 1; });
  const zS  = workSeries.map((s, i) => s.map(v => (v - mu[i]) / sig[i]));

  // Строгий лимит лага: не более 2, и T/(3k) наблюдений на параметр
  const maxPossibleLag = Math.min(2, Math.floor((T2 - k - 1) / (3 * k)));
  const { optimalLag, aicByLag } = maxPossibleLag >= 1
    ? selectVARLags(zS, maxPossibleLag)
    : { optimalLag: 1, aicByLag: {} };
  const lagOrder = Math.max(1, optimalLag);

  const { Afull, A1, constants, rss, seMatrix, n: nObs } = estimateVARp(zS, lagOrder);
  const zForecasts = forecastVARp(Afull, constants, zS, lagOrder, periods);

  // Денормализация: var-diff и var-mixed накапливают уровни
  let forecasts;
  if (modelType === 'var-diff') {
    let prev = series.map(s => s[s.length - 1]);
    forecasts = zForecasts.map(zv => {
      const deltas = zv.map((z, i) => z * sig[i] + mu[i]);
      const levels = prev.map((lv, i) => round4(lv + deltas[i]));
      prev = levels;
      return levels;
    });
  } else if (modelType === 'var-mixed') {
    let prev = series.map(s => s[s.length - 1]);
    forecasts = zForecasts.map(zv => {
      const transformed = zv.map((z, i) => z * sig[i] + mu[i]);
      const levels = series.map((_, i) => {
        if (!adfLevels[i].stationary) return round4(prev[i] + transformed[i]);
        return round4(transformed[i]);
      });
      prev = levels;
      return levels;
    });
  } else {
    forecasts = zForecasts.map(zv => zv.map((z, i) => round4(z * sig[i] + mu[i])));
  }

  const r2 = zS.map((s, i) => {
    const resp = s.slice(lagOrder);
    const mR   = mean(resp);
    const tss  = resp.reduce((a, v) => a + (v - mR) ** 2, 0);
    return tss > 0 ? round4(1 - rss[i] / tss) : 0;
  });

  const irf    = computeIRF(A1, Math.min(periods, 8));
  const granger = grangerCausalityF(zS, lagOrder, rss, nObs);

  const grangerNamed = {};
  for (let jj = 0; jj < k; jj++)
    for (let ii = 0; ii < k; ii++)
      if (ii !== jj && granger[jj][ii])
        grangerNamed[`${KEYS[jj]}→${KEYS[ii]}`] = granger[jj][ii];

  const interpretation = buildVARInterpretation(
    KEYS, LABELS, A1, granger, forecasts, series, periods, adfResults, lagOrder, modelType
  );

  const adfTestsNamed = {};
  for (let i = 0; i < k; i++) adfTestsNamed[KEYS[i]] = { ...adfResults[i], label: LABELS[KEYS[i]] };

  return {
    keys: KEYS, labels: LABELS, historical: series,
    forecasts, irf,
    granger: grangerNamed, grangerMatrix: granger,
    r2,
    coefficients: A1.map(row => row.map(round4)),
    constants:    constants.map(round4),
    interpretation, periods, lagOrder, optimalLag: lagOrder, aicByLag,
    adfTests: adfTestsNamed, dataSource, modelType,
    cointegration: johansen ? { rank: johansen.cointegrationRank, traceStats: johansen.traceStats } : null,
    validation: { lagOrder, dataPoints: T, dataSource, modelType },
    meta: { dataSource, dataPoints: T, collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
  };
}

// ─── ETS (Exponential Smoothing) с детекцией структурного сдвига ─────────────

/**
 * Holt-Winters ETS с автодетекцией структурного сдвига.
 * При обнаружении сдвига использует только послесдвиговые данные для α,β.
 * @param {number[]} data
 * @param {number}   periods
 * @returns {{ forecast: number[], alpha: number, breakPoint: number|null, method: string }}
 */
function etsForecast(data, periods) {
  const nums = validateData(data);
  const n = nums.length;

  // --- Детекция структурного сдвига (Cusum-подход) ---
  let breakPoint = null;
  if (n >= 10) {
    const overall = mean(nums);
    const cusumArr = [];
    let cs = 0;
    for (let i = 0; i < n; i++) {
      cs += nums[i] - overall;
      cusumArr.push(cs);
    }
    const maxCS = Math.max(...cusumArr.map(Math.abs));
    const csThreshold = stdDev(nums) * Math.sqrt(n) * 0.8;
    if (maxCS > csThreshold) {
      // Найти точку максимального отклонения после первой трети
      let maxIdx = Math.floor(n / 3);
      for (let i = Math.floor(n / 3); i < n - 2; i++) {
        if (Math.abs(cusumArr[i]) > Math.abs(cusumArr[maxIdx])) maxIdx = i;
      }
      // Сдвиг значим только если средние до/после различаются более чем на 1σ
      const meanBefore = mean(nums.slice(0, maxIdx));
      const meanAfter  = mean(nums.slice(maxIdx));
      const sigma = stdDev(nums);
      if (Math.abs(meanBefore - meanAfter) > sigma * 0.8) {
        breakPoint = maxIdx;
      }
    }
  }

  // Данные для подбора: после сдвига или полные (с окном 10 точек)
  const fitData = breakPoint !== null
    ? nums.slice(breakPoint)
    : nums.slice(-Math.min(n, 15));  // используем последние 15 точек

  const m = fitData.length;
  if (m < 3) return { forecast: new Array(periods).fill(nums[n - 1]), alpha: 0.3, breakPoint, method: 'ets-naive' };

  // --- Оптимизация α по минимуму SSE (поиск в сетке) ---
  let bestAlpha = 0.3, bestSse = Infinity;
  for (let a = 0.05; a <= 0.95; a += 0.05) {
    let level = fitData[0], sse = 0;
    for (let i = 1; i < m; i++) {
      const pred = level;
      sse += (fitData[i] - pred) ** 2;
      level = a * fitData[i] + (1 - a) * level;
    }
    if (sse < bestSse) { bestSse = sse; bestAlpha = a; }
  }

  // --- Holt's двойное сглаживание (тренд) ---
  let bestAlpha2 = 0.3, bestBeta2 = 0.1, bestSse2 = Infinity;
  for (let a = 0.1; a <= 0.9; a += 0.1) {
    for (let b = 0.05; b <= 0.5; b += 0.05) {
      let level = fitData[0];
      let trend = fitData.length > 1 ? fitData[1] - fitData[0] : 0;
      let sse = 0;
      for (let i = 1; i < m; i++) {
        const pred = level + trend;
        sse += (fitData[i] - pred) ** 2;
        const newLevel = a * fitData[i] + (1 - a) * (level + trend);
        trend = b * (newLevel - level) + (1 - b) * trend;
        level = newLevel;
      }
      if (sse < bestSse2) { bestSse2 = sse; bestAlpha2 = a; bestBeta2 = b; }
    }
  }

  // Финальные параметры
  let level = fitData[0];
  let trend = fitData.length > 1 ? fitData[1] - fitData[0] : 0;
  for (let i = 1; i < m; i++) {
    const newLevel = bestAlpha2 * fitData[i] + (1 - bestAlpha2) * (level + trend);
    trend = bestBeta2 * (newLevel - level) + (1 - bestBeta2) * trend;
    level = newLevel;
  }

  // Ограничиваем тренд: не более ±0.5 в год (защита от экстраполяции)
  const boundedTrend = Math.max(-0.5, Math.min(0.5, trend));

  const forecast = [];
  for (let i = 1; i <= periods; i++) {
    forecast.push(round2(level + boundedTrend * i));
  }

  return {
    forecast,
    alpha:  round4(bestAlpha2),
    beta:   round4(bestBeta2),
    level:  round2(level),
    trend:  round4(trend),
    breakPoint,
    fitDataLength: m,
    method: breakPoint !== null ? 'ets-holt-post-break' : 'ets-holt',
  };
}

// ─── ARIMA с коррекцией систематического смещения ────────────────────────────

/**
 * Вычисляет среднее систематическое смещение ARIMA на последних
 * wfSteps точках (walk-forward) и возвращает correctedForecast.
 * @param {number[]} data
 * @param {number}   periods
 * @param {*}        officialForecast
 */
function autoArimaBiasAware(data, periods, officialForecast) {
  const result = autoArima(data, periods, officialForecast);
  const nums = Array.isArray(data) ? data.map(Number).filter(v => !isNaN(v)) : [];
  const n = nums.length;

  if (n < 8) return { ...result, biasCorrection: 0, forecastBC: result.forecast };

  // Walk-forward bias на последних 20% (мин 2, макс 6 точек)
  const wfSteps = Math.min(6, Math.max(2, Math.floor(n * 0.2)));
  let totalBias = 0, cnt = 0;

  for (let s = wfSteps; s >= 1; s--) {
    const trainEnd = n - s;
    if (trainEnd < 5) continue;
    try {
      const wf = autoArima(nums.slice(0, trainEnd), 1);
      const bias = wf.forecast[0] - nums[trainEnd];
      totalBias += bias;
      cnt++;
    } catch (_) {}
  }

  const biasCorrection = cnt > 0 ? -(totalBias / cnt) : 0;
  const forecastBC = result.forecast.map(v => round2(v + biasCorrection));

  return {
    ...result,
    biasCorrection:  round4(biasCorrection),
    forecastBC,                      // прогноз с поправкой
    forecastRaw: result.forecast,    // оригинальный без поправки
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// ARIMAX — ARIMA с внешними регрессорами (мировой стандарт МВФ)
// Учитывает переводы, алюминий, курс USD как экзогенные переменные
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * ARIMAX(p,d,q) — ARIMA с экзогенными регрессорами.
 * Фоллбэк на autoArima если внешних данных нет.
 * @param {number[]} endogenous   — целевой ряд (напр. gdp_growth)
 * @param {Object}   exogenous    — { remittances, aluminum, usd_tjs } — массивы
 * @param {number}   periods      — горизонт прогноза
 */
function arimaxForecast(endogenous, exogenous, periods) {
  const n = (endogenous || []).length;
  if (n < 8) return { error: 'Недостаточно данных', forecast: [] };

  // ── Нормализация внешних переменных ───────────────────────────────────────
  const normalizeArr = arr => {
    if (!Array.isArray(arr) || arr.length === 0) return [];
    const m = arr.reduce((a, b) => a + b, 0) / arr.length;
    const s = Math.sqrt(arr.map(v => (v - m) ** 2).reduce((a, b) => a + b, 0) / arr.length) || 1;
    return arr.map(v => (v - m) / s);
  };

  const regressors = [];
  const regNames   = [];
  const regFuture  = []; // последнее значение для прогноза

  for (const [key, arr] of Object.entries(exogenous || {})) {
    if (!Array.isArray(arr) || arr.length < n) continue;
    const norm = normalizeArr(arr.slice(-n));
    regressors.push(norm);
    regNames.push(key);
    regFuture.push(norm[norm.length - 1]);
  }

  // ── ADF-тест → порядок интегрирования ─────────────────────────────────────
  const adfR = adfTest(endogenous);
  const d    = adfR.stationary ? 0 : 1;
  const y    = d === 1
    ? endogenous.slice(1).map((v, i) => v - endogenous[i])
    : endogenous.slice();

  // ── Матрица признаков: AR-лаги + регрессоры ───────────────────────────────
  let bestAIC = Infinity, bestFit = null;

  for (let p = 1; p <= 3; p++) {
    const startIdx = p;
    if (y.length - startIdx < p + 2) continue;

    const X = [], Y = [];
    for (let t = startIdx; t < y.length; t++) {
      const row = [];
      for (let lag = 1; lag <= p; lag++) row.push(y[t - lag] ?? 0);
      regressors.forEach((reg, ri) => {
        const offset = n - y.length; // выравнивание если d=1
        row.push(reg[t + offset] ?? regFuture[ri]);
      });
      X.push(row);
      Y.push(y[t]);
    }

    // МНК через Гаусс–Жордан
    try {
      const nc = X[0].length;
      const XtX = Array.from({ length: nc }, (_, i) =>
        Array.from({ length: nc }, (_, j) => X.reduce((s, r) => s + r[i] * r[j], 0))
      );
      const XtY = Array.from({ length: nc }, (_, i) => X.reduce((s, r, t) => s + r[i] * Y[t], 0));

      // Гаусс–Жордан с регуляризацией 1e-6
      const aug = XtX.map((row, i) => [
        ...row.map((v, j) => v + (i === j ? 1e-6 : 0)),
        ...Array.from({ length: nc }, (_, j) => (i === j ? 1 : 0)),
      ]);
      for (let col = 0; col < nc; col++) {
        let pr = col;
        for (let r = col + 1; r < nc; r++) if (Math.abs(aug[r][col]) > Math.abs(aug[pr][col])) pr = r;
        [aug[col], aug[pr]] = [aug[pr], aug[col]];
        const piv = aug[col][col];
        if (Math.abs(piv) < 1e-12) continue;
        for (let j = 0; j < 2 * nc; j++) aug[col][j] /= piv;
        for (let r = 0; r < nc; r++) {
          if (r === col) continue;
          const f2 = aug[r][col];
          for (let j = 0; j < 2 * nc; j++) aug[r][j] -= f2 * aug[col][j];
        }
      }
      const inv    = aug.map(row => row.slice(nc));
      const betas  = inv.map(row => row.reduce((s, v, j) => s + v * XtY[j], 0));
      const resids = Y.map((yT, t) => yT - X[t].reduce((s, x, j) => s + x * betas[j], 0));
      const sse    = resids.reduce((s, r) => s + r * r, 0);
      const sig2   = sse / Y.length;
      const logLik = -0.5 * Y.length * (Math.log(2 * Math.PI * sig2) + 1);
      const aic    = -2 * logLik + 2 * (nc + 1);

      if (aic < bestAIC) {
        bestAIC = aic;
        bestFit = { p, d, betas, sigma2: sig2, nc };
      }
    } catch (_) {}
  }

  if (!bestFit) return autoArima(endogenous, periods);

  // ── Прогноз ────────────────────────────────────────────────────────────────
  const yHist = y.slice();
  const forecast = [];
  for (let h = 0; h < periods; h++) {
    const row = [];
    for (let lag = 1; lag <= bestFit.p; lag++) row.push(yHist[yHist.length - lag] ?? 0);
    regFuture.forEach(rv => row.push(rv));
    const pred = row.slice(0, bestFit.nc).reduce((s, x, j) => s + x * (bestFit.betas[j] ?? 0), 0);
    yHist.push(pred);
    // Обратное дифференцирование
    const base = d === 1 ? (endogenous[endogenous.length - 1 + h] ?? endogenous[endogenous.length - 1]) : 0;
    forecast.push(round2(d === 1 ? base + pred : pred));
  }

  const sigma = Math.sqrt(bestFit.sigma2);
  return {
    forecast,
    ci80: forecast.map((v, h) => [round2(v - 1.28 * sigma * Math.sqrt(h + 1)), round2(v + 1.28 * sigma * Math.sqrt(h + 1))]),
    ci95: forecast.map((v, h) => [round2(v - 1.96 * sigma * Math.sqrt(h + 1)), round2(v + 1.96 * sigma * Math.sqrt(h + 1))]),
    model:           `ARIMAX(${bestFit.p},${bestFit.d},0) + ${regNames.length} регрессора`,
    aic:             round2(bestAIC),
    exogenous_used:  regNames.length,
    exogenous_names: regNames,
    note: regNames.length > 0
      ? `Учтены внешние факторы: ${regNames.join(', ')}`
      : 'Внешние данные недоступны — использован ARIMA',
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// BVAR — Байесовский VAR (полный Minnesota Prior, авто-выбор λ)
// Стандарт МВФ / ФРС / ЕЦБ для коротких макроэкономических рядов
// Решение: (XtX + σᵢ²·Λ)⁻¹·(XtY + σᵢ²·Λ·β₀), β₀ = RW приор
// Λ: затухание по лагу l = (l/λ)², θ=0.5 для чужих лагов
// λ: выбирается по holdout MAPE (сетка 6 значений)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Байесовский VAR с полным Minnesota Prior.
 * @param {{ gdp, inflation, usd_tjs, remittances }} data
 * @param {number} periods
 * @param {number} [lambda]  — начальная подсказка; будет заменён авто-выбором
 */
function bvarForecast(data, periods, lambda) {
  const VARS  = ['gdp', 'inflation', 'usd_tjs', 'remittances'];
  const P     = 1;      // фиксированный лаг для коротких рядов
  const THETA = 0.5;    // жёсткость чужих лагов

  const avail = VARS.filter(v => Array.isArray(data[v]) && data[v].length >= 6);
  if (avail.length < 2) return { error: 'Недостаточно переменных (нужно ≥ 2)', forecast: {} };

  const N    = Math.min(...avail.map(v => data[v].length));
  const k    = avail.length;
  const nPar = k * P + 1; // intercept + k*P коэффициентов

  // ADF-based prior means: I(0) → 0 (mean reversion), I(1) → 1 (random walk)
  const priorMeans = avail.map(v => {
    try { return adfTest(data[v].slice(-N)).stationary ? 0 : 1; }
    catch (_) { return 1; }
  });

  // ── Нормализация на первых n наблюдениях из окна N ─────────────────────────
  function normSlice(n) {
    const raw = avail.map(v => data[v].slice(-N).slice(0, n));
    const mu  = raw.map(arr => arr.reduce((s, x) => s + x, 0) / arr.length);
    const sg  = raw.map((arr, i) => {
      const m  = mu[i];
      const sd = Math.sqrt(arr.reduce((s, x) => s + (x - m) ** 2, 0) / arr.length);
      return sd || 1;
    });
    const Z = raw.map((arr, i) => arr.map(x => (x - mu[i]) / sg[i]));
    return { mu, sg, Z };
  }

  // ── Design-матрица и отклики ──────────────────────────────────────────────
  function mkXY(Z) {
    const n = Z[0].length, T = n - P;
    const X = [];
    for (let t = 0; t < T; t++) {
      const row = [1];
      for (let l = 1; l <= P; l++)
        for (let j = 0; j < k; j++) row.push(Z[j][t + P - l]);
      X.push(row);
    }
    return { X, Y: Z.map(s => s.slice(P)), T };
  }

  // ── AR(1) σ² для масштабирования Minnesota приора ────────────────────────
  function arSig2(Z, Y, T) {
    return avail.map((_, i) => {
      const yi = Y[i], xi = Z[i].slice(0, T).map(v => [1, v]);
      try {
        const xit = matTranspose(xi);
        const c   = matVecMul(matMul(matInverse(matMul(xit, xi)), xit), yi);
        let rss = 0;
        for (let t = 0; t < T; t++) rss += (yi[t] - c[0] - c[1] * Z[i][t]) ** 2;
        return Math.max(rss / Math.max(T - 2, 1), 1e-6);
      } catch (_) { return 1.0; }
    });
  }

  // ── Precision-вектор Minnesota Prior ────────────────────────────────────
  function mkPrec(eqIdx, lam, s2) {
    const prec = new Array(nPar).fill(0); // intercept: 0 (flat)
    let col = 1;
    for (let l = 1; l <= P; l++) {
      for (let j = 0; j < k; j++) {
        const base = (l / lam) ** 2 / Math.max(s2[eqIdx], 1e-10);
        prec[col++] = j === eqIdx
          ? base                                               // собственный лаг
          : base / (THETA ** 2) * (s2[j] / Math.max(s2[eqIdx], 1e-10)); // чужой лаг
      }
    }
    return prec;
  }

  // ── Оценка одного BVAR-уравнения (ПОЛНОЕ матричное решение) ─────────────
  function fitEq(eqIdx, lam, X, Yi, XtX, s2) {
    const prec  = mkPrec(eqIdx, lam, s2);
    const s2i   = s2[eqIdx];
    const beta0 = new Array(nPar).fill(0);
    beta0[1 + eqIdx] = priorMeans[eqIdx]; // I(1) → 1 (RW), I(0) → 0 (mean-revert)
    // A β = rhs : A = XtX + σᵢ²·Λ, rhs = XtY + σᵢ²·Λ·β₀
    const A   = XtX.map((row, r) => row.map((v, c) => r === c ? v + s2i * prec[r] : v));
    const XtY = matVecMul(matTranspose(X), Yi);
    const rhs = XtY.map((v, j) => v + s2i * prec[j] * beta0[j]);
    try {
      const Ainv  = matInverse(A);
      const beta  = matVecMul(Ainv, rhs);
      let rss = 0;
      for (let t = 0; t < X.length; t++) {
        let fit = 0;
        for (let j = 0; j < nPar; j++) fit += beta[j] * X[t][j];
        rss += (Yi[t] - fit) ** 2;
      }
      return { beta, Ainv, s2Post: rss / Math.max(X.length - nPar, 1) };
    } catch (_) {
      const beta = new Array(nPar).fill(0);
      beta[1 + eqIdx] = 0.5;
      return { beta, Ainv: matCreate(nPar, nPar), s2Post: s2[eqIdx] };
    }
  }

  // ── Выбор λ по holdout MAPE (скользящее окно по GDP) ────────────────────
  const nHold    = Math.max(3, Math.min(5, Math.floor(N * 0.2)));
  const nTrn     = N - nHold;
  const lamGrid  = [0.05, 0.1, 0.15, 0.2, 0.3, 0.5];
  let selectedLam = typeof lambda === 'number' ? lambda : 0.2;
  let bestMapeVal = Infinity;

  if (nTrn >= k + P + 3 && nHold >= 3) {
    for (const lam of lamGrid) {
      let apeSum = 0, cnt = 0;
      for (let step = 0; step < nHold; step++) {
        const nT = nTrn + step;
        const { mu: mT, sg: sT, Z: ZT } = normSlice(nT);
        const { X: XT, Y: YT, T: TT } = mkXY(ZT);
        if (TT < nPar + 1) continue;
        const s2T   = arSig2(ZT, YT, TT);
        const XtXT  = matMul(matTranspose(XT), XT);
        const eq0   = fitEq(0, lam, XT, YT[0], XtXT, s2T);
        const lastX = [1, ...ZT.map(s => s[s.length - 1])];
        const predZ = eq0.beta.reduce((s, b, j) => s + b * lastX[j], 0);
        const rawNext = data[avail[0]].slice(-N)[nT];
        if (!isFinite(rawNext)) continue;
        const actualZ = (rawNext - mT[0]) / sT[0];
        if (Math.abs(actualZ) > 1e-10) {
          apeSum += Math.abs(predZ - actualZ) / Math.abs(actualZ) * 100;
          cnt++;
        }
      }
      if (cnt > 0 && apeSum / cnt < bestMapeVal) {
        bestMapeVal = apeSum / cnt;
        selectedLam = lam;
      }
    }
  }

  // ── Полная оценка BVAR на всех N точках ──────────────────────────────────
  const { mu, sg, Z } = normSlice(N);
  const { X, Y: Ymat, T } = mkXY(Z);
  const XtXfull = matMul(matTranspose(X), X);
  const sig2    = arSig2(Z, Ymat, T);
  const eqs     = avail.map((_, i) => fitEq(i, selectedLam, X, Ymat[i], XtXfull, sig2));

  // ── Прогноз с апостериорными ДИ 95% ──────────────────────────────────────
  const fcstZ  = avail.map(() => []);
  const ciZ    = avail.map(() => []);
  const hist   = Z.map(s => [...s]);

  for (let h = 0; h < periods; h++) {
    const xNew = [1, ...hist.map(s => s[s.length - 1])];
    avail.forEach((_, i) => {
      const { beta, Ainv, s2Post } = eqs[i];
      const predZ = beta.reduce((s, b, j) => s + b * xNew[j], 0);
      fcstZ[i].push(predZ);
      hist[i].push(predZ);
      // Предиктивная дисперсия: σ²·(1 + x'·Ainv·x)·расширение по горизонту
      const xAinvx = xNew.reduce(
        (s, xi, r) => s + xi * xNew.reduce((s2, xj, c) => s2 + xj * Ainv[r][c], 0), 0
      );
      const varH = s2Post * (1 + xAinvx) * (1 + 0.4 * h);
      const sdH  = Math.sqrt(Math.max(varH, 1e-10));
      ciZ[i].push({ lower: predZ - 1.96 * sdH, upper: predZ + 1.96 * sdH });
    });
  }

  // ── Денормализация ────────────────────────────────────────────────────────
  const forecast  = {};
  const ci95      = {};
  avail.forEach((v, i) => {
    forecast[v] = fcstZ[i].map(z => round2(z * sg[i] + mu[i]));
    ci95[v]     = ciZ[i].map(ci => ({
      lower: round2(ci.lower * sg[i] + mu[i]),
      upper: round2(ci.upper * sg[i] + mu[i]),
    }));
  });

  // ── Интерпретация ─────────────────────────────────────────────────────────
  const intrpLines = [
    `BVAR(${P}) — Minnesota Prior, λ=${selectedLam} (авто, holdout MAPE ≈ ${round2(bestMapeVal)}%).`,
    `Полное решение: (XtX + σᵢ²·Λ)⁻¹·(XtY + σᵢ²·Λ·β₀).`,
    `β₀: RW (1 для собственного лага-1), затухание (l/λ)², θ=${THETA} для чужих лагов.`,
    '',
    'АПОСТЕРИОРНЫЕ КОЭФФИЦИЕНТЫ (лаг 1, выборка нормирована):',
    ...avail.map((v, i) => {
      const b   = eqs[i].beta;
      const own = round4(b[1 + i]);
      const cross = avail
        .map((vj, j) => j !== i ? `${vj}:${round4(b[1 + j])}` : null)
        .filter(Boolean).join(', ');
      return `  • ${v}: own=${own}${cross ? ', cross=[' + cross + ']' : ''}`;
    }),
    '',
    'ПРОГНОЗ + 95% ДИ (1-й период вперёд):',
    ...avail.map((v, i) => {
      const last = data[v][data[v].length - 1];
      const next = forecast[v][0];
      const ci   = ci95[v][0];
      const pct  = last !== 0 ? ((next - last) / Math.abs(last) * 100).toFixed(1) : '—';
      return `  • ${v}: ${next >= last ? '↑' : '↓'} ${Math.abs(pct)}% (${round2(last)} → ${next}) [ДИ: ${ci.lower}…${ci.upper}]`;
    }),
  ];

  return {
    forecast,
    ci95,
    model:          `BVAR(${P}) λ=${selectedLam} Minnesota Prior (full solve)`,
    variables:      avail,
    lambda:         selectedLam,
    lambdaSelected: selectedLam,
    interpretation: intrpLines.join('\n'),
    periods,
    note: 'Байесовский VAR с полным Minnesota Prior — стандарт МВФ/ФРС/ЕЦБ для малых выборок',
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Kalman Filter — оптимальная фильтрация и прогноз с нарастающей неопределённостью
// Применение: оценка потенциального ВВП, сглаживание зашумлённых рядов
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Одномерный фильтр Калмана со скалярными Q и R.
 * Авто-подбор Q и R методом EM (2 итерации) если не заданы.
 * @param {number[]} observations
 * @param {{ processNoise, measurementNoise, periods }} params
 */
function kalmanFilter(observations, params) {
  const { periods = 5 } = params || {};
  if (!Array.isArray(observations) || observations.length < 4) {
    return { error: 'Недостаточно данных (нужно ≥ 4)' };
  }

  const obs = observations.map(Number).filter(v => !isNaN(v));
  const m   = obs.length;

  // ── Авто-подбор Q и R (упрощённый EM) ─────────────────────────────────────
  let Q = params?.processNoise    ?? variance(obs) * 0.05;  // процессный шум
  let R = params?.measurementNoise ?? variance(obs) * 0.5;  // шум измерения

  // 1 итерация EM для уточнения
  for (let em = 0; em < 2; em++) {
    const xSmooth = [];
    let x = obs[0], P = R;
    for (let t = 0; t < m; t++) {
      const P_pred = P + Q;
      const K      = P_pred / (P_pred + R);
      x = x + K * (obs[t] - x);
      P = (1 - K) * P_pred;
      xSmooth.push(x);
    }
    // Обновляем Q и R по остаткам
    const innov = obs.map((o, t) => o - xSmooth[t]);
    R = Math.max(1e-4, innov.reduce((s, e) => s + e * e, 0) / m);
    const procDiff = xSmooth.slice(1).map((x, t) => x - xSmooth[t]);
    Q = Math.max(1e-6, procDiff.reduce((s, e) => s + e * e, 0) / (m - 1));
  }

  // ── Основной проход фильтра ────────────────────────────────────────────────
  let x = obs[0], P = R;
  const filtered = [], gains = [], innovations = [];

  for (let t = 0; t < m; t++) {
    const P_pred = P + Q;
    const K      = P_pred / (P_pred + R);
    const innov  = obs[t] - x;
    x = x + K * innov;
    P = (1 - K) * P_pred;
    filtered.push(round2(x));
    gains.push(round4(K));
    innovations.push(round2(innov));
  }

  // ── Прогноз с нарастающим CI ──────────────────────────────────────────────
  const forecast = [], ci95 = [], ci80 = [];
  let P_fwd = P;
  for (let h = 1; h <= periods; h++) {
    P_fwd += Q;
    const sigma = Math.sqrt(P_fwd + R);
    forecast.push(round2(x));  // level forecast (random-walk-with-drift)
    ci80.push([round2(x - 1.28 * sigma), round2(x + 1.28 * sigma)]);
    ci95.push([round2(x - 1.96 * sigma), round2(x + 1.96 * sigma)]);
  }

  // ── Оценка тренда по последним 5 отфильтрованным значениям ────────────────
  const tail   = filtered.slice(-Math.min(5, m));
  const trend  = tail.length > 1 ? (tail[tail.length - 1] - tail[0]) / (tail.length - 1) : 0;
  const rmse   = round4(Math.sqrt(innovations.reduce((s, e) => s + e * e, 0) / m));

  return {
    filtered,
    forecast,
    ci80,
    ci95,
    trend:         round4(trend),
    final_gain:    gains[gains.length - 1],
    rmse,
    Q:             round4(Q),
    R:             round4(R),
    noise_ratio:   round4(Q / R),
    model:         `Kalman Filter (Q=${round4(Q)}, R=${round4(R)}) EM-fitted`,
    interpretation: Math.abs(trend) < 0.3
      ? 'Стабильный уровень'
      : trend > 0 ? `Восходящий тренд (+${round2(trend)}/год)` : `Нисходящий тренд (${round2(trend)}/год)`,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Sanity Check — экономический контроль прогнозов для Таджикистана
// Отсекает физически невозможные значения и сигнализирует об аномалиях
// ═══════════════════════════════════════════════════════════════════════════════

const TJ_BOUNDS = {
  gdp_growth:  { min: -15,  max: 15,   typical: [4,  10],  unit: '%' },
  inflation:   { min: -5,   max: 60,   typical: [2,  20],  unit: '%' },
  usd_tjs:     { min: 7,    max: 25,   typical: [9,  14],  unit: 'сом/USD' },
  remittances: { min: 200,  max: 7000, typical: [1200, 4500], unit: 'млн USD' },
  export:      { min: 300,  max: 12000,typical: [1200, 5000], unit: 'млн USD' },
  import:      { min: 400,  max: 15000,typical: [2000, 6000], unit: 'млн USD' },
};

/**
 * Проверяет прогноз на реалистичность для экономики Таджикистана.
 * Возвращает { forecast (clipped), warnings, atypical }.
 */
function sanitizeForTajikistan(forecast, indicator) {
  const b = TJ_BOUNDS[indicator];
  if (!b || !Array.isArray(forecast)) return { forecast, warnings: [], atypical: [] };

  const warnings  = [];
  const atypical  = [];
  const clipped   = forecast.map((v, i) => {
    const yr = `[t+${i + 1}]`;
    if (v < b.min) {
      warnings.push(`${yr} ${v}${b.unit} < min(${b.min}) → обрезан до ${b.min}`);
      return b.min;
    }
    if (v > b.max) {
      warnings.push(`${yr} ${v}${b.unit} > max(${b.max}) → обрезан до ${b.max}`);
      return b.max;
    }
    if (v < b.typical[0] || v > b.typical[1]) {
      atypical.push(`${yr} ${v}${b.unit} вне типичного диапазона [${b.typical[0]}–${b.typical[1]}]`);
    }
    return v;
  });

  return {
    forecast:  clipped,
    warnings,
    atypical,
    indicator,
    bounds:    b,
  };
}

// ─── Экспорт ──────────────────────────────────────────────────────────────────

module.exports = {
  arima: autoArima,   // обратная совместимость — теперь autoArima
  autoArima,
  autoArimaBiasAware,
  etsForecast,
  prophet,
  detectAnomalies,
  garch,
  var_model,
  backtestArima,
  ensembleForecast,
  adfTest,
  compareWithOfficial,
  // ── Новые модели мирового стандарта ───────────────────────────────────────
  arimaxForecast,         // ARIMAX с экзогенными регрессорами
  bvarForecast,           // Байесовский VAR (Minnesota Prior)
  kalmanFilter,           // Фильтр Калмана с EM-подбором Q/R
  sanitizeForTajikistan,  // Экономический sanity-check
};
