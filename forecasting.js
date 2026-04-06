/**
 * forecasting.js — ARIMA и Prophet-подобные модели прогнозирования
 * Использует ml-regression и simple-statistics
 */

const ss = require('simple-statistics');
const { SimpleLinearRegression } = require('ml-regression');

// ─── Вспомогательные функции ──────────────────────────────────────────────────

function round2(v) {
  return Math.round(v * 100) / 100;
}

function validateData(data) {
  if (!Array.isArray(data) || data.length < 4) {
    throw new Error('Необходимо минимум 4 точки данных');
  }
  const nums = data.map(Number);
  if (nums.some(isNaN)) {
    throw new Error('Все значения должны быть числами');
  }
  return nums;
}

// ─── ARIMA (1,1,0) ────────────────────────────────────────────────────────────
// Алгоритм: первое разностное преобразование + авторегрессия AR(1)

function arima(data, periods) {
  const nums = validateData(data);

  // Шаг 1: первое разностное преобразование (I(1))
  const diff = [];
  for (let i = 1; i < nums.length; i++) {
    diff.push(nums[i] - nums[i - 1]);
  }

  // Шаг 2: AR(1) на разностном ряду
  // y_t = c + φ·y_{t-1}
  let phi = 0;
  let intercept = 0;

  if (diff.length >= 2) {
    const xVals = diff.slice(0, -1);
    const yVals = diff.slice(1);
    try {
      const reg = new SimpleLinearRegression(xVals, yVals);
      phi = reg.slope;
      intercept = reg.intercept;
    } catch {
      // fallback: бесконечно-периодическая модель
      phi = ss.sampleCorrelation(diff.slice(0, -1), diff.slice(1));
      intercept = ss.mean(diff) * (1 - phi);
    }
  } else {
    intercept = ss.mean(diff);
  }

  // Ограничиваем phi для стационарности
  phi = Math.max(-0.99, Math.min(0.99, phi));

  // Шаг 3: прогноз разностного ряда
  const forecastDiff = [];
  let lastDiff = diff[diff.length - 1];
  for (let i = 0; i < periods; i++) {
    const next = round2(intercept + phi * lastDiff);
    forecastDiff.push(next);
    lastDiff = next;
  }

  // Шаг 4: обратное разностное преобразование
  const forecast = [];
  let prev = nums[nums.length - 1];
  for (const d of forecastDiff) {
    prev = round2(prev + d);
    forecast.push(prev);
  }

  return forecast;
}

// ─── Prophet-подобная модель ──────────────────────────────────────────────────
// Алгоритм: линейный тренд + сезонная компонента (скользящее среднее по периоду)

function prophet(data, periods) {
  const nums = validateData(data);
  const n = nums.length;

  // Шаг 1: линейный тренд
  const xs = Array.from({ length: n }, (_, i) => i);
  const reg = new SimpleLinearRegression(xs, nums);

  // Шаг 2: остатки (сезонность)
  const trendLine = xs.map(x => reg.predict(x));
  const residuals = nums.map((v, i) => v - trendLine[i]);

  // Шаг 3: период сезонности (авто-определение, min 3, max 12)
  const period = Math.min(12, Math.max(3, Math.floor(n / 2)));
  const seasonSum = new Array(period).fill(0);
  const seasonCount = new Array(period).fill(0);
  residuals.forEach((r, i) => {
    const idx = i % period;
    seasonSum[idx] += r;
    seasonCount[idx]++;
  });
  const seasonal = seasonSum.map((s, i) =>
    seasonCount[i] > 0 ? s / seasonCount[i] : 0
  );

  // Нормализуем сезонность (сумма = 0)
  const seasonMean = ss.mean(seasonal);
  const seasonAdj = seasonal.map(s => s - seasonMean);

  // Шаг 4: прогноз = тренд + сезонность
  const forecast = [];
  for (let i = 0; i < periods; i++) {
    const t = n + i;
    const trend = reg.predict(t);
    const season = seasonAdj[t % period];
    forecast.push(round2(trend + season));
  }

  return forecast;
}

// ─── Обнаружение аномалий (метод Z-score + IQR) ───────────────────────────────

function detectAnomalies(data) {
  const nums = validateData(data);
  const m = ss.mean(nums);
  const s = ss.standardDeviation(nums);
  const q1 = ss.quantile(nums, 0.25);
  const q3 = ss.quantile(nums, 0.75);
  const iqr = q3 - q1;
  const iqrLow = q1 - 1.5 * iqr;
  const iqrHigh = q3 + 1.5 * iqr;

  const anomalies = [];
  nums.forEach((v, i) => {
    const z = s !== 0 ? Math.abs(v - m) / s : 0;
    const isZscore = z > 2;
    const isIQR = v < iqrLow || v > iqrHigh;

    if (isZscore || isIQR) {
      anomalies.push({
        index: i,
        value: v,
        zscore: round2(z),
        direction: v > m ? 'high' : 'low',
        method: isZscore && isIQR ? 'Z-score + IQR' : isZscore ? 'Z-score' : 'IQR',
      });
    }
  });

  return anomalies;
}

module.exports = { arima, prophet, detectAnomalies };
