/**
 * forecasting.js — ARIMA и Prophet-подобные модели прогнозирования
 * Чистый JavaScript без внешних зависимостей
 */

// ─── Математические вспомогательные функции ───────────────────────────────────

function round2(v) {
  return Math.round(v * 100) / 100;
}

function mean(arr) {
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

function variance(arr) {
  const m = mean(arr);
  return arr.reduce((s, v) => s + (v - m) ** 2, 0) / arr.length;
}

function stdDev(arr) {
  return Math.sqrt(variance(arr));
}

// Простая линейная регрессия: y = slope * x + intercept
function linearRegression(xs, ys) {
  const n = xs.length;
  const mx = mean(xs);
  const my = mean(ys);
  const num = xs.reduce((s, x, i) => s + (x - mx) * (ys[i] - my), 0);
  const den = xs.reduce((s, x) => s + (x - mx) ** 2, 0);
  const slope = den !== 0 ? num / den : 0;
  const intercept = my - slope * mx;
  return { slope, intercept, predict: t => slope * t + intercept };
}

// Квантиль методом линейной интерполяции
function quantile(arr, p) {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = p * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
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

  // Шаг 2: AR(1) на разностном ряду — y_t = intercept + phi * y_{t-1}
  let phi = 0;
  let intercept = 0;

  if (diff.length >= 2) {
    const reg = linearRegression(diff.slice(0, -1), diff.slice(1));
    phi = reg.slope;
    intercept = reg.intercept;
  } else {
    intercept = mean(diff);
  }

  // Ограничиваем phi для стационарности
  phi = Math.max(-0.99, Math.min(0.99, phi));

  // Шаг 3: прогноз разностного ряда
  const forecastDiff = [];
  let lastDiff = diff[diff.length - 1];
  for (let i = 0; i < periods; i++) {
    const next = intercept + phi * lastDiff;
    forecastDiff.push(next);
    lastDiff = next;
  }

  // Шаг 4: обратное разностное преобразование (cumsum)
  const forecast = [];
  let prev = nums[nums.length - 1];
  for (const d of forecastDiff) {
    prev = round2(prev + d);
    forecast.push(prev);
  }

  return forecast;
}

// ─── Prophet-подобная модель ──────────────────────────────────────────────────
// Алгоритм: линейный тренд + сезонная компонента (усреднение по периоду)

function prophet(data, periods) {
  const nums = validateData(data);
  const n = nums.length;

  // Шаг 1: линейный тренд
  const xs = Array.from({ length: n }, (_, i) => i);
  const reg = linearRegression(xs, nums);

  // Шаг 2: остатки = реальные значения − тренд
  const residuals = nums.map((v, i) => v - reg.predict(i));

  // Шаг 3: период сезонности (авто-определение, min 3, max 12)
  const period = Math.min(12, Math.max(3, Math.floor(n / 2)));
  const seasonSum   = new Array(period).fill(0);
  const seasonCount = new Array(period).fill(0);
  residuals.forEach((r, i) => {
    seasonSum[i % period]   += r;
    seasonCount[i % period] += 1;
  });
  const seasonal = seasonSum.map((s, i) =>
    seasonCount[i] > 0 ? s / seasonCount[i] : 0
  );

  // Нормализуем сезонность (среднее = 0)
  const seasonMean = mean(seasonal);
  const seasonAdj  = seasonal.map(s => s - seasonMean);

  // Шаг 4: прогноз = тренд + сезонность
  const forecast = [];
  for (let i = 0; i < periods; i++) {
    const t = n + i;
    forecast.push(round2(reg.predict(t) + seasonAdj[t % period]));
  }

  return forecast;
}

// ─── Обнаружение аномалий (Z-score + IQR) ────────────────────────────────────

function detectAnomalies(data) {
  const nums = validateData(data);
  const m    = mean(nums);
  const s    = stdDev(nums);
  const q1   = quantile(nums, 0.25);
  const q3   = quantile(nums, 0.75);
  const iqr  = q3 - q1;
  const iqrLow  = q1 - 1.5 * iqr;
  const iqrHigh = q3 + 1.5 * iqr;

  const anomalies = [];
  nums.forEach((v, i) => {
    const z        = s !== 0 ? Math.abs(v - m) / s : 0;
    const isZscore = z > 2;
    const isIQR    = v < iqrLow || v > iqrHigh;

    if (isZscore || isIQR) {
      anomalies.push({
        index:     i,
        value:     v,
        zscore:    round2(z),
        direction: v > m ? 'high' : 'low',
        method:    isZscore && isIQR ? 'Z-score + IQR' : isZscore ? 'Z-score' : 'IQR',
      });
    }
  });

  return anomalies;
}

module.exports = { arima, prophet, detectAnomalies };
