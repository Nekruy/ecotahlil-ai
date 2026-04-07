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

// ─── GARCH(1,1) — моделирование волатильности ────────────────────────────────

function round4(v) {
  return Math.round(v * 10000) / 10000;
}

/**
 * Метод Нелдера–Мида (Simplex) для минимизации f(x) без производных.
 */
function nelderMead(fn, x0, { maxIter = 2000, tol = 1e-10 } = {}) {
  const n = x0.length;
  const A = 1, G = 2, R = 0.5, S = 0.5;

  // Строим начальный симплекс
  const simplex = [x0.slice()];
  for (let i = 0; i < n; i++) {
    const p = x0.slice();
    p[i] = p[i] !== 0 ? p[i] * 1.1 : 0.0005;
    simplex.push(p);
  }
  let fvals = simplex.map(fn);

  for (let iter = 0; iter < maxIter; iter++) {
    // Сортировка вершин по значению функции
    const idx = Array.from({ length: n + 1 }, (_, i) => i)
      .sort((a, b) => fvals[a] - fvals[b]);
    const s = idx.map(i => simplex[i].slice());
    const f = idx.map(i => fvals[i]);

    if (f[n] - f[0] < tol) break;

    // Центроид (без худшей точки)
    const c = new Array(n).fill(0);
    for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) c[j] += s[i][j] / n;

    // Отражение
    const xr = c.map((v, j) => v + A * (v - s[n][j]));
    const fr = fn(xr);

    if (fr < f[0]) {
      // Растяжение
      const xe = c.map((v, j) => v + G * (xr[j] - v));
      const fe = fn(xe);
      s[n] = fe < fr ? xe : xr;
      f[n] = fe < fr ? fe : fr;
    } else if (fr < f[n - 1]) {
      s[n] = xr; f[n] = fr;
    } else {
      // Сжатие
      const xc = c.map((v, j) => v + R * (s[n][j] - v));
      const fc = fn(xc);
      if (fc < f[n]) {
        s[n] = xc; f[n] = fc;
      } else {
        // Усадка
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

/**
 * Оценка параметров GARCH(1,1) методом максимального правдоподобия.
 * Работает с доходностями в процентах (r * 100) для численной стабильности.
 * Модель: h_t = omega + alpha * r_{t-1}^2 + beta * h_{t-1}
 */
function estimateGARCH(retsPct) {
  const n       = retsPct.length;
  const initVar = variance(retsPct);

  function negLogLik([omega, alpha, beta]) {
    if (omega <= 1e-8 || alpha <= 0 || beta <= 0) return 1e15;
    if (alpha + beta >= 0.9999) return 1e15;

    let h  = initVar;
    let ll = 0;
    for (let t = 0; t < n; t++) {
      if (t > 0) h = omega + alpha * retsPct[t - 1] ** 2 + beta * h;
      if (h <= 0) return 1e15;
      ll += Math.log(h) + retsPct[t] ** 2 / h;
    }
    return 0.5 * ll;
  }

  // Несколько стартовых точек — берём лучшую
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
  omega = Math.max(1e-8,  omega);
  alpha = Math.max(0.001, Math.min(0.4999, alpha));
  beta  = Math.max(0.001, Math.min(0.9979 - alpha, beta));

  return { omega, alpha, beta };
}

/**
 * GARCH(1,1) — прогноз волатильности курса валюты.
 *
 * @param {number[]} data    — временной ряд курсов (минимум 10 значений)
 * @param {number}   periods — горизонт прогноза в периодах
 * @returns {{
 *   omega, alpha, beta, persistence,
 *   returns, historicalVol, forecastVol,
 *   ci1Lower, ci1Upper, ci2Lower, ci2Upper,
 *   currentDailyVol, annualizedVol,
 *   riskLevel, signal
 * }}
 */
function garch(data, periods) {
  const nums = validateData(data);
  if (nums.length < 10) throw new Error('Для GARCH необходимо минимум 10 точек данных');

  // 1. Логарифмические доходности r_t = ln(P_t / P_{t-1}) в процентах
  const rets    = [];
  const retsPct = [];
  for (let i = 1; i < nums.length; i++) {
    if (nums[i - 1] <= 0 || nums[i] <= 0) {
      throw new Error('Все значения курса должны быть положительными');
    }
    const r = Math.log(nums[i] / nums[i - 1]);
    rets.push(r);
    retsPct.push(r * 100);
  }

  // 2. Оценка параметров (работаем с % доходностями для стабильности)
  // omega, h_t теперь в единицах (%)^2; volатильность = sqrt(h) в %
  const { omega, alpha, beta } = estimateGARCH(retsPct);
  const persistence = alpha + beta;

  // 3. Исторические условные дисперсии (единицы: (%)^2)
  const initVar = variance(retsPct);
  const condVar = new Array(retsPct.length);
  condVar[0]    = initVar;
  for (let t = 1; t < retsPct.length; t++) {
    condVar[t] = omega + alpha * retsPct[t - 1] ** 2 + beta * condVar[t - 1];
  }
  const historicalVol = condVar.map(h => round4(Math.sqrt(Math.max(0, h)))); // дневная vol %

  // 4. Долгосрочная (безусловная) дисперсия
  const longRunVar = persistence < 1 ? omega / (1 - persistence) : initVar;
  const lastH      = condVar[condVar.length - 1];

  // 5. Прогноз дисперсии на N периодов вперёд
  // E[h_{t+k}] = longRunVar + persistence^k * (h_t - longRunVar)
  const fwdVar = [];
  for (let k = 1; k <= periods; k++) {
    const v = longRunVar + Math.pow(persistence, k) * (lastH - longRunVar);
    fwdVar.push(Math.max(0, v));
  }
  const forecastVol = fwdVar.map(v => round4(Math.sqrt(v))); // дневная vol %

  // 6. Доверительные интервалы (неопределённость растёт с горизонтом)
  const histVolArr = condVar.map(h => Math.sqrt(Math.max(0, h)));
  const histVolStd = stdDev(histVolArr);
  const ci1Lower = fwdVar.map((v, i) => round4(Math.max(0, Math.sqrt(v) - histVolStd * 0.5 * Math.sqrt(i + 1))));
  const ci1Upper = fwdVar.map((v, i) => round4(Math.sqrt(v) + histVolStd * 0.5 * Math.sqrt(i + 1)));
  const ci2Lower = fwdVar.map((v, i) => round4(Math.max(0, Math.sqrt(v) - histVolStd * 1.0 * Math.sqrt(i + 1))));
  const ci2Upper = fwdVar.map((v, i) => round4(Math.sqrt(v) + histVolStd * 1.0 * Math.sqrt(i + 1)));

  // 7. Текущая и годовая волатильность
  // annualized = daily_vol_pct * sqrt(252)
  const currentDailyVol = round4(Math.sqrt(Math.max(0, lastH)));         // %
  const annualizedVol   = round4(Math.sqrt(Math.max(0, lastH)) * Math.sqrt(252)); // %

  // 8. Уровень риска и сигнал для руководства
  let riskLevel, signal;
  if (annualizedVol < 5) {
    riskLevel = 'низкий';
    signal =
      `Волатильность курса находится на низком уровне — ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). ` +
      `Устойчивость α+β = ${round4(persistence)}. ` +
      `Валютный риск минимален. Рекомендуется сохранить текущую структуру валютных позиций и продолжить плановый мониторинг.`;
  } else if (annualizedVol < 15) {
    riskLevel = 'умеренный';
    signal =
      `Волатильность курса — умеренная: ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). ` +
      `Коэффициент устойчивости α+β = ${round4(persistence)}. ` +
      `Рекомендуется усилить мониторинг валютных рисков, рассмотреть частичное хеджирование ` +
      `экспортно-импортных операций и ограничить краткосрочные спекулятивные позиции.`;
  } else if (annualizedVol < 25) {
    riskLevel = 'высокий';
    signal =
      `ВНИМАНИЕ: Высокая волатильность курса — ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). ` +
      `Коэффициент устойчивости α+β = ${round4(persistence)} указывает на ${persistence > 0.95 ? 'высокую персистентность шоков' : 'умеренное затухание'}. ` +
      `Необходимо незамедлительно принять меры: активировать хеджирование ключевых валютных позиций, ` +
      `ограничить открытые позиции в иностранной валюте, проинформировать ключевых партнёров.`;
  } else {
    riskLevel = 'критический';
    signal =
      `КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ: Экстремальная волатильность — ${annualizedVol}% годовых (дневная: ${currentDailyVol}%). ` +
      `Коэффициент устойчивости α+β = ${round4(persistence)}: шоки крайне медленно затухают. ` +
      `ТРЕБУЕТСЯ НЕМЕДЛЕННОЕ ВМЕШАТЕЛЬСТВО РУКОВОДСТВА: приостановить крупные валютные операции, ` +
      `задействовать инструменты стабилизации курса, оценить валютную экспозицию всех подразделений ` +
      `и провести экстренное совещание по управлению рисками.`;
  }

  return {
    omega:            round4(omega),
    alpha:            round4(alpha),
    beta:             round4(beta),
    persistence:      round4(persistence),
    returns:          rets.map(r => round4(r * 100)),
    historicalVol,
    forecastVol,
    ci1Lower,
    ci1Upper,
    ci2Lower,
    ci2Upper,
    currentDailyVol,
    annualizedVol,
    riskLevel,
    signal,
  };
}

module.exports = { arima, prophet, detectAnomalies, garch };
