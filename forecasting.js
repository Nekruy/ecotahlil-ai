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

// ─── VAR(1) — векторная авторегрессия ─────────────────────────────────────────

// ── Матричные утилиты ─────────────────────────────────────────────────────────

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
  const C = matCreate(rA, cB);
  for (let i = 0; i < rA; i++)
    for (let m = 0; m < cA; m++)
      if (A[i][m] !== 0)
        for (let j = 0; j < cB; j++)
          C[i][j] += A[i][m] * B[m][j];
  return C;
}

// Гаусс-Жордан: инверсия квадратной матрицы
function matInverse(A) {
  const n = A.length;
  const aug = A.map((row, i) => {
    const id = new Array(n).fill(0); id[i] = 1;
    return [...row, ...id];
  });
  for (let col = 0; col < n; col++) {
    let pr = col;
    for (let r = col + 1; r < n; r++)
      if (Math.abs(aug[r][col]) > Math.abs(aug[pr][col])) pr = r;
    [aug[col], aug[pr]] = [aug[pr], aug[col]];
    const piv = aug[col][col];
    if (Math.abs(piv) < 1e-14)
      throw new Error('Матрица вырождена — проверьте данные на мультиколлинеарность или добавьте наблюдений');
    for (let j = 0; j < 2 * n; j++) aug[col][j] /= piv;
    for (let r = 0; r < n; r++) {
      if (r === col) continue;
      const f = aug[r][col];
      for (let j = 0; j < 2 * n; j++) aug[r][j] -= f * aug[col][j];
    }
  }
  return aug.map(row => row.slice(n));
}

function matVecMul(A, v) {
  return A.map(row => row.reduce((s, a, j) => s + a * v[j], 0));
}

function vecAdd(a, b) { return a.map((v, i) => v + b[i]); }

// ── OLS-оценка VAR(1) ─────────────────────────────────────────────────────────
// series — массив нормализованных рядов [k × T]
function estimateVAR(series) {
  const k = series.length;
  const T = series[0].length;
  const n = T - 1; // число строк после лагирования

  // Design matrix X: n × (k+1), строки = [1, y1_{t-1}, ..., yk_{t-1}]
  const X = [];
  for (let t = 1; t < T; t++) X.push([1, ...series.map(s => s[t - 1])]);

  // Целевая матрица Ymat: n × k
  const Ymat = [];
  for (let t = 1; t < T; t++) Ymat.push(series.map(s => s[t]));

  const Xt       = matTranspose(X);
  const XtX      = matMul(Xt, X);
  const XtXinv   = matInverse(XtX);
  const XtXinvXt = matMul(XtXinv, Xt);

  const constants = [];
  const A         = matCreate(k, k); // A[i][j]: коэф. y_j,t-1 в уравнении i
  const rss       = new Array(k).fill(0);
  const residuals = Array.from({ length: k }, () => []);

  for (let i = 0; i < k; i++) {
    const yi   = Ymat.map(row => row[i]);
    const beta = matVecMul(XtXinvXt, yi);
    constants[i] = beta[0];
    for (let j = 0; j < k; j++) A[i][j] = beta[j + 1];

    for (let t = 0; t < n; t++) {
      let fit = beta[0];
      for (let j = 0; j < k; j++) fit += A[i][j] * series[j][t];
      const res = series[i][t + 1] - fit;
      residuals[i].push(res);
      rss[i] += res * res;
    }
  }

  const df = Math.max(1, n - k - 1);
  // Стандартные ошибки коэффициентов: SE(β_{i,p}) = sqrt(XtXinv[p][p] * σ²_i)
  const seMatrix = A.map((_, i) => {
    const s2 = rss[i] / df;
    return Array.from({ length: k + 1 }, (_, p) =>
      Math.sqrt(Math.max(0, XtXinv[p][p] * s2))
    );
  });

  return { A, constants, residuals, seMatrix, rss, df };
}

// ── Прогноз VAR на periods шагов ──────────────────────────────────────────────
function forecastVAR(A, constants, lastZ, periods) {
  const out = [];
  let prev = lastZ.slice();
  for (let h = 0; h < periods; h++) {
    const next = vecAdd(constants, matVecMul(A, prev));
    out.push(next.slice());
    prev = next;
  }
  return out;
}

// ── Импульсные функции отклика (IRF) ─────────────────────────────────────────
// irf[j][h][i] = отклик переменной i на горизонте h при единичном шоке в j
function computeIRF(A, maxH) {
  const k   = A.length;
  const irf = Array.from({ length: k }, () => []);
  for (let j = 0; j < k; j++) {
    let resp = new Array(k).fill(0);
    resp[j] = 1;
    for (let h = 0; h <= maxH; h++) {
      irf[j].push(resp.map(v => round4(v)));
      if (h < maxH) resp = matVecMul(A, resp);
    }
  }
  return irf;
}

// ── Тест причинности Грейнджера (t-тест на a_ij) ─────────────────────────────
// granger[j][i] = { coefficient, tStat, significant }: влияет ли j на i
function grangerCausality(A, seMatrix) {
  const k = A.length;
  const g = Array.from({ length: k }, () => new Array(k).fill(null));
  for (let i = 0; i < k; i++)
    for (let j = 0; j < k; j++) {
      if (i === j) continue;
      const se = seMatrix[i][j + 1]; // +1: beta[0] — константа
      const t  = se > 1e-10 ? A[i][j] / se : 0;
      g[j][i]  = { coefficient: round4(A[i][j]), tStat: round4(t), significant: Math.abs(t) > 1.96 };
    }
  return g;
}

// ── Текстовая интерпретация для руководства ───────────────────────────────────
function buildVARInterpretation(keys, labels, A, granger, forecasts, series, periods) {
  const k       = keys.length;
  const lastObs = keys.map((_, i) => series[i][series[i].length - 1]);
  const lines   = [];

  lines.push(`Анализ VAR(1) макроэкономических показателей Таджикистана. Горизонт прогноза: ${periods} периодов.`);
  lines.push('');

  // Причинность Грейнджера
  const sigLinks = [];
  for (let j = 0; j < k; j++)
    for (let i = 0; i < k; i++)
      if (i !== j && granger[j][i]?.significant)
        sigLinks.push({ cause: j, effect: i, coef: granger[j][i].coefficient });

  if (sigLinks.length > 0) {
    lines.push('ПРИЧИННОСТЬ ГРЕЙНДЖЕРА (α = 5%):');
    for (const s of sigLinks) {
      const sign = s.coef > 0 ? 'положительно влияет на' : 'отрицательно влияет на';
      lines.push(`  • ${labels[keys[s.cause]]} ${sign} ${labels[keys[s.effect]]} (коэф. ${s.coef > 0 ? '+' : ''}${s.coef})`);
    }
  } else {
    lines.push('ПРИЧИННОСТЬ ГРЕЙНДЖЕРА: при текущем объёме данных статистически значимых связей не выявлено. Рекомендуется расширить временной ряд для более точных выводов.');
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

  // Сильнейшие межпеременные взаимодействия
  const cross = [];
  for (let i = 0; i < k; i++)
    for (let j = 0; j < k; j++)
      if (i !== j) cross.push({ from: j, to: i, coef: A[i][j] });
  cross.sort((a, b) => Math.abs(b.coef) - Math.abs(a.coef));

  if (cross.length > 0) {
    lines.push('СИЛЬНЕЙШИЕ ВЗАИМОДЕЙСТВИЯ (стандартизованные β):');
    for (const cc of cross.slice(0, 4)) {
      const eff = cc.coef > 0 ? 'усиливает' : 'сдерживает';
      lines.push(`  • ${labels[keys[cc.from]]} ${eff} ${labels[keys[cc.to]]} (A = ${cc.coef > 0 ? '+' : ''}${round4(cc.coef)})`);
    }
    lines.push('');
  }

  lines.push('РЕКОМЕНДАЦИИ: При формировании монетарной и фискальной политики учитывайте выявленные взаимозависимости. Динамика денежных переводов традиционно поддерживает внутренний спрос и может усиливать инфляционное давление. Курс USD/TJS влияет на импортную инфляцию через ценовой канал. Прогноз основан на исторических данных — при структурных шоках в экономике рекомендуется переоценка модели.');

  return lines.join('\n');
}

/**
 * VAR(1) — Векторная авторегрессия для макроэкономических показателей.
 *
 * @param {{ gdp, inflation, exchange_rate, remittances }: Record<string,number[]>} data
 * @param {number} periods  горизонт прогноза
 */
function var_model(data, periods) {
  const KEYS   = ['gdp', 'inflation', 'exchange_rate', 'remittances'];
  const LABELS = {
    gdp:           'ВВП',
    inflation:     'Инфляция',
    exchange_rate: 'Курс USD/TJS',
    remittances:   'Переводы мигрантов',
  };

  // 1. Валидация входных данных
  const raw = [];
  for (const key of KEYS) {
    const arr = (Array.isArray(data[key]) ? data[key] : []).map(Number);
    if (arr.length < 6)  throw new Error(`${LABELS[key]}: необходимо минимум 6 наблюдений`);
    if (arr.some(isNaN)) throw new Error(`${LABELS[key]}: все значения должны быть числами`);
    raw.push(arr);
  }

  const k = KEYS.length;
  const T = Math.min(...raw.map(s => s.length));
  if (T < k + 3) throw new Error(`Недостаточно наблюдений (нужно минимум ${k + 3}, есть ${T})`);

  const series = raw.map(s => s.slice(0, T));

  // 2. Z-нормализация для численной стабильности OLS
  const mu  = series.map(mean);
  const sig = series.map(s => { const sd = stdDev(s); return sd > 1e-10 ? sd : 1; });
  const zS  = series.map((s, i) => s.map(v => (v - mu[i]) / sig[i]));

  // 3. Оценка VAR(1) методом МНК
  const { A, constants, rss, seMatrix } = estimateVAR(zS);

  // 4. Прогноз в нормализованных единицах, затем денормализация
  const lastZ      = zS.map(s => s[s.length - 1]);
  const zForecasts = forecastVAR(A, constants, lastZ, periods);
  const forecasts  = zForecasts.map(zv => zv.map((z, i) => round4(z * sig[i] + mu[i])));

  // 5. R² каждого уравнения (инвариантен к нормализации)
  const r2 = zS.map((s, i) => {
    const resp = s.slice(1);
    const mR   = mean(resp);
    const tss  = resp.reduce((a, v) => a + (v - mR) ** 2, 0);
    return tss > 0 ? round4(1 - rss[i] / tss) : 0;
  });

  // 6. IRF (до 8 горизонтов)
  const irf = computeIRF(A, Math.min(periods, 8));

  // 7. Тест Грейнджера
  const granger = grangerCausality(A, seMatrix);

  // 8. Интерпретация
  const interpretation = buildVARInterpretation(KEYS, LABELS, A, granger, forecasts, series, periods);

  return {
    keys:         KEYS,
    labels:       LABELS,
    historical:   series,
    forecasts,
    irf,
    granger,
    r2,
    coefficients: A.map(row => row.map(round4)),
    constants:    constants.map(round4),
    interpretation,
    periods,
  };
}

module.exports = { arima, prophet, detectAnomalies, garch, var_model };
