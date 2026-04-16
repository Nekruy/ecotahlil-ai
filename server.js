const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');

const MODEL_VERSION = '2.0';

const PORT         = 3000;
const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_URL     = 'https://api.groq.com/openai/v1/chat/completions';
const GROQ_MODEL   = 'llama-3.3-70b-versatile';

// ─── Отладка переменных окружения ────────────────────────────────────────────
console.log('GROQ_API_KEY присутствует:', !!process.env.GROQ_API_KEY);
console.log('GROQ_API_KEY длина:', process.env.GROQ_API_KEY ? process.env.GROQ_API_KEY.length : 0);
console.log('NODE_ENV:', process.env.NODE_ENV || 'не задан');

// ─── Groq API helper ─────────────────────────────────────────────────────────
function groqChat(systemPrompt, userContent, maxTokens = 4000) {
  if (!GROQ_API_KEY) {
    return Promise.reject(new Error('GROQ_API_KEY не задан — проверь переменную окружения на сервере'));
  }
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model:       GROQ_MODEL,
      messages:    [
        { role: 'system', content: systemPrompt },
        { role: 'user',   content: userContent  },
      ],
      max_tokens:  maxTokens,
      temperature: 0.3,
    });

    const options = {
      hostname: 'api.groq.com',
      path:     '/openai/v1/chat/completions',
      method:   'POST',
      headers:  {
        'Authorization': 'Bearer ' + GROQ_API_KEY,
        'Content-Type':  'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };

    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
          if (parsed.error) return reject(new Error(parsed.error.message || 'Groq API error'));
          const text = parsed.choices?.[0]?.message?.content || '';
          resolve(text);
        } catch (e) {
          reject(new Error('Groq: invalid JSON response'));
        }
      });
    });

    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error('Groq request timeout')); });
    req.write(body);
    req.end();
  });
}

// ─── Инициализация базы данных ───────────────────────────────────────────────
const { initDB, saveReport: dbSaveReport, getReports: dbGetReports } = require('./database');
const authModule = require('./auth');
initDB();

// ─── File text extraction ────────────────────────────────────────────────────

async function extractText(buffer, fileFormat, filename) {
  const fmt = fileFormat.toLowerCase();

  if (fmt === 'txt' || fmt === 'json') {
    return buffer.toString('utf8');
  }

  if (fmt === 'csv') {
    return buffer.toString('utf8')
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean)
      .join('\n');
  }

  if (fmt === 'pdf') {
    const pdfParse = require('pdf-parse');
    const data = await pdfParse(buffer);
    const text = data.text ? data.text.trim() : '';
    if (!text) {
      throw new Error(
        'PDF не содержит извлекаемого текста. ' +
        'Возможно, это сканированный документ (изображение). ' +
        'Пожалуйста, загрузите текстовый PDF или скопируйте текст вручную.'
      );
    }
    return text;
  }

  if (fmt === 'docx') {
    const mammoth = require('mammoth');
    const result = await mammoth.extractRawText({ buffer });
    return result.value;
  }

  if (fmt === 'xls' || fmt === 'xlsx') {
    const XLSX = require('xlsx');
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const lines = [];
    for (const sheetName of workbook.SheetNames) {
      const sheet = workbook.Sheets[sheetName];
      const csv = XLSX.utils.sheet_to_csv(sheet);
      lines.push(`=== Sheet: ${sheetName} ===`);
      lines.push(csv);
    }
    return lines.join('\n');
  }

  throw new Error(`Unsupported file format: ${fmt}`);
}

// ─── Multipart/form-data parser ──────────────────────────────────────────────

function parseMultipart(body, boundary) {
  const fields = {};
  const sep = Buffer.from(`--${boundary}`);
  const parts = [];

  let start = 0;
  while (start < body.length) {
    const idx = body.indexOf(sep, start);
    if (idx === -1) break;
    const end = body.indexOf(sep, idx + sep.length);
    const chunk = end === -1 ? body.slice(idx + sep.length) : body.slice(idx + sep.length, end);
    parts.push(chunk);
    start = end === -1 ? body.length : end;
  }

  for (const part of parts) {
    // Find the blank line separating headers from body
    const headerEnd = part.indexOf('\r\n\r\n');
    if (headerEnd === -1) continue;

    const headerSection = part.slice(0, headerEnd).toString('utf8');
    // Content starts after \r\n\r\n, ends before trailing \r\n
    let content = part.slice(headerEnd + 4);
    if (content.slice(-2).toString() === '\r\n') {
      content = content.slice(0, -2);
    }

    const dispositionMatch = headerSection.match(/Content-Disposition:[^\r\n]*name="([^"]+)"/i);
    if (!dispositionMatch) continue;
    const fieldName = dispositionMatch[1];

    // RFC 5987: filename*=UTF-8''...  (кириллица, таджикский и т.д.)
    const filenameStarMatch = headerSection.match(/filename\*=UTF-8''([^\r\n;]+)/i);
    const filenameMatch = !filenameStarMatch && headerSection.match(/filename="([^"]+)"/i);
    const filename = filenameStarMatch
      ? decodeURIComponent(filenameStarMatch[1])
      : filenameMatch ? filenameMatch[1] : null;

    if (filename !== null) {
      // It's a file field
      const contentTypeMatch = headerSection.match(/Content-Type:\s*([^\r\n]+)/i);
      fields[fieldName] = {
        filename,
        contentType: contentTypeMatch ? contentTypeMatch[1].trim() : 'application/octet-stream',
        data: content,
      };
    } else {
      fields[fieldName] = content.toString('utf8');
    }
  }

  return fields;
}

// ─── Read full request body ──────────────────────────────────────────────────

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

// ─── CORS headers ────────────────────────────────────────────────────────────

function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// ─── HTTP server ─────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  setCORS(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // Serve index.html
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    const filePath = path.join(__dirname, 'index.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        return res.end('index.html not found');
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // Serve report.html
  if (req.method === 'GET' && req.url === '/report') {
    const filePath = path.join(__dirname, 'report.html');
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); return res.end('report.html not found'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // GET /api/worldbank?indicator=GDP
  if (req.method === 'GET' && req.url.startsWith('/api/worldbank')) {
    try {
      const qs        = new URL('http://x' + req.url).searchParams;
      const indicator = qs.get('indicator') || 'GDP';
      const { fetchWorldBank } = require('./dataCollector');
      const result = await fetchWorldBank(indicator);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/worldbank]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/commodities — нефть, алюминий, пшеница
  if (req.method === 'GET' && req.url === '/api/commodities') {
    try {
      const { fetchOilPrice, fetchAluminumPrice, fetchWheatPrice } = require('./dataCollector');
      const [oil, aluminum, wheat] = await Promise.allSettled([
        fetchOilPrice(),
        fetchAluminumPrice(),
        fetchWheatPrice(),
      ]);

      const result = {
        oil:      oil.status      === 'fulfilled' ? oil.value      : { error: oil.reason?.message },
        aluminum: aluminum.status === 'fulfilled' ? aluminum.value : { error: aluminum.reason?.message },
        wheat:    wheat.status    === 'fulfilled' ? wheat.value    : { error: wheat.reason?.message },
        fetched:  new Date().toISOString(),
      };

      console.log(`[/api/commodities] oil=${oil.status} alu=${aluminum.status} wheat=${wheat.status}`);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/commodities]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/external — все внешние источники
  if (req.method === 'GET' && req.url === '/api/external') {
    try {
      const {
        fetchADB, fetchWTO, fetchCBR,
        fetchOilPrice, fetchAluminumPrice, fetchWheatPrice,
      } = require('./dataCollector');

      const [adb, wto, cbr, oil, aluminum, wheat] = await Promise.allSettled([
        fetchADB(),
        fetchWTO(),
        fetchCBR(),
        fetchOilPrice(),
        fetchAluminumPrice(),
        fetchWheatPrice(),
      ]);

      const pick = r => r.status === 'fulfilled' ? r.value : { error: r.reason?.message };
      const result = {
        adb:      pick(adb),
        wto:      pick(wto),
        cbr:      pick(cbr),
        oil:      pick(oil),
        aluminum: pick(aluminum),
        wheat:    pick(wheat),
        fetched:  new Date().toISOString(),
      };

      console.log(`[/api/external] adb=${adb.status} wto=${wto.status} cbr=${cbr.status} oil=${oil.status} alu=${aluminum.status} wheat=${wheat.status}`);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/external]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/nbt
  if (req.method === 'GET' && req.url === '/api/nbt') {
    try {
      const { fetchNBT, getRatesHistory } = require('./dataCollector');
      const current = await fetchNBT();
      const history = getRatesHistory();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ...current, history }));
    } catch (err) {
      console.error('[/api/nbt]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/rates-history?currency=usd&days=365
  if (req.method === 'GET' && req.url.startsWith('/api/rates-history')) {
    try {
      const qs       = new URL('http://x' + req.url).searchParams;
      const currency = (qs.get('currency') || 'usd').toLowerCase();
      const days     = Math.min(3650, Math.max(1, parseInt(qs.get('days') || '365')));

      const { getRatesHistory: getNBTHistory, loadTimeseries } = require('./nbtParser');
      const data = await getNBTHistory(currency, days);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        currency,
        days,
        count: data.length,
        data,
        meta: { dataSource: 'nbt.tj', collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
      }));
    } catch (err) {
      console.error('[/api/rates-history]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/report
  if (req.method === 'POST' && req.url === '/api/report') {
    try {
      const body   = await readBody(req);
      const entry  = JSON.parse(body.toString('utf8'));
      const reportsFile = path.join(__dirname, 'reports.json');

      let reports = [];
      try {
        if (fs.existsSync(reportsFile)) {
          reports = JSON.parse(fs.readFileSync(reportsFile, 'utf8'));
        }
      } catch {}

      const record = {
        id:          reports.length + 1,
        district:    String(entry.district || '').trim(),
        date:        String(entry.date || '').trim(),
        product:     String(entry.product || '').trim(),
        price:       parseFloat(entry.price) || 0,
        unit:        String(entry.unit || 'кг').trim(),
        notes:       String(entry.notes || '').trim(),
        submittedAt: new Date().toISOString(),
      };

      if (!record.district || !record.product || record.price <= 0) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Заполните обязательные поля' }));
      }

      reports.push(record);
      fs.writeFileSync(reportsFile, JSON.stringify(reports, null, 2), 'utf8');

      console.log(`[/api/report] id=${record.id} district="${record.district}" product="${record.product}" price=${record.price}`);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, id: record.id }));
    } catch (err) {
      console.error('[/api/report]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/var
  if (req.method === 'POST' && req.url === '/api/var') {
    try {
      const body = await readBody(req);
      const { data, periods } = JSON.parse(body.toString('utf8'));

      const { var_model } = require('./forecasting');
      const hdb = require('./historicalDB');
      const n   = Math.max(1, Math.min(24, parseInt(periods) || 6));

      // Авто-подтягиваем данные если не переданы
      const varData = { ...data };
      let dataSource = 'user-provided';

      // Если есть ministry_gdp_model.json — используем для ВВП и инфляции
      try {
        const gdpH = hdb.getGDPHistory();
        const infH = hdb.getInflationHistory();
        const exH  = hdb.getExchangeRateHistory();
        const remH = hdb.getRemittancesHistory();

        if (!varData.gdp || varData.gdp.length < 6)
          varData.gdp = gdpH.map(r => r.gdp_growth || r.gdp_bln_somoni).filter(v => v != null);
        if (!varData.inflation || varData.inflation.length < 6)
          varData.inflation = infH.map(r => r.cpi || r.inflation).filter(v => v != null);
        if (!varData.exchange_rate || varData.exchange_rate.length < 6)
          varData.exchange_rate = exH.map(r => r.usd_tjs || r.USD).filter(v => v != null);
        if (!varData.remittances || varData.remittances.length < 6)
          varData.remittances = remH.map(r => r.amount_mln_usd || r.total_mln_usd || r.remittances).filter(v => v != null);

        dataSource = 'МЭРиТ/НБТ (auto)';
      } catch (_) {}

      varData._source = dataSource;
      const result = var_model(varData, n);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ...result,
        meta: { dataSource, dataPoints: Math.min(...['gdp','inflation','exchange_rate','remittances'].map(k => (varData[k]||[]).length)), collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
      }));
    } catch (err) {
      console.error('[/api/var]', err.message);
      const code = /необходимо|Недостаточно/i.test(err.message) ? 400 : 500;
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/stress-test
  if (req.method === 'POST' && req.url === '/api/stress-test') {
    try {
      const body = await readBody(req);
      const { scenario, params } = JSON.parse(body.toString('utf8'));

      if (!scenario || !params) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Необходимо указать scenario и params' }));
      }

      const { runStressTest } = require('./stressTest');
      const result = runStressTest(scenario, params);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/stress-test]', err.message);
      const code = /Неизвестный сценарий/i.test(err.message) ? 400 : 500;
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/garch
  if (req.method === 'POST' && req.url === '/api/garch') {
    try {
      const body = await readBody(req);
      const { data, periods, currency } = JSON.parse(body.toString('utf8'));

      const { garch } = require('./forecasting');

      let numData = (Array.isArray(data) ? data : []).map(Number).filter(v => !isNaN(v));
      let dataSource = 'user-provided';
      let collectedAt = new Date().toISOString();

      // Авто-загрузка из rates_timeseries.json если данных мало
      if (numData.length < 10) {
        const cur = (currency || 'usd').toLowerCase();
        try {
          const { loadTimeseries } = require('./nbtParser');
          const ts = loadTimeseries();
          const tsVals = ts.map(r => r[cur]).filter(v => v != null);
          if (tsVals.length >= numData.length) {
            numData = tsVals;
            dataSource = 'nbt.tj (rates_timeseries)';
          }
        } catch (_) {}

        // Fallback: rates-history.json
        if (numData.length < 10) {
          try {
            const histPath = path.join(__dirname, 'rates-history.json');
            if (fs.existsSync(histPath)) {
              const hist = JSON.parse(fs.readFileSync(histPath, 'utf8'));
              const key  = cur.toUpperCase();
              const hVals = hist.map(r => r[key] || r[cur]).filter(v => v != null);
              if (hVals.length > numData.length) { numData = hVals; dataSource = 'rates-history.json'; }
            }
          } catch (_) {}
        }
      }

      if (numData.length < 10) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Для GARCH необходимо минимум 10 точек данных' }));
      }

      const n      = Math.max(1, Math.min(30, parseInt(periods) || 10));
      const result = garch(numData, n);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ...result,
        meta: { dataSource, dataPoints: numData.length, collectedAt, modelVersion: MODEL_VERSION },
      }));
    } catch (err) {
      console.error('[/api/garch]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/backtest
  if (req.method === 'POST' && req.url === '/api/backtest') {
    try {
      const body = await readBody(req);
      const { indicator, method, data, currency } = JSON.parse(body.toString('utf8'));

      const { backtestArima } = require('./forecasting');

      let numData = (Array.isArray(data) ? data : []).map(Number).filter(v => !isNaN(v));
      let dataSource = 'user-provided';

      // Авто-загрузка данных
      if (numData.length < 6 && currency) {
        try {
          const { loadTimeseries } = require('./nbtParser');
          const cur = currency.toLowerCase();
          const ts  = loadTimeseries();
          numData   = ts.map(r => r[cur]).filter(v => v != null);
          dataSource = 'nbt.tj';
        } catch (_) {}
      }

      if (numData.length < 6 && indicator) {
        try {
          const hdb = require('./historicalDB');
          numData = hdb.getDataForForecasting(indicator);
          dataSource = 'МЭРиТ/НБТ';
        } catch (_) {}
      }

      if (numData.length < 6) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Необходимо минимум 6 точек данных для бэктестинга' }));
      }

      const validation = backtestArima(numData);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        validation,
        historical: numData,
        forecasts:  null,
        meta: { dataSource, dataPoints: numData.length, method: method || 'arima', collectedAt: new Date().toISOString(), modelVersion: MODEL_VERSION },
      }));
    } catch (err) {
      console.error('[/api/backtest]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/cge
  if (req.method === 'POST' && req.url === '/api/cge') {
    try {
      const body = await readBody(req);
      const shock = JSON.parse(body.toString('utf8'));

      const { cgeSimulate } = require('./cgeModel');
      const result = cgeSimulate(shock);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/cge]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Forecast endpoint
  if (req.method === 'POST' && req.url === '/forecast') {
    try {
      const body = await readBody(req);
      const { data, periods, method, currency, indicator } = JSON.parse(body.toString('utf8'));

      const { autoArima, prophet, detectAnomalies, ensembleForecast } = require('./forecasting');

      // Авто-загрузка данных НБТ или исторических макро-данных, если data не передан
      let numData = (Array.isArray(data) ? data : []).map(Number).filter(v => !isNaN(v));
      let dataSource = 'user-provided';

      if (numData.length < 4 && currency) {
        try {
          const { getRatesHistory } = require('./nbtParser');
          const hist = await getRatesHistory(currency, 365);
          numData = hist.map(r => r[currency.toLowerCase()]).filter(v => v != null);
          dataSource = 'nbt.tj';
        } catch (_) {}
      }

      if (numData.length < 4 && indicator) {
        try {
          const hdb = require('./historicalDB');
          numData = hdb.getDataForForecasting(indicator);
          dataSource = 'МЭРиТ/НБТ';
        } catch (_) {}
      }

      if (numData.length < 4) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Необходимо минимум 4 точки данных' }));
      }

      const n = Math.max(1, Math.min(24, parseInt(periods) || 6));
      const collectedAt = new Date().toISOString();

      let result;
      if (method === 'prophet') {
        const forecast = prophet(numData, n);
        result = { forecast, method: 'prophet' };
      } else if (method === 'ensemble') {
        result = ensembleForecast(numData, n);
        result.method = 'ensemble';
      } else {
        // auto-arima (default)
        const ar = autoArima(numData, n);
        result = { forecast: ar.forecast, bestP: ar.bestP, bestD: ar.bestD, bestQ: ar.bestQ, aic: ar.aic, method: 'auto-arima', adfResults: ar.adfResults };
      }

      const anomalies = detectAnomalies(numData);
      const histLabels    = numData.map((_, i) => `T${i + 1}`);
      const forecastLabels = Array.from({ length: n }, (_, i) => `T${numData.length + i + 1}`);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        historical: numData,
        histLabels,
        forecastLabels,
        anomalies,
        periods: n,
        ...result,
        meta: { dataSource, dataPoints: numData.length, collectedAt, modelVersion: MODEL_VERSION },
      }));
    } catch (err) {
      console.error('Error handling /forecast:', err);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Webhook endpoint
  if (req.method === 'POST' && req.url === '/webhook') {
    try {
      const contentType = req.headers['content-type'] || '';
      const boundaryMatch = contentType.match(/boundary=([^\s;]+)/);
      if (!boundaryMatch) {
        res.writeHead(400);
        return res.end('Missing multipart boundary');
      }

      const body = await readBody(req);
      const fields = parseMultipart(body, boundaryMatch[1]);

      const question = fields.question || '';
      const userRole = fields.userRole || '';
      const fileFormat = fields.fileFormat || '';
      const fileField = fields.file;

      let fileText = '';
      if (fileField && fileField.data && fileField.data.length > 0) {
        fileText = await extractText(fileField.data, fileFormat, fileField.filename);
      }

      const trimmedText = fileText.trim();
      console.log(`[webhook] question="${question.slice(0, 80)}" fileFormat="${fileFormat}" extractedBytes=${trimmedText.length}`);

      const systemPrompt = `Ты — профессиональный макроэкономический аналитик Таджикистана. При анализе используй следующие методы и формулы:

1. ИНФЛЯЦИЯ: ИПЦ = (Стоимость корзины текущий / Стоимость корзины базовый) × 100. Уравнение Фишера: номинальная ставка = реальная ставка + инфляция.

2. РОСТ ВВП: Темп роста = (ВВП_т - ВВП_т-1) / ВВП_т-1 × 100. Декомпозиция: ВВП = C + I + G + (Ex - Im). Функция Кобба-Дугласа: Y = A × K^α × L^β

3. ПРОГНОЗИРОВАНИЕ: Применяй метод ARIMA для временных рядов. Используй регрессионный анализ для выявления зависимостей. Модель VAR для взаимосвязи макропеременных.

4. ДЕНЕЖНАЯ ПОЛИТИКА: Правило Тейлора для оценки ставки НБТ. Анализ денежного мультипликатора M2/M0.

5. ВНЕШНИЙ СЕКТОР: Анализ платёжного баланса. Курс TJS/USD влияние на инфляцию. Денежные переводы как % ВВП.

При каждом анализе:
- Выяви ключевые показатели и рассчитай их
- Сравни с предыдущим периодом
- Выяви тренды и аномалии
- Дай прогноз на следующий период
- Укажи риски и рекомендации
- Форматируй ответ в красивый HTML с таблицами и цветными блоками
- Отвечай на русском языке профессионально`;

      const userContent = trimmedText
        ? `Вопрос: ${question}\n\nСодержимое документа:\n${trimmedText}`
        : `Вопрос: ${question}`;

      const answer = await groqChat(systemPrompt, userContent, 4000);

      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(answer);
    } catch (err) {
      console.error('Error handling /webhook:', err);
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(`Server error: ${err.message}`);
    }
    return;
  }

  // ── Статические страницы ─────────────────────────────────────────────────

  if (req.method === 'GET' && req.url === '/login') {
    const fp = path.join(__dirname, 'login.html');
    fs.readFile(fp, (err, data) => {
      if (err) { res.writeHead(404); return res.end('login.html not found'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  if (req.method === 'GET' && req.url === '/dashboard') {
    const fp = path.join(__dirname, 'dashboard.html');
    fs.readFile(fp, (err, data) => {
      if (err) { res.writeHead(404); return res.end('dashboard.html not found'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // ── POST /auth/login ──────────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/auth/login') {
    try {
      const body = await readBody(req);
      const { login, password } = JSON.parse(body.toString('utf8'));
      if (!login || !password) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Введите логин и пароль' }));
      }
      const result = authModule.login(login.trim(), password);
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Set-Cookie': authModule.makeCookie(result.token),
      });
      res.end(JSON.stringify({ ok: true, token: result.token, user: result.user }));
    } catch (err) {
      console.error('[/auth/login]', err.message);
      res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // ── POST /auth/logout ─────────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/auth/logout') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Set-Cookie': authModule.clearCookie(),
    });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // ── GET /auth/me ──────────────────────────────────────────────────────────
  if (req.method === 'GET' && req.url === '/auth/me') {
    try {
      const user = authModule.verifyToken(req);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ user }));
    } catch (err) {
      res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // ── POST /api/reports — сохранить ежедневный отчёт района ────────────────
  if (req.method === 'POST' && req.url === '/api/reports') {
    try {
      const user = authModule.verifyToken(req);
      const body = await readBody(req);
      const payload = JSON.parse(body.toString('utf8'));

      if (!payload.date) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Укажите дату отчёта' }));
      }

      const record = dbSaveReport({
        userId:   user.id,
        login:    user.login,
        name:     user.name,
        role:     user.role,
        region:   user.region,
        district: user.district,
        date:     payload.date,
        prices:   payload.prices   || {},
        metrics:  payload.metrics  || {},
        notes:    payload.notes    || '',
      });

      console.log(`[/api/reports] ${user.login} → ${payload.date}`);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, id: record.id }));
    } catch (err) {
      console.error('[/api/reports]', err.message);
      const code = err.message.includes('токен') || err.message.includes('Нет') ? 401 : 500;
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // ── GET /api/reports — получить историю отчётов ───────────────────────────
  if (req.method === 'GET' && req.url.startsWith('/api/reports')) {
    try {
      const user = authModule.verifyToken(req);
      const qs   = new URL('http://x' + req.url).searchParams;
      const days = Math.max(1, Math.min(90, parseInt(qs.get('days') || '7')));

      const from = new Date();
      from.setDate(from.getDate() - days);
      const fromStr = from.toISOString().slice(0, 10);

      const filters = { from: fromStr };
      if (user.role === 'district') filters.userId   = user.id;
      if (user.role === 'region')   filters.region   = user.region;
      // admin sees everything

      const reports = dbGetReports(filters).sort((a, b) => b.date.localeCompare(a.date));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ reports }));
    } catch (err) {
      console.error('[GET /api/reports]', err.message);
      const code = err.message.includes('токен') || err.message.includes('Нет') ? 401 : 500;
      res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/imf?indicator=PCPIPCH — данные МВФ по одному индикатору Таджикистана
  if (req.method === 'GET' && req.url.startsWith('/api/imf')) {
    try {
      const urlObj    = new URL(req.url, 'http://localhost');
      const indicator = urlObj.searchParams.get('indicator');
      if (!indicator) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          error: 'Параметр indicator обязателен',
          example: '/api/imf?indicator=PCPIPCH',
          available: ['PCPIPCH', 'NGDP_RPCH', 'BCA_NGDPD', 'GGXWDG_NGDP', 'LUR', 'NGDPDPC'],
        }));
        return;
      }
      const country = urlObj.searchParams.get('country') || 'TJK';
      const { fetchIMF } = require('./dataCollector');
      const series = await fetchIMF(indicator, country);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ indicator, country, series, source: 'imf.org/external/datamapper' }));
    } catch (err) {
      console.error('[/api/imf]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/correlation — матрица корреляций
  if (req.method === 'GET' && req.url === '/api/correlation') {
    try {
      const { getPipelineData, runNightlyPipeline } = require('./dataPipeline');
      let pd = getPipelineData();

      // Если нет свежих данных — запускаем прямо сейчас
      if (!pd || !pd.correlations) {
        console.log('[/api/correlation] Нет данных пайплайна, запускаем ETL...');
        const result = await runNightlyPipeline();
        pd = result.pipelineData;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(pd?.correlations || { error: 'Нет данных' }));
    } catch (err) {
      console.error('[/api/correlation]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/nowcast — nowcasting ВВП
  if (req.method === 'GET' && req.url === '/api/nowcast') {
    try {
      const { getPipelineData, runNightlyPipeline } = require('./dataPipeline');
      let pd = getPipelineData();

      if (!pd || !pd.nowcast) {
        console.log('[/api/nowcast] Нет данных пайплайна, запускаем ETL...');
        const result = await runNightlyPipeline();
        pd = result.pipelineData;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(pd?.nowcast || { error: 'Нет данных' }));
    } catch (err) {
      console.error('[/api/nowcast]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/morning-digest — утренний дайджест
  if (req.method === 'GET' && req.url === '/api/morning-digest') {
    try {
      const { getMorningDigest, runNightlyPipeline } = require('./dataPipeline');
      let digest = getMorningDigest();

      if (!digest) {
        console.log('[/api/morning-digest] Нет дайджеста, запускаем ETL...');
        const result = await runNightlyPipeline();
        digest = result.digest;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(digest || { error: 'Нет данных' }));
    } catch (err) {
      console.error('[/api/morning-digest]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/health-index — индекс экономического здоровья
  if (req.method === 'GET' && req.url === '/api/health-index') {
    try {
      const { getPipelineData, runNightlyPipeline } = require('./dataPipeline');
      let pd = getPipelineData();

      if (!pd || !pd.healthIndex) {
        console.log('[/api/health-index] Нет данных, запускаем ETL...');
        const result = await runNightlyPipeline();
        pd = result.pipelineData;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(pd?.healthIndex || { error: 'Нет данных' }));
    } catch (err) {
      console.error('[/api/health-index]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/pipeline-log — лог пайплайна
  if (req.method === 'GET' && req.url.startsWith('/api/pipeline-log')) {
    try {
      const { getPipelineLog } = require('./dataPipeline');
      const qs    = new URL('http://x' + req.url).searchParams;
      const limit = Math.min(200, parseInt(qs.get('limit') || '50'));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ log: getPipelineLog(limit) }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // POST /api/pipeline-run — запустить ETL вручную
  if (req.method === 'POST' && req.url === '/api/pipeline-run') {
    try {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ ok: true, message: 'Пайплайн запущен в фоне' }));

      // Запускаем асинхронно чтобы не блокировать ответ
      const { runNightlyPipeline } = require('./dataPipeline');
      runNightlyPipeline().catch(e => console.error('[pipeline-run]', e.message));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/dashboard-data — агрегированные данные для дашборда
  if (req.method === 'GET' && req.url === '/api/dashboard-data') {
    const DASHBOARD_CACHE = path.join(__dirname, 'dashboard_cache.json');
    const DASHBOARD_TTL   = 60 * 60 * 1000; // 1 час

    // Отдаём из кэша если свежий
    try {
      if (fs.existsSync(DASHBOARD_CACHE)) {
        const cached = JSON.parse(fs.readFileSync(DASHBOARD_CACHE, 'utf8'));
        const age = Date.now() - new Date(cached.last_updated).getTime();
        if (age < DASHBOARD_TTL) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ...cached, from_cache: true }));
          return;
        }
      }
    } catch {}

    try {
      const {
        fetchNBT, fetchWorldBank,
        fetchOilPrice, fetchAluminumPrice, fetchWheatPrice,
        fetchIMFRealtime,
      } = require('./dataCollector');

      const [nbtR, wbR, oilR, alR, wheatR, imfR] = await Promise.allSettled([
        fetchNBT(),
        fetchWorldBank('FP.CPI.TOTL.ZG'),
        fetchOilPrice(),
        fetchAluminumPrice(),
        fetchWheatPrice(),
        fetchIMFRealtime(),
      ]);

      // Курсы НБТ
      let usd_tjs = null, eur_tjs = null, rub_tjs = null, nbtDate = null;
      if (nbtR.status === 'fulfilled') {
        const r = nbtR.value.rates;
        usd_tjs = r.USD?.rate ?? null;
        eur_tjs = r.EUR?.rate ?? null;
        rub_tjs = r.RUB?.rate ?? null;
        nbtDate = nbtR.value.date;
      }

      // ИПЦ из Всемирного банка — последнее значение
      let cpi = null, cpiYear = null;
      if (wbR.status === 'fulfilled') {
        const series = wbR.value.series.filter(s => s.value !== null);
        if (series.length > 0) {
          const last = series[series.length - 1];
          cpi     = last.value;
          cpiYear = last.year;
        }
      }

      // Товарные цены
      const oil_price       = oilR.status       === 'fulfilled' ? oilR.value.price       : null;
      const aluminum_price  = alR.status         === 'fulfilled' ? alR.value.price         : null;
      const wheat_price     = wheatR.status      === 'fulfilled' ? wheatR.value.price      : null;
      const oil_change_pct  = oilR.status        === 'fulfilled' ? oilR.value.changePct    : null;
      const wheat_change_pct = wheatR.status     === 'fulfilled' ? wheatR.value.changePct  : null;
      const al_change_pct   = alR.status          === 'fulfilled' ? alR.value.changePct     : null;

      const sources = [
        nbtR.status       === 'fulfilled' ? 'nbt.tj'           : null,
        wbR.status        === 'fulfilled' ? 'worldbank.org'    : null,
        oilR.status       === 'fulfilled' ? 'yahoo.finance'    : null,
      ].filter(Boolean);

      if (imfR.status === 'fulfilled') sources.push('imf.org');

      const data = {
        cpi,
        cpi_year:       cpiYear,
        usd_tjs,
        eur_tjs,
        rub_tjs,
        nbt_date:       nbtDate,
        oil_price,
        oil_change_pct,
        wheat_price,
        wheat_change_pct,
        aluminum_price,
        al_change_pct,
        imf:            imfR.status === 'fulfilled' ? imfR.value : null,
        last_updated:   new Date().toISOString(),
        source:         sources.join(', ') || 'нет данных',
        errors: {
          nbt:       nbtR.status  === 'rejected' ? nbtR.reason.message  : null,
          worldbank: wbR.status   === 'rejected' ? wbR.reason.message   : null,
          yahoo:     oilR.status  === 'rejected' ? oilR.reason.message  : null,
          imf:       imfR.status  === 'rejected' ? imfR.reason.message  : null,
        },
      };

      // Сохраняем кэш
      try { fs.writeFileSync(DASHBOARD_CACHE, JSON.stringify(data, null, 2), 'utf8'); } catch {}

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(data));
    } catch (err) {
      // Отдаём устаревший кэш если есть
      try {
        const stale = JSON.parse(fs.readFileSync(path.join(__dirname, 'dashboard_cache.json'), 'utf8'));
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ...stale, from_cache: true, stale: true }));
        return;
      } catch {}
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/smart-data — гибридные данные: районы + НБТ + МВФ
  if (req.method === 'GET' && req.url === '/api/smart-data') {
    try {
      const { mergeDataSources, getCachedSmartData } = require('./dataManager');

      let data;
      try {
        data = await mergeDataSources();
      } catch (fetchErr) {
        console.warn('[/api/smart-data] Ошибка загрузки, используем кэш:', fetchErr.message);
        data = getCachedSmartData();
        if (!data) throw fetchErr;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(data));
    } catch (err) {
      console.error('[/api/smart-data]', err.message);
      res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // GET /api/history?indicator=gdp — исторические данные по показателю
  if (req.method === 'GET' && req.url.startsWith('/api/history')) {
    try {
      const urlObj    = new URL(req.url, 'http://localhost');
      const indicator = urlObj.searchParams.get('indicator');
      const from      = parseInt(urlObj.searchParams.get('from') || '2015');
      const to        = parseInt(urlObj.searchParams.get('to')   || '2024');
      const hdb = require('./historicalDB');

      // /api/history/all — все данные
      if (req.url === '/api/history/all' || indicator === 'all') {
        const data = hdb.getAllHistory();
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(data));
      }

      if (!indicator) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({
          error: 'Параметр indicator обязателен',
          available: ['gdp','gdp_growth','gdp_per_capita','inflation','food_inflation',
                      'usd_tjs','eur_tjs','rub_tjs','remittances','remittances_pct_gdp',
                      'export','import','trade_balance','var','correlation','cge_calibration',
                      'last_year','crisis'],
        }));
      }

      let result;
      switch (indicator) {
        case 'gdp':
          result = hdb.getYearRange(hdb.getGDPHistory(), from, to);
          break;
        case 'inflation':
          result = hdb.getYearRange(hdb.getInflationHistory(), from, to);
          break;
        case 'exchange_rates':
          result = hdb.getYearRange(hdb.getExchangeRateHistory(), from, to);
          break;
        case 'remittances':
          result = hdb.getYearRange(hdb.getRemittancesHistory(), from, to);
          break;
        case 'trade':
          result = hdb.getYearRange(hdb.getTradeHistory(), from, to);
          break;
        case 'var':
          result = hdb.getVARData();
          break;
        case 'correlation':
          result = hdb.getCorrelationData();
          break;
        case 'cge_calibration':
          result = hdb.getCGECalibration();
          break;
        case 'last_year':
          result = hdb.getLastYear();
          break;
        case 'crisis':
          result = hdb.getCrisisContext(urlObj.searchParams.get('scenario') || 'oil');
          break;
        default:
          // Числовой ряд для прогнозирования
          result = {
            indicator,
            values: hdb.getDataForForecasting(indicator),
            years:  hdb.getGDPHistory().map(r => r.year),
            source: 'МЭРиТ / НБТ / Агентство по статистике РТ',
          };
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/history]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// Запускаем планировщик ночного ETL при старте сервера
(function initPipeline() {
  try {
    const { schedulePipeline } = require('./dataPipeline');
    schedulePipeline({ runNow: false }); // Не запускаем сразу, ждём 02:00
    console.log('[pipeline] Планировщик ETL активирован (02:00 ежедневно)');
  } catch (e) {
    console.warn('[pipeline] Не удалось запустить планировщик:', e.message);
  }
})();

// Инициализация NBT-парсера: загружаем курсы в фоне + авто-обновление каждые 24ч
(function initNBTParser() {
  try {
    const nbtParser = require('./nbtParser');
    // Фоновая загрузка курсов при старте
    nbtParser.saveRatesToDB()
      .then(r => console.log(`[nbtParser] Инициализация завершена: ${r.entries} записей, последняя: ${r.latest?.date}`))
      .catch(e => console.warn('[nbtParser] Ошибка инициализации:', e.message));
    // Авто-обновление каждые 24 ч
    nbtParser.startAutoRefresh();
  } catch (e) {
    console.warn('[nbtParser] Не удалось инициализировать:', e.message);
  }
})();

server.listen(PORT, () => {
  console.log(`EcotahlilAI server running at http://localhost:${PORT}`);
  if (!GROQ_API_KEY) {
    console.warn('WARNING: GROQ_API_KEY environment variable is not set');
  }
});
