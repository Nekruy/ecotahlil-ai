const http = require('http');
const fs = require('fs');
const path = require('path');
const Anthropic = require('@anthropic-ai/sdk');

const PORT = 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

const client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

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
      const n      = Math.max(1, Math.min(24, parseInt(periods) || 6));
      const result = var_model(data, n);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
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
      const { data, periods } = JSON.parse(body.toString('utf8'));

      const { garch } = require('./forecasting');

      const numData = (Array.isArray(data) ? data : []).map(Number).filter(v => !isNaN(v));
      if (numData.length < 10) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Для GARCH необходимо минимум 10 точек данных' }));
      }

      const n      = Math.max(1, Math.min(30, parseInt(periods) || 10));
      const result = garch(numData, n);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(result));
    } catch (err) {
      console.error('[/api/garch]', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Forecast endpoint
  if (req.method === 'POST' && req.url === '/forecast') {
    try {
      const body = await readBody(req);
      const { data, periods, method } = JSON.parse(body.toString('utf8'));

      const { arima, prophet, detectAnomalies } = require('./forecasting');

      const numData = (Array.isArray(data) ? data : []).map(Number).filter(v => !isNaN(v));
      if (numData.length < 4) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ error: 'Необходимо минимум 4 точки данных' }));
      }

      const n = Math.max(1, Math.min(24, parseInt(periods) || 6));

      let forecast;
      if (method === 'prophet') {
        forecast = prophet(numData, n);
      } else {
        forecast = arima(numData, n);
      }

      const anomalies = detectAnomalies(numData);

      // Метки для исторических данных
      const histLabels = numData.map((_, i) => `T${i + 1}`);

      // Метки для прогноза
      const forecastLabels = Array.from({ length: n }, (_, i) => `T${numData.length + i + 1}`);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        historical: numData,
        forecast,
        histLabels,
        forecastLabels,
        anomalies,
        method: method || 'arima',
        periods: n,
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

      const message = await client.messages.create({
        model: 'claude-sonnet-4-5',
        max_tokens: 4096,
        system: systemPrompt,
        messages: [{ role: 'user', content: userContent }],
      });

      const answer = message.content
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n');

      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(answer);
    } catch (err) {
      console.error('Error handling /webhook:', err);
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(`Server error: ${err.message}`);
    }
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, () => {
  console.log(`EcotahlilAI server running at http://localhost:${PORT}`);
  if (!ANTHROPIC_API_KEY) {
    console.warn('WARNING: ANTHROPIC_API_KEY environment variable is not set');
  }
});
