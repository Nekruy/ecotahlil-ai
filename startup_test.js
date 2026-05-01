const http = require('http');
const PORT = process.env.PORT || 3000;

console.log('=== STARTUP TEST ===');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('PORT:', PORT);
console.log('Node.js:', process.version, process.platform);

const mods = [
  'tajik', 'database', 'auth',
  'forecasting', 'historicalDB',
  'nbtParser', 'dataPipeline', 'liveData',
  'masterDataLoader', 'dataCollector', 'cgeModel',
];

for (const m of mods) {
  try { require('./' + m); console.log('OK:', m); }
  catch (e) { console.log('FAIL:', m, '—', e.message); }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('startup_test OK\nNODE=' + process.version + '\nPORT=' + PORT);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('=== SERVER LISTENING on port', PORT, '===');
});
