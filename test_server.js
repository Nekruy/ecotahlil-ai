const http = require('http');
const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  console.log('[test] request:', req.method, req.url);
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK port=' + PORT + ' NODE_ENV=' + (process.env.NODE_ENV || 'none'));
});
server.listen(PORT, '0.0.0.0', () => {
  console.log('[test] TEST SERVER running on port', PORT, 'address=0.0.0.0');
  console.log('[test] process.env.PORT =', process.env.PORT);
  console.log('[test] Node.js', process.version, process.platform);
});
