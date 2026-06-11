'use strict';
const fs   = require('fs');
const path = require('path');

const raw = fs.readFileSync(
  path.join(__dirname, 'system_prompt_macro_2026Q1.md'),
  'utf8'
);

// Strip служебные строки "# " (одиночный #), сохранить "## "
const SNAPSHOT = raw
  .split('\n')
  .filter(line => !/^#(?!#)/.test(line))
  .join('\n')
  .trim();

module.exports = { SNAPSHOT };
