#!/usr/bin/env node
'use strict';

// Exercise the real Rust example server. Use a disposable QA store: this test
// creates and archives a fictional project, preserving its append-only history.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const args = process.argv.slice(2);
function option(name, fallback) {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  if (!args[index + 1] || args[index + 1].startsWith('--')) throw new Error(`${name} requires a value`);
  return args[index + 1];
}
const url = option('--url', 'http://127.0.0.1:4192').replace(/\/$/, '');
const output = option('--output', 'artifacts/campus-ledger-browser');
fs.mkdirSync(output, { recursive: true });
const checks = [];
const projectId = `qa-campus-${Date.now()}`;
const projectTitle = `Campus energy research <example> ${projectId}`;
const errors = [];
let browser;

async function waitCount(page, selector, count) {
  await page.waitForFunction(({ selector, count }) => document.querySelectorAll(selector).length === count, { selector, count });
  assert.equal(await page.locator(selector).count(), count);
}
function assertPublic(value) {
  if (Array.isArray(value)) value.forEach(assertPublic);
  else if (value && typeof value === 'object') {
    for (const [key, child] of Object.entries(value)) {
      assert.ok(!['secret_key', 'private_key', 'signing_key', 'seed'].includes(key.toLowerCase()), `Signing-secret field in bundle: ${key}`);
      assertPublic(child);
    }
  }
}

(async () => {
  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'], ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE } : {}) });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(String(error)));
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.waitForFunction(() => document.querySelector('#new-project-button').disabled === false);
  const health = await (await page.request.get(`${url}/api/health`)).json();
  assert.equal(health.runtime, 'rust');
  assert.equal(health.info.writable, true);
  assert.equal(await page.locator('#mode-label').innerText(), 'Rust · local writer');
  checks.push('Actual Rust writer connected');

  for (const [width, height] of [[1440, 1000], [768, 1100], [390, 844]]) {
    await page.setViewportSize({ width, height });
    for (const view of ['projects', 'log', 'guide']) {
      await page.locator(`.nav-item[data-view="${view}"]`).click();
      assert.equal(await page.locator(`#view-${view}`).isVisible(), true);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, `Overflow: ${width}px ${view}`);
      await page.screenshot({ path: path.join(output, `${view}-${width}.png`), fullPage: true });
    }
    checks.push(`Three responsive views, no page overflow at ${width}px`);
  }

  await page.locator('.nav-item[data-view="projects"]').click();
  await page.locator('#new-project-button').click();
  const form = page.locator('#project-form');
  for (const [name, value] of Object.entries({ id: projectId, title: projectTitle, summary: 'Fictional browser verification project for a native Rust append-only university registry.', course: 'QA 301', supervisor: 'Dr Example', team: 'Alex Example, Morgan Example', tags: 'energy, rust' })) {
    await form.locator(`[name="${name}"]`).fill(value);
  }
  assert.equal(await form.locator('[name="status"]').isDisabled(), true);
  await page.screenshot({ path: path.join(output, 'create-mobile.png'), fullPage: true });
  await page.locator('#save-project-button').click();
  await page.waitForFunction(() => !document.querySelector('#project-dialog').open);
  await page.locator('#search').fill(projectTitle);
  await waitCount(page, '.project-card', 1);
  assert.equal(await page.locator('.project-card h3').innerText(), projectTitle);
  assert.equal(await page.locator('.project-card h3 example').count(), 0);
  await page.locator('#status-filter').selectOption('active');
  await waitCount(page, '.project-card', 0);
  await page.locator('#status-filter').selectOption('planned');
  await waitCount(page, '.project-card', 1);
  await page.locator('#course-filter').selectOption('QA 301');
  await waitCount(page, '.project-card', 1);
  checks.push('Create planned project; filters and literal user text work');

  await page.locator('.open-project').click();
  await waitCount(page, '#detail-content .event-row', 1);
  await page.locator('[data-mutation="edit"]').click();
  await form.locator('[name="status"]').selectOption('active');
  await form.locator('[name="summary"]').fill('Fictional edit: the energy study has begun.');
  await page.locator('#save-project-button').click();
  await page.waitForFunction(() => !document.querySelector('#project-dialog').open);
  await waitCount(page, '#detail-content .event-row', 2);
  assert.equal(await page.locator('#detail-content .status').innerText(), 'Active');
  checks.push('Edit appends version 2 and preserves creation event');

  await page.locator('[data-mutation="edit"]').click();
  await form.locator('[name="status"]').selectOption('completed');
  await page.locator('#save-project-button').click();
  await page.waitForFunction(() => !document.querySelector('#project-dialog').open);
  await waitCount(page, '#detail-content .event-row', 3);
  assert.equal(await page.locator('#detail-content .status').innerText(), 'Completed');
  await page.screenshot({ path: path.join(output, 'detail-mobile.png'), fullPage: true });
  checks.push('Valid active-to-completed transition and three-event history');

  await page.locator('[data-mutation="archive"]').click();
  await page.locator('#archive-form [name="reason"]').fill('Fictional browser verification completed; preserve its history.');
  await page.locator('#archive-submit').click();
  await page.waitForFunction(() => !document.querySelector('#archive-dialog').open);
  await waitCount(page, '#detail-content .event-row', 4);
  assert.equal(await page.locator('#detail-content .status').innerText(), 'Archived');
  assert.equal(await page.locator('[data-mutation]').count(), 0);
  await page.locator('#detail-dialog .close-dialog').click();
  await page.locator('#status-filter').selectOption('all');
  await waitCount(page, '.project-card', 1);
  assert.equal(await page.locator('.project-card .status').innerText(), 'Archived');
  checks.push('Archive appends event 4; history survives; all-status filter includes archive');

  await page.locator('.nav-item[data-view="log"]').click();
  await page.locator('#audit-button').click();
  await page.waitForFunction(() => document.querySelector('#audit-status').textContent === 'AUDIT COMPLETED');
  const audit = JSON.parse(await page.locator('#audit-report').innerText());
  assert.ok(audit && typeof audit === 'object' && !Array.isArray(audit));
  assert.equal(audit.signed_head_verified, true);
  assert.equal(audit.missing_blocks, 0);
  assert.equal(audit.verified_blocks, audit.length);
  assert.ok(audit.verified_blocks >= 4);
  checks.push('Real server audit verifies signed head and all stored blocks');
  const downloadPromise = page.waitForEvent('download');
  await page.locator('#export-button').click();
  const download = await downloadPromise;
  const downloadPath = path.join(output, download.suggestedFilename());
  await download.saveAs(downloadPath);
  const bundle = JSON.parse(fs.readFileSync(downloadPath, 'utf8'));
  assert.ok(bundle && typeof bundle === 'object' && !Array.isArray(bundle));
  assertPublic(bundle);
  checks.push('Signed JSON bundle downloaded with no private signing-key fields');

  await page.route('**/api/audit', route => route.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"Deliberate browser test failure"}' }));
  await page.locator('#audit-button').click();
  await page.waitForFunction(() => document.querySelector('#audit-status').textContent === 'AUDIT FAILED');
  assert.match(await page.locator('#audit-report').innerText(), /Deliberate browser test failure/);
  await page.unroute('**/api/audit');
  checks.push('Failed audit cannot retain a success label (injected HTTP failure)');

  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.locator('.nav-item[data-view="projects"]').click();
  await page.locator('#search').fill('');
  await page.locator('#course-filter').selectOption('all');
  await page.reload({ waitUntil: 'networkidle' });
  await page.waitForFunction(() => document.querySelector('#new-project-button').disabled === false);
  const archived = await (await page.request.get(`${url}/api/projects/${projectId}`)).json();
  assert.equal(archived.status, 'archived');
  assert.equal(archived.version, 4);
  checks.push('Reload reconstructs persisted archived version 4');

  await context.setOffline(true);
  await page.locator('#refresh-button').click();
  await page.waitForFunction(() => document.querySelector('#mode-label').textContent === 'Connection unavailable');
  assert.equal(await page.locator('#new-project-button').isDisabled(), true);
  assert.match(await page.locator('#global-alert').innerText(), /Previously loaded records remain visible/);
  await context.setOffline(false);
  await page.locator('#refresh-button').click();
  await page.waitForFunction(() => document.querySelector('#new-project-button').disabled === false);
  checks.push('Network failure retains visible data, disables mutation and recovers on refresh');

  const replicaHealth = { ...health, info: { ...health.info, writable: false } };
  await page.route('**/api/health', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(replicaHealth) }));
  await page.locator('#refresh-button').click();
  await page.waitForFunction(() => document.querySelector('#mode-label').textContent === 'Rust · read-only replica');
  assert.equal(await page.locator('#new-project-button').isDisabled(), true);
  assert.match(await page.locator('#global-alert').innerText(), /Read-only replica/);
  await page.unroute('**/api/health');
  checks.push('Read-only UI state disables mutation (injected replica health contract)');
  assert.deepEqual(errors, []);
  checks.push('No browser JavaScript errors');
  const report = { url, project_id: projectId, checks_passed: checks.length, checks, page_errors: errors, screenshots: fs.readdirSync(output).filter(file => file.endsWith('.png')).sort() };
  fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify(report, null, 2));
})().catch(error => {
  fs.writeFileSync(path.join(output, 'failure.json'), JSON.stringify({ url, project_id: projectId, checks_completed: checks, page_errors: errors, error: String(error.stack || error) }, null, 2) + '\n');
  console.error(error);
  process.exitCode = 1;
}).finally(async () => { if (browser) await browser.close(); });
