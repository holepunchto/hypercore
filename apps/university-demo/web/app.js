'use strict';

const $ = (selector, root = document) => root.querySelector(selector);
const state = { projects: [], health: null, events: [], view: 'projects', loaded: false, editing: null, selected: null, busy: false, refreshing: false, detailRequest: 0 };
let toastTimer;
const statusNames = { planned: 'Planned', active: 'Active', completed: 'Completed', archived: 'Archived' };

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = String(text);
  return node;
}
function date(value, includeTime = false) {
  const parsed = new Date(value);
  if (!value || !Number.isFinite(parsed.getTime())) return 'Date unavailable';
  return parsed.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', ...(includeTime ? { hour: '2-digit', minute: '2-digit' } : {}) });
}
function number(value) { return typeof value === 'number' ? value.toLocaleString() : '—'; }
function toast(message) {
  const node = $('#toast');
  node.textContent = message;
  node.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { node.hidden = true; }, 5500);
}
function alertMessage(message) {
  $('#global-alert').textContent = message;
  $('#global-alert').hidden = !message;
}
async function api(path, options = {}) {
  const response = await fetch(path, { cache: 'no-store', ...options, headers: { Accept: 'application/json', ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers } });
  const content = await response.text();
  let data;
  try { data = JSON.parse(content); } catch { throw new Error(`The Rust server returned a non-JSON response (${response.status}).`); }
  if (!response.ok) throw new Error(typeof data.error === 'string' ? data.error : `Request failed (${response.status}).`);
  return data;
}
function writable() { return state.loaded && state.health?.info?.writable === true; }
function updateControls() {
  $('#new-project-button').disabled = !writable() || state.busy;
  $('#audit-button').disabled = !state.loaded || state.busy;
  $('#export-button').disabled = !state.loaded || state.busy;
  $('#refresh-button').disabled = state.refreshing || state.busy;
  $('#save-project-button').disabled = !writable() || state.busy;
  $('#archive-submit').disabled = !writable() || state.busy;
  document.querySelectorAll('[data-mutation]').forEach(node => { node.disabled = !writable() || state.busy; });
}
function setView(view) {
  state.view = ['projects', 'log', 'guide'].includes(view) ? view : 'projects';
  document.querySelectorAll('.view').forEach(node => { node.hidden = node.id !== `view-${state.view}`; });
  document.querySelectorAll('.nav-item').forEach(node => {
    const selected = node.dataset.view === state.view;
    node.classList.toggle('selected', selected);
    if (selected) node.setAttribute('aria-current', 'page'); else node.removeAttribute('aria-current');
  });
  $('#breadcrumb-page').textContent = { projects: 'Project registry', log: 'Verified log', guide: 'API & learning' }[state.view];
  history.replaceState(null, '', `#${state.view}`);
}
function statusBadge(status) { return el('span', `status ${Object.hasOwn(statusNames, status) ? status : ''}`, statusNames[status] || status); }
function initials(team) { return (team?.[0] || '?').split(/\s+/).filter(Boolean).slice(0, 2).map(value => value[0]).join('').toUpperCase(); }
function tagsList(tags) {
  const container = el('div', 'tags');
  (tags || []).forEach(tag => container.append(el('span', 'tag', tag)));
  return container;
}
function card(project) {
  const article = el('article', 'project-card');
  const top = el('div', 'card-top');
  top.append(el('span', 'course-label', project.course), statusBadge(project.status));
  article.append(top, el('h3', '', project.title), el('p', 'summary', project.summary), tagsList(project.tags));
  const bottom = el('div', 'card-bottom');
  const team = el('span', 'team-line');
  team.append(el('span', 'avatar', initials(project.team)), document.createTextNode(`${project.team?.length || 0} team member${project.team?.length === 1 ? '' : 's'}`));
  const open = el('button', 'open-project', 'View project ↗');
  open.setAttribute('aria-label', `View project: ${project.title}`);
  open.addEventListener('click', () => openProject(project.id));
  bottom.append(team, open);
  article.append(bottom, el('p', 'card-update', `Updated ${date(project.updated_at)} · Version ${project.version}`));
  return article;
}
function renderProjects() {
  const query = $('#search').value.trim().toLocaleLowerCase();
  const status = $('#status-filter').value;
  const course = $('#course-filter').value;
  const matches = state.projects.filter(project => {
    const text = [project.title, project.summary, project.course, project.supervisor, ...(project.team || []), ...(project.tags || [])].join(' ').toLocaleLowerCase();
    return (!query || text.includes(query)) && (status === 'all' || project.status === status) && (course === 'all' || project.course === course);
  }).sort((a, b) => String(b.updated_at).localeCompare(String(a.updated_at)) || String(a.title).localeCompare(String(b.title)));
  $('#project-count').textContent = number(matches.length);
  const list = $('#project-list');
  list.replaceChildren();
  if (!matches.length) {
    const empty = el('div', 'empty-state');
    empty.append(el('strong', '', state.projects.length ? 'No projects match those filters.' : 'Make room for your first idea.'), el('p', '', state.projects.length ? 'Try another course, status or search term.' : 'Create a fictional university project to start its append-only history.'));
    list.append(empty);
  } else matches.forEach(project => list.append(card(project)));
}
function renderStats() {
  const stats = state.health?.stats || {};
  for (const [id, key] of [['all', 'all'], ['active', 'active'], ['completed', 'completed'], ['events', 'event_count']]) $(`#stat-${id}`).textContent = number(stats[key]);
}
function renderIdentity() {
  const info = state.health?.info || {};
  $('#public-key').textContent = info.public_key || 'Public key unavailable';
  $('#copy-key-button').disabled = !info.public_key;
  const facts = $('#log-facts');
  facts.replaceChildren();
  [['Log blocks', number(info.length)], ['Payload bytes', number(info.byte_length)], ['Contiguous blocks', number(info.contiguous_length)], ['Fork', number(info.fork)], ['Store mode', info.writable ? 'Writer — append enabled' : 'Read-only replica'], ['Runtime', state.health?.runtime || 'Unknown']].forEach(([name, value]) => {
    const row = el('div'); row.append(el('dt', '', name), el('dd', '', value)); facts.append(row);
  });
  $('#mode-label').textContent = info.writable ? 'Rust · local writer' : 'Rust · read-only replica';
  $('#mode-label').title = info.writable ? 'Local teaching server. No authentication or encryption.' : 'This replica can be inspected and audited but cannot append project events.';
}
function eventKind(event) {
  const kind = event.kind;
  if (typeof kind === 'string') return kind.replace(/[_-]/g, ' ');
  if (kind && typeof kind === 'object') {
    const label = kind.type || kind.tag || Object.keys(kind)[0] || 'project event';
    return String(label).replace(/[_-]/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2');
  }
  return 'Project event';
}
function eventRow(event) {
  const row = el('article', 'event-row');
  row.append(el('span', 'event-seq', event.sequence ?? '—'));
  const content = el('div');
  const project = state.projects.find(value => value.id === event.project_id);
  content.append(el('h3', '', `${eventKind(event)} · ${project?.title || event.project_id || 'Project'}`), el('p', '', `Recorded actor: ${event.actor || 'Not specified'}`));
  const details = el('details');
  details.append(el('summary', '', 'Inspect event payload'), el('pre', '', JSON.stringify(event, null, 2)));
  content.append(details);
  const time = el('time', '', date(event.occurred_at, true));
  if (Number.isFinite(Date.parse(event.occurred_at))) time.dateTime = event.occurred_at;
  row.append(content, time);
  return row;
}
function renderEvents() {
  $('#event-count').textContent = number(state.events.length);
  const list = $('#event-list');
  list.replaceChildren();
  if (!state.events.length) list.append(el('p', 'empty-state', 'No recorded events yet. Create a project to begin the log.'));
  else [...state.events].sort((a, b) => b.sequence - a.sequence).forEach(event => list.append(eventRow(event)));
}
async function refresh({ silent = false } = {}) {
  if (state.refreshing) return;
  state.refreshing = true; updateControls();
  try {
    const [health, projects, events] = await Promise.all([api('/api/health'), api('/api/projects'), api('/api/events')]);
    if (!Array.isArray(projects) || !Array.isArray(events) || !health.info || !health.stats) throw new Error('The server response did not match the Campus Ledger API.');
    state.health = health; state.projects = projects; state.events = events; state.loaded = true;
    const selectedCourse = $('#course-filter').value;
    const courseFilter = $('#course-filter'); courseFilter.replaceChildren(new Option('All courses', 'all'));
    [...new Set(projects.map(project => project.course))].sort().forEach(course => courseFilter.append(new Option(course, course)));
    if ([...courseFilter.options].some(option => option.value === selectedCourse)) courseFilter.value = selectedCourse;
    renderStats(); renderProjects(); renderIdentity(); renderEvents();
    $('#connection-state').textContent = 'Connected to Rust server';
    $('#connection-dot').classList.add('connected');
    alertMessage(health.info.writable ? '' : 'Read-only replica: project history, audit and export are available. Creating, editing and archiving require the writer.');
    if (!silent) toast('Registry refreshed from the Rust server.');
  } catch (error) {
    $('#connection-state').textContent = 'Server unavailable';
    $('#connection-dot').classList.remove('connected');
    $('#mode-label').textContent = 'Connection unavailable';
    state.loaded = false;
    alertMessage(`${error.message} ${state.projects.length ? 'Previously loaded records remain visible; mutations are disabled until refresh succeeds.' : 'Start the Rust example server, then refresh.'}`);
    if (!state.projects.length) $('#project-list').replaceChildren(el('p', 'empty-state', 'Waiting for the Rust server. No project data has been loaded.'));
  } finally { state.refreshing = false; updateControls(); }
}
function showDialog(dialog) {
  if (!dialog.open) dialog.showModal();
}
function fillForm(project) {
  const form = $('#project-form'); form.reset();
  $('#project-id-field').hidden = Boolean(project);
  form.elements.namedItem('id').disabled = Boolean(project);
  const statusField = form.elements.namedItem('status');
  const allowed = project ? { planned: ['planned', 'active'], active: ['active', 'completed'], completed: ['completed', 'active'] }[project.status] : ['planned'];
  statusField.replaceChildren(...allowed.map(status => new Option(statusNames[status], status)));
  statusField.disabled = !project;
  for (const field of ['id', 'title', 'summary', 'course', 'supervisor', 'status']) if (project) form.elements.namedItem(field).value = project[field];
  for (const field of ['team', 'tags']) form.elements.namedItem(field).value = project?.[field]?.join(', ') || '';
  $('#form-error').hidden = true;
  $('#form-title').textContent = project ? 'Continue the project’s story.' : 'A new chapter starts here.';
  $('#form-eyebrow').textContent = project ? `APPEND AN EDIT · CURRENT VERSION ${project.version}` : 'ADD TO THE REGISTRY';
  $('#form-description').textContent = project ? 'This saves a new event. Earlier versions remain in the log.' : 'Create a project. Its history starts with this first recorded event.';
  $('#save-project-button').textContent = project ? 'Append project edit' : 'Create project';
}
function editProject(project = null) {
  if (!writable() || state.busy) return;
  state.editing = project;
  fillForm(project);
  showDialog($('#project-dialog'));
  $('#project-form input[name="title"]').focus();
}
function detailFacts(project) {
  const facts = el('dl', 'detail-facts');
  [['Course', project.course], ['Supervisor', project.supervisor], ['Team', project.team.join(', ')], ['Version', project.version], ['Created', date(project.created_at, true)], ['Updated', date(project.updated_at, true)], ['Project ID', project.id]].forEach(([name, value]) => {
    const row = el('div'); row.append(el('dt', '', name), el('dd', '', value)); facts.append(row);
  });
  return facts;
}
async function openProject(id) {
  const request = ++state.detailRequest;
  const content = $('#detail-content');
  content.replaceChildren(el('h2', '', 'Loading project…'));
  content.firstChild.id = 'detail-title';
  showDialog($('#detail-dialog'));
  try {
    const project = await api(`/api/projects/${encodeURIComponent(id)}`);
    if (request !== state.detailRequest || !$('#detail-dialog').open) return;
    state.selected = project;
    const title = el('h2', '', project.title); title.id = 'detail-title';
    const header = el('div', 'detail-header'); header.append(statusBadge(project.status), el('span', 'course-label', project.course));
    content.replaceChildren(header, title, el('p', 'detail-summary', project.summary), tagsList(project.tags), detailFacts(project));
    if (project.status !== 'archived') {
      const actions = el('div', 'detail-actions');
      const edit = el('button', 'button primary', 'Edit project'); edit.dataset.mutation = 'edit'; edit.addEventListener('click', () => editProject(project));
      const archive = el('button', 'button secondary', 'Archive project'); archive.dataset.mutation = 'archive'; archive.addEventListener('click', () => {
        if (!writable() || state.busy) return;
        state.selected = project; $('#archive-form').reset(); $('#archive-error').hidden = true; showDialog($('#archive-dialog')); $('#archive-form textarea').focus();
      });
      actions.append(edit, archive); content.append(actions);
    } else content.append(el('p', 'replica-note', 'Archived project. The project and all of its earlier events remain in the log.'));
    if (!writable()) content.append(el('p', 'replica-note', 'Read-only view. Connect to a writable server to append changes.'));
    content.append(el('h3', 'detail-history-heading', 'The project’s history'));
    const historyContainer = el('div', 'event-list'); historyContainer.append(el('p', 'empty-state', 'Loading recorded events…')); content.append(historyContainer);
    updateControls();
    try {
      const events = await api(`/api/projects/${encodeURIComponent(id)}/history`);
      if (request !== state.detailRequest) return;
      if (!Array.isArray(events)) throw new Error('Invalid event history returned by the server.');
      historyContainer.replaceChildren();
      if (!events.length) historyContainer.append(el('p', 'empty-state', 'No history was returned.'));
      else [...events].sort((a, b) => b.sequence - a.sequence).forEach(event => historyContainer.append(eventRow(event)));
    } catch (error) { historyContainer.replaceChildren(el('p', 'history-error', `History could not be loaded: ${error.message}`)); }
  } catch (error) {
    if (request !== state.detailRequest) return;
    const title = el('h2', '', 'Project unavailable'); title.id = 'detail-title';
    content.replaceChildren(title, el('p', 'form-error', error.message));
  }
}
function csv(value) { return String(value).split(',').map(item => item.trim()).filter(Boolean); }
function bounded(label, value, max, empty = false) {
  if ((!empty && !value.trim()) || new TextEncoder().encode(value).length > max || /[\x00-\x1f\x7f-\x9f]/.test(value)) throw new Error(`${label} must be ${empty ? '0' : '1'}–${max} UTF-8 bytes without line breaks or control characters.`);
}
function validateFields(values) {
  for (const [label, key, limit, empty] of [['Title', 'title', 200], ['Summary', 'summary', 8000, true], ['Course', 'course', 120], ['Supervisor', 'supervisor', 160], ['Recorded actor', 'actor', 160]]) bounded(label, values[key], limit, empty);
  for (const [label, key, maxCount, byteLimit] of [['Team member', 'team', 30, 160], ['Tag', 'tags', 20, 64]]) {
    if (values[key].length > maxCount) throw new Error(`Use at most ${maxCount} ${key}.`);
    const seen = new Set();
    for (const item of values[key]) { bounded(label, item, byteLimit); const normalized = item.trim().toLowerCase(); if (seen.has(normalized)) throw new Error(`Duplicate ${label.toLowerCase()}: ${item}`); seen.add(normalized); }
  }
}
function formError(selector, message) { $(selector).textContent = message; $(selector).hidden = false; }
$('#project-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (!writable() || state.busy) return;
  const data = new FormData(event.currentTarget);
  const values = {};
  for (const key of ['title', 'summary', 'course', 'supervisor', 'actor']) values[key] = String(data.get(key)).trim();
  values.status = state.editing ? String(data.get('status')) : 'planned';
  if (!state.editing) values.id = String(data.get('id')).trim();
  values.team = csv(data.get('team')); values.tags = csv(data.get('tags'));
  if (!values.team.length) { formError('#form-error', 'Add at least one fictional team member.'); return; }
  try { validateFields(values); } catch (error) { formError('#form-error', error.message); return; }
  const editing = state.editing;
  if (editing) values.expected_version = editing.version;
  state.busy = true; updateControls(); $('#form-error').hidden = true;
  try {
    const project = await api(editing ? `/api/projects/${encodeURIComponent(editing.id)}` : '/api/projects', { method: editing ? 'PATCH' : 'POST', body: JSON.stringify(values) });
    $('#project-dialog').close();
    toast(editing ? 'Project edit appended to the log.' : 'Project created and recorded in the log.');
    await refresh({ silent: true });
    if ($('#detail-dialog').open || editing) await openProject(project.id);
  } catch (error) { formError('#form-error', `${error.message} Your inputs are still here. For a version conflict, close this form and reopen the latest project before editing again.`); }
  finally { state.busy = false; updateControls(); }
});
$('#archive-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (!writable() || state.busy || !state.selected) return;
  const data = new FormData(event.currentTarget), project = state.selected;
  const values = { expected_version: project.version, actor: String(data.get('actor')).trim(), reason: String(data.get('reason')).trim() };
  try { bounded('Recorded actor', values.actor, 160); bounded('Archive reason', values.reason, 2000); } catch (error) { formError('#archive-error', error.message); return; }
  state.busy = true; updateControls(); $('#archive-error').hidden = true;
  try {
    await api(`/api/projects/${encodeURIComponent(project.id)}/archive`, { method: 'POST', body: JSON.stringify(values) });
    $('#archive-dialog').close(); toast('Archive event appended. Earlier history is preserved.');
    await refresh({ silent: true }); await openProject(project.id);
  } catch (error) { formError('#archive-error', `${error.message} Close and reopen the project if its version has changed.`); }
  finally { state.busy = false; updateControls(); }
});
$('#audit-button').addEventListener('click', async () => {
  if (state.busy || !state.loaded) return;
  state.busy = true; updateControls(); $('#audit-button').textContent = 'Auditing stored log…';
  $('#audit-panel').hidden = false; $('#audit-status').textContent = 'RUNNING'; $('#audit-report').textContent = 'Waiting for the Rust audit…';
  try {
    const report = await api('/api/audit', { method: 'POST' });
    $('#audit-status').textContent = 'AUDIT COMPLETED'; $('#audit-report').textContent = JSON.stringify(report, null, 2);
    toast('Integrity audit completed. Inspect the report for verification details.');
  } catch (error) { $('#audit-status').textContent = 'AUDIT FAILED'; $('#audit-report').textContent = error.message; }
  finally { state.busy = false; $('#audit-button').textContent = 'Run integrity audit'; updateControls(); }
});
$('#export-button').addEventListener('click', async () => {
  if (state.busy || !state.loaded) return;
  state.busy = true; updateControls();
  try {
    const bundle = await api('/api/export');
    const url = URL.createObjectURL(new Blob([JSON.stringify(bundle, null, 2) + '\n'], { type: 'application/json' }));
    const link = el('a'); link.href = url; link.download = `campus-ledger-signed-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 30000);
    toast('Signed bundle downloaded. No private signing key is included.');
  } catch (error) { alertMessage(`Export failed: ${error.message}`); }
  finally { state.busy = false; updateControls(); }
});
$('#copy-key-button').addEventListener('click', async () => {
  const key = state.health?.info?.public_key;
  if (!key) return;
  try { await navigator.clipboard.writeText(key); toast('Public writer key copied.'); }
  catch { const selection = window.getSelection(), range = document.createRange(); range.selectNodeContents($('#public-key')); selection.removeAllRanges(); selection.addRange(range); toast('Select and copy the highlighted public key. Clipboard access is unavailable.'); }
});
$('#refresh-button').addEventListener('click', () => refresh());
$('#new-project-button').addEventListener('click', () => editProject());
for (const selector of ['#search', '#status-filter', '#course-filter']) $(selector).addEventListener(selector === '#search' ? 'input' : 'change', renderProjects);
document.querySelectorAll('[data-view]').forEach(button => button.addEventListener('click', () => setView(button.dataset.view)));
document.querySelectorAll('.close-dialog').forEach(button => button.addEventListener('click', () => button.closest('dialog').close()));
$('#detail-dialog').addEventListener('close', () => { state.detailRequest++; });
$('.brand').addEventListener('click', event => { event.preventDefault(); setView('projects'); });
window.addEventListener('hashchange', () => setView(location.hash.slice(1)));
$('#api-example').textContent = `# Start the native Rust example (from the repository root)\ncargo run -p university-demo -- --help\n\n# Read the current project registry\ncurl '${location.origin}/api/projects'\n\n# Verify stored log integrity\ncurl -X POST '${location.origin}/api/audit'\n\n# Export signed data (not the private signing key)\ncurl '${location.origin}/api/export' -o signed-log.json`;
setView(location.hash.slice(1));
updateControls();
refresh({ silent: true });
