/**
 * TITLE: Discovery Engine Pipeline - Filter-V2
 * SHEET: Discovery AI - Filter-V2 (Nandan Filter)
 * REFERENCE: Tracxn SR Publishing Architecture
 */

// --- Configuration ---
const CONFIG = {
  TYPE: 'Filter-V2',
  SHEET_NAME: 'Discovery AI - Filter-V2 (Nandan Filter)',
  WORKER_URLS: [
    { name: '5321-Filter-V2-Pipeline', url: 'https://5321-sheet4.rajath.world', hostId: '5321', isProduction: true },
    { name: '5080-Filter-V2-Pipeline', url: 'https://5080-sheet4.rajath.world', hostId: '5080', isProduction: true },
    { name: 'Vishnu-Filter-V2-Pipeline', url: 'https://vishnu-sheet4.rajath.world', hostId: 'Vishnu', isTesting: true }
  ],
  AUTH_TOKEN: '', // Loaded dynamically from Script Properties (SERVICE_AUTH_TOKEN)
  DEFAULT_MODE: 'full',
  MAX_RETRIES: 3,
  ACTIVE_WORKER_PROP: 'DISCOVERY_ACTIVE_WORKER_SHEET_4',
  MAX_DEVICE_SHEETS: 2
};

/**
 * Retrieves the authorization token for worker FastAPI endpoints.
 * Prioritizes Script Properties (SERVICE_AUTH_TOKEN) or Document Properties,
 * falling back to CONFIG.AUTH_TOKEN.
 */
function getAuthToken_() {
  try {
    const sp = PropertiesService.getScriptProperties().getProperty('SERVICE_AUTH_TOKEN');
    if (sp && sp.trim()) return sp.trim();
  } catch (e) {}

  try {
    const dp = PropertiesService.getDocumentProperties().getProperty('SERVICE_AUTH_TOKEN');
    if (dp && dp.trim()) return dp.trim();
  } catch (e) {}

  return CONFIG.AUTH_TOKEN || '';
}

/**
 * Menu action allowing operators to configure or update the Service Auth Token
 * securely without modifying script source code.
 */
function uiSetServiceAuthToken() {
  const ui = SpreadsheetApp.getUi();
  const currentToken = getAuthToken_();
  const masked = currentToken ? (currentToken.substring(0, 3) + '***' + currentToken.slice(-2)) : 'Not Set';

  const res = ui.prompt(
    '🔑 Set Service Auth Token',
    'Current Token: ' + masked + '\n\nEnter the new Service Auth Token (matches SERVICE_AUTH_TOKEN in .env):',
    ui.ButtonSet.OK_CANCEL
  );

  if (res.getSelectedButton() === ui.Button.OK) {
    const newToken = res.getResponseText().trim();
    if (newToken) {
      PropertiesService.getScriptProperties().setProperty('SERVICE_AUTH_TOKEN', newToken);
      ui.alert('✅ Token Saved', 'Service Auth Token has been securely stored in Script Properties.', ui.ButtonSet.OK);
    } else {
      ui.alert('⚠️ Warning', 'Token cannot be empty.', ui.ButtonSet.OK);
    }
  }
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('\uD83D\uDE80 Discovery Engine (Filter-V2)')
    .addItem('\u25B6\uFE0F Run Pipeline', 'uiStartRun')
    .addItem('\uD83D\uDD04 Clear & Fetch Domains', 'uiClearAndFetchDomains')
    .addSeparator()
    .addItem('\uD83D\uDEA6 Check Status', 'uiCheckStatus')
    .addItem('\uD83D\uDCCA Check Health', 'uiCheckHealth')
    .addItem("🔑 Set Auth Token", "uiSetServiceAuthToken")
    .addSeparator()
    .addItem("🧹 Clean Sheet", "cleanSheet")
    .addItem("\u26A0\uFE0F Force Reset", "forceReset")
    .addToUi();
}

function uiStartRun(resumeOnly = false) {
  const isResuming = resumeOnly === true;

  let htmlContent = '<html><head>';
  htmlContent += '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">';
  htmlContent += '<style>';
  htmlContent += 'body{font-family:"Inter",sans-serif;background:#f8fafc;color:#1e2937;padding:20px;font-size:14px;margin:0;}';
  htmlContent += '.btn-primary{background:#2563eb;color:white;border:none;border-radius:8px;font-weight:600;cursor:pointer;padding:12px;}';
  htmlContent += '.btn-primary:hover{background:#1d4ed8;}';
  htmlContent += '.btn-secondary{background:white;color:#475569;border:1px solid #cbd5e1;border-radius:8px;font-weight:600;cursor:pointer;padding:12px;}';
  htmlContent += '.btn-secondary:hover{background:#f1f5f9;}';
  htmlContent += '</style>';
  htmlContent += '</head><body>';

  htmlContent += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;padding-bottom:14px;border-bottom:1px solid #e2e8f0;">';
  htmlContent += '<h1 style="font-size:17px;font-weight:700;margin:0;color:#0f172a;">\uD83D\uDE80 Discovery Engine (Filter-V2)</h1>';
  htmlContent += '<span style="font-size:12px;background:#e0f2fe;color:#0369a1;padding:4px 8px;border-radius:6px;font-weight:600;">Discovery AI - Filter-V2 (Nandan Filter)</span>';
  htmlContent += '</div>';

  // Step 1: Worker Select
  htmlContent += '<div id="worker-view"' + (isResuming ? ' style="display:none;"' : '') + '>';
  htmlContent += '<p style="color:#64748b;font-size:13px;margin-top:0;">Select a production desktop (max 2 sheets per device) or test on Vishnu:</p>';
  htmlContent += '<div id="worker-list" style="display:flex;flex-direction:column;gap:10px;margin-bottom:20px;">';
  htmlContent += '<div style="padding:16px;text-align:center;color:#64748b;background:white;border-radius:8px;border:1px solid #e2e8f0;">Loading workers...</div>';
  htmlContent += '</div>';

  htmlContent += '<div style="display:flex;gap:10px;">';
  htmlContent += '<button id="fetch-btn" onclick="fetchStatuses()" class="btn-secondary" style="flex:1;">\uD83D\uDD04 Refresh Status</button>';
  htmlContent += '<button id="next-btn" onclick="goToSetup()" class="btn-primary" style="flex:1;" disabled>Next \u2192</button>';
  htmlContent += '</div></div>';

  // Step 2: Setup
  htmlContent += '<div id="setup-view" style="display:none;">';
  htmlContent += '<div style="background:white;border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin-bottom:16px;">';
  htmlContent += '<div id="selected-worker-label" style="background:#f0f9ff;color:#0369a1;padding:10px 12px;border-radius:8px;font-weight:600;margin-bottom:14px;font-size:13px;">\u2705 <span id="selected-name"></span></div>';

  htmlContent += '<label style="display:block;font-weight:600;font-size:13px;color:#475569;margin-bottom:6px;">\uD83D\uDD22 Start Row</label>';
  htmlContent += '<input type="number" id="startRow" value="3" min="3" style="width:100%;box-sizing:border-box;padding:10px;border:1px solid #cbd5e1;border-radius:8px;font-size:14px;margin-bottom:14px;">';

  htmlContent += '<label style="display:block;font-weight:600;font-size:13px;color:#475569;margin-bottom:6px;">\u2699\uFE0F Execution Phase / Mode</label>';
  htmlContent += '<select id="runMode" style="width:100%;box-sizing:border-box;padding:10px;border:1px solid #cbd5e1;border-radius:8px;font-size:13.5px;background:white;color:#1e293b;font-weight:500;">';
  htmlContent += '<option value="full">\uD83D\uDE80 1: Full Run Mode (Smart Auto-Resume)</option>';
  htmlContent += '<option value="phase1">\uD83D\uDD77\uFE0F 2: Phase 1 Only: Tech Crawler Only</option>';
  htmlContent += '<option value="phase2">\uD83E\uDDE0 3: Phase 2 Only: NIM Summary Generation</option>';
  htmlContent += '<option value="phase3">\uD83D\uDD2E 4: Phase 3 Only: Gemini Predictions</option>';
  htmlContent += '<option value="phase4">\uD83D\uDCDD 5: Phase 4 Only: Tracxn Write API</option>';
  htmlContent += '</select>';

  htmlContent += '<div id="phase-box" style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:12px;margin-top:14px;">';
  htmlContent += '<div style="font-weight:600;color:#1e40af;font-size:13px;">Target: Filter-V2 Pipeline</div>';
  htmlContent += '<div style="font-size:12px;color:#3b82f6;margin-top:4px;">Domain Scraping + Gemini/NIM Prediction + Tracxn Write APIs</div>';
  htmlContent += '</div></div>';

  htmlContent += '<div style="display:flex;gap:10px;">';
  htmlContent += '<button onclick="goToWorkers()" class="btn-secondary" style="flex:1;">\u2190 Back</button>';
  htmlContent += '<button onclick="runPipeline()" class="btn-primary" style="flex:2;">\u25B6\uFE0F Start Pipeline</button>';
  htmlContent += '</div></div>';

  // Step 3: Progress
  htmlContent += '<div id="progress-view" style="display:none;">';
  htmlContent += '<div id="reroute-alert" style="display:none;background:#fef3c7;border:1px solid #f59e0b;color:#92400e;padding:12px;border-radius:8px;font-size:12.5px;font-weight:600;margin-bottom:14px;line-height:1.4;"></div>';
  htmlContent += '<div style="background:white;border:1px solid #e2e8f0;border-radius:12px;padding:16px;">';
  htmlContent += '<div id="status-text" style="font-weight:600;margin-bottom:10px;color:#1e293b;">Connecting to worker...</div>';
  htmlContent += '<div style="height:10px;background:#e2e8f0;border-radius:10px;margin:14px 0;overflow:hidden;">';
  htmlContent += '<div id="progress-fill" style="height:100%;background:#2563eb;width:0%;border-radius:10px;transition:width 0.4s ease;"></div>';
  htmlContent += '</div>';
  htmlContent += '<div style="display:flex;justify-content:space-between;font-size:13px;font-weight:500;">';
  htmlContent += '<span id="count-text" style="color:#475569;">0 / 0 rows</span>';
  htmlContent += '<span id="yield-text" style="color:#10b981;font-weight:600;">0 Successful</span>';
  htmlContent += '</div>';
  htmlContent += '<div id="worker-info-progress" style="font-size:12px;color:#64748b;text-align:center;margin-top:12px;"></div>';
  htmlContent += '</div>';

  htmlContent += '<div id="completion-info" style="display:none;background:#f0fdf4;border:1px solid #86efac;border-radius:10px;padding:14px;text-align:center;margin-top:16px;">';
  htmlContent += '<strong style="color:#166534;font-size:14px;">\u2728 Run Completed</strong>';
  htmlContent += '<div id="completion-msg" style="margin-top:6px;color:#15803d;font-size:12.5px;"></div>';
  htmlContent += '</div>';

  htmlContent += '<div style="display:flex;gap:10px;margin-top:20px;">';
  htmlContent += '<button id="stop-btn" onclick="cancelRun()" style="flex:1;padding:12px;background:#fee2e2;color:#b91c1c;border:1px solid #fecaca;border-radius:8px;font-weight:600;cursor:pointer;">\u23F9\uFE0F Stop Pipeline</button>';
  htmlContent += '<button onclick="google.script.host.close()" class="btn-secondary" style="flex:1;">Close</button>';
  htmlContent += '</div></div>';

  // Script
  htmlContent += '<script>';
  htmlContent += 'let pollTimer; let isStopping = false; let workersList = []; const isResuming = ' + isResuming + ';';
  htmlContent += 'function fitHeight(){setTimeout(()=>google.script.host.setHeight(Math.max(document.body.scrollHeight+50, 420)), 150);}';

  htmlContent += 'window.onload = function(){';
  htmlContent += '  if(isResuming){ showProgress(); startPolling(); }';
  htmlContent += '  else { fitHeight(); google.script.run.withSuccessHandler(initWorkers).getConfigWorkersForUi(); }';
  htmlContent += '};';

  htmlContent += 'function initWorkers(data){';
  htmlContent += '  workersList = [';
  htmlContent += '    { url: "auto", name: "Auto-Select (Recommended)", isTesting: false, statusHtml: "Finds idle production worker (5321/5080)" },';
  htmlContent += '    ...data.map(w => ({ url: w.url, name: w.name, isTesting: w.isTesting, statusHtml: "Unknown" }))';
  htmlContent += '  ];';
  htmlContent += '  renderWorkers();';
  htmlContent += '  fetchStatuses();';
  htmlContent += '}';

  htmlContent += 'function renderWorkers(){';
  htmlContent += '  let html = "";';
  htmlContent += '  workersList.forEach((w, i) => {';
  htmlContent += '    let dotColor = "#94a3b8";';
  htmlContent += '    if(w.url === "auto") dotColor = "#10b981";';
  htmlContent += '    else if(w.statusHtml.includes("Idle")) dotColor = "#10b981";';
  htmlContent += '    else if(w.statusHtml.includes("Capacity")) dotColor = "#ef4444";';
  htmlContent += '    else if(w.statusHtml.includes("Busy")) dotColor = "#f59e0b";';
  htmlContent += '    html += `<div onclick="selectWorker(${i})" style="display:flex;align-items:center;gap:12px;padding:10px 12px;background:#fafbfc;border:1px solid #e2e8f0;border-radius:10px;cursor:pointer;">`;';
  htmlContent += '    html += `<input type="radio" name="workerChoice" id="w${i}" value="${w.url}" style="margin:0;">`;';
  htmlContent += '    html += `<div><div style="font-weight:600;font-size:13.5px;color:#1e293b;">${w.name}</div>`;';
  htmlContent += '    html += `<div style="font-size:12px;color:#64748b;"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${dotColor};margin-right:6px;"></span>${w.statusHtml}</div></div></div>`;';
  htmlContent += '  });';
  htmlContent += '  document.getElementById("worker-list").innerHTML = html;';
  htmlContent += '  fitHeight();';
  htmlContent += '}';

  htmlContent += 'function selectWorker(i){ document.getElementById("w" + i).checked = true; document.getElementById("next-btn").disabled = false; }';

  htmlContent += 'function fetchStatuses(){';
  htmlContent += '  const token = "' + getAuthToken_() + '";';
  htmlContent += '  const targetWorkers = workersList.filter(w => w.url !== "auto");';
  htmlContent += '  document.getElementById("fetch-btn").textContent = "\u23F3 Checking...";';
  htmlContent += '  function applyResults(results){';
  htmlContent += '    document.getElementById("fetch-btn").textContent = "\uD83D\uDD04 Refresh Status";';
  htmlContent += '    results.forEach(r => {';
  htmlContent += '      const w = workersList.find(x => x.url === r.url);';
  htmlContent += '      if(w) {';
  htmlContent += '        if(!r.isOnline) { w.statusHtml = "Offline"; }';
  htmlContent += '        else if(w.isTesting) { w.statusHtml = r.isIdle ? "\uD83D\uDFE2 Idle (Testing Only)" : "\uD83D\uDFE1 Running (Testing Only)"; }';
  htmlContent += '        else if(r.isAtCapacity) { w.statusHtml = "\uD83D\uDD34 At Capacity (" + r.device_active_sheets + "/2 active - will reroute)"; }';
  htmlContent += '        else if(!r.isIdle) { w.statusHtml = "\uD83D\uDFE1 Busy (" + r.device_active_sheets + "/2 active)"; }';
  htmlContent += '        else { w.statusHtml = "\uD83D\uDFE2 Idle (" + r.device_active_sheets + "/2 active)"; }';
  htmlContent += '      }';
  htmlContent += '    });';
  htmlContent += '    renderWorkers();';
  htmlContent += '  }';
  htmlContent += '  Promise.all(targetWorkers.map(w => {';
  htmlContent += '    const cleanUrl = (w.url || "").trim().replace(/\/+$/, "");';
  htmlContent += '    return fetch(cleanUrl + "/discovery/status", { headers: { "Authorization": "Bearer " + token } })';
  htmlContent += '      .then(res => res.json().then(s => ({ url: w.url, isOnline: true, isIdle: s.status === "idle", isAtCapacity: (s.device_active_sheets || 0) >= (s.device_max_sheets || 2), device_active_sheets: s.device_active_sheets || 0 })))';
  htmlContent += '      .catch(() => ({ url: w.url, isOnline: false, isIdle: false, isAtCapacity: false, device_active_sheets: 0 }));';
  htmlContent += '  })).then(applyResults).catch(() => {';
  htmlContent += '    const urls = targetWorkers.map(w => w.url);';
  htmlContent += '    google.script.run.withSuccessHandler(applyResults).checkMultipleWorkerStatusesForUi(urls);';
  htmlContent += '  });';
  htmlContent += '}';

  htmlContent += 'function goToSetup(){';
  htmlContent += '  const selected = document.querySelector("input[name=workerChoice]:checked");';
  htmlContent += '  if(!selected) return;';
  htmlContent += '  const w = workersList.find(x => x.url === selected.value);';
  htmlContent += '  document.getElementById("selected-name").textContent = w ? w.name : "Auto-Select";';
  htmlContent += '  document.getElementById("worker-view").style.display = "none";';
  htmlContent += '  document.getElementById("setup-view").style.display = "block";';
  htmlContent += '  fitHeight();';
  htmlContent += '}';

  htmlContent += 'function goToWorkers(){';
  htmlContent += '  document.getElementById("setup-view").style.display = "none";';
  htmlContent += '  document.getElementById("worker-view").style.display = "block";';
  htmlContent += '  fitHeight();';
  htmlContent += '}';

  htmlContent += 'function runPipeline(){';
  htmlContent += '  const startRow = parseInt(document.getElementById("startRow").value) || 3;';
  htmlContent += '  const selected = document.querySelector("input[name=workerChoice]:checked");';
  htmlContent += '  const workerUrl = selected ? selected.value : "auto";';
  htmlContent += '  const runMode = document.getElementById("runMode") ? document.getElementById("runMode").value : "full";';
  htmlContent += '  document.getElementById("setup-view").style.display = "none";';
  htmlContent += '  showProgress();';
  htmlContent += '  google.script.run.withSuccessHandler(function(res){';
  htmlContent += '    if(res && res.rerouteNotice) {';
  htmlContent += '      const b = document.getElementById("reroute-alert");';
  htmlContent += '      b.innerHTML = res.rerouteNotice;';
  htmlContent += '      b.style.display = "block";';
  htmlContent += '    }';
  htmlContent += '    startPolling();';
  htmlContent += '  }).withFailureHandler(function(err){';
  htmlContent += '    document.getElementById("status-text").textContent = "\u274C " + (err.message || err);';
  htmlContent += '    document.getElementById("status-text").style.color = "#dc2626";';
  htmlContent += '  }).executeRunFromUi(startRow, workerUrl, runMode, false);';
  htmlContent += '}';

  htmlContent += 'function showProgress(){';
  htmlContent += '  const wv = document.getElementById("worker-view"); if(wv) wv.style.display = "none";';
  htmlContent += '  const sv = document.getElementById("setup-view"); if(sv) sv.style.display = "none";';
  htmlContent += '  document.getElementById("progress-view").style.display = "block";';
  htmlContent += '  fitHeight();';
  htmlContent += '}';

  htmlContent += 'function startPolling(){ pollProgress(); pollTimer = setInterval(pollProgress, 3000); }';

  htmlContent += 'let activeWorkerUrl = null;';
  htmlContent += 'function pollProgress(){';
  htmlContent += '  const token = "' + getAuthToken_() + '";';
  htmlContent += '  const prodWorkers = ' + JSON.stringify(CONFIG.WORKER_URLS.filter(w => w.isProduction)) + ';';
  htmlContent += '  function handleStatus(s){';
  htmlContent += '    if(!s) return;';
  htmlContent += '    updateProgressUi(s);';
  htmlContent += '    if(s.status === "succeeded" || s.status === "failed" || s.status === "stopped"){';
  htmlContent += '      clearInterval(pollTimer);';
  htmlContent += '      document.getElementById("completion-info").style.display = "block";';
  htmlContent += '      document.getElementById("stop-btn").style.display = "none";';
  htmlContent += '      fitHeight();';
  htmlContent += '    }';
  htmlContent += '  }';
  htmlContent += '  if(activeWorkerUrl){';
  htmlContent += '    fetch(activeWorkerUrl + "/discovery/status", { headers: { "Authorization": "Bearer " + token } })';
  htmlContent += '      .then(r => r.json())';
  htmlContent += '      .then(s => {';
  htmlContent += '        if(!s.active && s.status === "idle"){ activeWorkerUrl = null; }';
  htmlContent += '        handleStatus(s);';
  htmlContent += '      })';
  htmlContent += '      .catch(() => { google.script.run.withSuccessHandler(handleStatus).getStatusJson(); });';
  htmlContent += '  } else {';
  htmlContent += '    Promise.all(prodWorkers.map(w =>';
  htmlContent += '      fetch(w.url + "/discovery/status", { headers: { "Authorization": "Bearer " + token } })';
  htmlContent += '        .then(r => r.json().then(data => ({ worker: w, data })))';
  htmlContent += '        .catch(() => null)';
  htmlContent += '    )).then(results => {';
  htmlContent += '      const active = results.find(r => r && r.data && r.data.active);';
  htmlContent += '      if(active){';
  htmlContent += '        activeWorkerUrl = active.worker.url;';
  htmlContent += '        handleStatus(active.data);';
  htmlContent += '      } else {';
  htmlContent += '        const anyValid = results.find(r => r && r.data);';
  htmlContent += '        handleStatus(anyValid ? anyValid.data : { active: false, status: "idle" });';
  htmlContent += '      }';
  htmlContent += '    }).catch(() => { google.script.run.withSuccessHandler(handleStatus).getStatusJson(); });';
  htmlContent += '  }';
  htmlContent += '}';

  htmlContent += 'function updateProgressUi(s){';
  htmlContent += '  const pFill = document.getElementById("progress-fill");';
  htmlContent += '  const sText = document.getElementById("status-text");';
  htmlContent += '  const compMsg = document.getElementById("completion-msg");';
  htmlContent += '  const total = s.progress_total || 0;';
  htmlContent += '  const cur = s.progress_current || 0;';
  htmlContent += '  const pct = total > 0 ? Math.round((cur / total) * 100) : 0;';
  htmlContent += '  if(s.active){';
  htmlContent += '    if(s.is_stopping) sText.textContent = "⏳ Stopping process...";';
  htmlContent += '    else {';
  htmlContent += '      let phaseTitle = s.phase || "Running";';
  htmlContent += '      let detailsHtml = s.stage_details ? "<div style=\\"font-size:11.5px;color:#64748b;font-weight:400;margin-top:3px;\\">" + s.stage_details + "</div>" : "";';
  htmlContent += '      sText.innerHTML = "⚡ <strong>" + phaseTitle + "</strong>" + detailsHtml;';
  htmlContent += '    }';
  htmlContent += '  } else {';
  htmlContent += '    if(s.status === "stopped"){ pFill.style.background = "#eab308"; sText.textContent = "⏹️ Stopped"; compMsg.textContent = "Pipeline stopped by user."; }';
  htmlContent += '    else if(s.status === "failed"){ pFill.style.background = "#ef4444"; sText.textContent = "❌ Failed"; compMsg.textContent = "Process encountered an error."; }';
  htmlContent += '    else { pFill.style.background = "#10b981"; sText.textContent = "✅ Completed!"; compMsg.textContent = "All domains and phases processed."; }';
  htmlContent += '  }';
  htmlContent += '  pFill.style.width = pct + "%";';
  htmlContent += '  let countStr = cur + " / " + total + " rows (" + pct + "%)";';
  htmlContent += '  if(s.pending_tc && s.pending_tc > 0){ countStr += " • <span style=\\"color:#f59e0b;font-weight:600;\\">" + s.pending_tc + " in crawl</span>"; }';
  htmlContent += '  document.getElementById("count-text").innerHTML = countStr;';
  htmlContent += '  document.getElementById("yield-text").textContent = (s.progress_success || 0) + " Successful";';
  htmlContent += '  if(s.workerName) document.getElementById("worker-info-progress").innerHTML = "Connected to: <strong>" + s.workerName + "</strong>";';
  htmlContent += '  fitHeight();';
  htmlContent += '}';

  htmlContent += 'function cancelRun(){';
  htmlContent += '  const btn = document.getElementById("stop-btn");';
  htmlContent += '  if(!isStopping){';
  htmlContent += '    if(btn){ btn.textContent = "⏳ Stopping..."; btn.disabled = true; }';
  htmlContent += '    google.script.run.withSuccessHandler(function(){ pollProgress(); }).executeCancelFromUi();';
  htmlContent += '    isStopping = true;';
  htmlContent += '  }';
  htmlContent += '}';

  htmlContent += '</script></body></html>';

  const html = HtmlService.createHtmlOutput(htmlContent).setWidth(430).setHeight(isResuming ? 340 : 480);
  SpreadsheetApp.getUi().showModelessDialog(html, isResuming ? '🚦 Discovery Engine - Status Monitor' : '🚀 Discovery Engine - Filter-V2 Wizard');
}

function uiCheckStatus() {
  uiStartRun(true);
}

function normalizeUrl_(url) {
  return (url || '').toString().trim().replace(/\/+$/, '');
}

function uiCheckHealth() {
  const workersJson = JSON.stringify(CONFIG.WORKER_URLS);
  const token = getAuthToken_();
  let htmlContent = '<html><body style="font-family:Inter,sans-serif;padding:16px;">';
  htmlContent += '<h3>\uD83D\uDCCA Discovery Engine Fleet Health (' + CONFIG.TYPE + ')</h3>';
  htmlContent += '<div id="results-box"><p style="color:#64748b;">\u23F3 Checking fleet connectivity directly...</p></div>';
  htmlContent += '<script>';
  htmlContent += 'const workers = ' + workersJson + ';';
  htmlContent += 'const token = "' + token + '";';
  htmlContent += 'Promise.all(workers.map(w => {';
  htmlContent += '  const cleanUrl = (w.url || "").trim().replace(/\/+$/, "");';
  htmlContent += '  const start = Date.now();';
  htmlContent += '  return fetch(cleanUrl + "/discovery/health", { headers: { "Authorization": "Bearer " + token } })';
  htmlContent += '    .then(res => {';
  htmlContent += '      const lat = Date.now() - start;';
  htmlContent += '      return { name: w.name, isOnline: res.status === 200, details: "HTTP " + res.status + " (" + lat + "ms)" };';
  htmlContent += '    })';
  htmlContent += '    .catch(err => ({ name: w.name, isOnline: false, details: err.message || "Offline" }));';
  htmlContent += '})).then(results => {';
  htmlContent += '  let html = "";';
  htmlContent += '  results.forEach(r => {';
  htmlContent += '    const badge = r.isOnline ? "<span style=\"color:#16a34a;font-weight:600;\">Online</span>" : "<span style=\"color:#dc2626;font-weight:600;\">Offline</span>";';
  htmlContent += '    const icon = r.isOnline ? "\u2705 " : "\u274C ";';
  htmlContent += '    html += "<p style=\"margin:8px 0;\">" + icon + "<strong>" + r.name + "</strong>: " + badge + " <span style=\"color:#64748b;font-size:12px;\">(" + r.details + ")</span></p>";';
  htmlContent += '  });';
  htmlContent += '  document.getElementById("results-box").innerHTML = html;';
  htmlContent += '});';
  htmlContent += '</script></body></html>';

  const html = HtmlService.createHtmlOutput(htmlContent).setWidth(440).setHeight(320);
  SpreadsheetApp.getUi().showModalDialog(html, '\uD83D\uDCCA Discovery Engine Fleet Health (' + CONFIG.TYPE + ')');
}

function uiCheckHealthServer() {
  return CONFIG.WORKER_URLS.map(w => {
    let isOnline = false;
    let details = '';
    const cleanUrl = normalizeUrl_(w.url);
    try {
      const res = UrlFetchApp.fetch(cleanUrl + '/discovery/health', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      const code = res.getResponseCode();
      isOnline = (code === 200);
      details = 'HTTP ' + code;
    } catch(e) {
      details = e.message || 'Error';
    }
    return { name: w.name, url: w.url, isOnline: isOnline, details: details };
  });
}

function getConfigWorkersForUi() { return CONFIG.WORKER_URLS; }

function checkMultipleWorkerStatusesForUi(urls) {
  return urls.map(url => {
    try {
      const res = UrlFetchApp.fetch(normalizeUrl_(url) + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) {
        const s = JSON.parse(res.getContentText());
        const worker = CONFIG.WORKER_URLS.find(w => normalizeUrl_(w.url) === normalizeUrl_(url));
        const isTesting = worker ? worker.isTesting : false;
        const activeSheets = s.device_active_sheets || 0;
        const maxSheets = s.device_max_sheets || 2;
        const isAtCapacity = !isTesting && (activeSheets >= maxSheets);

        return {
          url: url,
          isOnline: true,
          isIdle: !s.active,
          isTesting: isTesting,
          device_active_sheets: activeSheets,
          device_max_sheets: maxSheets,
          isAtCapacity: isAtCapacity,
          statusText: s.active ? 'Busy (running this sheet)' : (isAtCapacity ? ('At Capacity (' + activeSheets + '/2 sheets)') : ('Idle (' + activeSheets + '/2 active)'))
        };
      }
    } catch(e) {}
    return { url: url, isOnline: false, isIdle: false, isAtCapacity: false, statusText: 'Offline' };
  });
}

function findIdleWorker_() {
  // Only evaluate production workers (5321 and 5080) - NEVER Vishnu
  const prodWorkers = CONFIG.WORKER_URLS.filter(w => w.isProduction);
  const candidates = [];

  for (const w of prodWorkers) {
    try {
      const res = UrlFetchApp.fetch(normalizeUrl_(w.url) + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) {
        const s = JSON.parse(res.getContentText());
        const activeSheets = s.device_active_sheets || 0;
        const maxSheets = s.device_max_sheets || 2;
        if (!s.active && activeSheets < maxSheets) {
          candidates.push({ worker: w, activeSheets: activeSheets });
        }
      }
    } catch(e) {}
  }

  if (candidates.length === 0) {
    throw new Error('All production servers (5321 & 5080) are at maximum capacity (2 sheets each) or offline. Cannot start run.');
  }

  // Load balance: pick candidate with lowest activeSheets
  candidates.sort((a, b) => a.activeSheets - b.activeSheets);
  return candidates[0].worker;
}

function executeRunFromUi(startRow, chosenWorkerUrl, mode = 'full', silent = false) {
  let targetWorker = null;
  let rerouteNotice = null;

  if (!chosenWorkerUrl || chosenWorkerUrl === 'auto') {
    targetWorker = findIdleWorker_();
  } else if (chosenWorkerUrl.indexOf('vishnu') !== -1) {
    // Explicit run on Vishnu testing worker (unrestricted)
    targetWorker = CONFIG.WORKER_URLS.find(w => w.url === chosenWorkerUrl);
    if (!targetWorker) targetWorker = { name: 'Vishnu-Filter-V2-Pipeline', url: chosenWorkerUrl };
  } else {
    // Explicit production worker chosen (e.g. 5321 or 5080)
    const selected = CONFIG.WORKER_URLS.find(w => w.url === chosenWorkerUrl);
    let isChosenAvailable = false;
    let chosenStatus = null;

    try {
      const res = UrlFetchApp.fetch(normalizeUrl_(chosenWorkerUrl) + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) {
        chosenStatus = JSON.parse(res.getContentText());
        const activeSheets = chosenStatus.device_active_sheets || 0;
        const maxSheets = chosenStatus.device_max_sheets || 2;
        if (!chosenStatus.active && activeSheets < maxSheets) {
          isChosenAvailable = true;
          targetWorker = selected;
        }
      }
    } catch(e) {}

    if (!isChosenAvailable) {
      // Must reroute to other idle production worker!
      const otherProdWorkers = CONFIG.WORKER_URLS.filter(w => w.isProduction && w.url !== chosenWorkerUrl);
      let alternativeFound = false;

      for (const alt of otherProdWorkers) {
        try {
          const res = UrlFetchApp.fetch(normalizeUrl_(alt.url) + '/discovery/status', {
            headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
            muteHttpExceptions: true
          });
          if (res.getResponseCode() === 200) {
            const altStatus = JSON.parse(res.getContentText());
            const altActive = altStatus.device_active_sheets || 0;
            const altMax = altStatus.device_max_sheets || 2;
            if (!altStatus.active && altActive < altMax) {
              targetWorker = alt;
              alternativeFound = true;
              const reason = (chosenStatus && chosenStatus.device_active_sheets >= 2)
                ? 'device reached capacity (' + chosenStatus.device_active_sheets + '/2 sheets active)'
                : 'device is busy running this sheet';
              rerouteNotice = '\u26A0\uFE0F ' + selected.name + ' ' + reason + '. Automatically rerouted to idle worker ' + alt.name + '.';
              break;
            }
          }
        } catch(e) {}
      }

      if (!alternativeFound) {
        throw new Error('All production servers (5321 & 5080) are at maximum capacity (2 sheets each) or offline. Cannot run on ' + selected.name + '.');
      }
    }
  }

  // Save active worker details
  PropertiesService.getDocumentProperties().setProperty(CONFIG.ACTIVE_WORKER_PROP, targetWorker.url);
  PropertiesService.getDocumentProperties().setProperty(CONFIG.ACTIVE_WORKER_PROP + '_NAME', targetWorker.name);
  if (rerouteNotice) {
    PropertiesService.getDocumentProperties().setProperty(CONFIG.ACTIVE_WORKER_PROP + '_REROUTE', rerouteNotice);
    if (!silent) {
      try {
        SpreadsheetApp.getActiveSpreadsheet().toast(rerouteNotice, '\u26A1 Rerouted to Idle Device', 8);
      } catch(e) {}
    }
  } else {
    PropertiesService.getDocumentProperties().deleteProperty(CONFIG.ACTIVE_WORKER_PROP + '_REROUTE');
  }

  // Launch pipeline
  const startRes = UrlFetchApp.fetch(normalizeUrl_(targetWorker.url) + '/discovery/start', {
    method: 'post',
    contentType: 'application/json',
    headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
    payload: JSON.stringify({ 
      start_row: startRow, 
      mode: mode, 
      sheet_id: SpreadsheetApp.getActiveSpreadsheet().getId()
    }),
    muteHttpExceptions: true
  });

  if (startRes.getResponseCode() >= 400) {
    throw new Error('Worker rejected start (' + startRes.getResponseCode() + '): ' + startRes.getContentText());
  }

  return {
    success: true,
    workerUrl: targetWorker.url,
    workerName: targetWorker.name,
    rerouteNotice: rerouteNotice
  };
}

/**
 * Dedicated Daily Automated Trigger Function
 * Automatically balances and selects an idle production worker (5321 / 5080)
 * and starts the pipeline from Row 3 and below in silent headless mode.
 * Select this function when configuring Time-driven Triggers in Google Apps Script.
 */
function triggerDailyDiscoveryRun() {
  Logger.log('\u23F0 Starting daily automated discovery run (startRow=3, auto worker selection, silent headless)...');
  try {
    const result = executeRunFromUi(3, 'auto', 'full', true);
    Logger.log('\u2705 Automated pipeline run successfully dispatched to ' + result.workerName + ' (' + result.workerUrl + ')');
    return result;
  } catch(err) {
    Logger.log('\u274C Automated daily trigger failed: ' + err.message);
    throw err;
  }
}

function getStatusJson() {
  let url = PropertiesService.getDocumentProperties().getProperty(CONFIG.ACTIVE_WORKER_PROP);
  const prodWorkers = CONFIG.WORKER_URLS.filter(w => w.isProduction);

  // 1. If stored worker exists, test if it is running
  if (url) {
    try {
      const cleanUrl = normalizeUrl_(url);
      const res = UrlFetchApp.fetch(cleanUrl + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) {
        const data = JSON.parse(res.getContentText());
        if (data.active) {
          return data;
        }
      }
    } catch(e) {}
  }

  // 2. If stored worker is idle or unreachable, auto-scan all production workers to find where it's actively running
  for (const w of prodWorkers) {
    if (normalizeUrl_(w.url) === normalizeUrl_(url)) continue;
    try {
      const cleanUrl = normalizeUrl_(w.url);
      const res = UrlFetchApp.fetch(cleanUrl + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) {
        const data = JSON.parse(res.getContentText());
        if (data.active) {
          // Auto-latch onto the running worker!
          PropertiesService.getDocumentProperties().setProperty(CONFIG.ACTIVE_WORKER_PROP, w.url);
          PropertiesService.getDocumentProperties().setProperty(CONFIG.ACTIVE_WORKER_PROP + '_NAME', w.name);
          return data;
        }
      }
    } catch(e) {}
  }

  // 3. Fallback: return status of stored worker if available
  if (url) {
    try {
      const cleanUrl = normalizeUrl_(url);
      const res = UrlFetchApp.fetch(cleanUrl + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) return JSON.parse(res.getContentText());
    } catch(e) {}
  }

  return { active: false, status: 'idle' };
}

function executeCancelFromUi() {
  const url = PropertiesService.getDocumentProperties().getProperty(CONFIG.ACTIVE_WORKER_PROP);
  if (!url) return;
  UrlFetchApp.fetch(normalizeUrl_(url) + '/discovery/cancel', {
    method: 'post',
    headers: { 'Authorization': 'Bearer ' + getAuthToken_() },
    muteHttpExceptions: true
  });
}

function cleanSheet() {
  SpreadsheetApp.getActiveSpreadsheet().toast("\uD83E\uDDF9 Cleaning completed.");
}

function forceReset() {
  PropertiesService.getDocumentProperties().deleteAllProperties();
  SpreadsheetApp.getActiveSpreadsheet().toast("\u2705 All cache cleared.");
}

/**
 * Core Headless Executor to Clear & Fetch Domains
 * Safe for BOTH UI triggers and Time-driven background triggers (ZERO UI dependencies).
 * 
 * @param {boolean} silent When true, suppresses spreadsheet toasts.
 * @return {Object} Status containing success, workerUrl, workerName.
 */
function executeClearAndFetch(silent = true) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheetId = sheet.getId();

  if (!silent) {
    try {
      sheet.toast('Connecting to worker to clear and ingest domains...', '\uD83D\uDD04 Ingesting', 10);
    } catch(e) {}
  }

  let targetWorker = null;
  try {
    targetWorker = findIdleWorker_();
  } catch (e) {
    const prodWorkers = CONFIG.WORKER_URLS.filter(w => w.isProduction);
    targetWorker = prodWorkers.length > 0 ? prodWorkers[0] : CONFIG.WORKER_URLS[0];
  }

  Logger.log('Dispathing clear & fetch to worker: ' + targetWorker.name + ' (' + targetWorker.url + ') for sheetId: ' + sheetId);

  const token = getAuthToken_();

  const res = UrlFetchApp.fetch(targetWorker.url + '/discovery/clear-and-fetch', {
    method: 'post',
    contentType: 'application/json',
    headers: { 'Authorization': 'Bearer ' + token },
    payload: JSON.stringify({
      sheet_id: sheetId
    }),
    muteHttpExceptions: true
  });

  const statusCode = res.getResponseCode();
  const responseText = res.getContentText();

  if (statusCode >= 200 && statusCode < 300) {
    Logger.log('\u2705 Worker accepted Clear & Fetch (' + statusCode + '): ' + responseText);
    if (!silent) {
      try {
        sheet.toast('Clear & Fetch accepted on ' + targetWorker.name, '\u2705 Dispatched', 6);
      } catch(e) {}
    }
    return {
      success: true,
      workerUrl: targetWorker.url,
      workerName: targetWorker.name,
      response: responseText
    };
  } else {
    throw new Error('Worker rejected clear & fetch (' + statusCode + '): ' + responseText);
  }
}

/**
 * Dedicated Daily Automated Trigger Function to Clear & Fetch Domains
 * Automatically selects an idle production worker (5321 / 5080) and
 * clears Row 3+ to ingest fresh domains based on "CF_IF Engine - Input Payload".
 * Strictly headless with ZERO UI calls - select this when configuring Time-driven Triggers.
 */
function triggerDailyClearAndFetchDomains() {
  Logger.log('\u23F0 Starting automated daily clear & fetch domains (silent headless)...');
  try {
    const result = executeClearAndFetch(true);
    Logger.log('\u2705 Automated clear & fetch successfully dispatched to ' + result.workerName + ' (' + result.workerUrl + ')');
    return result;
  } catch(err) {
    Logger.log('\u274C Automated daily clear & fetch trigger failed: ' + err.message);
    throw err;
  }
}

/**
 * Menu action to clear Row 3+ in CF_IF_Engine_Database and fetch fresh domains
 * directly from Tracxn API based on "CF_IF Engine - Input Payload".
 * Interactive UI wrapper around executeClearAndFetch().
 */
function uiClearAndFetchDomains() {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet();

  const confirm = ui.alert(
    '\uD83D\uDD04 Clear & Fetch Domains',
    'Are you sure you want to clear this sheet (Rows 3+) and pull fresh domains from Tracxn?\n\n' +
    '\u2022 All existing domain data in CF_IF_Engine_Database (Row 3+) will be cleared.\n' +
    '\u2022 Fresh domains will be fetched via Tracxn API using payload definitions in "CF_IF Engine - Input Payload".\n' +
    '\u2022 The downstream processing pipeline will NOT be started automatically.\n\n' +
    'Do you want to proceed?',
    ui.ButtonSet.YES_NO
  );

  if (confirm !== ui.Button.YES) {
    sheet.toast('Clear & Fetch operation cancelled.', '\u2139\uFE0F Cancelled', 5);
    return;
  }

  try {
    const result = executeClearAndFetch(false);
    ui.alert(
      '\u2705 Domain Ingestion Dispatched',
      'Successfully dispatched Clear & Fetch on ' + result.workerName + ' (' + result.workerUrl + ').\n\n' +
      'The worker is clearing old data and pulling fresh domains from Tracxn in the background.\n' +
      'Fresh domains will populate CF_IF_Engine_Database starting at Row 3.\n\n' +
      'Note: Downstream pipeline was NOT triggered as requested.',
      ui.ButtonSet.OK
    );
  } catch (err) {
    ui.alert('\u274C Clear & Fetch Failed', 'Error: ' + err.message, ui.ButtonSet.OK);
  }
}
