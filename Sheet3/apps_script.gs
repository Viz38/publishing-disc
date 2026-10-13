/**
 * TITLE: Discovery Engine Pipeline - CF-3
 * SHEET: Discovery AI - CF-3 (Nandan Domains DB)
 * REFERENCE: Tracxn SR Publishing Architecture
 */

// --- Configuration ---
const CONFIG = {
  TYPE: 'CF-3',
  SHEET_NAME: 'Discovery AI - CF-3 (Nandan Domains DB)',
  WORKER_URLS: [
    { name: '5321-CF-3-Pipeline', url: 'https://5321-sheet3.ecosuyaenergies.com', hostId: '5321', isProduction: true },
    { name: '5080-CF-3-Pipeline', url: 'https://5080-sheet3.ecosuyaenergies.com', hostId: '5080', isProduction: true },
    { name: 'Vishnu-CF-3-Pipeline', url: 'https://vishnu-sheet3.ecosuyaenergies.com', hostId: 'Vishnu', isTesting: true }
  ],
  AUTH_TOKEN: '',
  DEFAULT_MODE: 'full',
  MAX_RETRIES: 3,
  ACTIVE_WORKER_PROP: 'DISCOVERY_ACTIVE_WORKER_SHEET_3',
  MAX_DEVICE_SHEETS: 2
};

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('🚀 Tracxn Menu (CF-3)')
    .addItem('▶️ Run Pipeline', 'uiStartRun')
    .addSeparator()
    .addItem('⚡ Run Daily Auto-Trigger (Row 3)', 'triggerDailyDiscoveryRun')
    .addSeparator()
    .addItem('🚦 Check Status', 'uiCheckStatus')
    .addItem('📊 Check Health', 'uiCheckHealth')
    .addSeparator()
    .addItem("🧹 Clean Sheet", "cleanSheet")
    .addItem("⚠️ Force Reset", "forceReset")
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
  htmlContent += '<h1 style="font-size:17px;font-weight:700;margin:0;color:#0f172a;">🚀 CF-3 Pipeline</h1>';
  htmlContent += '<span style="font-size:12px;background:#e0f2fe;color:#0369a1;padding:4px 8px;border-radius:6px;font-weight:600;">Discovery AI - CF-3 (Nandan Domains DB)</span>';
  htmlContent += '</div>';

  // Step 1: Worker Select
  htmlContent += '<div id="worker-view">';
  htmlContent += '<p style="color:#64748b;font-size:13px;margin-top:0;">Select a production desktop (max 2 sheets per device) or test on Vishnu:</p>';
  htmlContent += '<div id="worker-list" style="display:flex;flex-direction:column;gap:10px;margin-bottom:20px;">';
  htmlContent += '<div style="padding:16px;text-align:center;color:#64748b;background:white;border-radius:8px;border:1px solid #e2e8f0;">Loading workers...</div>';
  htmlContent += '</div>';

  htmlContent += '<div style="display:flex;gap:10px;">';
  htmlContent += '<button id="fetch-btn" onclick="fetchStatuses()" class="btn-secondary" style="flex:1;">🔄 Refresh Status</button>';
  htmlContent += '<button id="next-btn" onclick="goToSetup()" class="btn-primary" style="flex:1;" disabled>Next →</button>';
  htmlContent += '</div></div>';

  // Step 2: Setup
  htmlContent += '<div id="setup-view" style="display:none;">';
  htmlContent += '<div style="background:white;border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin-bottom:16px;">';
  htmlContent += '<div id="selected-worker-label" style="background:#f0f9ff;color:#0369a1;padding:10px 12px;border-radius:8px;font-weight:600;margin-bottom:14px;font-size:13px;">✅ <span id="selected-name"></span></div>';

  htmlContent += '<label style="display:block;font-weight:600;font-size:13px;color:#475569;margin-bottom:6px;">🔢 Start Row</label>';
  htmlContent += '<input type="number" id="startRow" value="4" min="2" style="width:100%;box-sizing:border-box;padding:10px;border:1px solid #cbd5e1;border-radius:8px;font-size:14px;">';

  htmlContent += '<div id="phase-box" style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:12px;margin-top:14px;">';
  htmlContent += '<div style="font-weight:600;color:#1e40af;font-size:13px;">Target: CF-3 Pipeline</div>';
  htmlContent += '<div style="font-size:12px;color:#3b82f6;margin-top:4px;">Domain Scraping + Gemini/NIM Prediction + Tracxn Write APIs</div>';
  htmlContent += '</div></div>';

  htmlContent += '<div style="display:flex;gap:10px;">';
  htmlContent += '<button onclick="goToWorkers()" class="btn-secondary" style="flex:1;">← Back</button>';
  htmlContent += '<button onclick="runPipeline()" class="btn-primary" style="flex:2;">▶️ Start Pipeline</button>';
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
  htmlContent += '<strong style="color:#166534;font-size:14px;">✨ Run Completed</strong>';
  htmlContent += '<div id="completion-msg" style="margin-top:6px;color:#15803d;font-size:12.5px;"></div>';
  htmlContent += '</div>';

  htmlContent += '<div style="display:flex;gap:10px;margin-top:20px;">';
  htmlContent += '<button id="stop-btn" onclick="cancelRun()" style="flex:1;padding:12px;background:#fee2e2;color:#b91c1c;border:1px solid #fecaca;border-radius:8px;font-weight:600;cursor:pointer;">⏹️ Stop Pipeline</button>';
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
  htmlContent += '  const urls = workersList.filter(w => w.url !== "auto").map(w => w.url);';
  htmlContent += '  document.getElementById("fetch-btn").textContent = "⏳ Checking...";';
  htmlContent += '  google.script.run.withSuccessHandler(function(results){';
  htmlContent += '    document.getElementById("fetch-btn").textContent = "🔄 Refresh Status";';
  htmlContent += '    results.forEach(r => {';
  htmlContent += '      const w = workersList.find(x => x.url === r.url);';
  htmlContent += '      if(w) {';
  htmlContent += '        if(!r.isOnline) { w.statusHtml = "Offline"; }';
  htmlContent += '        else if(w.isTesting) { w.statusHtml = r.isIdle ? "🟢 Idle (Testing Only)" : "🟡 Running (Testing Only)"; }';
  htmlContent += '        else if(r.isAtCapacity) { w.statusHtml = "🔴 At Capacity (" + r.device_active_sheets + "/2 active - will reroute)"; }';
  htmlContent += '        else if(!r.isIdle) { w.statusHtml = "🟡 Busy (" + r.device_active_sheets + "/2 active)"; }';
  htmlContent += '        else { w.statusHtml = "🟢 Idle (" + r.device_active_sheets + "/2 active)"; }';
  htmlContent += '      }';
  htmlContent += '    });';
  htmlContent += '    renderWorkers();';
  htmlContent += '  }).checkMultipleWorkerStatusesForUi(urls);';
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
  htmlContent += '  const startRow = parseInt(document.getElementById("startRow").value) || 4;';
  htmlContent += '  const selected = document.querySelector("input[name=workerChoice]:checked");';
  htmlContent += '  const workerUrl = selected ? selected.value : "auto";';
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
  htmlContent += '    document.getElementById("status-text").textContent = "❌ " + (err.message || err);';
  htmlContent += '    document.getElementById("status-text").style.color = "#dc2626";';
  htmlContent += '  }).executeRunFromUi(startRow, workerUrl);';
  htmlContent += '}';

  htmlContent += 'function showProgress(){';
  htmlContent += '  document.getElementById("progress-view").style.display = "block";';
  htmlContent += '  fitHeight();';
  htmlContent += '}';

  htmlContent += 'function startPolling(){ pollProgress(); pollTimer = setInterval(pollProgress, 3000); }';

  htmlContent += 'function pollProgress(){';
  htmlContent += '  google.script.run.withSuccessHandler(function(s){';
  htmlContent += '    updateProgressUi(s);';
  htmlContent += '    if(s.status === "succeeded" || s.status === "failed" || s.status === "stopped"){';
  htmlContent += '      clearInterval(pollTimer);';
  htmlContent += '      document.getElementById("completion-info").style.display = "block";';
  htmlContent += '      document.getElementById("stop-btn").style.display = "none";';
  htmlContent += '      fitHeight();';
  htmlContent += '    }';
  htmlContent += '  }).getStatusJson();';
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
  htmlContent += '    else sText.textContent = "⚡ Running (Row " + cur + " of " + total + ")...";';
  htmlContent += '  } else {';
  htmlContent += '    if(s.status === "stopped"){ pFill.style.background = "#eab308"; sText.textContent = "⏹️ Stopped"; compMsg.textContent = "Pipeline stopped by user."; }';
  htmlContent += '    else if(s.status === "failed"){ pFill.style.background = "#ef4444"; sText.textContent = "❌ Failed"; compMsg.textContent = "Process encountered an error."; }';
  htmlContent += '    else { pFill.style.background = "#10b981"; sText.textContent = "✅ Completed!"; compMsg.textContent = "All domains processed."; }';
  htmlContent += '  }';
  htmlContent += '  pFill.style.width = pct + "%";';
  htmlContent += '  document.getElementById("count-text").textContent = cur + " / " + total + " rows (" + pct + "%)";';
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

  const html = HtmlService.createHtmlOutput(htmlContent).setWidth(430).setHeight(460);
  SpreadsheetApp.getUi().showModalDialog(html, '🚀 CF-3 Pipeline Wizard');
}

function uiCheckStatus() {
  const s = getStatusJson();
  const ui = SpreadsheetApp.getUi();
  const worker = PropertiesService.getDocumentProperties().getProperty(CONFIG.ACTIVE_WORKER_PROP + '_NAME') || 'None';
  ui.alert('🚦 Status: ' + s.status.toUpperCase() + '\nWorker: ' + worker + '\nProgress: ' + (s.progress_current || 0) + '/' + (s.progress_total || 0));
}

function uiCheckHealth() {
  const results = CONFIG.WORKER_URLS.map(w => {
    let isOnline = false;
    try {
      const res = UrlFetchApp.fetch(w.url + '/discovery/health', {
        headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
        muteHttpExceptions: true
      });
      isOnline = (res.getResponseCode() === 200);
    } catch(e) {}
    return { name: w.name, isOnline: isOnline };
  });
  let htmlContent = '<html><body style="font-family:Inter,sans-serif;padding:16px;"><h3>📊 CF-3 Fleet Health</h3>';
  results.forEach(r => {
    htmlContent += '<p>' + (r.isOnline ? '✅ ' : '❌ ') + '<strong>' + r.name + '</strong>: ' + (r.isOnline ? 'Online' : 'Offline') + '</p>';
  });
  htmlContent += '</body></html>';
  const html = HtmlService.createHtmlOutput(htmlContent).setWidth(380).setHeight(320);
  SpreadsheetApp.getUi().showModalDialog(html, '📊 CF-3 Fleet Health');
}

function uiCheckHealthServer() {
  return CONFIG.WORKER_URLS.map(w => {
    let isOnline = false;
    try {
      const res = UrlFetchApp.fetch(w.url + '/discovery/health', {
        headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
        muteHttpExceptions: true
      });
      isOnline = (res.getResponseCode() === 200);
    } catch(e) {}
    return { name: w.name, url: w.url, isOnline: isOnline };
  });
}

function getConfigWorkersForUi() { return CONFIG.WORKER_URLS; }

function checkMultipleWorkerStatusesForUi(urls) {
  return urls.map(url => {
    try {
      const res = UrlFetchApp.fetch(url + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() === 200) {
        const s = JSON.parse(res.getContentText());
        const worker = CONFIG.WORKER_URLS.find(w => w.url === url);
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
      const res = UrlFetchApp.fetch(w.url + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
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

function executeRunFromUi(startRow, chosenWorkerUrl) {
  let targetWorker = null;
  let rerouteNotice = null;

  if (!chosenWorkerUrl || chosenWorkerUrl === 'auto') {
    targetWorker = findIdleWorker_();
  } else if (chosenWorkerUrl.indexOf('vishnu') !== -1) {
    // Explicit run on Vishnu testing worker (unrestricted)
    targetWorker = CONFIG.WORKER_URLS.find(w => w.url === chosenWorkerUrl);
    if (!targetWorker) targetWorker = { name: 'Vishnu-CF-3-Pipeline', url: chosenWorkerUrl };
  } else {
    // Explicit production worker chosen (e.g. 5321 or 5080)
    const selected = CONFIG.WORKER_URLS.find(w => w.url === chosenWorkerUrl);
    let isChosenAvailable = false;
    let chosenStatus = null;

    try {
      const res = UrlFetchApp.fetch(chosenWorkerUrl + '/discovery/status', {
        headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
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
          const res = UrlFetchApp.fetch(alt.url + '/discovery/status', {
            headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
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
              rerouteNotice = '⚠️ ' + selected.name + ' ' + reason + '. Automatically rerouted to idle worker ' + alt.name + '.';
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
    try {
      SpreadsheetApp.getActiveSpreadsheet().toast(rerouteNotice, '⚡ Rerouted to Idle Device', 8);
    } catch(e) {}
  } else {
    PropertiesService.getDocumentProperties().deleteProperty(CONFIG.ACTIVE_WORKER_PROP + '_REROUTE');
  }

  // Launch pipeline
  const startRes = UrlFetchApp.fetch(targetWorker.url + '/discovery/start', {
    method: 'post',
    contentType: 'application/json',
    headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
    payload: JSON.stringify({ 
      start_row: startRow, 
      mode: 'full', 
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
 * and starts the pipeline from Row 3 and below.
 * Select this function when configuring Time-driven Triggers in Google Apps Script.
 */
function triggerDailyDiscoveryRun() {
  Logger.log('⏰ Starting daily automated discovery run (startRow=3, auto worker selection)...');
  try {
    const result = executeRunFromUi(3, 'auto');
    Logger.log('✅ Automated pipeline run successfully dispatched to ' + result.workerName + ' (' + result.workerUrl + ')');
    try {
      SpreadsheetApp.getActiveSpreadsheet().toast('Pipeline started on ' + result.workerName + ' (Row 3+)', '🚀 Daily Trigger Active', 10);
    } catch(e) {}
    return result;
  } catch(err) {
    Logger.log('❌ Automated daily trigger failed: ' + err.message);
    throw err;
  }
}

function getStatusJson() {
  const url = PropertiesService.getDocumentProperties().getProperty(CONFIG.ACTIVE_WORKER_PROP);
  if (!url) return { active: false, status: 'idle' };

  try {
    const res = UrlFetchApp.fetch(url + '/discovery/status', {
      headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
      muteHttpExceptions: true
    });
    if (res.getResponseCode() === 200) return JSON.parse(res.getContentText());
  } catch(e) {}
  return { active: false, status: 'unknown' };
}

function executeCancelFromUi() {
  const url = PropertiesService.getDocumentProperties().getProperty(CONFIG.ACTIVE_WORKER_PROP);
  if (!url) return;
  UrlFetchApp.fetch(url + '/discovery/cancel', {
    method: 'post',
    headers: { 'Authorization': 'Bearer ' + CONFIG.AUTH_TOKEN },
    muteHttpExceptions: true
  });
}

function cleanSheet() {
  SpreadsheetApp.getActiveSpreadsheet().toast("🧹 Cleaning completed.");
}

function forceReset() {
  PropertiesService.getDocumentProperties().deleteAllProperties();
  SpreadsheetApp.getActiveSpreadsheet().toast("✅ All cache cleared.");
}
