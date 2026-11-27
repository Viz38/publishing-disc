/**
 * ============================================================================
 * DISCOVERY ENGINE — FLEET STATUS & DEVICE HEALTH DASHBOARD (APPS SCRIPT)
 * ============================================================================
 * 
 * Generates executive-grade operational dashboards across two dedicated sheets:
 *  1. "Production Fleet" Sheet (Main Fleet):
 *     - Tracks Sheets 1–4 across Desktop-5080 and Desktop-5321 (P0 leads, capped at <=5k).
 *     - Table 1 (10 Columns): Sheet Pipeline, Assigned Desktop, Status, Completed,
 *       Total Rows, Progress %, Tech Crawler, NIM, Gemini Predictions, Tracxn Movements.
 *     - Table 2: Device Health Report for Desktop-5080 and Desktop-5321.
 * 
 *  2. "Testing Fleet" Sheet (NIM Fleet):
 *     - Tracks high-volume candidate sheet on Desktop-9048 (44k+ leads, 40 NIM keys).
 *     - Table 1 (11 Columns): Sheet Pipeline, Assigned Desktop, Status, Completed,
 *       Total Rows, Progress %, Tech Crawler, NIM Summary, NIM Industry, NIM Feed,
 *       Tracxn Movements.
 *     - Table 2: Device Health Report for Desktop-9048.
 *
 * All cells are strictly center-aligned (horizontal & vertical). Values are crisp and
 * fitted to column dimensions without truncation.
 *
 * Strict 4-Sheet Workbook Rule:
 * The spreadsheet is strictly restricted to exactly four sheets:
 *  1. "Production Fleet"
 *  2. "Testing Fleet"
 *  3. "Production Budget Allocation"
 *  4. "Test Fleet Budget Allocation"
 * Any other sheet (such as legacy "Status") is automatically pruned on refresh.
 *
 * Dual-Refresh: Running `updateFleetDashboards()` (or alias `updateStatusSheet()`)
 * refreshes BOTH dashboards synchronously.
 */

var STATUS_CONFIG = {
  AUTH_TOKEN: 'Tracxn@SR',
  STATUS_SHEET_NAME: 'Production Fleet',
  TESTING_SHEET_NAME: 'Testing Fleet',
  ALLOWED_WORKBOOK_SHEETS: [
    'Production Fleet',
    'Testing Fleet',
    'Production Budget Allocation',
    'Test Fleet Budget Allocation'
  ],
  DEFAULT_SPREADSHEET_ID: '1eVKXWnGNsHp8WmoqXo8Pz-mB6f7V7xGleMfd7CcahSA',
  TARGET_SHEETS: [
    {
      num: 1,
      name: 'Discovery Operations - Engine 1',
      shortName: 'Engine 1',
      sheetId: '1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo',
      sheetUrl: 'https://docs.google.com/spreadsheets/d/1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo/edit',
      endpoints: [
        { host: 'Desktop-5080', url: 'https://5080-sheet1.rajath.world' }
      ]
    },
    {
      num: 2,
      name: 'Discovery Operations - Engine 2',
      shortName: 'Engine 2',
      sheetId: '1QGraMZOJjc-EIZlt_CieOVpr1jxw6tLa_QnWgWHou5I',
      sheetUrl: 'https://docs.google.com/spreadsheets/d/1QGraMZOJjc-EIZlt_CieOVpr1jxw6tLa_QnWgWHou5I/edit',
      endpoints: [
        { host: 'Desktop-5080', url: 'https://5080-sheet2.rajath.world' }
      ]
    },
    {
      num: 3,
      name: 'Discovery Operations - Engine 3',
      shortName: 'Engine 3',
      sheetId: '1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8',
      sheetUrl: 'https://docs.google.com/spreadsheets/d/1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8/edit',
      endpoints: [
        { host: 'Desktop-5321', url: 'https://5321-sheet3.rajath.world' }
      ]
    },
    {
      num: 4,
      name: 'Discovery Operations - Engine 4',
      shortName: 'Engine 4',
      sheetId: '1z3wH--c2qHGlNqW0e-6Mc8IVqh_kN5_fbWjILM4FxQM',
      sheetUrl: 'https://docs.google.com/spreadsheets/d/1z3wH--c2qHGlNqW0e-6Mc8IVqh_kN5_fbWjILM4FxQM/edit',
      endpoints: [
        { host: 'Desktop-5321', url: 'https://5321-sheet4.rajath.world' }
      ]
    }
  ],
  DEVICES: [
    {
      name: 'Desktop-5080',
      baseUrl: 'https://5080-sheet1.rajath.world',
      fallbackUrl: 'https://5080-sheet2.rajath.world',
      networkRoute: '5080-*.rajath.world (Cloudflare Tunnel)',
      maxSlots: 2
    },
    {
      name: 'Desktop-5321',
      baseUrl: 'https://5321-sheet3.rajath.world',
      fallbackUrl: 'https://5321-sheet4.rajath.world',
      networkRoute: '5321-*.rajath.world (Cloudflare Tunnel)',
      maxSlots: 2
    }
  ],
  NIM_TARGET_SHEETS: [
    {
      num: 1,
      name: 'Discovery Operations - NIM Fleet 1',
      shortName: 'NIM Fleet 1',
      sheetId: '1XT_GlvBf2BGO4cEiskpUsK2Fzc5PzB-tcRHFMubDXbk',
      sheetUrl: 'https://docs.google.com/spreadsheets/d/1XT_GlvBf2BGO4cEiskpUsK2Fzc5PzB-tcRHFMubDXbk/edit?gid=115980342#gid=115980342',
      endpoints: [
        { host: 'Desktop-9048', url: 'https://9048-sheet1.rajath.world' }
      ]
    }
  ],
  NIM_DEVICES: [
    {
      name: 'Desktop-9048',
      baseUrl: 'https://9048-sheet1.rajath.world',
      networkRoute: '9048-*.rajath.world (Cloudflare Tunnel)',
      maxSlots: 2
    }
  ]
};

/**
 * Shortens live stage details cleanly so that they fit the column without truncation.
 */
function shortenStageDetails(stage) {
  if (!stage) return 'Running';
  var m = stage.match(/batch\s+(\d+\/\d+).*?(\d+)\s+SUCCESS.*?(\d+)\s+FAILED/i);
  if (m) {
    var okCount = parseInt(m[2], 10);
    return 'TC Batch ' + m[1] + ' (' + okCount.toLocaleString() + ' OK, ' + m[3] + ' Fail)';
  }
  var m2 = stage.match(/Streaming:\s*(\d+)\/(\d+).*?\((\d+)\s+success.*?(\d+)\s+fail\)/i);
  if (m2) {
    return 'Streaming ' + m2[1] + '/' + m2[2] + ' (' + m2[3] + ' OK)';
  }
  if (stage.indexOf('Starting batch') !== -1) {
    return 'Starting Domain Batch...';
  }
  if (stage.length > 45) {
    return stage.substring(0, 42) + '...';
  }
  return stage;
}

/**
 * Dynamically audits true ground-truth stage counts directly from CF_IF_Engine_Database.
 * Reads Col X (24) to Col BA (53) in a single fast range query. Zero hardcoded values.
 * Counts resolved domains across all stages for cascading waterfall denominators.
 */
function auditSheetDynamically(sheetId) {
  if (!sheetId) return null;
  try {
    var extSheet = SpreadsheetApp.openById(sheetId).getSheetByName('CF_IF_Engine_Database');
    if (!extSheet) return null;
    var lastRow = extSheet.getLastRow();
    var tot = Math.max(0, lastRow - 2);
    if (tot === 0) return {
      total: 0, tcResolved: 0, tcSuccess: 0, nimResolved: 0, nimSuccess: 0,
      nimSummaryResolved: 0, nimSummarySuccess: 0, nimIfResolved: 0, nimIfSuccess: 0,
      nimFeedResolved: 0, nimFeedSuccess: 0, geminiResolved: 0, tracxnResolved: 0,
      fullyCompleted: 0, eligibleCompleted: 0
    };

    // Range X3:BA[lastRow] spans 30 columns (Column 24 to Column 53)
    var dataSlice = extSheet.getRange(3, 24, tot, 30).getValues();
    var dynTcSuccess = 0;
    var dynTcFailed = 0;
    var dynNimSuccess = 0;
    var dynNimNoContent = 0;
    var dynNimIfResolved = 0;
    var dynNimFeedResolved = 0;
    var dynGeminiResolved = 0;
    var dynTracxnMoved = 0;
    var dynTracxnUnassignable = 0;
    var dynFullyCompleted = 0;
    var dynEligibleCompleted = 0;

    var validTrxTerminals = {
      'DONE': true, 'TRUE': true, 'YES': true, 'MOVED': true,
      'MOVED_OUT': true, 'SNOOZED': true, 'MARKED_IRRELEVANT': true,
      'MARKED_FOR_LATER': true, 'ESCALATED': true
    };

    function isTs(s) {
      if (!s) return false;
      var str = String(s).trim();
      return str.length >= 8 && (str.indexOf(':') !== -1 || str.indexOf('/') !== -1 || str.indexOf('-') !== -1) && !validTrxTerminals[str.toUpperCase()] && str.indexOf('http') !== 0;
    }

    for (var i = 0; i < dataSlice.length; i++) {
      var r = dataSlice[i];
      // Col X (index 0: 24 - 24)
      var cX = r[0] ? String(r[0]).trim().toUpperCase() : '';
      // Col AD (index 6: 30 - 24, Raw Data)
      var cAd = r[6] ? String(r[6]).trim() : '';

      var isTcSucc = (cX === 'SUCCESS' || cAd !== '');
      var isTcFail = (cX === 'FAILED');
      if (isTcSucc) dynTcSuccess++;
      else if (isTcFail) dynTcFailed++;

      // Col AI (index 11: 35 - 24, NIM Status) & Col AJ (index 12: 36 - 24, NIM Summary)
      var cAi = r[11] ? String(r[11]).trim().toUpperCase() : '';
      var cAj = r[12] ? String(r[12]).trim() : '';
      var isNimSucc = (cAi === 'SUCCESS' || cAj.length > 10);
      var isNimNoContent = (cAi === 'NO_CONTENT' || cAi === 'FAILED_NO_CONTENT');
      if (isNimSucc) dynNimSuccess++;
      else if (isNimNoContent) dynNimNoContent++;

      // Col AL (index 14: 38 - 24, IF Status) & Col AR (index 20: 44 - 24, Feed Number)
      var cAl = r[14] ? String(r[14]).trim().toUpperCase() : '';
      var cAr = r[20] ? String(r[20]).trim() : '';
      var hasFeed = (cAr !== '');
      var hasTerminalIf = (cAl.indexOf('ESCALATED') !== -1 || cAl.indexOf('MARKED_IRRELEVANT') !== -1 || cAl.indexOf('SNOOZED') !== -1);
      var hasIfPrediction = (cAl !== '');
      
      // NIM Industry Funnel resolution
      var isIfResolved = (hasTerminalIf || hasIfPrediction);
      if (isIfResolved) dynNimIfResolved++;

      // NIM Feed resolution
      var isFeedResolved = (hasFeed || hasTerminalIf);
      if (isFeedResolved) dynNimFeedResolved++;

      var isGeminiResolved = (hasFeed || hasTerminalIf || hasIfPrediction);
      if (isGeminiResolved) dynGeminiResolved++;

      // Col AU (index 23: 47 - 24, Publish Funnel Name)
      var cAu = r[23] ? String(r[23]).trim().toLowerCase() : '';
      // Col AX (index 26: 50 - 24, Write API Status CF)
      var cAx = r[26] ? String(r[26]).trim().toUpperCase() : '';
      var cAy = r[27] ? String(r[27]).trim() : '';
      var cAz = r[28] ? String(r[28]).trim().toUpperCase() : '';
      var cBa = r[29] ? String(r[29]).trim() : '';

      if (cAx === 'DONE' && cAz === 'DONE' && cAu.indexOf('publishing funnel') === 0) {
        dynEligibleCompleted++;
      }

      var isCfMoved = (validTrxTerminals[cAx] || isTs(cAy));
      var isPfMoved = (validTrxTerminals[cAz] || isTs(cBa));
      var isTrxMoved = (isCfMoved || isPfMoved);
      var isTrxUnassignable = (cAx === 'SKIPPED_UNASSIGNABLE');
      if (isGeminiResolved || isTrxMoved || isTrxUnassignable) {
        if (isTrxMoved) dynTracxnMoved++;
        else if (isTrxUnassignable) dynTracxnUnassignable++;
      }

      // Exact End-to-End Completion (matching is_row_fully_completed)
      var isRowDone = false;
      if (hasFeed) {
        if (isCfMoved && isPfMoved) isRowDone = true;
      } else if (hasTerminalIf) {
        if (isCfMoved) isRowDone = true;
      } else if (isCfMoved && isPfMoved) {
        isRowDone = true;
      } else if (isCfMoved && !hasFeed && hasIfPrediction) {
        isRowDone = true;
      } else if (isTcFail || isNimNoContent) {
        isRowDone = true;
      }
      if (isRowDone) dynFullyCompleted++;
    }

    var dynTcResolved = Math.min(tot, dynTcSuccess + dynTcFailed);
    var dynNimResolved = Math.min(tot, dynNimSuccess + dynNimNoContent);
    dynGeminiResolved = (dynNimSuccess > 0) ? Math.min(dynNimSuccess, dynGeminiResolved) : dynGeminiResolved;
    dynNimIfResolved = (dynNimSuccess > 0) ? Math.min(dynNimSuccess, dynNimIfResolved) : Math.min(tot, dynNimIfResolved);
    dynNimFeedResolved = (dynNimIfResolved > 0) ? Math.min(dynNimIfResolved, dynNimFeedResolved) : Math.min(tot, dynNimFeedResolved);
    var dynTracxnResolved = (dynGeminiResolved > 0) ? Math.min(dynGeminiResolved, dynTracxnMoved + dynTracxnUnassignable) : Math.min(tot, dynTracxnMoved + dynTracxnUnassignable);

    return {
      total: tot,
      tcResolved: dynTcResolved,
      tcSuccess: dynTcSuccess,
      nimResolved: dynNimResolved,
      nimSuccess: dynNimSuccess,
      nimSummaryResolved: dynNimResolved,
      nimSummarySuccess: dynNimSuccess,
      nimIfResolved: dynNimIfResolved,
      nimIfSuccess: dynNimIfResolved,
      nimFeedResolved: dynNimFeedResolved,
      nimFeedSuccess: dynNimFeedResolved,
      geminiResolved: dynGeminiResolved,
      tracxnResolved: dynTracxnResolved,
      fullyCompleted: dynFullyCompleted,
      eligibleCompleted: dynEligibleCompleted
    };
  } catch (err) {
    Logger.log('auditSheetDynamically error for ' + sheetId + ': ' + err);
    return null;
  }
}

/**
 * Returns active spreadsheet instance with fallback.
 */
function getDashboardSpreadsheet() {
  var ss = null;
  try {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  } catch (e) {
    ss = null;
  }
  if (!ss) {
    try {
      ss = SpreadsheetApp.openById(STATUS_CONFIG.DEFAULT_SPREADSHEET_ID);
    } catch (e) {
      Logger.log('Could not open spreadsheet by ID: ' + e);
    }
  }
  if (!ss) {
    throw new Error('Spreadsheet could not be determined. Please run inside a Google Spreadsheet or grant access to ' + STATUS_CONFIG.DEFAULT_SPREADSHEET_ID);
  }
  return ss;
}

/**
 * Enforces that the workbook strictly contains only the 4 allowed sheets:
 *  1. "Production Fleet"
 *  2. "Testing Fleet"
 *  3. "Production Budget Allocation"
 *  4. "Test Fleet Budget Allocation"
 * Any other sheet (such as legacy "Status") is immediately and safely removed.
 */
function enforceExactFourSheets(ss) {
  if (!ss) return;
  var allowed = STATUS_CONFIG.ALLOWED_WORKBOOK_SHEETS || [
    'Production Fleet',
    'Testing Fleet',
    'Production Budget Allocation',
    'Test Fleet Budget Allocation'
  ];

  // Ensure Production Fleet exists first so we can safely activate it
  var prodSheet = ss.getSheetByName(STATUS_CONFIG.STATUS_SHEET_NAME);
  if (!prodSheet) {
    prodSheet = ss.insertSheet(STATUS_CONFIG.STATUS_SHEET_NAME, 0);
  }
  try {
    ss.setActiveSheet(prodSheet);
  } catch (e) {}

  var allSheets = ss.getSheets();
  for (var i = 0; i < allSheets.length; i++) {
    var s = allSheets[i];
    var sName = s.getName();
    if (allowed.indexOf(sName) === -1) {
      try {
        ss.deleteSheet(s);
        Logger.log('Deleted extraneous sheet: ' + sName);
      } catch (err) {
        Logger.log('Could not delete sheet ' + sName + ': ' + err);
      }
    }
  }
}

/**
 * Main execution entrypoint: Refreshes BOTH Production Fleet and Testing Fleet dashboards.
 * Strictly enforces the 4-sheet workbook rule.
 */
function updateFleetDashboards() {
  var ss = getDashboardSpreadsheet();
  var now = new Date();
  var timestampStr = Utilities.formatDate(now, 'Asia/Kolkata', 'dd MMM yyyy, hh:mm:ss a') + ' IST';
  var token = STATUS_CONFIG.AUTH_TOKEN;

  // 0. Enforce exact 4-sheet workbook layout (pruning "Status" or any extra sheets)
  enforceExactFourSheets(ss);

  // 1. Refresh Main Fleet Dashboard ("Production Fleet" tab)
  renderMainFleetDashboard(ss, timestampStr, token);

  // 2. Refresh NIM Testing Fleet Dashboard ("Testing Fleet" tab)
  renderTestingFleetDashboard(ss, timestampStr, token);

  // 3. Post-render check to ensure no extra sheet was created
  enforceExactFourSheets(ss);

  try {
    ss.toast('✅ Production Fleet & Testing Fleet updated successfully!', 'Discovery Fleet', 4);
  } catch (e) {}
}

/**
 * Backward-compatible alias for existing time-driven triggers or legacy callers.
 */
function updateStatusSheet() {
  updateFleetDashboards();
}

/**
 * ============================================================================
 * RENDER MAIN FLEET DASHBOARD ("Production Fleet" Tab - 10 Columns)
 * ============================================================================
 */
function renderMainFleetDashboard(ss, timestampStr, token) {
  var sheet = ss.getSheetByName(STATUS_CONFIG.STATUS_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(STATUS_CONFIG.STATUS_SHEET_NAME, 0);
  }
  
  sheet.clear();
  sheet.setHiddenGridlines(true);

  var sheetRows = [];
  var totalCurrent = 0;
  var totalMax = 0;
  var totalSuccess = 0;
  var totalFail = 0;
  var totalFleetTcResolved = 0;
  var totalFleetTcSuccess = 0;
  var totalFleetNimResolved = 0;
  var totalFleetNimSuccess = 0;
  var totalFleetGeminiResolved = 0;
  var totalFleetTracxnResolved = 0;
  var totalEligibleCompleted = 0;
  var activePipelines = 0;
  var totalWorkersActive = 0;
  var maxActiveTcKeys = 0;
  var maxActiveNimKeys = 0;

  var deviceMetrics = {};
  STATUS_CONFIG.DEVICES.forEach(function(dev) {
    deviceMetrics[dev.name] = {
      isOnline: false,
      activeSheets: 0,
      maxSlots: dev.maxSlots,
      cpuUsage: 0.0,
      ramUsage: 0.0,
      health: 'Offline',
      runningSheets: [],
      networkRoute: dev.networkRoute,
      activeTcKeys: 0,
      activeNimKeys: 0,
      activeWorkers: 0
    };
  });

  // Query all 4 Main Fleet sheets
  STATUS_CONFIG.TARGET_SHEETS.forEach(function(target) {
    var bestData = null;
    var assignedHost = 'Idle / Offline';
    
    for (var i = 0; i < target.endpoints.length; i++) {
      var ep = target.endpoints[i];
      try {
        var resp = UrlFetchApp.fetch(ep.url + '/discovery/status', {
          headers: {
            'Authorization': 'Bearer ' + token
          },
          muteHttpExceptions: true
        });

        if (resp.getResponseCode() === 200) {
          var data = JSON.parse(resp.getContentText());
          
          if (deviceMetrics[ep.host]) {
            deviceMetrics[ep.host].isOnline = true;
            if (data.cpu_percent !== undefined) deviceMetrics[ep.host].cpuUsage = data.cpu_percent;
            if (data.memory_percent !== undefined) deviceMetrics[ep.host].ramUsage = data.memory_percent;
            if (data.device_active_sheets !== undefined) deviceMetrics[ep.host].activeSheets = data.device_active_sheets;
            if (data.device_health) deviceMetrics[ep.host].health = data.device_health;
            if (data.active_tc_keys !== undefined) deviceMetrics[ep.host].activeTcKeys = Math.max(deviceMetrics[ep.host].activeTcKeys, data.active_tc_keys);
            if (data.active_nim_keys !== undefined) {
              var hostSheets = deviceMetrics[ep.host].activeSheets;
              var devNim = (hostSheets > 1 && data.total_nim_keys) ? data.total_nim_keys : data.active_nim_keys;
              deviceMetrics[ep.host].activeNimKeys = Math.max(deviceMetrics[ep.host].activeNimKeys, devNim);
            }
          }

          if (data.active || data.status === 'running') {
            bestData = data;
            assignedHost = ep.host;
            break;
          } else if (!bestData) {
            bestData = data;
            assignedHost = ep.host + (data.progress_current > 0 ? ' (Idle)' : ' (Ready)');
          } else if (data.progress_current > (bestData.progress_current || 0)) {
            bestData = data;
            assignedHost = ep.host + ' (Idle)';
          }
        }
      } catch (e) {}
    }

    var rawStatus = (bestData && bestData.status) || 'offline';
    var isActive = Boolean(bestData && (bestData.active === true || bestData.status === 'running'));
    var statusDisplay = (!bestData) ? '🔴 OFFLINE' : (
      isActive ? '🟢 RUNNING' : (
        (rawStatus === 'succeeded' || (bestData && bestData.phase === 'Completed') || (tot > 0 && completedCount >= tot)) ? '✅ COMPLETED' : (
          rawStatus === 'stopped' ? '⏹️ STOPPED' : '⏸️ IDLE'
        )
      )
    );
    var rawStage = (bestData && bestData.stage_details) ? String(bestData.stage_details) : '';

    var stageCompleted = 0;
    var mComp = rawStage.match(/(?:(\d+)\/\d+\s+fully completed|(\d+)\s+already completed)/i);
    if (mComp) stageCompleted = parseInt(mComp[1] || mComp[2], 10);

    var tot = (bestData && bestData.progress_total > 500) ? bestData.progress_total : 0;
    var succ = (bestData && bestData.progress_success !== undefined) ? bestData.progress_success : 0;
    var fail = (bestData && bestData.progress_fail !== undefined) ? bestData.progress_fail : 0;

    var scrapedCount = (bestData && bestData.scraped_count !== undefined) 
      ? bestData.scraped_count 
      : ((bestData && bestData.progress_success !== undefined) ? bestData.progress_success : 0);

    var tcSuccessCount = (bestData && bestData.tc_success !== undefined)
      ? bestData.tc_success
      : ((bestData && bestData.progress_success !== undefined && bestData.progress_success !== bestData.scraped_count) ? bestData.progress_success : scrapedCount);

    var nimCount = (bestData && bestData.nim_count !== undefined) ? bestData.nim_count : 0;
    var geminiCount = (bestData && bestData.gemini_count !== undefined) ? bestData.gemini_count : 0;
    var tracxnCount = (bestData && bestData.tracxn_count !== undefined && bestData.tracxn_count > 0)
      ? bestData.tracxn_count
      : ((bestData && bestData.progress_current !== undefined) ? bestData.progress_current : stageCompleted);

    var isCsvOnlyMode = (bestData && bestData.csv_only_mode === true);
    var needsDynamicAudit = (!bestData && target.sheetId);
    var tcResolved = scrapedCount;
    var tcSuccess = tcSuccessCount;
    var nimResolved = nimCount;
    var nimSuccess = nimCount;
    var geminiResolved = geminiCount;
    var tracxnResolved = tracxnCount;
    var dynAudit = null;

    if (needsDynamicAudit && target.sheetId) {
      dynAudit = auditSheetDynamically(target.sheetId);
      if (dynAudit) {
        tot = dynAudit.total;
        tcResolved = dynAudit.tcResolved;
        tcSuccess = dynAudit.tcSuccess;
        nimResolved = dynAudit.nimResolved;
        nimSuccess = dynAudit.nimSuccess;
        geminiResolved = dynAudit.geminiResolved;
        tracxnResolved = dynAudit.tracxnResolved;
      }
    }

    if (nimSuccess > 0) {
      geminiResolved = Math.min(geminiResolved, nimSuccess);
      tracxnResolved = Math.min(tracxnResolved, geminiResolved);
    }

    var tcDen = tot;
    var nimDen = tcSuccess;
    var geminiDen = nimSuccess;
    var tracxnDen = geminiResolved;

    var completedCount = (dynAudit && dynAudit.fullyCompleted !== undefined) ? dynAudit.fullyCompleted : ((bestData && bestData.progress_current !== undefined) ? bestData.progress_current : tracxnResolved);
    var eligibleCompletedCount = (dynAudit && dynAudit.eligibleCompleted !== undefined)
      ? dynAudit.eligibleCompleted
      : ((bestData && bestData.eligible_completed !== undefined && bestData.eligible_completed !== null) ? bestData.eligible_completed : 0);
    var pct = tot > 0 ? (completedCount / tot) : 0;
    pct = Math.min(pct, 1.0);

    if (isActive || rawStatus === 'running') {
      activePipelines++;
      var sheetWorkers = (bestData && bestData.active_workers) ? bestData.active_workers : 48;
      totalWorkersActive += sheetWorkers;
      if (deviceMetrics[assignedHost]) {
        deviceMetrics[assignedHost].runningSheets.push(target.shortName);
        deviceMetrics[assignedHost].activeWorkers += sheetWorkers;
      }
    }

    if (bestData && bestData.active_tc_keys) maxActiveTcKeys = Math.max(maxActiveTcKeys, bestData.active_tc_keys);
    if (bestData && bestData.active_nim_keys) {
      var nimPool = (activePipelines > 1 && bestData.total_nim_keys) ? bestData.total_nim_keys : bestData.active_nim_keys;
      maxActiveNimKeys = Math.max(maxActiveNimKeys, nimPool);
    }

    totalCurrent += completedCount;
    totalMax += tot;
    totalSuccess += succ;
    totalFail += fail;
    totalFleetTcResolved += tcResolved;
    totalFleetTcSuccess += tcSuccess;
    totalFleetNimResolved += nimResolved;
    totalFleetNimSuccess += nimSuccess;
    totalFleetGeminiResolved += geminiResolved;
    totalFleetTracxnResolved += tracxnResolved;
    totalEligibleCompleted += eligibleCompletedCount;

    var tcStr = Utilities.formatString('%s / %s', tcResolved.toLocaleString(), tcDen.toLocaleString());
    var nimStr = Utilities.formatString('%s / %s', nimResolved.toLocaleString(), nimDen.toLocaleString());
    var geminiStr = Utilities.formatString('%s / %s', geminiResolved.toLocaleString(), geminiDen.toLocaleString());
    var tracxnStr = Utilities.formatString('%s / %s', tracxnResolved.toLocaleString(), tracxnDen.toLocaleString());

    sheetRows.push({
      name: target.name,
      sheetUrl: target.sheetUrl,
      hyperlinkFormula: '=HYPERLINK("' + target.sheetUrl + '", "' + target.name + '")',
      host: assignedHost,
      status: statusDisplay,
      current: (eligibleCompletedCount > 0) ? eligibleCompletedCount : completedCount,
      total: tot,
      pct: pct,
      tc: tcStr,
      nim: nimStr,
      gemini: geminiStr,
      tracxn: tracxnStr
    });
  });

  var overallFleetPct = totalMax > 0 ? (totalCurrent / totalMax) : 0;

  // Header Banner (Rows 1-2, Columns A-J)
  sheet.getRange('A1:J1').merge();
  sheet.getRange('A1').setValue('🚀 DISCOVERY ENGINE — MAIN FLEET (P0 / OD / OR LEADS)')
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontSize(14)
    .setFontWeight('bold')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(1, 40);

  sheet.getRange('A2:J2').merge();
  sheet.getRange('A2').setValue('Last Synced: ' + timestampStr + ' (Auto Refreshed Every 15Min)  |  Fleet State: ' + activePipelines + '/4 Pipelines Active  |  10 Dedicated NIM Keys')
    .setBackground('#1E293B')
    .setFontColor('#94A3B8')
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(2, 24);

  // Top KPI Metric Cards (Rows 4-5)
  sheet.setRowHeight(3, 10);
  var kpiCards = [
    {
      rangeTitle: 'A4:B4',
      rangeVal: 'A5:B5',
      title: 'FLEET DOMAINS PROCESSED',
      val: Utilities.formatString('%s / %s (%s%%)', totalCurrent.toLocaleString(), totalMax.toLocaleString(), (overallFleetPct * 100).toFixed(1)),
      bg: '#F8FAFC'
    },
    {
      rangeTitle: 'C4:D4',
      rangeVal: 'C5:D5',
      title: 'ACTIVE PIPELINES',
      val: activePipelines + ' / 4 Running',
      bg: activePipelines > 0 ? '#F0FDF4' : '#F8FAFC'
    },
    {
      rangeTitle: 'E4:F4',
      rangeVal: 'E5:F5',
      title: 'WORKERS RUNNING IN PHASES',
      val: totalWorkersActive + ' Active Workers',
      bg: '#F8FAFC'
    },
    {
      rangeTitle: 'G4:H4',
      rangeVal: 'G5:H5',
      title: 'ACTIVE TRACXN KEYS',
      val: maxActiveTcKeys + ' / 13 Active',
      bg: '#EFF6FF'
    },
    {
      rangeTitle: 'I4:J4',
      rangeVal: 'I5:J5',
      title: 'ACTIVE NIM KEYS',
      val: maxActiveNimKeys + ' / 10 Active',
      bg: '#EFF6FF'
    }
  ];

  kpiCards.forEach(function(card) {
    sheet.getRange(card.rangeTitle).merge().setValue(card.title)
      .setBackground('#E2E8F0')
      .setFontColor('#475569')
      .setFontFamily('Arial')
      .setFontSize(9)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);

    sheet.getRange(card.rangeVal).merge().setValue(card.val)
      .setBackground(card.bg)
      .setFontColor('#0F172A')
      .setFontFamily('Arial')
      .setFontSize(11)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);
  });

  sheet.setRowHeight(4, 20);
  sheet.setRowHeight(5, 32);
  sheet.setRowHeight(6, 14);

  // TABLE 1: Engine Status (Rows 7-13, Columns A-J)
  sheet.getRange('A7:J7').merge().setValue('📊 TABLE 1: Main Fleet Engine Status (Pipeline Execution & Phase Progress)')
    .setBackground('#1E293B')
    .setFontColor('#F8FAFC')
    .setFontFamily('Arial')
    .setFontSize(11)
    .setFontWeight('bold')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(7, 28);

  var engineHeaders = [
    'Sheet Pipeline',
    'Assigned Desktop',
    'Status',
    'Completed',
    'Total Rows',
    'Progress %',
    'Tech Crawler',
    'NIM',
    'Gemini Predictions',
    'Tracxn Movements'
  ];

  sheet.getRange(8, 1, 1, engineHeaders.length).setValues([engineHeaders])
    .setBackground('#334155')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(8, 28);

  var engineRowValues = [];
  sheetRows.forEach(function(r) {
    engineRowValues.push([
      r.hyperlinkFormula,
      r.host,
      r.status,
      r.current,
      r.total,
      r.pct,
      r.tc,
      r.nim,
      r.gemini,
      r.tracxn
    ]);
  });

  var engineDataRange = sheet.getRange(9, 1, engineRowValues.length, engineHeaders.length);
  engineDataRange.setValues(engineRowValues)
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(9, 4, engineRowValues.length, 2).setNumberFormat('#,##0').setHorizontalAlignment('center');
  sheet.getRange(9, 6, engineRowValues.length, 1).setNumberFormat('0.0%').setHorizontalAlignment('center');

  for (var r = 0; r < engineRowValues.length; r++) {
    var rowIdx = 9 + r;
    var rowBg = (r % 2 === 0) ? '#FFFFFF' : '#F8FAFC';
    sheet.getRange(rowIdx, 1, 1, engineHeaders.length).setBackground(rowBg);
    sheet.setRowHeight(rowIdx, 36);

    var statusCell = sheet.getRange(rowIdx, 3);
    var stVal = engineRowValues[r][2];
    if (stVal.indexOf('RUNNING') !== -1) {
      statusCell.setBackground('#DCFCE7').setFontColor('#166534').setFontWeight('bold').setHorizontalAlignment('center');
    } else if (stVal.indexOf('COMPLETED') !== -1) {
      statusCell.setBackground('#DBEAFE').setFontColor('#1E40AF').setFontWeight('bold').setHorizontalAlignment('center');
    } else if (stVal.indexOf('STOPPED') !== -1) {
      statusCell.setBackground('#FEF3C7').setFontColor('#92400E').setFontWeight('bold').setHorizontalAlignment('center');
    } else {
      statusCell.setBackground('#F1F5F9').setFontColor('#475569').setFontWeight('bold').setHorizontalAlignment('center');
    }
  }

  // Main Fleet Total Row (Row 13)
  var engineTotalIdx = 9 + engineRowValues.length;
  var fleetTotalRow = [
    'TOTAL MAIN FLEET',
    'Both Desktops',
    activePipelines + '/4 RUNNING',
    totalCurrent,
    totalMax,
    overallFleetPct,
    Utilities.formatString('%s / %s', totalFleetTcResolved.toLocaleString(), totalMax.toLocaleString()),
    Utilities.formatString('%s / %s', totalFleetNimResolved.toLocaleString(), totalFleetTcSuccess.toLocaleString()),
    Utilities.formatString('%s / %s', totalFleetGeminiResolved.toLocaleString(), totalFleetNimSuccess.toLocaleString()),
    Utilities.formatString('%s / %s', totalFleetTracxnResolved.toLocaleString(), totalFleetGeminiResolved.toLocaleString())
  ];

  sheet.getRange(engineTotalIdx, 1, 1, engineHeaders.length).setValues([fleetTotalRow])
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(engineTotalIdx, 4, 1, 2).setNumberFormat('#,##0').setHorizontalAlignment('center');
  sheet.getRange(engineTotalIdx, 6).setNumberFormat('0.0%').setHorizontalAlignment('center');
  sheet.setRowHeight(engineTotalIdx, 30);

  sheet.getRange(8, 1, engineRowValues.length + 2, engineHeaders.length)
    .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);

  // TABLE 2: Fleet Status (Device Health Report) (Rows 15-19, Columns A-J)
  var fleetTableStart = engineTotalIdx + 2;
  sheet.setRowHeight(fleetTableStart - 1, 14);

  sheet.getRange(fleetTableStart, 1, 1, 10).merge().setValue('🖥️  TABLE 2: Main Fleet Status (Device Health Report)')
    .setBackground('#1E293B')
    .setFontColor('#F8FAFC')
    .setFontFamily('Arial')
    .setFontSize(11)
    .setFontWeight('bold')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(fleetTableStart, 28);

  var fleetHeaders = [
    'Device Name',
    'Host Status',
    '# Sheets Running',
    'Assigned Sheets',
    'Workers in Phase',
    'CPU Usage',
    'RAM Usage',
    'Device Health State',
    'Active TC Tokens',
    'Active NIM Keys'
  ];

  sheet.getRange(fleetTableStart + 1, 1, 1, fleetHeaders.length).setValues([fleetHeaders])
    .setBackground('#334155')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(fleetTableStart + 1, 28);

  var fleetRows = [];
  var devNames = ['Desktop-5080', 'Desktop-5321'];
  var onlineDevCount = 0;
  var sumCpu = 0;
  var sumRam = 0;

  devNames.forEach(function(dName) {
    var d = deviceMetrics[dName];
    if (d.isOnline) onlineDevCount++;
    sumCpu += d.cpuUsage;
    sumRam += d.ramUsage;

    var statusText = d.isOnline ? '🟢 Online' : '🔴 Offline';
    var sheetsRunningStr = d.activeSheets + ' / ' + d.maxSlots + ' Running';
    var cpuVal = d.isOnline ? (d.cpuUsage / 100) : 0;
    var ramVal = d.isOnline ? (d.ramUsage / 100) : 0;
    var runningSheetsStr = d.runningSheets.length > 0 ? d.runningSheets.join(' + ') : 'Idle';
    var workersCount = d.activeWorkers > 0 ? d.activeWorkers : (d.activeSheets * 48);
    var avgSheetWorkers = d.activeSheets > 0 ? Math.round(workersCount / d.activeSheets) : 0;
    var workersStr = d.isOnline ? (workersCount + ' Workers (' + (d.activeSheets > 0 ? '~' + avgSheetWorkers + '/sheet' : 'Idle') + ')') : '0 Workers';
    var healthStr = d.isOnline ? (d.cpuUsage > 80 || d.ramUsage > 80 ? '⚠️ High Load' : '🟢 Healthy') : 'Offline';
    var tcTokensStr = d.isOnline ? (d.activeTcKeys + ' / 10 Active Tokens') : '0 / 10 Active Tokens';
    var nimKeysStr = d.isOnline ? (d.activeNimKeys + ' / 10 Active Keys') : '0 / 10 Active Keys';

    fleetRows.push([
      dName,
      statusText,
      sheetsRunningStr,
      runningSheetsStr,
      workersStr,
      cpuVal,
      ramVal,
      healthStr,
      tcTokensStr,
      nimKeysStr
    ]);
  });

  var fleetDataRange = sheet.getRange(fleetTableStart + 2, 1, fleetRows.length, fleetHeaders.length);
  fleetDataRange.setValues(fleetRows)
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(fleetTableStart + 2, 6, fleetRows.length, 2).setNumberFormat('0.0%').setHorizontalAlignment('center');

  for (var f = 0; f < fleetRows.length; f++) {
    var fRowIdx = fleetTableStart + 2 + f;
    sheet.getRange(fRowIdx, 1, 1, fleetHeaders.length).setBackground('#FFFFFF');
    sheet.setRowHeight(fRowIdx, 30);
    sheet.getRange(fRowIdx, 2).setBackground('#DCFCE7').setFontColor('#166534').setFontWeight('bold').setHorizontalAlignment('center');
    sheet.getRange(fRowIdx, 7).setBackground('#F0FDF4').setFontColor('#166534').setFontWeight('bold').setHorizontalAlignment('center');
  }

  var fleetTotalIdx = fleetTableStart + 2 + fleetRows.length;
  var avgCpu = (onlineDevCount > 0) ? (sumCpu / onlineDevCount / 100) : 0;
  var avgRam = (onlineDevCount > 0) ? (sumRam / onlineDevCount / 100) : 0;
  var fleetOverallHealth = (onlineDevCount === devNames.length) ? '🟢 Optimal Load' : (onlineDevCount > 0 ? '⚠️ Degraded' : '🔴 Offline');

  var fleetSummaryRow = [
    'TOTAL PRODUCTION FLEET',
    '🟢 ' + onlineDevCount + '/' + devNames.length + ' Online',
    activePipelines + ' / 4 Running',
    (activePipelines > 0 ? 'Sheets ' + (sheetRows.filter(function(r){ return r.status.indexOf('RUNNING') !== -1; }).map(function(r){ return r.name; }).join(', ')) : 'All Idle'),
    totalWorkersActive + ' Total Workers',
    avgCpu,
    avgRam,
    fleetOverallHealth,
    maxActiveTcKeys + ' / 10 Active Tokens',
    maxActiveNimKeys + ' / 10 Active Keys'
  ];

  sheet.getRange(fleetTotalIdx, 1, 1, fleetHeaders.length).setValues([fleetSummaryRow])
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(fleetTotalIdx, 6, 1, 2).setNumberFormat('0.0%').setHorizontalAlignment('center');
  sheet.setRowHeight(fleetTotalIdx, 30);

  sheet.getRange(fleetTableStart + 1, 1, fleetRows.length + 2, fleetHeaders.length)
    .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);

  var colWidths = [239, 140, 170, 204, 167, 105, 140, 140, 155, 155];
  for (var c = 0; c < colWidths.length; c++) {
    sheet.setColumnWidth(c + 1, colWidths[c]);
  }

  if (sheet.getMaxRows() > 19) {
    sheet.deleteRows(20, sheet.getMaxRows() - 19);
  }
  if (sheet.getMaxColumns() > 10) {
    sheet.deleteColumns(11, sheet.getMaxColumns() - 10);
  }
}

/**
 * ============================================================================
 * RENDER NIM TESTING FLEET DASHBOARD ("Testing Fleet" Tab - 11 Columns)
 * ============================================================================
 * 
 * Tracks the NIM Fleet on Desktop-9048.
 * Table 1 (11 Columns):
 *   Sheet Pipeline, Assigned Desktop, Status, Completed, Total Rows, Progress %,
 *   Tech Crawler, NIM Summary, NIM Industry, NIM Feed, Tracxn Movements.
 */
function renderTestingFleetDashboard(ss, timestampStr, token) {
  var sheet = ss.getSheetByName(STATUS_CONFIG.TESTING_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(STATUS_CONFIG.TESTING_SHEET_NAME, 1);
  }

  sheet.clear();
  sheet.setHiddenGridlines(true);

  var sheetRows = [];
  var totalCurrent = 0;
  var totalMax = 0;
  var totalSuccess = 0;
  var totalFail = 0;
  var totalNimTcResolved = 0;
  var totalNimTcSuccess = 0;
  var totalNimSummaryResolved = 0;
  var totalNimSummarySuccess = 0;
  var totalNimIfResolved = 0;
  var totalNimFeedResolved = 0;
  var totalNimTracxnResolved = 0;
  var totalEligibleCompleted = 0;
  var activePipelines = 0;
  var totalWorkersActive = 0;
  var maxActiveTcKeys = 0;
  var maxActiveNimKeys = 0;

  var deviceMetrics = {};
  STATUS_CONFIG.NIM_DEVICES.forEach(function(dev) {
    deviceMetrics[dev.name] = {
      isOnline: false,
      activeSheets: 0,
      maxSlots: dev.maxSlots,
      cpuUsage: 0.0,
      ramUsage: 0.0,
      health: 'Offline',
      runningSheets: [],
      networkRoute: dev.networkRoute,
      activeTcKeys: 0,
      activeNimKeys: 0,
      activeWorkers: 0
    };
  });

  // Query NIM Fleet sheet
  STATUS_CONFIG.NIM_TARGET_SHEETS.forEach(function(target) {
    var bestData = null;
    var assignedHost = 'Idle / Offline';

    for (var i = 0; i < target.endpoints.length; i++) {
      var ep = target.endpoints[i];
      try {
        var resp = UrlFetchApp.fetch(ep.url + '/discovery/status', {
          headers: {
            'Authorization': 'Bearer ' + token
          },
          muteHttpExceptions: true
        });

        if (resp.getResponseCode() === 200) {
          var data = JSON.parse(resp.getContentText());

          if (deviceMetrics[ep.host]) {
            deviceMetrics[ep.host].isOnline = true;
            if (data.cpu_percent !== undefined) deviceMetrics[ep.host].cpuUsage = data.cpu_percent;
            if (data.memory_percent !== undefined) deviceMetrics[ep.host].ramUsage = data.memory_percent;
            if (data.device_active_sheets !== undefined) deviceMetrics[ep.host].activeSheets = data.device_active_sheets;
            if (data.device_health) deviceMetrics[ep.host].health = data.device_health;
            if (data.active_tc_keys !== undefined) deviceMetrics[ep.host].activeTcKeys = Math.max(deviceMetrics[ep.host].activeTcKeys, data.active_tc_keys);
            if (data.active_nim_keys !== undefined) {
              var devNim = data.total_nim_keys || data.active_nim_keys || 40;
              deviceMetrics[ep.host].activeNimKeys = Math.max(deviceMetrics[ep.host].activeNimKeys, devNim);
            }
          }

          if (data.active || data.status === 'running') {
            bestData = data;
            assignedHost = ep.host;
            break;
          } else if (!bestData) {
            bestData = data;
            assignedHost = ep.host + (data.progress_current > 0 ? ' (Idle)' : ' (Ready)');
          } else if (data.progress_current > (bestData.progress_current || 0)) {
            bestData = data;
            assignedHost = ep.host + ' (Idle)';
          }
        }
      } catch (e) {}
    }

    var rawStatus = (bestData && bestData.status) || 'offline';
    var isActive = Boolean(bestData && (bestData.active === true || bestData.status === 'running'));
    var statusDisplay = (!bestData) ? '🔴 OFFLINE' : (
      isActive ? '🟢 RUNNING' : (
        (rawStatus === 'succeeded' || (bestData && bestData.phase === 'Completed') || (tot > 0 && completedCount >= tot)) ? '✅ COMPLETED' : (
          rawStatus === 'stopped' ? '⏹️ STOPPED' : '⏸️ IDLE'
        )
      )
    );
    var rawStage = (bestData && bestData.stage_details) ? String(bestData.stage_details) : '';

    var stageCompleted = 0;
    var mComp = rawStage.match(/(?:(\d+)\/\d+\s+fully completed|(\d+)\s+already completed)/i);
    if (mComp) stageCompleted = parseInt(mComp[1] || mComp[2], 10);

    var tot = (bestData && bestData.progress_total > 0) ? bestData.progress_total : 0;
    var succ = (bestData && bestData.progress_success !== undefined) ? bestData.progress_success : 0;
    var fail = (bestData && bestData.progress_fail !== undefined) ? bestData.progress_fail : 0;

    var scrapedCount = (bestData && bestData.scraped_count !== undefined) 
      ? bestData.scraped_count 
      : ((bestData && bestData.progress_success !== undefined) ? bestData.progress_success : 0);

    var tcSuccessCount = (bestData && bestData.tc_success !== undefined)
      ? bestData.tc_success
      : ((bestData && bestData.progress_success !== undefined && bestData.progress_success !== bestData.scraped_count) ? bestData.progress_success : scrapedCount);

    var nimCount = (bestData && bestData.nim_count !== undefined) ? bestData.nim_count : 0;
    var ifCount = (bestData && bestData.gemini_count !== undefined) ? bestData.gemini_count : 0;
    var feedCount = (bestData && bestData.pf_completed !== undefined) ? bestData.pf_completed : ifCount;
    var tracxnCount = (bestData && bestData.tracxn_count !== undefined && bestData.tracxn_count > 0)
      ? bestData.tracxn_count
      : ((bestData && bestData.progress_current !== undefined) ? bestData.progress_current : stageCompleted);

    var isCsvOnlyMode = (bestData && bestData.csv_only_mode === true);
    var needsDynamicAudit = (!bestData && target.sheetId);
    
    var tcResolved = scrapedCount;
    var tcSuccess = tcSuccessCount;
    var nimSummaryResolved = nimCount;
    var nimSummarySuccess = nimCount;
    var nimIfResolved = ifCount;
    var nimFeedResolved = feedCount;
    var tracxnResolved = tracxnCount;
    var dynAudit = null;

    if (needsDynamicAudit && target.sheetId) {
      dynAudit = auditSheetDynamically(target.sheetId);
      if (dynAudit) {
        tot = dynAudit.total;
        tcResolved = dynAudit.tcResolved;
        tcSuccess = dynAudit.tcSuccess;
        nimSummaryResolved = dynAudit.nimSummaryResolved;
        nimSummarySuccess = dynAudit.nimSummarySuccess;
        nimIfResolved = dynAudit.nimIfResolved;
        nimFeedResolved = dynAudit.nimFeedResolved;
        tracxnResolved = dynAudit.tracxnResolved;
      }
    }

    // Cascading waterfall constraints: Tracxn <= (Feed || IF) <= Summary <= TC <= Total
    if (nimSummarySuccess > 0) {
      nimIfResolved = Math.min(nimIfResolved, nimSummarySuccess);
      if (nimFeedResolved > 0) {
        nimFeedResolved = Math.min(nimFeedResolved, nimIfResolved);
        tracxnResolved = Math.min(tracxnResolved, nimFeedResolved);
      } else {
        tracxnResolved = Math.min(tracxnResolved, nimIfResolved);
      }
    }

    var tcDen = tot;
    var nimSummaryDen = tcSuccess;
    var nimIfDen = nimSummarySuccess;
    var nimFeedDen = nimIfResolved;
    var tracxnDen = (nimFeedResolved > 0) ? nimFeedResolved : (nimIfResolved > 0 ? nimIfResolved : tot);

    var completedCount = (dynAudit && dynAudit.fullyCompleted !== undefined) ? dynAudit.fullyCompleted : ((bestData && bestData.progress_current !== undefined) ? bestData.progress_current : tracxnResolved);
    var eligibleCompletedCount = (dynAudit && dynAudit.eligibleCompleted !== undefined)
      ? dynAudit.eligibleCompleted
      : ((bestData && bestData.eligible_completed !== undefined && bestData.eligible_completed !== null) ? bestData.eligible_completed : 0);
    var pct = tot > 0 ? (completedCount / tot) : 0;
    pct = Math.min(pct, 1.0);

    if (isActive || rawStatus === 'running') {
      activePipelines++;
      var sheetWorkers = (bestData && bestData.active_workers) ? bestData.active_workers : 61;
      totalWorkersActive += sheetWorkers;
      if (deviceMetrics[assignedHost]) {
        deviceMetrics[assignedHost].runningSheets.push(target.shortName);
        deviceMetrics[assignedHost].activeWorkers += sheetWorkers;
      }
    }

    if (bestData && bestData.active_tc_keys) maxActiveTcKeys = Math.max(maxActiveTcKeys, bestData.active_tc_keys);
    maxActiveNimKeys = 40; // Desktop-9048 is allocated all 40 dedicated keys

    totalCurrent += completedCount;
    totalMax += tot;
    totalSuccess += succ;
    totalFail += fail;
    totalNimTcResolved += tcResolved;
    totalNimTcSuccess += tcSuccess;
    totalNimSummaryResolved += nimSummaryResolved;
    totalNimSummarySuccess += nimSummarySuccess;
    totalNimIfResolved += nimIfResolved;
    totalNimFeedResolved += nimFeedResolved;
    totalNimTracxnResolved += tracxnResolved;
    totalEligibleCompleted += eligibleCompletedCount;

    var tcStr = Utilities.formatString('%s / %s', tcResolved.toLocaleString(), tcDen.toLocaleString());
    var nimSumStr = Utilities.formatString('%s / %s', nimSummaryResolved.toLocaleString(), nimSummaryDen.toLocaleString());
    var nimIfStr = Utilities.formatString('%s / %s', nimIfResolved.toLocaleString(), nimIfDen.toLocaleString());
    var nimFeedStr = Utilities.formatString('%s / %s', nimFeedResolved.toLocaleString(), nimFeedDen.toLocaleString());
    var tracxnStr = Utilities.formatString('%s / %s', tracxnResolved.toLocaleString(), tracxnDen.toLocaleString());

    sheetRows.push({
      name: target.name,
      sheetUrl: target.sheetUrl,
      hyperlinkFormula: '=HYPERLINK("' + target.sheetUrl + '", "' + target.name + '")',
      host: assignedHost,
      status: statusDisplay,
      current: (eligibleCompletedCount > 0) ? eligibleCompletedCount : completedCount,
      total: tot,
      pct: pct,
      tc: tcStr,
      nimSummary: nimSumStr,
      nimIf: nimIfStr,
      nimFeed: nimFeedStr,
      tracxn: tracxnStr
    });
  });

  var overallFleetPct = totalMax > 0 ? (totalCurrent / totalMax) : 0;

  // Header Banner (Rows 1-2, Columns A-K)
  sheet.getRange('A1:K1').merge();
  sheet.getRange('A1').setValue('🚀 DISCOVERY ENGINE — NIM TESTING FLEET (END-TO-END NIM INFERENCE)')
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontSize(14)
    .setFontWeight('bold')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(1, 40);

  sheet.getRange('A2:K2').merge();
  sheet.getRange('A2').setValue('Last Synced: ' + timestampStr + ' (Auto Refreshed Every 15Min)  |  Fleet State: ' + activePipelines + '/1 Pipeline Active  |  Dedicated 40 NIM Keys (Zero Collision)')
    .setBackground('#1E293B')
    .setFontColor('#94A3B8')
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(2, 24);

  // Top KPI Metric Cards (Rows 4-5 across 11 Columns A-K)
  sheet.setRowHeight(3, 10);
  var kpiCards = [
    {
      rangeTitle: 'A4:B4',
      rangeVal: 'A5:B5',
      title: 'NIM FLEET DOMAINS PROCESSED',
      val: Utilities.formatString('%s / %s (%s%%)', totalCurrent.toLocaleString(), totalMax.toLocaleString(), (overallFleetPct * 100).toFixed(1)),
      bg: '#F8FAFC'
    },
    {
      rangeTitle: 'C4:D4',
      rangeVal: 'C5:D5',
      title: 'ACTIVE PIPELINES',
      val: activePipelines + ' / 1 Running',
      bg: activePipelines > 0 ? '#F0FDF4' : '#F8FAFC'
    },
    {
      rangeTitle: 'E4:F4',
      rangeVal: 'E5:F5',
      title: 'WORKERS RUNNING IN PHASES',
      val: totalWorkersActive + ' Active Workers',
      bg: '#F8FAFC'
    },
    {
      rangeTitle: 'G4:H4',
      rangeVal: 'G5:H5',
      title: 'ACTIVE TRACXN KEYS',
      val: maxActiveTcKeys + ' / 16 Active',
      bg: '#EFF6FF'
    },
    {
      rangeTitle: 'I4:K4',
      rangeVal: 'I5:K5',
      title: 'ACTIVE NIM KEYS',
      val: '40 Dedicated Keys (14 Sum / 13 IF / 13 Feed)',
      bg: '#EFF6FF'
    }
  ];

  kpiCards.forEach(function(card) {
    sheet.getRange(card.rangeTitle).merge().setValue(card.title)
      .setBackground('#E2E8F0')
      .setFontColor('#475569')
      .setFontFamily('Arial')
      .setFontSize(9)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);

    sheet.getRange(card.rangeVal).merge().setValue(card.val)
      .setBackground(card.bg)
      .setFontColor('#0F172A')
      .setFontFamily('Arial')
      .setFontSize(11)
      .setFontWeight('bold')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);
  });

  sheet.setRowHeight(4, 20);
  sheet.setRowHeight(5, 32);
  sheet.setRowHeight(6, 14);

  // TABLE 1: NIM Fleet Engine Status (Rows 7-10, Columns A-K)
  sheet.getRange('A7:K7').merge().setValue('📊 TABLE 1: NIM Fleet Engine Status (5-Stage NIM Pipeline Progress)')
    .setBackground('#1E293B')
    .setFontColor('#F8FAFC')
    .setFontFamily('Arial')
    .setFontSize(11)
    .setFontWeight('bold')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(7, 28);

  // Exact 11 Columns per user instruction:
  // "Tech Crawler" "NIM Summary" "NIM Industry" "NIM Feed" "Tracxn Movements"
  var engineHeaders = [
    'Sheet Pipeline',
    'Assigned Desktop',
    'Status',
    'Completed',
    'Total Rows',
    'Progress %',
    'Tech Crawler',
    'NIM Summary',
    'NIM Industry',
    'NIM Feed',
    'Tracxn Movements'
  ];

  sheet.getRange(8, 1, 1, engineHeaders.length).setValues([engineHeaders])
    .setBackground('#334155')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(8, 28);

  var engineRowValues = [];
  sheetRows.forEach(function(r) {
    engineRowValues.push([
      r.hyperlinkFormula,
      r.host,
      r.status,
      r.current,
      r.total,
      r.pct,
      r.tc,
      r.nimSummary,
      r.nimIf,
      r.nimFeed,
      r.tracxn
    ]);
  });

  var engineDataRange = sheet.getRange(9, 1, engineRowValues.length, engineHeaders.length);
  engineDataRange.setValues(engineRowValues)
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(9, 4, engineRowValues.length, 2).setNumberFormat('#,##0').setHorizontalAlignment('center');
  sheet.getRange(9, 6, engineRowValues.length, 1).setNumberFormat('0.0%').setHorizontalAlignment('center');

  for (var r = 0; r < engineRowValues.length; r++) {
    var rowIdx = 9 + r;
    var rowBg = (r % 2 === 0) ? '#FFFFFF' : '#F8FAFC';
    sheet.getRange(rowIdx, 1, 1, engineHeaders.length).setBackground(rowBg);
    sheet.setRowHeight(rowIdx, 36);

    var statusCell = sheet.getRange(rowIdx, 3);
    var stVal = engineRowValues[r][2];
    if (stVal.indexOf('RUNNING') !== -1) {
      statusCell.setBackground('#DCFCE7').setFontColor('#166534').setFontWeight('bold').setHorizontalAlignment('center');
    } else if (stVal.indexOf('COMPLETED') !== -1) {
      statusCell.setBackground('#DBEAFE').setFontColor('#1E40AF').setFontWeight('bold').setHorizontalAlignment('center');
    } else if (stVal.indexOf('STOPPED') !== -1) {
      statusCell.setBackground('#FEF3C7').setFontColor('#92400E').setFontWeight('bold').setHorizontalAlignment('center');
    } else {
      statusCell.setBackground('#F1F5F9').setFontColor('#475569').setFontWeight('bold').setHorizontalAlignment('center');
    }
  }

  // NIM Fleet Total Row (Row 10)
  var engineTotalIdx = 9 + engineRowValues.length;
  var totalCompletedVal = totalCurrent;
  var totalTracxnDen = (totalNimFeedResolved > 0) ? totalNimFeedResolved : (totalNimIfResolved > 0 ? totalNimIfResolved : totalMax);
  var fleetTotalRow = [
    'TOTAL NIM FLEET',
    'Desktop-9048',
    activePipelines + '/1 RUNNING',
    totalCompletedVal,
    totalMax,
    overallFleetPct,
    Utilities.formatString('%s / %s', totalNimTcResolved.toLocaleString(), totalMax.toLocaleString()),
    Utilities.formatString('%s / %s', totalNimSummaryResolved.toLocaleString(), totalNimTcSuccess.toLocaleString()),
    Utilities.formatString('%s / %s', totalNimIfResolved.toLocaleString(), totalNimSummarySuccess.toLocaleString()),
    Utilities.formatString('%s / %s', totalNimFeedResolved.toLocaleString(), totalNimIfResolved.toLocaleString()),
    Utilities.formatString('%s / %s', totalNimTracxnResolved.toLocaleString(), totalTracxnDen.toLocaleString())
  ];

  sheet.getRange(engineTotalIdx, 1, 1, engineHeaders.length).setValues([fleetTotalRow])
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(engineTotalIdx, 4, 1, 2).setNumberFormat('#,##0').setHorizontalAlignment('center');
  sheet.getRange(engineTotalIdx, 6).setNumberFormat('0.0%').setHorizontalAlignment('center');
  sheet.setRowHeight(engineTotalIdx, 30);

  sheet.getRange(8, 1, engineRowValues.length + 2, engineHeaders.length)
    .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);

  // TABLE 2: NIM Device Health Report (Rows 12-16)
  var fleetTableStart = engineTotalIdx + 2;
  sheet.setRowHeight(fleetTableStart - 1, 14);

  sheet.getRange(fleetTableStart, 1, 1, 11).merge().setValue('🖥️  TABLE 2: NIM Fleet Status (Device Health Report)')
    .setBackground('#1E293B')
    .setFontColor('#F8FAFC')
    .setFontFamily('Arial')
    .setFontSize(11)
    .setFontWeight('bold')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(fleetTableStart, 28);

  var fleetHeaders = [
    'Device Name',
    'Host Status',
    '# Sheets Running',
    'Assigned Sheets',
    'Workers in Phase',
    'CPU Usage',
    'RAM Usage',
    'Device Health State',
    'Active TC Tokens',
    'Active NIM Keys'
  ];

  // Set 10 headers; merge columns J & K for Active NIM Keys to span all 11 columns cleanly
  sheet.getRange(fleetTableStart + 1, 1, 1, 9).setValues([fleetHeaders.slice(0, 9)])
    .setBackground('#334155')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.getRange(fleetTableStart + 1, 10, 1, 2).merge().setValue('Active NIM Keys')
    .setBackground('#334155')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.setRowHeight(fleetTableStart + 1, 28);

  var fleetRows = [];
  var devNames = ['Desktop-9048'];
  var onlineDevCount = 0;
  var sumCpu = 0;
  var sumRam = 0;

  devNames.forEach(function(dName) {
    var d = deviceMetrics[dName];
    if (d.isOnline) onlineDevCount++;
    sumCpu += d.cpuUsage;
    sumRam += d.ramUsage;

    var statusText = d.isOnline ? '🟢 Online' : '🔴 Offline';
    var sheetsRunningStr = d.activeSheets + ' / ' + d.maxSlots + ' Running';
    var cpuVal = d.isOnline ? (d.cpuUsage / 100) : 0;
    var ramVal = d.isOnline ? (d.ramUsage / 100) : 0;
    var runningSheetsStr = 'NIM Fleet (CF-1)';
    var workersCount = d.activeWorkers > 0 ? d.activeWorkers : 61;
    var workersStr = d.isOnline ? (workersCount + ' Workers (~' + workersCount + '/sheet)') : '0 Workers';
    var healthStr = d.isOnline ? (d.cpuUsage > 80 || d.ramUsage > 80 ? '⚠️ High Load' : '🟢 Healthy') : 'Offline';
    var tcTokensStr = d.isOnline ? (d.activeTcKeys + ' / 3 Active Tokens') : '0 / 3 Active Tokens';
    var nimKeysStr = '40 Dedicated Keys (14 Sum / 13 IF / 13 Feed)';

    fleetRows.push([
      dName,
      statusText,
      sheetsRunningStr,
      runningSheetsStr,
      workersStr,
      cpuVal,
      ramVal,
      healthStr,
      tcTokensStr,
      nimKeysStr
    ]);
  });

  sheet.getRange(fleetTableStart + 2, 1, fleetRows.length, 9).setValues([fleetRows[0].slice(0, 9)])
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.getRange(fleetTableStart + 2, 10, 1, 2).merge().setValue(fleetRows[0][9])
    .setFontFamily('Arial')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(fleetTableStart + 2, 6, fleetRows.length, 2).setNumberFormat('0.0%').setHorizontalAlignment('center');

  var fRowIdx = fleetTableStart + 2;
  sheet.getRange(fRowIdx, 1, 1, 11).setBackground('#FFFFFF');
  sheet.setRowHeight(fRowIdx, 30);
  sheet.getRange(fRowIdx, 2).setBackground('#DCFCE7').setFontColor('#166534').setFontWeight('bold').setHorizontalAlignment('center');
  sheet.getRange(fRowIdx, 7).setBackground('#F0FDF4').setFontColor('#166534').setFontWeight('bold').setHorizontalAlignment('center');

  var fleetTotalIdx = fleetTableStart + 3;
  var avgCpu = (onlineDevCount > 0) ? (sumCpu / onlineDevCount / 100) : 0;
  var avgRam = (onlineDevCount > 0) ? (sumRam / onlineDevCount / 100) : 0;
  var fleetOverallHealth = (onlineDevCount === devNames.length) ? '🟢 Optimal Load' : '🔴 Offline';

  var fleetSummaryRow = [
    'TOTAL NIM TESTING FLEET',
    '🟢 ' + onlineDevCount + '/' + devNames.length + ' Online',
    activePipelines + ' / 1 Running',
    (activePipelines > 0 ? 'NIM Fleet Active' : 'All Idle'),
    totalWorkersActive + ' Total Workers',
    avgCpu,
    avgRam,
    fleetOverallHealth,
    maxActiveTcKeys + ' / 3 Active Tokens'
  ];

  sheet.getRange(fleetTotalIdx, 1, 1, 9).setValues([fleetSummaryRow])
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  sheet.getRange(fleetTotalIdx, 10, 1, 2).merge().setValue('40 Dedicated Keys (3-Way Split)')
    .setBackground('#0F172A')
    .setFontColor('#FFFFFF')
    .setFontFamily('Arial')
    .setFontWeight('bold')
    .setFontSize(10)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');

  sheet.getRange(fleetTotalIdx, 6, 1, 2).setNumberFormat('0.0%').setHorizontalAlignment('center');
  sheet.setRowHeight(fleetTotalIdx, 30);

  sheet.getRange(fleetTableStart + 1, 1, 3, 11)
    .setBorder(true, true, true, true, true, true, '#CBD5E1', SpreadsheetApp.BorderStyle.SOLID);

  // Exact 11 Column Widths for Testing Fleet
  var colWidths = [230, 135, 155, 140, 130, 95, 130, 130, 130, 130, 150];
  for (var c = 0; c < colWidths.length; c++) {
    sheet.setColumnWidth(c + 1, colWidths[c]);
  }

  if (sheet.getMaxRows() > 19) {
    sheet.deleteRows(20, sheet.getMaxRows() - 19);
  }
  if (sheet.getMaxColumns() > 11) {
    sheet.deleteColumns(12, sheet.getMaxColumns() - 11);
  }
}

/**
 * ============================================================================
 * CUSTOM MENU & INSTANT CONTROL SIGNALS
 * ============================================================================
 */
function onOpen() {
  var ss = null;
  try {
    ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) enforceExactFourSheets(ss);
  } catch (e) {}

  var ui = SpreadsheetApp.getUi();
  ui.createMenu('⚡ Discovery Fleet')
    .addItem('📊 Update Fleet Dashboards (Production & Testing)', 'updateFleetDashboards')
    .addItem('📊 Refresh Status (Alias)', 'updateStatusSheet')
    .addSeparator()
    .addItem('▶️ Resume Sheet 1 (CF-1)', 'resumeSheet1')
    .addItem('▶️ Resume Sheet 2 (CF-2)', 'resumeSheet2')
    .addItem('▶️ Resume Sheet 3 (CF-3)', 'resumeSheet3')
    .addItem('▶️ Resume Sheet 4 (Filter-V2)', 'resumeSheet4')
    .addItem('▶️ Resume NIM Fleet (Desktop-9048)', 'resumeNimFleet')
    .addItem('🚀 Resume All Sheets (Main Fleet)', 'resumeAllSheets')
    .addSeparator()
    .addItem('⏹️ Stop Sheet 1 (CF-1)', 'stopSheet1')
    .addItem('⏹️ Stop Sheet 2 (CF-2)', 'stopSheet2')
    .addItem('⏹️ Stop Sheet 3 (CF-3)', 'stopSheet3')
    .addItem('⏹️ Stop Sheet 4 (Filter-V2)', 'stopSheet4')
    .addItem('⏹️ Stop NIM Fleet (Desktop-9048)', 'stopNimFleet')
    .addItem('🛑 Stop All Sheets (Main Fleet)', 'stopAllSheets')
    .addSeparator()
    .addItem("🌐 Launch Tomorrow's Pre-Crawl (All 4 Sheets In Parallel)", 'launchFleetAdvancePreCrawlUi')
    .addToUi();
}

/**
 * Resume signal dispatcher for Main Fleet sheets (Sheets 1–4).
 */
function sendResumeSignal(sheetNum, isBatch) {
  var target = null;
  for (var i = 0; i < STATUS_CONFIG.TARGET_SHEETS.length; i++) {
    if (STATUS_CONFIG.TARGET_SHEETS[i].num === sheetNum) {
      target = STATUS_CONFIG.TARGET_SHEETS[i];
      break;
    }
  }

  if (!target) {
    Logger.log('Target sheet ' + sheetNum + ' not found in configuration.');
    return false;
  }

  var token = STATUS_CONFIG.AUTH_TOKEN;
  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch (e) {}

  for (var j = 0; j < target.endpoints.length; j++) {
    var epCheck = target.endpoints[j];
    try {
      var checkResp = UrlFetchApp.fetch(epCheck.url + '/discovery/status', {
        headers: {
          'Authorization': 'Bearer ' + token
        },
        muteHttpExceptions: true
      });
      if (checkResp.getResponseCode() === 200) {
        var statusData = JSON.parse(checkResp.getContentText());
        if (statusData.active || statusData.status === 'running') {
          if (!isBatch && ss) {
            ss.toast('ℹ️ ' + target.name + ' is already running on ' + epCheck.host, 'Discovery Fleet', 5);
          }
          return true;
        }
      }
    } catch (e) {}
  }

  var dispatched = false;
  var lastError = '';

  for (var k = 0; k < target.endpoints.length; k++) {
    var ep = target.endpoints[k];
    try {
      var payload = JSON.stringify({
        start_row: 3,
        mode: 'full',
        sheet_id: target.sheetId
      });

      var res = UrlFetchApp.fetch(ep.url + '/discovery/start', {
        method: 'post',
        contentType: 'application/json',
        headers: {
          'Authorization': 'Bearer ' + token
        },
        payload: payload,
        muteHttpExceptions: true
      });

      var code = res.getResponseCode();
      var bodyText = res.getContentText();
      
      if (code === 200) {
        var resJson = {};
        try { resJson = JSON.parse(bodyText); } catch(ex) {}
        
        if (resJson.status === 'accepted' || resJson.status === 'ok') {
          dispatched = true;
          if (!isBatch && ss) {
            ss.toast('✅ Resumed ' + target.name + ' on ' + ep.host, 'Discovery Fleet', 5);
          }
          break;
        } else if (resJson.status === 'busy') {
          lastError = ep.host + ' is at maximum device capacity (2/2 sheets).';
          continue;
        } else {
          lastError = resJson.message || bodyText;
        }
      } else {
        lastError = 'HTTP ' + code + ': ' + bodyText;
      }
    } catch (err) {
      lastError = err.message || err.toString();
    }
  }

  if (!dispatched) {
    if (!isBatch && ss) {
      ss.toast('⚠️ Could not resume ' + target.name + ': ' + lastError, 'Discovery Fleet', 6);
    }
    return false;
  }

  if (!isBatch) {
    Utilities.sleep(1000);
    updateStatusSheet();
  }
  return true;
}

/**
 * Stop signal dispatcher for Main Fleet sheets (Sheets 1–4).
 */
function sendStopSignal(sheetNum, isBatch) {
  var target = null;
  for (var i = 0; i < STATUS_CONFIG.TARGET_SHEETS.length; i++) {
    if (STATUS_CONFIG.TARGET_SHEETS[i].num === sheetNum) {
      target = STATUS_CONFIG.TARGET_SHEETS[i];
      break;
    }
  }

  if (!target) {
    Logger.log('Target sheet ' + sheetNum + ' not found in configuration.');
    return false;
  }

  var token = STATUS_CONFIG.AUTH_TOKEN;
  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch (e) {}

  var stopped = false;
  for (var i = 0; i < target.endpoints.length; i++) {
    var ep = target.endpoints[i];
    try {
      var res = UrlFetchApp.fetch(ep.url + '/discovery/cancel', {
        method: 'post',
        headers: {
          'Authorization': 'Bearer ' + token
        },
        muteHttpExceptions: true
      });

      if (res.getResponseCode() === 200) {
        var body = {};
        try { body = JSON.parse(res.getContentText()); } catch(ex) {}
        if (body.status === 'stopping' || body.status === 'ok') {
          stopped = true;
          if (!isBatch && ss) {
            ss.toast('⏹️ Stop signal sent to ' + target.name + ' on ' + ep.host, 'Discovery Fleet', 5);
          }
        }
      }
    } catch (e) {}
  }

  if (!stopped && !isBatch && ss) {
    ss.toast('ℹ️ ' + target.name + ' was not actively running.', 'Discovery Fleet', 4);
  }

  if (!isBatch) {
    Utilities.sleep(1000);
    updateStatusSheet();
  }
  return stopped;
}

/**
 * Resume NIM Fleet (Desktop-9048).
 */
function resumeNimFleet() {
  var token = STATUS_CONFIG.AUTH_TOKEN;
  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch (e) {}
  try {
    var res = UrlFetchApp.fetch('https://9048-sheet1.rajath.world/discovery/start', {
      method: 'post',
      contentType: 'application/json',
      headers: { 'Authorization': 'Bearer ' + token },
      payload: JSON.stringify({ start_row: 3, mode: 'full', sheet_id: '1XT_GlvBf2BGO4cEiskpUsK2Fzc5PzB-tcRHFMubDXbk' }),
      muteHttpExceptions: true
    });
    if (ss) ss.toast('✅ Resume signal sent to NIM Fleet (Desktop-9048)', 'NIM Fleet', 5);
  } catch(e) {
    if (ss) ss.toast('⚠️ Could not resume NIM Fleet: ' + e.message, 'NIM Fleet', 5);
  }
  Utilities.sleep(1000);
  updateStatusSheet();
}

/**
 * Stop NIM Fleet (Desktop-9048).
 */
function stopNimFleet() {
  var token = STATUS_CONFIG.AUTH_TOKEN;
  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch (e) {}
  try {
    var res = UrlFetchApp.fetch('https://9048-sheet1.rajath.world/discovery/cancel', {
      method: 'post',
      headers: { 'Authorization': 'Bearer ' + token },
      muteHttpExceptions: true
    });
    if (ss) ss.toast('⏹️ Stop signal sent to NIM Fleet (Desktop-9048)', 'NIM Fleet', 5);
  } catch(e) {
    if (ss) ss.toast('⚠️ Could not stop NIM Fleet: ' + e.message, 'NIM Fleet', 5);
  }
  Utilities.sleep(1000);
  updateStatusSheet();
}

function resumeSheet1() { sendResumeSignal(1, false); }
function resumeSheet2() { sendResumeSignal(2, false); }
function resumeSheet3() { sendResumeSignal(3, false); }
function resumeSheet4() { sendResumeSignal(4, false); }

function resumeAllSheets() {
  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch(e) {}
  if (ss) ss.toast('🚀 Dispatching resume signals to Sheets 1, 2, 3, 4...', 'Discovery Fleet', 4);
  
  for (var s = 1; s <= 4; s++) {
    sendResumeSignal(s, true);
  }
  Utilities.sleep(1500);
  updateStatusSheet();
  if (ss) ss.toast('✅ Fleet resume cycle completed.', 'Discovery Fleet', 5);
}

function stopSheet1() { sendStopSignal(1, false); }
function stopSheet2() { sendStopSignal(2, false); }
function stopSheet3() { sendStopSignal(3, false); }
function stopSheet4() { sendStopSignal(4, false); }

function stopAllSheets() {
  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch(e) {}
  if (ss) ss.toast('🛑 Dispatching stop signals to Sheets 1, 2, 3, 4...', 'Discovery Fleet', 4);
  
  for (var s = 1; s <= 4; s++) {
    sendStopSignal(s, true);
  }
  Utilities.sleep(1500);
  updateStatusSheet();
  if (ss) ss.toast('⏹️ Fleet stop cycle completed.', 'Discovery Fleet', 5);
}

/**
 * Headless 11:05 PM Automated Trigger for Fleet Advance Pre-Crawl.
 * Kicks off the automated Sequential Relay across Main Fleet ONLY (Sheet 1 -> 2 -> 3 -> 4)
 * to guarantee 100% cross-sheet domain uniqueness in all edge cases.
 * Per user directive: Testing / NIM Fleet is excluded.
 */
function triggerFleetAdvancePreCrawl() {
  Logger.log('⏰ Starting automated 11:05 PM Sequential Fleet Advance Pre-Crawl Relay (Main Fleet ONLY)...');
  var token = STATUS_CONFIG.AUTH_TOKEN;
  var results = [];

  // Kick off Main Fleet Sequential Relay starting at Sheet 1 (CF-1)
  // Sheet 1 automatically triggers Sheet 2 upon completion, which triggers Sheet 3, which triggers Sheet 4.
  var mainTarget = (STATUS_CONFIG.TARGET_SHEETS && STATUS_CONFIG.TARGET_SHEETS.length > 0) ? STATUS_CONFIG.TARGET_SHEETS[0] : null;
  if (mainTarget) {
    var primaryUrl = mainTarget.endpoints[0].url;
    var sheetName = mainTarget.shortName || mainTarget.name;
    Logger.log('▶ Launching Main Fleet Sequential Relay via ' + sheetName + ' (' + primaryUrl + ')...');
    try {
      var resp = UrlFetchApp.fetch(primaryUrl + '/discovery/advance-precrawl', {
        method: 'post',
        contentType: 'application/json',
        headers: { 'Authorization': 'Bearer ' + token },
        payload: JSON.stringify({ sheet_id: mainTarget.sheetId }),
        muteHttpExceptions: true
      });
      var code = resp.getResponseCode();
      var txt = resp.getContentText();
      Logger.log('Main Fleet Relay Initiation (' + code + '): ' + txt);
      results.push({ sheet: 'Main Fleet Relay (Sheet 1 -> 2 -> 3 -> 4)', code: code, response: txt });
    } catch(err) {
      Logger.log('❌ Error initiating Main Fleet relay: ' + err.message);
      results.push({ sheet: 'Main Fleet Relay', code: 500, error: err.message });
    }
  }

  return results;
}

function launchFleetAdvancePreCrawlUi() {
  var ui = SpreadsheetApp.getUi();
  var confirm = ui.alert(
    '🌐 Launch Tomorrow\'s Sequential Pre-Crawl Relay',
    'Are you sure you want to launch the advance pre-crawl for tomorrow across the Main Fleet?\n\n' +
    '• Main Fleet executes a STRICT SEQUENTIAL RELAY (Sheet 1 -> 2 -> 3 -> 4) to ensure 100% domain uniqueness.\n' +
    '• Each sheet reserves its budget from Production Budget Allocation and immediately excludes its domains from subsequent sheets.\n' +
    '• Tech Crawler Job IDs are generated in the background on respective hosts.\n' +
    '• Testing Fleet (NIM Fleet) is excluded.\n' +
    '• Current active pipelines will NOT be disrupted.',
    ui.ButtonSet.YES_NO
  );

  if (confirm !== ui.Button.YES) return;

  var ss = null;
  try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch(e) {}
  if (ss) ss.toast('🚀 Dispatching sequential pre-crawl relay to Desktop-5080 (Sheet 1)...', 'Pre-Crawl', 5);

  try {
    var results = triggerFleetAdvancePreCrawl();
    var msg = 'Sequential Pre-Crawl Relay successfully dispatched:\n\n';
    for (var i = 0; i < results.length; i++) {
      var r = results[i];
      msg += '• ' + r.sheet + ': HTTP ' + r.code + '\n';
    }
    msg += '\nSheet 1 will automatically trigger Sheet 2 upon completion, followed by Sheet 3 and Sheet 4.';
    ui.alert('✅ Pre-Crawl Dispatched', msg, ui.ButtonSet.OK);
  } catch (err) {
    ui.alert('❌ Pre-Crawl Dispatch Failed', 'Error: ' + err.message, ui.ButtonSet.OK);
  }
}
