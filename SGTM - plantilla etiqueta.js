var getAllEventData    = require('getAllEventData');
var getRequestHeader   = require('getRequestHeader');
var setResponseStatus  = require('setResponseStatus');
var setResponseHeader  = require('setResponseHeader');
var setResponseBody    = require('setResponseBody');
var sendHttpRequest    = require('sendHttpRequest');
var logToConsole       = require('logToConsole');
var JSON               = require('JSON');
var makeString         = require('makeString');
var getType            = require('getType');
var getGoogleAuth      = require('getGoogleAuth');
var toBase64           = require('toBase64');
var getTimestampMillis = require('getTimestampMillis');
const encodeUriComponent = require('encodeUriComponent');

const projectId = data.pubsubProjectId || 'gtm-t2fr3sdw-ndrmz';
const topicId = data.pubsubTopicId || 'risku2';

// ===== Helpers =====
function get(obj, path, def) {
  if (obj == null || path == null) return def;
  var parts = (path + '').split('.');
  var cur = obj;
  for (var i = 0; i < parts.length; i++) { 
    if (cur == null) return def; 
    cur = cur[parts[i]]; 
  }
  return (cur === undefined) ? def : cur;
}

function toBool(x){ return !!x; }

var _WS=' \t\n\r\f\u00a0';
function _isSpaceChar(ch){ return _WS.indexOf(ch)!==-1; }
function _toStr(v){ return '' + (v==null ? '' : v); }
function trimStr(v){ 
  var s=_toStr(v),i=0,j=s.length; 
  while(i<j&&_isSpaceChar(s.charAt(i)))i++; 
  while(j>i&&_isSpaceChar(s.charAt(j-1)))j--; 
  return s.substring(i,j); 
}
function isEmptyStr(v){ return trimStr(v) === ''; }

function log(label, value){
  var t = typeof value;
  if (value && (t === 'object')) logToConsole('[RISKU] ' + label + ': ' + JSON.stringify(value));
  else logToConsole('[RISKU] ' + label + ': ' + makeString(value));
}

function primaryLang(s){
  var x = trimStr(s);
  var p = x.indexOf(','); if (p !== -1) x = x.substring(0,p);
  p = x.indexOf(';'); if (p !== -1) x = x.substring(0,p);
  p = x.indexOf('-'); if (p !== -1) x = x.substring(0,p);
  return x.toLowerCase();
}

function isLeap(y){ return (y%4===0 && y%100!==0) || (y%400===0); }
function daysToYear(y){ var d=0; for (var i=1970;i<y;i++){ d += isLeap(i)?366:365; } return d; }
function daysToMonth(y,m){ 
  var md=[31,28,31,30,31,30,31,31,30,31,30,31],d=0; 
  for (var i=1;i<m;i++){ d+= (i===2 && isLeap(y)) ? 29 : md[i-1]; } 
  return d; 
}
function abs(n){ return n < 0 ? -n : n; }

function toInt(s){
  var str = '' + (s == null ? '' : s);
  if (str.length === 0) return null;
  var neg = false, i = 0, n = 0;
  if (str.slice(0,1) === '-') { neg = true; i = 1; }
  for (; i < str.length; i++){
    var ch = str.slice(i, i+1);
    if (ch < '0' || ch > '9') return null;
    n = n * 10 + (ch - '0');
  }
  return neg ? -n : n;
}

function isoZToMs(iso){
  if (!iso) return null;
  var s = trimStr(iso);
  if (s.length < 20) return null;
  if (s.charAt(s.length-1) !== 'Z') return null;
  var Y = toInt(s.substring(0,4));
  var M = toInt(s.substring(5,7));
  var D = toInt(s.substring(8,10));
  var h = toInt(s.substring(11,13));
  var m = toInt(s.substring(14,16));
  var sec = toInt(s.substring(17,19));
  if (Y==null||M==null||D==null||h==null||m==null||sec==null) return null;
  var ms = 0;
  var dot = s.indexOf('.', 19);
  if (dot !== -1) {
    var end = s.length-1;
    var frac = s.substring(dot+1, end);
    if (frac.length > 3) frac = frac.substring(0,3);
    while (frac.length < 3) frac += '0';
    var mm = toInt(frac); if (mm == null) mm = 0; ms = mm;
  }
  var days = daysToYear(Y) + daysToMonth(Y,M) + (D-1);
  return days*86400000 + h*3600000 + m*60000 + sec*1000 + ms;
}

// ===== SCORING FUNCTION CON VISITOR_TYPE =====
function scoreRisk(evt) {
  var sig        = get(evt, 'signals', {});
  var features   = get(sig, 'features', {});
  var screen     = get(sig, 'screen', {});
  var viewport   = get(sig, 'viewport', {});
  var page       = get(sig, 'page', {});
  var perf       = get(sig, 'performance', {});
  var conn       = get(sig, 'conn', null);

  var ua         = _toStr(get(evt, 'user_agent', get(sig, 'ua', '')));
  var lang       = _toStr(get(sig, 'lang', get(evt, 'language', '')));
  var languages  = _toStr(get(sig, 'languages', ''));
  var tzName     = _toStr(get(sig, 'tz_name', ''));
  var pluginsLen = get(sig, 'plugins_len', null);
  var vis        = _toStr(get(page,'vis',''));
  var ref        = _toStr(get(page,'ref',''));
  var pageUrl    = _toStr(get(page,'url',''));

  var isDebug = pageUrl.indexOf('gtm_debug=') !== -1;

  var score = 0;
  var reasons = [];
  var visitor_type = 'human'; // Por defecto asumimos humano

  // ===== 1. DETECCIÓN DE WEBDRIVER (Selenium) =====
  if (toBool(features.webdriver)) { 
    score += 60; 
    reasons.push('webdriver_true');
    visitor_type = 'scraper';
  }

  // ===== 2. DETECCIÓN DE AUTOMATION TOOLS =====
  if (toBool(features.has_phantom)) {
    score += 60;
    reasons.push('phantom_detected');
    visitor_type = 'scraper';
  }
  
  if (toBool(features.has_nightmare)) {
    score += 60;
    reasons.push('nightmare_detected');
    visitor_type = 'scraper';
  }
  
  if (toBool(features.cdp_runtime)) {
    score += 50;
    reasons.push('chrome_automation');
    visitor_type = 'scraper';
  }

  // ===== 3. USER AGENT ANALYSIS =====
  var uaLower = ua.toLowerCase();
  
  // Headless browsers
  if (uaLower.indexOf('headlesschrome') !== -1 ||
      uaLower.indexOf('phantomjs') !== -1 ||
      uaLower.indexOf('slimerjs') !== -1 ||
      uaLower.indexOf('splash') !== -1) {
    score += 60;
    reasons.push('ua_headless');
    visitor_type = 'scraper';
  }
  
  // Automation tools en UA
  if (uaLower.indexOf('puppeteer') !== -1 ||
      uaLower.indexOf('playwright') !== -1 ||
      uaLower.indexOf('selenium') !== -1 ||
      uaLower.indexOf('cypress') !== -1 ||
      uaLower.indexOf('nightmare') !== -1 ||
      uaLower.indexOf('zombie') !== -1 ||
      uaLower.indexOf('mechanize') !== -1 ||
      uaLower.indexOf('webdriver') !== -1) {
    score += 60;
    reasons.push('ua_automation');
    visitor_type = 'scraper';
  }
  
  // Bots tradicionales
  if (uaLower.indexOf(' bot') !== -1 ||
      uaLower.indexOf('bot/') !== -1 ||
      uaLower.indexOf('bot-') !== -1 ||
      uaLower.indexOf('crawler') !== -1 ||
      uaLower.indexOf('spider') !== -1 ||
      uaLower.indexOf('scraper') !== -1 ||
      uaLower.indexOf('crawl') !== -1 ||
      uaLower.indexOf('slurp') !== -1 ||
      uaLower.indexOf('mediapartners') !== -1) {
    score += 40;
    reasons.push('ua_bot');
    if (visitor_type === 'human') visitor_type = 'bot';
  }
  
  // HTTP clients
  if (uaLower.indexOf('curl') !== -1 ||
      uaLower.indexOf('wget') !== -1 ||
      uaLower.indexOf('postman') !== -1 ||
      uaLower.indexOf('insomnia') !== -1 ||
      uaLower.indexOf('python-requests') !== -1 ||
      uaLower.indexOf('java/') !== -1 ||
      uaLower.indexOf('okhttp') !== -1 ||
      uaLower.indexOf('libwww') !== -1 ||
      uaLower.indexOf('libcurl') !== -1 ||
      uaLower.indexOf('node-fetch') !== -1 ||
      uaLower.indexOf('axios') !== -1 ||
      uaLower.indexOf('got') !== -1 ||
      uaLower.indexOf('superagent') !== -1 ||
      uaLower.indexOf('httpunit') !== -1) {
    score += 50;
    reasons.push('ua_http_client');
    if (visitor_type === 'human') visitor_type = 'bot';
  }
  
  // Testing frameworks
  if (uaLower.indexOf('electron') !== -1 ||
      uaLower.indexOf('jestjsdom') !== -1 ||
      uaLower.indexOf('jsdom') !== -1 ||
      uaLower.indexOf('htmlunit') !== -1) {
    score += 40;
    reasons.push('ua_testing_framework');
    if (visitor_type === 'human') visitor_type = 'bot';
  }

  // ===== 4. DETECCIÓN ESPECÍFICA DE LLMs =====
  var llmIndicators = 0;
  
  // LLMs no tienen WebAssembly
  if (features.has_webassembly === false) {
    llmIndicators++;
    score += 15;
    reasons.push('no_webassembly');
  }
  
  // LLMs no tienen battery API
  if (features.has_battery === false) {
    llmIndicators++;
    score += 5;
    reasons.push('no_battery_api');
  }
  
  // LLMs tienen history.length = 1
  var histLen = get(page, 'hist_len', null);
  if (histLen === 1) {
    llmIndicators++;
    score += 10;
    reasons.push('history_length_one');
  }
  
  // LLMs no tienen referrer
  if (isEmptyStr(ref) && histLen === 1) {
    llmIndicators++;
    score += 10;
    reasons.push('no_referrer_single_history');
  }
  
  // Si tiene 3+ indicadores de LLM, probablemente es un LLM
  if (llmIndicators >= 3 && pluginsLen === 0) {
    visitor_type = 'llm';
    score += 20;
    reasons.push('llm_pattern_detected');
  }

  // ===== 5. HARDWARE Y CAPACIDADES =====
  
  // Hardware concurrency
  var cores = get(features, 'hardware_concurrency', null);
  if (cores !== null) {
    if (cores < 1 || cores > 128) {
      score += 5;
      reasons.push('unusual_hardware_concurrency');
    }
  } else if (visitor_type === 'human') {
    score += 5;
    reasons.push('no_hardware_concurrency');
  }
  
  // Device memory
  var mem = get(features, 'device_memory', null);
  if (mem !== null) {
    // Valores válidos: 0.25, 0.5, 1, 2, 4, 8, 16, 32
    if (mem !== 0.25 && mem !== 0.5 && mem !== 1 && mem !== 2 && 
        mem !== 4 && mem !== 8 && mem !== 16 && mem !== 32) {
      score += 5;
      reasons.push('non_standard_device_memory');
    }
  }
  
  // Touch points
  var touchPoints = get(features, 'max_touch_points', 0);
  if (features.has_touch === true && touchPoints === 0) {
    score += 10;
    reasons.push('touch_inconsistency');
  }

  // ===== 6. PLUGINS Y LANGUAGES =====
  if ((pluginsLen === 0 || pluginsLen === null) && isEmptyStr(languages)) {
    score += 20;
    reasons.push('no_plugins_and_no_languages');
    if (visitor_type === 'human') visitor_type = 'bot';
  }

  // ===== 7. SCREEN/VIEWPORT VALIDATION =====
  
  // Dimensiones básicas
  if ((screen.w === 0 || screen.h === 0) || (viewport.w === 0 || viewport.h === 0)) {
    score += 20;
    reasons.push('invalid_screen_or_viewport');
    if (visitor_type === 'human') visitor_type = 'bot';
  } else {
    var dpr = viewport.dpr || 1;
    if (dpr < 0.1) dpr = 0.1;
    
    // Check extreme DPR values
    if (dpr < 0.5 || dpr > 4) {
      score += 10;
      reasons.push('extreme_device_pixel_ratio');
    }
    
    var effW = viewport.w * dpr, effH = viewport.h * dpr;
    if ((effW > screen.w * 1.25) || (effH > screen.h * 1.25)) {
      score += 5;
      reasons.push('viewport_exceeds_screen_unusual');
    }
    
    // availWidth/Height validation
    var availW = get(screen, 'availW', null);
    var availH = get(screen, 'availH', null);
    if (availW !== null && availH !== null) {
      if (availW > screen.w || availH > screen.h) {
        score += 15;
        reasons.push('impossible_avail_dimensions');
        visitor_type = 'scraper';
      }
    }
    
    // outerWidth/Height validation
    var outerW = get(viewport, 'outerW', null);
    var outerH = get(viewport, 'outerH', null);
    if (outerW !== null && outerH !== null) {
      if (outerW < viewport.w || outerH < viewport.h) {
        score += 15;
        reasons.push('impossible_outer_dimensions');
        visitor_type = 'scraper';
      }
    }
  }

  // ===== 8. CANVAS Y WEBGL =====
  if (features.has_canvas2d === false) {
    score += 10;
    reasons.push('no_canvas2d');
  }
  if (features.has_webgl === false) {
    score += 10;
    reasons.push('no_webgl');
  }

  // ===== 9. PERFORMANCE TIMING =====
  var loadTime = get(perf, 'load_time', null);
  if (loadTime !== null) {
    // Carga sospechosamente rápida
    if (loadTime < 100 && loadTime > 0) {
      score += 20;
      reasons.push('impossible_load_time');
      visitor_type = 'scraper';
    }
  }
  
  var domTime = get(perf, 'dom_time', null);
  if (domTime !== null && domTime < 0) {
    score += 15;
    reasons.push('negative_dom_time');
    visitor_type = 'scraper';
  }

  // ===== 10. BEHAVIORAL PATTERNS =====
  
  // Do Not Track with no plugins
  if (features.do_not_track === true && (pluginsLen === 0 || pluginsLen === null)) {
    score += 5;
    reasons.push('dnt_and_no_plugins');
  }
  
  // Chrome without chrome object
  if (uaLower.indexOf('chrome') !== -1 && features.has_chrome_obj === false) {
    score += 10;
    reasons.push('ua_chrome_no_object');
  }
  
  // Hidden page without referrer
  if (!isDebug && vis === 'hidden' && isEmptyStr(ref)) {
    score += 5;
    reasons.push('hidden_without_ref');
  }
  
  // Prerender detection
  if (features.prerender === true) {
    reasons.push('prerender');
  }

  // ===== 11. LANGUAGE/TIMEZONE VALIDATION =====
  if (!isEmptyStr(lang) && !isEmptyStr(tzName)) {
    var l = trimStr(lang).toLowerCase();
    var t = trimStr(tzName).toLowerCase();
    
    // Spanish pero no Europa
    if (l.indexOf('es') === 0 && t.indexOf('europe') === -1) {
      score += 10;
      reasons.push('lang_tz_mismatch_es');
    }
    
    // English US pero no America
    if (l.indexOf('en-us') === 0 && t.indexOf('america') === -1) {
      score += 10;
      reasons.push('lang_tz_mismatch_en');
    }
    
    // Chinese pero no Asia
    if (l.indexOf('zh') === 0 && t.indexOf('asia') === -1) {
      score += 10;
      reasons.push('lang_tz_mismatch_zh');
    }
  }
  
  // Accept-Language header validation
  var al = _toStr(getRequestHeader('accept-language') || '');
  if (isEmptyStr(al)) {
    score += 5;
    reasons.push('no_accept_language');
  } else if (!isEmptyStr(lang)) {
    var pAL = primaryLang(al);
    var pSIG = primaryLang(lang);
    if (pAL !== '' && pSIG !== '' && pAL !== pSIG) {
      score += 5;
      reasons.push('accept_language_mismatch');
    }
  }

  // ===== 12. CONNECTION INFO =====
  var hasConn = conn && (get(conn,'dl',null) !== null || !isEmptyStr(get(conn,'et','')));
  if (hasConn && get(conn,'dl',0) === 0 && isEmptyStr(get(conn,'et',''))) {
    score += 5;
    reasons.push('empty_connection_info');
  }
  
  // Connection type validation
  if (conn && conn.effectiveType) {
    var et = conn.effectiveType;
    if (et !== 'slow-2g' && et !== '2g' && et !== '3g' && et !== '4g' && et !== '5g') {
      score += 5;
      reasons.push('invalid_connection_type');
    }
  }

  // ===== 13. CLOCK SKEW =====
  var serverSec = get(evt,'timestamp', null);
  var clientIso = get(evt,'client_ts', null);
  if (serverSec && clientIso) {
    var cMs = isoZToMs(clientIso);
    var sMs = serverSec * 1000;
    log('skew_raw', { client_ts: clientIso, client_ms: cMs, server_sec: serverSec, server_ms: sMs });
    if (cMs !== null && sMs) {
      var diffMin = abs(cMs - sMs) / 60000;
      if (diffMin > 240) {
        score += 10;
        reasons.push('client_clock_skew_gt_4h');
      }
      
      // Extreme clock skew
      if (diffMin > 1440) { // > 24 hours
        score += 20;
        reasons.push('extreme_clock_skew');
      }
    }
  }

  // ===== CLASIFICACIÓN FINAL =====
  var level = 'low';
  var suspect = false;
  
  if (score >= 60) {
    level = 'high';
    suspect = true;
  } else if (score >= 30) {
    level = 'medium';
  }
  
  // Ajuste final del tipo basado en el score
  if (visitor_type === 'human' && score >= 60) {
    // Si tiene score alto pero no detectamos tipo específico, es bot genérico
    visitor_type = 'bot';
  }

  return {
    score: score,
    level: level,
    suspect: suspect,
    reasons: reasons,
    visitor_type: visitor_type // NUEVO: tipo de visitante
  };
}

// ===== Device Type Detection =====
function deviceTypeFromUA(ua) {
  var s = trimStr(ua).toLowerCase();
  if (s.indexOf('ipad') !== -1 || s.indexOf('tablet') !== -1) return 'tablet';
  if (s.indexOf('mobi') !== -1 || s.indexOf('iphone') !== -1 || s.indexOf('android') !== -1) return 'mobile';
  return 'desktop';
}

// ===== Main Function =====
(function main(){
  var evt = getAllEventData() || {};
  log('event_name', get(evt,'event_name','(none)'));
  log('incoming signals', get(evt,'signals',null));

  // Configuration
  var projectId = 'gtm-t2fr3sdw-ndrmz';
  var topicId  = 'risku2';
  
  // Project ID from event
  var project_id = _toStr(get(evt,'project_id',''));
  
  // IP Detection
  var ip = _toStr(get(evt,'ip_override',''));
  if (isEmptyStr(ip)) {
    var xff = _toStr(getRequestHeader('x-forwarded-for') || '');
    if (!isEmptyStr(xff)) ip = _toStr(xff.split(',')[0]);
  }
  if (isEmptyStr(ip)) {
    ip = _toStr(getRequestHeader('cf-connecting-ip') || 
                getRequestHeader('true-client-ip') || 
                getRequestHeader('x-real-ip') || '');
  }
  log('ip', ip);

  // User Agent fallback
  var uaHdr = _toStr(getRequestHeader('user-agent') || '');
  if (!get(evt,'signals.ua',null) && !isEmptyStr(uaHdr)) {
    if (!evt.signals) evt.signals = {};
    evt.signals.ua = uaHdr;
    log('ua injected from header', uaHdr);
  } else {
    log('ua from payload', get(evt,'signals.ua','(missing)'));
  }

  log('accept-language', _toStr(getRequestHeader('accept-language') || ''));

  // Calculate risk score
  var risk = scoreRisk(evt);
  log('risk', risk);
  log('visitor_type', risk.visitor_type); // LOG del tipo de visitante

  // HTTP Response
  var response = {
    ok: true,
    project_id: get(evt,'project_id', null),
    event_name: get(evt,'event_name',''),
    client_ts:  get(evt,'client_ts', null),
    ip:         isEmptyStr(ip) ? null : ip,
    user_agent: isEmptyStr(uaHdr) ? null : uaHdr,
    page:       get(evt,'page', null),
    risk:       risk
  };
  
  setResponseStatus(200);
  setResponseHeader('Content-Type', 'application/json; charset=utf-8');
  setResponseHeader('Cache-Control', 'no-store, max-age=0');
  setResponseHeader('X-Risk-Level', risk.level);
  setResponseHeader('X-Risk-Score', makeString(risk.score));
  setResponseHeader('X-Visitor-Type', risk.visitor_type); // NUEVO header
  setResponseBody(JSON.stringify(response));

  // Country detection
  var country = _toStr(
    getRequestHeader('x-appengine-country') ||
    getRequestHeader('cf-ipcountry') ||
    getRequestHeader('x-vercel-ip-country') ||
    getRequestHeader('cloudfront-viewer-country') || ''
  );
  country = isEmptyStr(country) ? null : trimStr(country).toUpperCase();

  // Device type
  var device_type = deviceTypeFromUA(uaHdr || get(evt,'signals.ua',''));
  
  // Language
  var language_final = _toStr(get(evt,'signals.lang', get(evt,'language','')));
  language_final = isEmptyStr(language_final) ? null : trimStr(language_final);
  
  // Timestamp
  var ts = get(evt,'client_ts', null);
  if (!ts) { 
    var tsSec = get(evt,'timestamp', null); 
    if (tsSec) ts = '' + tsSec; 
  }

  // Compact data for logging
  var compact = {
    ts: ts,
    page_url: get(evt,'page.url', null),
    country: country,
    device: device_type,
    language: language_final,
    risk: { 
      score: risk.score, 
      level: risk.level, 
      suspect: risk.suspect, 
      reasons: risk.reasons,
      visitor_type: risk.visitor_type // AÑADIDO
    }
  };
  log('compact', compact);

  // ====== Pub/Sub Publishing ======
  var publishUrl = 'https://pubsub.googleapis.com/v1/projects/' +
                   encodeUriComponent(projectId) + '/topics/' +
                   encodeUriComponent(topicId) + ':publish';

  // Convert arrays to string arrays
  function toArray(v){
    if (!v) return [];
    if (v.push) {
      var out = []; 
      for (var i=0;i<v.length;i++){ 
        out.push(_toStr(v[i])); 
      }
      return out;
    }
    if (typeof v === 'string') {
      var parts = v.split('|'), out2 = [];
      for (var j=0;j<parts.length;j++){ 
        var e=trimStr(parts[j]); 
        if (e!=='') out2.push(e); 
      }
      return out2;
    }
    return [_toStr(v)];
  }
  
  function endsWithZ(s){ 
    s=_toStr(s); 
    return s.length>0 && s.charAt(s.length-1)==='Z'; 
  }

  // Prepare row for BigQuery (CON visitor_type)
  var row = {
    received_ts: endsWithZ(ts) ? ts : null,
    ts: ts,
    page_url: compact.page_url || null,
    country: compact.country || null,
    device: compact.device || null,
    language: compact.language || null,
    risk_score: compact.risk ? compact.risk.score : null,
    risk_level: compact.risk ? compact.risk.level : null,
    risk_suspect: compact.risk ? !!compact.risk.suspect : null,
    risk_reasons: compact.risk ? toArray(compact.risk.reasons) : [],
    visitor_type: compact.risk ? compact.risk.visitor_type : null, // NUEVO CAMPO
    ip: ip || null,
    user_agent: uaHdr || null,
    fecha: project_id || null,
    event_name: get(evt,'event_name', null)
  };
  
  log('row_ready', row);

  // Add timestamp to message
  if (get(data, 'addTimestamp', true)) {
    row.pubsub_published_at_ms = getTimestampMillis();
  }

  // Prepare Pub/Sub message
  var message = {};
  if (row && JSON.stringify(row) !== '{}') {
    message.data = toBase64(JSON.stringify(row));
  }
  
  // Add attributes for filtering (con visitor_type)
  var attributes = {};
  if (risk.level) attributes.risk_level = risk.level;
  if (risk.suspect) attributes.is_bot = makeString(risk.suspect);
  if (risk.visitor_type) attributes.visitor_type = risk.visitor_type; // NUEVO
  if (device_type) attributes.device_type = device_type;
  if (country) attributes.country = country;
  
  if (attributes && JSON.stringify(attributes) !== '{}') {
    message.attributes = attributes;
  }

  var input = { messages: [message] };

  // Get Google Auth
  var auth = getGoogleAuth({ 
    scopes: ['https://www.googleapis.com/auth/pubsub'] 
  });

  // Send to Pub/Sub
  sendHttpRequest(
    publishUrl,
    { 
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      authorization: auth,
      timeout: 1500
    },
    JSON.stringify(input)
  ).then(function(){
      log('PubSub publish', {
        status: 200, 
        topic: topicId,
        risk_level: risk.level,
        score: risk.score,
        visitor_type: risk.visitor_type // LOG del tipo
      });
      data.gtmOnSuccess();
    }, function(error){
      log('PubSub error', {
        status: 500, 
        error: (error && error.message) ? error.message : 'Unknown'
      });
      data.gtmOnFailure();
    });

})();