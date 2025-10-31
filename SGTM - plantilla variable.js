var getAllEventData = require('getAllEventData');
var getEventData = require('getEventData');
var getRequestHeader = require('getRequestHeader');
var makeString = require('makeString');
var logToConsole = require('logToConsole');
var JSON = require('JSON');

// ==== HELPERS ====
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

var _WS = ' \t\n\r\f\u00a0';
function _isSpaceChar(ch){ return _WS.indexOf(ch) !== -1; }
function _toStr(v){ return '' + (v == null ? '' : v); }
function trimStr(v){
  var s = _toStr(v);
  var i = 0, j = s.length;
  while (i < j && _isSpaceChar(s.charAt(i))) i++;
  while (j > i && _isSpaceChar(s.charAt(j - 1))) j--;
  return s.substring(i, j);
}
function isEmptyStr(v){ return trimStr(v) === ''; }

function log(label, value){
  // Solo loguear si está habilitado (o si no existe el setting, default a true)
  if (data && data.enableLogging === false) return;
  
  var t = typeof value;
  if (value && (t === 'object')) {
    logToConsole('[RSV] ' + label + ': ' + JSON.stringify(value));
  } else {
    logToConsole('[RSV] ' + label + ': ' + makeString(value));
  }
}

// ==== SCORING FUNCTION ====
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
  
  // Page data
  var pageUrl    = _toStr(get(page, 'url', ''));
  var ref        = _toStr(get(page, 'ref', ''));
  var vis        = _toStr(get(page, 'vis', ''));
  var histLen    = get(page, 'hist_len', null);

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
      uaLower.indexOf('superagent') !== -1) {
    score += 50;
    reasons.push('ua_http_client');
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
  
  // LLMs no tienen Intl API
  if (features.has_intl === false) {
    llmIndicators++;
    score += 10;
    reasons.push('no_intl_api');
  }
  
  // LLMs no tienen battery API
  if (features.has_battery === false) {
    llmIndicators++;
    score += 5;
    reasons.push('no_battery_api');
  }
  
  // LLMs tienen history.length = 1
  if (histLen === 1) {
    llmIndicators++;
    score += 10;
    reasons.push('history_length_one');
  }
  
  // LLMs no tienen referrer con history = 1
  if (isEmptyStr(ref) && histLen === 1) {
    llmIndicators++;
    score += 10;
    reasons.push('no_referrer_single_history');
  }
  
  // Si tiene 3+ indicadores de LLM y no plugins, probablemente es un LLM
  if (llmIndicators >= 3 && (pluginsLen === 0 || pluginsLen === null)) {
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
    if (visitor_type === 'human' && llmIndicators < 2) visitor_type = 'bot';
  }

  // ===== 7. SCREEN/VIEWPORT VALIDATION =====
  
  // Dimensiones básicas
  if ((screen.w === 0 || screen.h === 0) || (viewport.w === 0 || viewport.h === 0)) {
    score += 20;
    reasons.push('invalid_screen_or_viewport');
    if (visitor_type === 'human' && llmIndicators < 2) visitor_type = 'bot';
  }
  
  // availWidth/Height validation
  var availW = get(screen, 'availW', null);
  var availH = get(screen, 'availH', null);
  if (availW !== null && availH !== null && screen.w && screen.h) {
    if (availW > screen.w || availH > screen.h) {
      score += 15;
      reasons.push('impossible_avail_dimensions');
      if (visitor_type === 'human') visitor_type = 'scraper';
    }
  }
  
  // outerWidth/Height validation
  var outerW = get(viewport, 'outerW', null);
  var outerH = get(viewport, 'outerH', null);
  if (outerW !== null && outerH !== null && viewport.w && viewport.h) {
    if (outerW < viewport.w || outerH < viewport.h) {
      score += 15;
      reasons.push('impossible_outer_dimensions');
      if (visitor_type === 'human') visitor_type = 'scraper';
    }
  }
  
  // Device pixel ratio
  var dpr = get(viewport, 'dpr', 1);
  if (dpr < 0.5 || dpr > 4) {
    score += 10;
    reasons.push('extreme_device_pixel_ratio');
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
      if (visitor_type === 'human') visitor_type = 'scraper';
    }
  }
  
  var domTime = get(perf, 'dom_time', null);
  if (domTime !== null && domTime < 0) {
    score += 15;
    reasons.push('negative_dom_time');
    if (visitor_type === 'human') visitor_type = 'scraper';
  }

  // ===== 10. CHROME OBJECT VALIDATION =====
  if (uaLower.indexOf('chrome') !== -1 && features.has_chrome_obj === false) {
    score += 10;
    reasons.push('ua_chrome_no_object');
  }

  // ===== 11. LANGUAGE/TIMEZONE =====
  if (!isEmptyStr(lang) && !isEmptyStr(tzName)) {
    var l = trimStr(lang).toLowerCase();
    var t = trimStr(tzName).toLowerCase();
    
    // Spanish pero no Europa
    if (l.indexOf('es') === 0 && t.indexOf('europe') === -1 && t.indexOf('madrid') === -1) {
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

  // ===== 12. CONNECTION INFO =====
  if (conn && get(conn,'dl',0) === 0 && isEmptyStr(get(conn,'et',''))) {
    score += 5;
    reasons.push('empty_connection_info');
  }

  // ===== 13. BEHAVIORAL PATTERNS =====
  
  // Do Not Track with no plugins
  if (features.do_not_track === true && (pluginsLen === 0 || pluginsLen === null)) {
    score += 5;
    reasons.push('dnt_and_no_plugins');
  }
  
  // Hidden page without referrer
  if (vis === 'hidden' && isEmptyStr(ref)) {
    score += 5;
    reasons.push('hidden_without_ref');
  }
  
  // Prerender detection
  if (features.prerender === true) {
    reasons.push('prerender');
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
    visitor_type: visitor_type
  };
}

// ==== MAIN EXECUTION ====
// Obtener todos los datos del evento
var evt = getAllEventData() || {};

log('=== Risk Detection Variable Start ===');
log('Event Name', get(evt, 'event_name', '(none)'));
log('Full Event', evt);

// Intentar obtener signals de diferentes formas
var signals = null;

// Opción 1: Directamente desde evt.signals
if (evt.signals) {
  signals = evt.signals;
  log('Signals from evt.signals', signals);
}

// Opción 2: Desde un campo específico del evento
if (!signals || JSON.stringify(signals) === '{}') {
  signals = getEventData('signals');
  if (signals) log('Signals from getEventData', signals);
}

// Opción 3: Construir signals desde campos individuales si no vienen agrupados
if (!signals || JSON.stringify(signals) === '{}') {
  log('Building signals from individual fields...');
  
  // Intentar obtener cada campo por separado
  var features = getEventData('features') || {};
  var screen = getEventData('screen') || {};
  var viewport = getEventData('viewport') || {};
  var page = getEventData('page') || {};
  var performance = getEventData('performance') || {};
  
  signals = {
    ua: getEventData('user_agent') || getEventData('ua') || getRequestHeader('user-agent') || '',
    lang: getEventData('language') || getEventData('lang') || '',
    languages: getEventData('languages') || '',
    tz_name: getEventData('tz_name') || getEventData('timezone') || '',
    plugins_len: getEventData('plugins_len') || getEventData('plugins_length'),
    features: features,
    screen: screen,
    viewport: viewport,
    page: page,
    performance: performance,
    conn: getEventData('conn') || getEventData('connection')
  };
  
  log('Constructed signals', signals);
}

// Si aún no tenemos signals válidos, crear estructura mínima
if (!signals) {
  log('WARNING: No signals found, using minimal structure');
  signals = {
    ua: getRequestHeader('user-agent') || '',
    features: {},
    screen: {},
    viewport: {},
    page: {}
  };
}

// Asegurar que evt tiene signals
evt.signals = signals;

// User Agent fallback desde header si no existe
if (!signals.ua || signals.ua === '') {
  var uaHdr = getRequestHeader('user-agent') || '';
  if (uaHdr) {
    signals.ua = uaHdr;
    log('UA from header fallback', uaHdr);
  }
}

// Log de señales críticas para debug
log('Final signals', signals);
log('Has WebDriver?', get(signals, 'features.webdriver'));
log('Has WebAssembly?', get(signals, 'features.has_webassembly'));
log('Has Battery?', get(signals, 'features.has_battery'));
log('History Length', get(signals, 'page.hist_len'));
log('Plugins Length', signals.plugins_len);

// Calcular risk
var risk = scoreRisk(evt);
log('Risk Result', risk);

// Obtener el tipo de output configurado
var outputType = (data && data.output) ? data.output : 'level';
log('Output Type', outputType);

// Variable de resultado
var result;

// Devolver según el tipo solicitado
switch(outputType) {
  case 'score':
    result = risk.score || 0;
    break;
  case 'level':
    result = risk.level || 'low';
    break;
  case 'suspect':
    result = risk.suspect || false;
    break;
  case 'visitor_type':
    result = risk.visitor_type || 'human';
    break;
  case 'reasons':
    result = (risk.reasons && risk.reasons.length > 0) ? risk.reasons.join('|') : '';
    break;
  case 'full':
    result = risk;
    break;
  default:
    result = risk.level || 'low';
}

log('=== Risk Detection Variable End ===');
log('Returning', result);

// Devolver el resultado
return result;