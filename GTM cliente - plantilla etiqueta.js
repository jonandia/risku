<script>
(function (W) {
  'use strict';

  // ────────── CONFIG ──────────
  var S_GTM_ENDPOINT = 'https://tracker.iacompliant.com/tracker';
  var SEND_PREVIEW_HEADER = true;
  var PREVIEW_HEADER_VALUE = 'ZW52LTd8MlhQQ20wbjBBdVk4ZVdqM0hjYnd6UXwxOWEyZWE2ODE1NjgwMzg4MjY3ODQ=';

  var PROJECT_ID = 'IACOMPLIANT';

  // Fase 1: SOLO ENVÍO. Si quieres volver a leer respuesta, pon true.
  var READ_RESPONSE = false;
  var READ_TIMEOUT_MS = 200;

  // Cookies (quedarán vacías en esta fase)
  var COOKIE_LEVEL = 'risk_level';
  var COOKIE_SCORE = 'risk_score';
  var COOKIE_TTL_SEC = 1800;
  var COOKIE_PATH = '/';
  var COOKIE_SAMESITE = 'Lax';

  // Señales extra
  var ENABLE_CANVAS = false;
  var ENABLE_WEBGL  = false;
  // ─────────────────────────────

  // Helpers cookies
  function setCookie(name, value, maxAgeSec) {
    var segs = [name + '=' + encodeURIComponent(value || '')];
    if (COOKIE_PATH) segs.push('Path=' + COOKIE_PATH);
    if (location.protocol === 'https:') segs.push('Secure');
    if (COOKIE_SAMESITE) segs.push('SameSite=' + COOKIE_SAMESITE);
    if (maxAgeSec && maxAgeSec > 0) segs.push('Max-Age=' + maxAgeSec);
    document.cookie = segs.join('; ');
  }
  function clearRiskCookies() {
    setCookie(COOKIE_LEVEL, '', COOKIE_TTL_SEC);
    setCookie(COOKIE_SCORE, '', COOKIE_TTL_SEC);
  }

  // Señales ligeras
  function gatherSignals() {
    var n = navigator || {};
    var d = document || {};
    var s = screen || {};
    var w = window || {};
    var lang = (n.language || n.userLanguage || '') + '';
    var langs = (n.languages && n.languages.join(',')) || (lang ? lang : '');
    var vw = w.innerWidth || 0, vh = w.innerHeight || 0, dpr = w.devicePixelRatio || 1;
    var pLen = null; try { pLen = (n.plugins && typeof n.plugins.length === 'number') ? n.plugins.length : null; } catch(e){}

    var features = {
      webdriver: !!n.webdriver,
      do_not_track: (n.doNotTrack == '1' || n.msDoNotTrack == '1' || (w.doNotTrack == '1')),
      has_chrome_obj: typeof w.chrome !== 'undefined',
      has_notification: !!w.Notification,
      has_indexeddb: !!w.indexedDB,
      has_serviceworker: !!n.serviceWorker,
      has_webrtc: !!(w.RTCPeerConnection || w.webkitRTCPeerConnection || w.mozRTCPeerConnection),
      has_websocket: !!w.WebSocket,
      prerender: (d.visibilityState === 'prerender')
    };
    if (ENABLE_CANVAS) {
      try { features.has_canvas2d = !!document.createElement('canvas').getContext; } catch(e){ features.has_canvas2d = false; }
    }
    if (ENABLE_WEBGL) {
      try { var c = document.createElement('canvas'); features.has_webgl = !!(c && (c.getContext('webgl') || c.getContext('experimental-webgl'))); }
      catch(e){ features.has_webgl = false; }
    }

    
// 1. DETECCIÓN RÁPIDA DE AUTOMATION (0ms)
features.has_phantom = !!(w._phantom);
features.has_nightmare = !!(w.__nightmare);
  features.cdp_runtime = !!(window.chrome && window.chrome.runtime && !window.chrome.webstore);
  
  // 2. APIS CRÍTICAS PARA LLMs (0ms)
  features.has_webassembly = typeof WebAssembly !== 'undefined';
  features.has_intl = typeof Intl !== 'undefined';
  features.has_battery = 'getBattery' in navigator;
  
  // 3. HARDWARE (0ms - ya está en memoria)
  features.hardware_concurrency = navigator.hardwareConcurrency || null;
  features.device_memory = navigator.deviceMemory || null;
  features.max_touch_points = navigator.maxTouchPoints || 0;
  
  // 4. SCREEN AVAIL (detecta headless - 0ms)
  var availScreen = {
    availW: screen.availWidth || null,
    availH: screen.availHeight || null,
    availTop: screen.availTop || null,
    availLeft: screen.availLeft || null
  };
  
  // 5. OUTER DIMENSIONS (detecta puppeteer - 0ms)
  var outerDims = {
    outerW: window.outerWidth || null,
    outerH: window.outerHeight || null
  };
  
  // 6. TIMING BÁSICO (si ya cargó - 0ms)
  var perfTiming = null;
  if (window.performance && performance.timing) {
    var t = performance.timing;
    if (t.loadEventEnd > 0) {
      perfTiming = {
        load_time: t.loadEventEnd - t.navigationStart,
        dom_time: t.domContentLoadedEventEnd - t.domContentLoadedEventStart
      };
    }
  }

return {
    ua: (n.userAgent || '') + '',
    lang: lang,
    languages: langs,
    tz_offset: (new Date()).getTimezoneOffset ? (new Date()).getTimezoneOffset() : null,
    tz_name: (W.Intl && Intl.DateTimeFormat && Intl.DateTimeFormat().resolvedOptions) ? Intl.DateTimeFormat().resolvedOptions().timeZone : '',
    plugins_len: pLen,
    screen: { 
      w: s.width || 0, 
      h: s.height || 0, 
      cd: s.colorDepth || 0,
      // NUEVO: availScreen
      availW: availScreen.availW,
      availH: availScreen.availH
    },
    viewport: { 
      w: vw, 
      h: vh, 
      dpr: dpr,
      // NUEVO: outer dimensions
      outerW: outerDims.outerW,
      outerH: outerDims.outerH
    },
    features: features,
    // NUEVO: performance (solo si ya existe)
    performance: perfTiming,
    page: { 
      url: location.href, 
      ref: document.referrer || '', 
      vis: (document.visibilityState || ''),
      // NUEVO: history length (LLMs = 1)
      hist_len: window.history.length
    }
  };
}

  function buildPayload() {
    return {
      event_name: 'page_risk_probe',
      project_id: PROJECT_ID,
      client_ts: new Date().toISOString(),
      page: { url: location.href, ref: document.referrer || '' },
      language: (navigator.language || navigator.userLanguage || '') + '',
      signals: gatherSignals()
    };
  }

  function makeFetchOptions(bodyStr) {
    var headers = { 'Content-Type': 'application/json' };
    if (SEND_PREVIEW_HEADER && PREVIEW_HEADER_VALUE) {
      headers['X-Gtm-Server-Preview'] = PREVIEW_HEADER_VALUE;
    }
    return {
      method: 'POST',
      headers: headers,
      body: bodyStr,
      cache: 'no-store',
      keepalive: true,
      credentials: 'omit'
    };
  }

  // Definida FUERA de bloques (para compatibilidad ES5)
  var readRiskNonBlocking = function (bodyStr) {
    var aborted = false;
    var timer = setTimeout(function () { aborted = true; }, READ_TIMEOUT_MS);
    try {
      fetch(S_GTM_ENDPOINT, makeFetchOptions(bodyStr))
        .then(function (r) { return (r && r.ok) ? r.json() : null; })
        .then(function (resp) {
          if (aborted || !resp || !resp.risk) return;
          // si activas lectura, aquí podrías setear cookies
          setCookie(COOKIE_LEVEL, (resp.risk.level || '') + '', COOKIE_TTL_SEC);
          setCookie(COOKIE_SCORE, (typeof resp.risk.score === 'number') ? String(resp.risk.score) : '', COOKIE_TTL_SEC);
          W.__risk = resp.risk;
        })
        .then(function(){ clearTimeout(timer); }, function(){ clearTimeout(timer); });
    } catch (e) { clearTimeout(timer); }
  };

  // 0) Cookies limpias
  clearRiskCookies();

  // 1) Envío fire-and-forget
  var payload = buildPayload();
  var bodyStr = JSON.stringify(payload);

  if (navigator.sendBeacon) {
    try { navigator.sendBeacon(S_GTM_ENDPOINT, bodyStr); } catch (e) {}
  } else {
    try { fetch(S_GTM_ENDPOINT, makeFetchOptions(bodyStr)).catch(function(){}); } catch(e){}
  }

  // 2) Lectura opcional (OFF por defecto)
  if (READ_RESPONSE) {
    if ('requestIdleCallback' in W) {
      requestIdleCallback(function(){ readRiskNonBlocking(bodyStr); }, { timeout: 500 });
    } else {
      W.addEventListener('load', function(){ setTimeout(function(){ readRiskNonBlocking(bodyStr); }, 0); });
    }
  }

})(window);
</script>
