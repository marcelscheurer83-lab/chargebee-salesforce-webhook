/**
 * Chargebee CRM Add-Ons → Google Sheet (runs in the cloud)
 *
 * Setup:
 * 1. In the Sheet: Extensions → Apps Script. Paste this file (replace Code.gs).
 * 2. Set script properties: File → Project properties → Script properties
 *    - CHARGEBEE_SITE   (e.g. dazos — no .chargebee.com)
 *    - CHARGEBEE_API_KEY
 *    - ADDON_IDS        optional; comma-separated exact item_price_ids. Leave EMPTY to match
 *                       any line whose item_price_id starts with Additional-CRM-User-Self-(Service|Serve)-USD-
 * 3. Add trigger: click the clock icon (Triggers) → Add Trigger
 *    - Function: runDailySync
 *    - Event: Time-driven → Day timer → 11:55 PM to 12:00 AM (or your preferred "end of day")
 * 4. First run: Run → runDailySync (or runManualSync) to test. Authorize when prompted.
 *
 * The script writes to THIS spreadsheet. Tab name: "Customers with CRM Add-Ons_YYYY-MM-DD_HH-MM-SS"
 */

const BASE_TAB_NAME = 'Customers with CRM Add-Ons';
/** Prefixes for item_price_id — any billing period / slug Chargebee uses after USD- */
const ADDON_ITEM_PRICE_ID_PREFIXES = [
  'Additional-CRM-User-Self-Service-USD-',
  'Additional-CRM-User-Self-Serve-USD-',
];
const HEADERS = ['company', 'customer_id', 'add_on_item', 'quantity', 'unit_price', 'date_added'];

/** Chargebee integer amounts are in cents for USD; zero-decimal currencies use whole units. */
var ZERO_DECIMAL_CURRENCIES = {
  BIF: 1, BYR: 1, CLF: 1, CLP: 1, CVE: 1, DJF: 1, GNF: 1, ISK: 1, JPY: 1, KMF: 1, KRW: 1,
  MGA: 1, PYG: 1, RWF: 1, UGX: 1, VND: 1, VUV: 1, XAF: 1, XOF: 1, XPF: 1
};

/** Prefix match: any billing-period slug after ...-USD- */
function isCrmAddonSubscriptionLine(ipId, itemType) {
  if (!ipId) return false;
  if ((itemType || '').toLowerCase() === 'plan') return false;
  for (var p = 0; p < ADDON_ITEM_PRICE_ID_PREFIXES.length; p++) {
    if (ipId.indexOf(ADDON_ITEM_PRICE_ID_PREFIXES[p]) === 0) return true;
  }
  return false;
}

function formatSubscriptionItemUnitPrice(item, currencyCode) {
  if (item == null || typeof item !== 'object') {
    return '';
  }
  var dec = item.unit_price_in_decimal;
  if (dec != null && String(dec).trim() !== '') {
    return String(dec).trim();
  }
  var raw = item.unit_price;
  if (raw == null) return '';
  var cur = (currencyCode || 'USD').toUpperCase();
  if (ZERO_DECIMAL_CURRENCIES[cur]) {
    return String(parseInt(raw, 10));
  }
  var major = parseInt(raw, 10) / 100;
  if (major === Math.floor(major)) {
    return String(Math.floor(major));
  }
  return major.toFixed(2);
}

function getProp(key) {
  return PropertiesService.getScriptProperties().getProperty(key) || '';
}

function runManualSync() {
  runSync(false);
}

function runDailySync() {
  runSync(true);
}

function runSync(isScheduled) {
  const site = getProp('CHARGEBEE_SITE').trim();
  const apiKey = getProp('CHARGEBEE_API_KEY').trim();
  if (!site || !apiKey) {
    throw new Error('Set CHARGEBEE_SITE and CHARGEBEE_API_KEY in Script properties');
  }
  const addonIdsStr = getProp('ADDON_IDS').trim();
  var usePrefixMatch = !addonIdsStr;
  var addonIds = usePrefixMatch
    ? null
    : new Set(addonIdsStr.split(',').map(function(s) { return s.trim(); }).filter(Boolean));

  const rows = fetchChargebeeRows(site, apiKey, addonIds, usePrefixMatch);
  const tabName = BASE_TAB_NAME + '_' + formatTimestamp(new Date());
  writeToSheet(rows, tabName);
  if (!isScheduled) {
    SpreadsheetApp.getUi().alert('Sync done. Wrote ' + rows.length + ' rows to tab: ' + tabName);
  }
}

function formatTimestamp(d) {
  const y = d.getFullYear();
  const m = ('0' + (d.getMonth() + 1)).slice(-2);
  const day = ('0' + d.getDate()).slice(-2);
  const h = ('0' + d.getHours()).slice(-2);
  const min = ('0' + d.getMinutes()).slice(-2);
  const s = ('0' + d.getSeconds()).slice(-2);
  return y + '-' + m + '-' + day + '_' + h + '-' + min + '-' + s;
}

function fetchChargebeeRows(site, apiKey, addonIds, usePrefixMatch) {
  const baseUrl = 'https://' + site + '.chargebee.com/api/v2/subscriptions';
  const basicAuth = Utilities.base64Encode(apiKey + ':');
  const rows = [HEADERS];
  let offset = null;

  do {
    const params = { limit: 100, 'status[is]': 'active' };
    if (offset) params.offset = offset;
    const query = Object.keys(params).map(function(k) { return k + '=' + encodeURIComponent(params[k]); }).join('&');
    const url = baseUrl + '?' + query;
    const resp = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: { Authorization: 'Basic ' + basicAuth },
      muteHttpExceptions: true
    });
    if (resp.getResponseCode() !== 200) {
      throw new Error('Chargebee API error: ' + resp.getResponseCode() + ' ' + resp.getContentText());
    }
    const data = JSON.parse(resp.getContentText());
    const list = data.list || [];
    for (let i = 0; i < list.length; i++) {
      const sub = list[i].subscription || {};
      const cust = list[i].customer || {};
      const cid = sub.customer_id || '';
      if (!cid) continue;
      const company = cust.company || '';
      const createdAt = sub.created_at;
      let dateAdded = '';
      if (createdAt != null) {
        const d = new Date(createdAt * 1000);
        dateAdded = d.getUTCFullYear() + '-' + ('0' + (d.getUTCMonth() + 1)).slice(-2) + '-' + ('0' + d.getUTCDate()).slice(-2);
      }
      const currency = sub.currency_code || 'USD';
      const items = sub.subscription_items || [];
      for (let j = 0; j < items.length; j++) {
        const si = items[j];
        if (si == null) continue;
        const ipId = si.item_price_id;
        if (!ipId) continue;
        if (usePrefixMatch) {
          if (!isCrmAddonSubscriptionLine(ipId, si.item_type)) continue;
        } else if (!addonIds.has(ipId)) {
          continue;
        }
        const qty = si.quantity;
        const unitPrice = formatSubscriptionItemUnitPrice(si, currency);
        rows.push([company, cid, ipId || '', qty != null ? String(qty) : '', unitPrice, dateAdded]);
      }
    }
    offset = data.next_offset || null;
  } while (offset);

  return rows;
}

function writeToSheet(rows, tabName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = null;
  const sheets = ss.getSheets();
  for (let i = 0; i < sheets.length; i++) {
    if (sheets[i].getName().indexOf(BASE_TAB_NAME) === 0) {
      sheet = sheets[i];
      break;
    }
  }
  if (!sheet) {
    sheet = ss.insertSheet(tabName, 0);
  } else {
    sheet.setName(tabName);
  }
  if (rows.length === 0) {
    sheet.getRange('A1:Z1000').clearContent();
    return;
  }
  const numRows = rows.length;
  const numCols = rows[0].length;
  const endCol = numCols <= 26 ? String.fromCharCode(64 + numCols) : 'E';
  const dataRange = 'A1:' + endCol + numRows;
  sheet.getRange(dataRange).clearContent();
  sheet.getRange(1, 1, numRows, numCols).setValues(rows);
}
