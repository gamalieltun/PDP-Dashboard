/**
 * ═══════════════════════════════════════════════════════════════
 *  PDP Dashboard — Cloudflare Worker v2.6.0
 *  Handles: auth, user management, data fetch, check-in writes,
 *           email alerts, audit logging, DCR Google Drive storage
 * ═══════════════════════════════════════════════════════════════
 *
 *  ENVIRONMENT VARIABLES (Workers → Settings → Variables & Secrets):
 *
 *    USERS_CONFIG          → JSON user store (managed by admin via dashboard)
 *    CF_API_TOKEN          → Cloudflare API token (to update USERS_CONFIG secret)
 *    CF_ACCOUNT_ID         → Cloudflare Account ID
 *    CF_WORKER_SCRIPT_NAME → Worker script name (e.g. pdp-dashboard)
 *    GOOGLE_API_KEY        → Google Sheets API key (read)
 *    SPREADSHEET_ID        → Google Sheet ID
 *    GMAIL_CLIENT_ID       → OAuth2 client ID (from Google Cloud Console)
 *    GMAIL_CLIENT_SECRET   → OAuth2 client secret
 *    GMAIL_REFRESH_TOKEN   → OAuth2 refresh token
 *    ALERT_EMAIL_TO        → email address to send alerts to
 *    ALERT_THRESHOLD       → number e.g. 0.70 for 70% threshold
 *    DRIVE_REFRESH_TOKEN   → OAuth2 refresh token with drive.file + spreadsheets + gmail.send scopes
 *    DRIVE_FOLDER_ID       → Google Drive folder ID for PDP_DCR_Reports
 *
 *  ── INITIAL USERS_CONFIG SETUP ──────────────────────────────
 *  Add this as a Secret in Cloudflare Workers → Settings → Variables:
 *
 *  {
 *    "director": {
 *      "name": "Director",
 *      "password": "ChangeMe123!",
 *      "role": "admin",
 *      "mustChangePassword": true
 *    }
 *  }
 *
 *  Then log in as director, go to Role Management → User Accounts
 *  to add remaining users from the dashboard.
 */

// ─── Sheet names ──────────────────────────────────
// Base sheets always fetched
const BASE_SHEETS = [
  'Main Projects',
  'Weekly Reports',
  // Login Audit is write-only — not needed in data fetch
];

// ─── Helpers ──────────────────────────────────────
function generateTempPassword() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#';
  let pw = '';
  // Use timestamp + random for uniqueness (Workers don't have crypto.randomBytes easily)
  const seed = Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  for (let i = 0; i < 12; i++) {
    pw += chars[Math.floor(Math.random() * chars.length)];
  }
  return pw;
}

// ─── CORS ─────────────────────────────────────────
function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
  };
}

// ─── Read sheet ───────────────────────────────────
async function fetchSheet(apiKey, sheetId, sheetName) {
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetName)}?key=${apiKey}&valueRenderOption=FORMATTED_VALUE`;
  const res = await fetch(url);
  if (res.status === 404) return [];
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(`Sheet "${sheetName}": ${err.error?.message || res.status}`);
  }
  return (await res.json()).values || [];
}

// ─── Append row to sheet (requires OAuth) ─────────
async function appendToSheet(accessToken, sheetId, sheetName, row) {
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(sheetName)}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ values: [row] }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(`Append failed: ${err.error?.message || res.status}`);
  }
  return res.json();
}

// ─── Get Gmail OAuth2 access token from refresh token ─
async function getAccessToken(clientId, clientSecret, refreshToken) {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id:     clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type:    'refresh_token',
    }),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Failed to get access token: ' + JSON.stringify(data));
  return data.access_token;
}

// ─── Google Drive helpers ─────────────────────────

async function getDriveToken(env) {
  if (!env.DRIVE_REFRESH_TOKEN || !env.GMAIL_CLIENT_ID || !env.GMAIL_CLIENT_SECRET)
    throw new Error('Missing Drive OAuth secrets (DRIVE_REFRESH_TOKEN, GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET)');
  return getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.DRIVE_REFRESH_TOKEN);
}

// Create a new JSON file in Drive, or update an existing one by file ID
async function driveWriteFile(token, folderId, filename, content, existingFileId = null) {
  const boundary = 'PDP_BOUNDARY_314159';
  const metadata = JSON.stringify({
    name: filename,
    mimeType: 'application/json',
    ...(existingFileId ? {} : { parents: [folderId] }),
  });
  const body = [
    `--${boundary}`,
    'Content-Type: application/json',
    '',
    metadata,
    `--${boundary}`,
    'Content-Type: application/json',
    '',
    JSON.stringify(content),
    `--${boundary}--`,
  ].join('\r\n');

  const url = existingFileId
    ? `https://www.googleapis.com/upload/drive/v3/files/${existingFileId}?uploadType=multipart`
    : `https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart`;

  const res = await fetch(url, {
    method: existingFileId ? 'PATCH' : 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': `multipart/related; boundary="${boundary}"`,
    },
    body,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error('Drive write failed: ' + (err.error?.message || res.status));
  }
  return res.json(); // { id, name }
}

// Upload HTML content as a Google Doc (Drive auto-converts)
async function driveWriteGoogleDoc(token, folderId, filename, htmlContent, existingFileId = null) {
  const boundary = 'PDP_GDOC_BOUNDARY_271828';
  const metadata = JSON.stringify({
    name: filename,
    mimeType: 'application/vnd.google-apps.document',
    ...(existingFileId ? {} : { parents: [folderId] }),
  });
  const body = [
    `--${boundary}`,
    'Content-Type: application/json',
    '',
    metadata,
    `--${boundary}`,
    'Content-Type: text/html; charset=UTF-8',
    '',
    htmlContent,
    `--${boundary}--`,
  ].join('\r\n');

  const url = existingFileId
    ? `https://www.googleapis.com/upload/drive/v3/files/${existingFileId}?uploadType=multipart`
    : `https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart`;

  const res = await fetch(url, {
    method: existingFileId ? 'PATCH' : 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': `multipart/related; boundary="${boundary}"`,
    },
    body,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error('Drive GDoc write failed: ' + (err.error?.message || res.status));
  }
  return res.json(); // { id, name }
}

// List all JSON files in the DCR folder with their metadata stored in description
async function driveListFiles(token, folderId) {
  const q = encodeURIComponent(`'${folderId}' in parents and mimeType='application/json' and trashed=false`);
  const res = await fetch(
    `https://www.googleapis.com/drive/v3/files?q=${q}&fields=files(id,name,description,modifiedTime)&orderBy=modifiedTime desc&pageSize=200`,
    { headers: { 'Authorization': `Bearer ${token}` } }
  );
  if (!res.ok) throw new Error('Drive list failed: ' + res.status);
  const data = await res.json();
  return data.files || [];
}

// Read one file's content
async function driveReadFile(token, fileId) {
  const res = await fetch(
    `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`,
    { headers: { 'Authorization': `Bearer ${token}` } }
  );
  if (!res.ok) throw new Error('Drive read failed: ' + res.status);
  return res.json();
}

// Find existing file by name in folder
async function driveFindFile(token, folderId, filename) {
  const q = encodeURIComponent(`'${folderId}' in parents and name='${filename}' and trashed=false`);
  const res = await fetch(
    `https://www.googleapis.com/drive/v3/files?q=${q}&fields=files(id,name)`,
    { headers: { 'Authorization': `Bearer ${token}` } }
  );
  if (!res.ok) return null;
  const data = await res.json();
  return data.files?.[0] || null;
}

// Trash a file (soft delete)
async function driveTrashFile(token, fileId) {
  const res = await fetch(
    `https://www.googleapis.com/drive/v3/files/${fileId}`,
    {
      method: 'PATCH',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trashed: true }),
    }
  );
  if (!res.ok) throw new Error('Drive trash failed: ' + res.status);
  return true;
}

// ─── Send email via Gmail API ─────────────────────
async function sendEmail(accessToken, to, subject, body) {
  const message = [
    `To: ${to}`,
    `Subject: ${subject}`,
    'Content-Type: text/html; charset=utf-8',
    '',
    body,
  ].join('\r\n');

  const encoded = btoa(unescape(encodeURIComponent(message)))
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

  const res = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ raw: encoded }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(`Email failed: ${err.error?.message || res.status}`);
  }
  return res.json();
}

// ─── Build alert email HTML ───────────────────────
function buildAlertEmail(cw, rate, threshold, weeklyData) {
  const pct = Math.round(rate * 100);
  const tpct = Math.round(threshold * 100);
  return `
    <div style="font-family:sans-serif;max-width:560px;margin:0 auto;">
      <div style="background:#1e3a5f;padding:24px 28px;border-radius:12px 12px 0 0;">
        <div style="color:#fff;font-size:20px;font-weight:800;">⚠️ PDP Performance Alert</div>
        <div style="color:#93c5fd;font-size:13px;margin-top:4px;">Week CW ${cw} — below threshold</div>
      </div>
      <div style="background:#fff;border:1px solid #e5e7eb;border-top:none;padding:24px 28px;border-radius:0 0 12px 12px;">
        <p style="color:#374151;font-size:15px;">This week's performance rate dropped below the <strong>${tpct}%</strong> threshold.</p>
        <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:16px;margin:16px 0;text-align:center;">
          <div style="font-size:48px;font-weight:800;color:#dc2626;">${pct}%</div>
          <div style="color:#9ca3af;font-size:13px;">CW ${cw} Performance Rate</div>
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <tr style="background:#f9fafb;">
            <th style="padding:8px 12px;text-align:left;color:#6b7280;border:1px solid #e5e7eb;">CW</th>
            <th style="padding:8px 12px;text-align:left;color:#6b7280;border:1px solid #e5e7eb;">Target</th>
            <th style="padding:8px 12px;text-align:left;color:#6b7280;border:1px solid #e5e7eb;">Completed</th>
            <th style="padding:8px 12px;text-align:left;color:#6b7280;border:1px solid #e5e7eb;">Rate</th>
          </tr>
          ${weeklyData.slice(-3).map(w => `
            <tr>
              <td style="padding:8px 12px;border:1px solid #e5e7eb;">CW ${w.cw}</td>
              <td style="padding:8px 12px;border:1px solid #e5e7eb;">${w.t}</td>
              <td style="padding:8px 12px;border:1px solid #e5e7eb;">${w.d}</td>
              <td style="padding:8px 12px;border:1px solid #e5e7eb;color:${w.p < threshold ? '#dc2626' : '#16a34a'}">${Math.round(w.p*100)}%</td>
            </tr>`).join('')}
        </table>
        <p style="color:#6b7280;font-size:12px;margin-top:16px;">This is an automated alert from the PDP Dashboard. Log in to view full details.</p>
      </div>
    </div>`;
}

// ─── Check & send performance alert ──────────────
async function checkAndSendAlert(env, weeklyRows) {
  if (!env.GMAIL_CLIENT_ID || !env.GMAIL_REFRESH_TOKEN || !env.ALERT_EMAIL_TO) return;

  const threshold = parseFloat(env.ALERT_THRESHOLD || '0.70');

  // Parse weekly data
  const weekly = weeklyRows.slice(1)
    .filter(r => r[0] && !isNaN(parseFloat(r[0])) && parseFloat(r[0]) >= 1)
    .map(r => ({
      cw: Math.round(parseFloat(r[0])),
      t:  parseFloat(String(r[1]||'0').replace(/[^0-9.]/g,'')) || 0,
      d:  parseFloat(String(r[2]||'0').replace(/[^0-9.]/g,'')) || 0,
      p:  0,
    }))
    .map(w => ({ ...w, p: w.t > 0 ? w.d / w.t : 0 }));

  // Find current week's data
  const now = new Date();
  const day = now.getUTCDay() || 7;
  const d = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const currentCW = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);

  const thisWeek = weekly.find(w => w.cw === currentCW);
  if (!thisWeek || thisWeek.t === 0) return; // No data yet

  if (thisWeek.p < threshold) {
    try {
      const token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN);
      const html  = buildAlertEmail(currentCW, thisWeek.p, threshold, weekly.slice(-5));
      await sendEmail(token, env.ALERT_EMAIL_TO, `⚠️ PDP Alert: CW ${currentCW} performance at ${Math.round(thisWeek.p*100)}%`, html);
    } catch(e) {
      console.error('Email alert failed:', e.message);
    }
  }
}

// ─── Main handler ─────────────────────────────────
export default {
  async fetch(request, env) {
    const headers = corsHeaders();

    // Always handle OPTIONS preflight
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers });

    try {
      if (request.method !== 'POST')
        return new Response(JSON.stringify({ error: 'Method not allowed' }), { status: 405, headers });

      let body;
      try { body = await request.json(); }
      catch { return new Response(JSON.stringify({ error: 'Invalid JSON' }), { status: 400, headers }); }

      const { action } = body;

    // ══════════════════════════════════════════════
    //  USER HELPERS
    // ══════════════════════════════════════════════

    function getUsers() {
      if (!env.USERS_CONFIG) return {};
      try { return JSON.parse(env.USERS_CONFIG); } catch { return {}; }
    }

    function validateUser(username, password) {
      const users = getUsers();
      const key = Object.keys(users).find(k => k.toLowerCase() === (username||'').toLowerCase());
      const u = key ? users[key] : null;
      if (!u) return null;
      if (u.password !== password) return null;
      return { ...u, _key: key };
    }

    function isAdminUser(username, password) {
      const u = validateUser(username, password);
      return u?.role === 'admin';
    }

    function usersPublicList(users) {
      // Return user list without passwords
      return Object.entries(users).map(([username, u]) => ({
        username, name: u.name, role: u.role,
        mustChangePassword: !!u.mustChangePassword
      }));
    }

    async function saveUsers(users) {
      // Update USERS_CONFIG secret via Cloudflare API
      if (!env.CF_API_TOKEN || !env.CF_ACCOUNT_ID || !env.CF_WORKER_SCRIPT_NAME) {
        throw new Error('CF_API_TOKEN, CF_ACCOUNT_ID, and CF_WORKER_SCRIPT_NAME secrets are required to save users.');
      }
      const res = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${env.CF_ACCOUNT_ID}/workers/scripts/${env.CF_WORKER_SCRIPT_NAME}/secrets`,
        {
          method: 'PUT',
          headers: {
            'Authorization': `Bearer ${env.CF_API_TOKEN}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            name: 'USERS_CONFIG',
            text: JSON.stringify(users),
            type: 'secret_text',
          })
        }
      );
      if (!res.ok) {
        const e = await res.json().catch(() => ({}));
        throw new Error('Cloudflare API error: ' + (e.errors?.[0]?.message || res.status));
      }
    }

    // ── Auth ─────────────────────────────────────
    if (action === 'auth') {
      const { username, password } = body;
      const users = getUsers();
      const u = users[username];

      if (!u || u.password !== password) {
        return new Response(JSON.stringify({ ok: false, error: 'Invalid credentials' }), { status: 401, headers });
      }

      // Log login to Google Sheets (fire and forget)
      console.log('Audit: GMAIL_REFRESH_TOKEN present:', !!env.GMAIL_REFRESH_TOKEN);
      console.log('Audit: SPREADSHEET_ID present:', !!env.SPREADSHEET_ID);
      if (env.GMAIL_REFRESH_TOKEN && env.GMAIL_CLIENT_ID && env.GMAIL_CLIENT_SECRET) {
        const resolvedUsername = u._key || username;
        const displayName = u.name;
        const userRole = u.role;
        const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
        (async () => {
          try {
            console.log('Audit: getting access token...');
            const token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN);
            console.log('Audit: token obtained, writing row...');
            const now = new Date();
            const row = [
              now.toISOString().split('T')[0],
              now.toTimeString().split(' ')[0],
              resolvedUsername, displayName, userRole, ip
            ];
            await appendToSheet(token, env.SPREADSHEET_ID, 'Login Audit', row);
            console.log('Audit: row written successfully');
          } catch(e) {
            console.error('Login audit write failed:', e.message);
          }
        })();
      } else {
        console.log('Audit: skipped - missing OAuth secrets');
      }

      // Build roleConfig from USERS_CONFIG (merged into each user record by save_roles)
      // This is keyed by display name for the Role Management panel
      let roleConfig = null;
      if (u.role === 'admin') {
        const allUsers = getUsers();
        roleConfig = {};
        Object.values(allUsers).forEach(usr => {
          roleConfig[usr.name] = {
            role:     usr.role     || 'staff',
            projects: usr.projects || [],
            features: usr.features || [],
          };
        });
        // Fallback: also merge STAFF_ROLES secret if it exists (legacy support)
        if (env.STAFF_ROLES) {
          try {
            const legacy = JSON.parse(env.STAFF_ROLES);
            Object.entries(legacy).forEach(([name, cfg]) => {
              if (!roleConfig[name]) roleConfig[name] = cfg;
            });
          } catch(e) {}
        }
      }

      // Return public user list to admin
      const usersList = u.role === 'admin' ? usersPublicList(getUsers()) : null;

      return new Response(JSON.stringify({
        ok: true,
        name: u.name,
        role: u.role,
        projects: u.projects || null,
        features: u.features || null,
        mustChangePassword: !!u.mustChangePassword,
        roleConfig,
        users: usersList,
      }), { status: 200, headers });
    }

    // ── Change own password ───────────────────────
    if (action === 'change_password') {
      const { username, oldPassword, newPassword } = body;
      const users = getUsers();
      const u = users[username];

      if (!u || u.password !== oldPassword) {
        return new Response(JSON.stringify({ ok: false, error: 'Current password is incorrect.' }), { status: 401, headers });
      }
      if (!newPassword || newPassword.length < 8) {
        return new Response(JSON.stringify({ ok: false, error: 'Password must be at least 8 characters.' }), { status: 400, headers });
      }

      users[username].password = newPassword;
      users[username].mustChangePassword = false;
      await saveUsers(users);

      return new Response(JSON.stringify({
        ok: true,
        user: { name: u.name, role: u.role, projects: u.projects || null, features: u.features || null }
      }), { status: 200, headers });
    }

    // ── Add new user (Admin only) ─────────────────
    if (action === 'add_user') {
      const { username, password, newUser } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      const users = getUsers();
      if (users[newUser.username])
        return new Response(JSON.stringify({ error: 'Username already exists' }), { status: 400, headers });

      // Generate secure temp password
      const tempPassword = generateTempPassword();

      users[newUser.username] = {
        name: newUser.name,
        password: tempPassword,
        role: newUser.role || 'staff',
        mustChangePassword: true,
      };
      await saveUsers(users);

      return new Response(JSON.stringify({
        ok: true, tempPassword,
        users: usersPublicList(users)
      }), { status: 200, headers });
    }

    // ── Reset user password (Admin only) ─────────
    if (action === 'reset_user_password') {
      const { username, password, targetUsername } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      const users = getUsers();
      if (!users[targetUsername])
        return new Response(JSON.stringify({ error: 'User not found' }), { status: 404, headers });
      if (targetUsername === username)
        return new Response(JSON.stringify({ error: 'Use Change Password to update your own password' }), { status: 400, headers });

      const tempPassword = generateTempPassword();
      users[targetUsername].password = tempPassword;
      users[targetUsername].mustChangePassword = true;
      await saveUsers(users);

      return new Response(JSON.stringify({
        ok: true, tempPassword,
        users: usersPublicList(users)
      }), { status: 200, headers });
    }

    // ── Delete user (Admin only) ──────────────────
    if (action === 'delete_user') {
      const { username, password, targetUsername } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });
      if (targetUsername === username)
        return new Response(JSON.stringify({ error: 'Cannot delete your own account' }), { status: 400, headers });

      const users = getUsers();
      if (!users[targetUsername])
        return new Response(JSON.stringify({ error: 'User not found' }), { status: 404, headers });

      delete users[targetUsername];
      await saveUsers(users);

      return new Response(JSON.stringify({
        ok: true, users: usersPublicList(users)
      }), { status: 200, headers });
    }

    // ── Data fetch ───────────────────────────────
    if (action === 'data') {
      const { username, password } = body;
      const u = validateUser(username, password);
      if (!u)
        return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      const results = {};
      const errors  = {};

      // Step 1: fetch base sheets always
      for (const sheet of BASE_SHEETS) {
        try   { results[sheet] = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, sheet); }
        catch(e) { errors[sheet] = e.message; results[sheet] = []; }
      }

      // Step 2: discover all project IDs from Main Projects, then fetch each task sheet
      const mainRows = results['Main Projects'] || [];
      const projectIds = mainRows.slice(1)
        .map(r => r[0])
        .filter(id => id && String(id).trim());

      for (const pid of projectIds) {
        const taskSheet = `${pid} Task_List`;
        try   { results[taskSheet] = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, taskSheet); }
        catch(e) { errors[taskSheet] = e.message; results[taskSheet] = []; }
      }

      // Build roleConfig from USERS_CONFIG (same as auth action)
      let roleConfig = null;
      if (u.role === 'admin') {
        const allUsers = getUsers();
        roleConfig = {};
        Object.values(allUsers).forEach(usr => {
          roleConfig[usr.name] = { role: usr.role || 'staff', projects: usr.projects || [], features: usr.features || [] };
        });
        if (env.STAFF_ROLES) {
          try { const lg = JSON.parse(env.STAFF_ROLES); Object.entries(lg).forEach(([n,c]) => { if (!roleConfig[n]) roleConfig[n]=c; }); } catch(e) {}
        }
      }
      const usersList  = u.role === 'admin' ? usersPublicList(getUsers()) : null;

      return new Response(JSON.stringify({ ok: true, sheets: results, errors, roleConfig, users: usersList }), { status: 200, headers });
    }

    // ── Add new project ──────────────────────────
    if (action === 'add_project') {
      const { username, password } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      const { project } = body;
      if (!project?.id || !project?.name)
        return new Response(JSON.stringify({ error: 'Missing project data' }), { status: 400, headers });

      if (!env.GMAIL_REFRESH_TOKEN)
        return new Response(JSON.stringify({ error: 'OAuth not configured' }), { status: 501, headers });

      let token;
      try { token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN); }
      catch(e) { return new Response(JSON.stringify({ error: 'OAuth failed: ' + e.message }), { status: 500, headers }); }

      const sheetName = `${project.id} Task_List`;
      const spreadsheetId = env.SPREADSHEET_ID;

      // 1. Create new sheet tab
      const addSheetRes = await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}:batchUpdate`,
        {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            requests: [{
              addSheet: {
                properties: { title: sheetName }
              }
            }]
          })
        }
      );
      if (!addSheetRes.ok) {
        const e = await addSheetRes.json().catch(() => ({}));
        return new Response(JSON.stringify({ error: 'Sheet creation failed: ' + (e.error?.message || addSheetRes.status) }), { status: 500, headers });
      }

      // 2. Add headers to new sheet — 8 clean data columns, no formulas
      const sheetHeaders = ['Task ID','Project ID','Project Name','Task Name','Quarter','CW','Status','Owner'];
      await appendToSheet(token, spreadsheetId, sheetName, sheetHeaders);

      // 3. Add project row to Main Projects sheet
      // No formulas — Target Tasks is updated by dashboard from actual task count
      const duration = project.end - project.start + 1;
      const projRow = [
        project.id, project.name, project.start, project.end,
        project.startQuarter, project.endQuarter, duration,
        0   // Target Tasks — starts at 0, grows as tasks are added
      ];
      await appendToSheet(token, spreadsheetId, 'Main Projects', projRow);

      return new Response(JSON.stringify({ ok: true, sheetName }), { status: 200, headers });
    }

    // ── Add task to project ───────────────────────
    if (action === 'add_task') {
      const { username, password } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      const { task } = body;
      if (!task?.projectId || !task?.name)
        return new Response(JSON.stringify({ error: 'Missing task data' }), { status: 400, headers });

      if (!env.GMAIL_REFRESH_TOKEN)
        return new Response(JSON.stringify({ error: 'OAuth not configured' }), { status: 501, headers });

      let token;
      try { token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN); }
      catch(e) { return new Response(JSON.stringify({ error: 'OAuth failed: ' + e.message }), { status: 500, headers }); }

      const sheetName = `${task.projectId} Task_List`;

      // Fetch existing rows to determine next task number
      const rows = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, sheetName);
      const cwNum = parseInt(task.cw) || 1;

      // Count real data rows (non-blank Task ID in col A) to generate unique Task ID
      const dataRows = rows.slice(1).filter(r => r[0] && String(r[0]).trim());
      const taskId   = `T${dataRows.length + 1}`;

      // 8 clean data columns — all KPI computed by dashboard from these
      const row = [
        taskId,               // col A — Task ID
        task.projectId,       // col B — Project ID
        task.projectName,     // col C — Project Name
        task.name,            // col D — Task Name
        task.quarter,         // col E — Quarter
        cwNum,                // col F — CW (number)
        task.status || 'Planned', // col G — Status
        task.owner,           // col H — Owner
      ];

      await appendToSheet(token, env.SPREADSHEET_ID, sheetName, row);

      // Update Target Tasks count in Main Projects sheet
      try {
        const mpRows = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, 'Main Projects');
        // Find the header row and the project row
        const headerIdx = mpRows.findIndex(r => r && r.some(c => String(c).trim() === 'Project ID'));
        if (headerIdx >= 0) {
          const projRowIdx = mpRows.findIndex((r, i) => i > headerIdx && String(r[0]||'').trim() === task.projectId);
          if (projRowIdx >= 0) {
            // Col H (index 7) = Target Tasks — count all non-empty Task ID rows in the task sheet
            const taskRows = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, sheetName);
            const taskCount = taskRows.slice(1).filter(r => r[0] && String(r[0]).trim()).length;
            const targetRange = `Main Projects!H${projRowIdx + 1}`;
            const updateUrl = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${encodeURIComponent(targetRange)}?valueInputOption=RAW`;
            await fetch(updateUrl, {
              method: 'PUT',
              headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ values: [[taskCount]] }),
            });
          }
        }
      } catch(e) {
        // Non-critical — continue even if Target Tasks update fails
        console.warn('Target Tasks update failed:', e.message);
      }

      return new Response(JSON.stringify({ ok: true, taskId, sheetName, row }), { status: 200, headers });
    }


    // ── Update single task status ─────────────────
    if (action === 'update_task') {
      const { username, password, taskName, projectId, newStatus } = body;
      if (!validateUser(username, password))
        return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      if (!['Planned','Delivered','In Progress','Cancelled'].includes(newStatus))
        return new Response(JSON.stringify({ error: 'Invalid status' }), { status: 400, headers });

      if (!env.GMAIL_REFRESH_TOKEN)
        return new Response(JSON.stringify({ error: 'OAuth not configured' }), { status: 501, headers });

      let token;
      try { token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN); }
      catch(e) { return new Response(JSON.stringify({ error: 'OAuth failed: ' + e.message }), { status: 500, headers }); }

      const sheetName = `${projectId} Task_List`;
      const rows = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, sheetName);
      const header = rows[0] || [];
      const statusCol = header.findIndex(h => String(h).toLowerCase().includes('status'));
      const nameCol   = header.findIndex(h => String(h).toLowerCase() === 'task name');

      if (statusCol === -1 || nameCol === -1)
        return new Response(JSON.stringify({ error: 'Sheet headers not found' }), { status: 500, headers });

      const rowIdx = rows.findIndex((r, i) => i > 0 && String(r[nameCol]||'').trim() === taskName.trim());
      if (rowIdx === -1)
        return new Response(JSON.stringify({ error: `Task "${taskName}" not found in ${sheetName}` }), { status: 404, headers });

      const colLetter = String.fromCharCode(65 + statusCol);
      const range     = `${encodeURIComponent(sheetName)}!${colLetter}${rowIdx + 1}`;
      const url       = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${range}?valueInputOption=USER_ENTERED`;
      const res = await fetch(url, {
        method: 'PUT',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ values: [[newStatus]] }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        return new Response(JSON.stringify({ error: err.error?.message || res.status }), { status: 500, headers });
      }
      return new Response(JSON.stringify({ ok: true, taskName, projectId, newStatus, row: rowIdx + 1 }), { status: 200, headers });
    }

    // ── Save role config (Admin only) ────────────
    if (action === 'save_roles') {
      const { username, password, config } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      if (!config) return new Response(JSON.stringify({ error: 'No config provided' }), { status: 400, headers });

      // Merge role config (projects, features, role) into each user in USERS_CONFIG
      // Config is keyed by display name: { "Gamaliel": { role, projects, features }, ... }
      const users = getUsers();

      // Build a lookup: display name → username key
      const nameToKey = {};
      Object.entries(users).forEach(([key, u]) => { nameToKey[u.name] = key; });

      let changed = 0;
      for (const [displayName, cfg] of Object.entries(config)) {
        const key = nameToKey[displayName];
        if (!key || !users[key]) continue; // unknown user — skip
        users[key].role     = cfg.role     || users[key].role;
        users[key].projects = cfg.projects || [];
        users[key].features = cfg.features || [];
        changed++;
      }

      if (changed === 0)
        return new Response(JSON.stringify({ ok: true, saved: 0, note: 'No matching users found in config' }), { status: 200, headers });

      try {
        await saveUsers(users);
        return new Response(JSON.stringify({ ok: true, saved: changed }), { status: 200, headers });
      } catch(e) {
        // saveUsers failed (CF API token missing etc.) — return config for manual paste
        return new Response(JSON.stringify({
          ok: false,
          manualPaste: true,
          config,
          error: e.message,
          note: 'Auto-save failed. Copy the config JSON and paste as STAFF_ROLES in Cloudflare.'
        }), { status: 200, headers });
      }
    }

    // ── Check-in write ───────────────────────────
    if (action === 'checkin') {
      const { username, password } = body;
      const checkinUser = validateUser(username, password);
      if (!checkinUser)
        return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      const { entry } = body;
      if (!entry || !entry.name) return new Response(JSON.stringify({ error: 'Missing entry' }), { status: 400, headers });

      // Staff can only submit for themselves — admin/manager can submit for anyone
      if (checkinUser.role === 'staff') {
        if (entry.name.trim().toLowerCase() !== checkinUser.name.trim().toLowerCase()) {
          return new Response(JSON.stringify({ error: `Staff can only submit their own check-in. You are logged in as "${checkinUser.name}".` }), { status: 403, headers });
        }
      }

      // Check secrets exist
      const missingSecrets = [];
      if (!env.GMAIL_CLIENT_ID)     missingSecrets.push('GMAIL_CLIENT_ID');
      if (!env.GMAIL_CLIENT_SECRET) missingSecrets.push('GMAIL_CLIENT_SECRET');
      if (!env.GMAIL_REFRESH_TOKEN) missingSecrets.push('GMAIL_REFRESH_TOKEN');
      if (missingSecrets.length > 0) {
        return new Response(JSON.stringify({ ok: false, error: 'Missing secrets: ' + missingSecrets.join(', ') }), { status: 500, headers });
      }

      let token;
      try {
        token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN);
      } catch(e) {
        return new Response(JSON.stringify({ ok: false, error: 'OAuth token failed: ' + e.message }), { status: 500, headers });
      }

      const taskErrors = [];

      // ── Update task statuses — lookup row by task name ──
      if (entry.taskUpdates?.length) {
        // Group by project sheet
        const bySheet = {};
        entry.taskUpdates.forEach(u => {
          const sheet = `${u.projectId} Task_List`;
          if (!bySheet[sheet]) bySheet[sheet] = [];
          bySheet[sheet].push(u);
        });

        for (const [sheet, updates] of Object.entries(bySheet)) {
          try {
            const rows     = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, sheet);
            const header   = rows[0] || [];
            const statusCol = header.findIndex(h => String(h).toLowerCase().includes('status'));
            const nameCol   = header.findIndex(h => String(h).toLowerCase().includes('task name'));
            if (statusCol === -1 || nameCol === -1) continue;

            for (const u of updates) {
              const rowIdx = rows.findIndex((r, i) => i > 0 && String(r[nameCol] || '') === u.taskName);
              if (rowIdx === -1) { taskErrors.push({ task: u.taskName, error: 'Row not found' }); continue; }

              const colLetter = String.fromCharCode(65 + statusCol);
              const range     = `${encodeURIComponent(sheet)}!${colLetter}${rowIdx + 1}`;
              const url       = `https://sheets.googleapis.com/v4/spreadsheets/${env.SPREADSHEET_ID}/values/${range}?valueInputOption=USER_ENTERED`;
              const res = await fetch(url, {
                method: 'PUT',
                headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ values: [[u.newStatus]] }),
              });
              if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                taskErrors.push({ task: u.taskName, error: err.error?.message || res.status });
              }
            }
          } catch(e) {
            taskErrors.push({ sheet, error: e.message });
          }
        }
      }

      // ── Append check-in summary to Weekly Reports ──
      try {
        const row = [
          entry.date || new Date().toLocaleDateString(),
          entry.cw, entry.name, entry.project,
          entry.done, entry.missed,
          entry.blockers || '', entry.notes || '',
          entry.time || '',
          entry.taskUpdates?.length || 0,
        ];
        await appendToSheet(token, env.SPREADSHEET_ID, 'Weekly Reports', row);
      } catch(e) {
        console.error('Check-in append failed:', e.message);
      }

      return new Response(JSON.stringify({ ok: true, taskErrors }), { status: 200, headers });
    }


    // ── Setup sheets (Admin only) — creates required sheets with headers ──
    if (action === 'setup_sheets') {
      const { username, password } = body;
      if (!isAdminUser(username, password))
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      if (!env.GMAIL_REFRESH_TOKEN)
        return new Response(JSON.stringify({ error: 'OAuth not configured — add Gmail OAuth secrets' }), { status: 501, headers });

      let token;
      try { token = await getAccessToken(env.GMAIL_CLIENT_ID, env.GMAIL_CLIENT_SECRET, env.GMAIL_REFRESH_TOKEN); }
      catch(e) { return new Response(JSON.stringify({ error: 'OAuth failed: ' + e.message }), { status: 500, headers }); }

      const spreadsheetId = env.SPREADSHEET_ID;
      const created = [];
      const skipped = [];

      // Sheets to ensure exist, with their header rows
      const sheetsToSetup = [
        {
          name: 'Main Projects',
          // Columns match what parseSheetData expects — no underscore, consistent naming
          headers: ['Project ID','Project Name','Start CW','End CW','Start Quarter','End Quarter','Duration Weeks','Target Tasks'],
        },
        {
          name: 'Weekly Reports',
          headers: ['Date','CW','Name','Project','Done','Missed','Blockers','Notes','Time'],
        },
        {
          name: 'Login Audit',
          headers: ['Date','Time','Username','Name','Role','IP'],
        },
      ];

      for (const sheet of sheetsToSetup) {
        // Check if sheet already has correct headers
        const existing = await fetchSheet(env.GOOGLE_API_KEY, spreadsheetId, sheet.name);

        // Find the header row — look for the row that contains 'Project ID' or the first expected header
        const firstExpected = sheet.headers[0];
        const hasCorrectHeader = existing.some(row =>
          row.some(cell => String(cell).trim() === firstExpected)
        );

        if (hasCorrectHeader) {
          skipped.push(sheet.name);
          continue;
        }

        // Sheet doesn't exist or has wrong/no headers — create it (ignore error if already exists)
        if (existing.length === 0) {
          const addRes = await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}:batchUpdate`,
            {
              method: 'POST',
              headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ requests: [{ addSheet: { properties: { title: sheet.name } } }] })
            }
          );
          if (!addRes.ok) {
            const e = await addRes.json().catch(() => ({}));
            if (!e.error?.message?.includes('already exists')) {
              // Real error creating sheet — skip it
              continue;
            }
          }
        }

        // Clear and re-write headers to row 1
        const clearUrl = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(sheet.name)}!A1:Z1:clear`;
        await fetch(clearUrl, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        });
        await appendToSheet(token, spreadsheetId, sheet.name, sheet.headers);
        created.push(sheet.name);
      }

      // ── Migrate any existing Px Task_List sheets with offset data ──
      // Old Excel structure had task data in cols J-Q instead of A-H
      // Detect this and move the data to the correct columns

      // First get all sheet names from the spreadsheet
      const metaRes = await fetch(
        `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}?fields=sheets.properties.title`,
        { headers: { 'Authorization': `Bearer ${token}` } }
      );
      const meta = await metaRes.json();
      const allSheetNames = (meta.sheets || []).map(s => s.properties.title);
      const taskSheets = allSheetNames.filter(n => /^P\d+ Task_List$/.test(n));

      const migrated = [];

      for (const tsName of taskSheets) {
        const tsRows = await fetchSheet(env.GOOGLE_API_KEY, spreadsheetId, tsName);
        if (tsRows.length < 2) continue;

        // Check if row 2 (first data row) has data starting at col J (index 9)
        // and cols A-H are mostly empty
        const dataRow = tsRows[1]; // index 1 = row 2
        const colAH_filled = dataRow.slice(0, 8).filter(v => v !== null && v !== undefined && v !== '').length;
        const colJQ_filled = (dataRow[9] || dataRow[10] || dataRow[11]) ? true : false;

        if (colAH_filled === 0 && colJQ_filled) {
          // Offset structure detected — migrate all data rows
          // The real data starts at col J (index 9) and is 8 cols wide
          const dataRows = tsRows.slice(1).filter(row =>
            row[9] !== null && row[9] !== undefined && row[9] !== ''
          );

          if (dataRows.length === 0) continue;

          // Build clean rows: take cols 9-16 (J-Q) as the new A-H
          const cleanRows = dataRows.map(r => [
            r[9]  || '',  // Task ID
            r[10] || '',  // Project ID
            r[11] || '',  // Project Name
            r[12] || '',  // Task Name
            r[13] || '',  // Quarter
            r[14] || '',  // CW
            r[15] || '',  // Status
            r[16] || '',  // Owner
          ]);

          // 1. Clear entire sheet
          await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(tsName)}:clear`,
            { method: 'POST', headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } }
          );

          // 2. Write clean header row
          const headers = ['Task ID','Project ID','Project Name','Task Name','Quarter','CW','Status','Owner'];
          await appendToSheet(token, spreadsheetId, tsName, headers);

          // 3. Write all migrated data rows
          for (const row of cleanRows) {
            await appendToSheet(token, spreadsheetId, tsName, row);
          }

          migrated.push(`${tsName} (${cleanRows.length} tasks)`);
        }
      }

      if (migrated.length > 0) {
        return new Response(JSON.stringify({ ok: true, created, skipped, migrated }), { status: 200, headers });
      }

      return new Response(JSON.stringify({ ok: true, created, skipped }), { status: 200, headers });
    }

    // ─── save_dcr ────────────────────────────────────────────
    if (action === 'save_dcr') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      const report = body.report;
      if (!report || !report.id || !report.type)
        return new Response(JSON.stringify({ error: 'Missing report data' }), { status: 400, headers });

      // Stamp server-side fields
      const resolvedUsername = user._key || body.username;
      report.pdpUsername  = resolvedUsername;
      report.submittedBy  = user.name || resolvedUsername;
      report.savedAt      = new Date().toISOString();
      if (body.submit) {
        report.status      = 'submitted';
        report.submittedAt = new Date().toISOString();
      } else {
        report.status = report.status || 'draft';
      }

      // Clean filename: DCR_Monthly_John_2026-03-13.json
      // Use report.id as a tie-breaker suffix to allow multiple reports of same type/user
      const typeLabel = { monthly: 'Monthly', biannual: 'Biannual', annual: 'Annual' }[report.type] || report.type;
      const safeName  = (user.name || resolvedUsername).replace(/[^a-zA-Z0-9]/g, '_');
      const dateStr   = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
      // Keep report.id suffix so the same report can be updated (not duplicated)
      const idSuffix  = report.id ? '_' + String(report.id).slice(-6) : '';
      const filename  = `DCR_${typeLabel}_${safeName}_${dateStr}${idSuffix}.json`;

      try {
        const token = await getDriveToken(env);
        const jsonFolderId = env.DRIVE_JSON_FOLDER_ID || env.DRIVE_FOLDER_ID;
        const pdfFolderId  = env.DRIVE_PDF_FOLDER_ID  || env.DRIVE_FOLDER_ID;

        // Save/update JSON to Reports_JSON folder
        const existing = await driveFindFile(token, jsonFolderId, filename);
        const file = await driveWriteFile(token, jsonFolderId, filename, report, existing?.id || null);

        // On submit: also upload a Google Doc to Reports_PDF folder
        let gdocId = null;
        if (body.submit && body.htmlContent) {
          const gdocName = filename.replace('.json', '');
          // Find existing GDoc to update instead of duplicating
          const existingDoc = await driveFindFile(token, pdfFolderId, gdocName);
          const gdoc = await driveWriteGoogleDoc(token, pdfFolderId, gdocName, body.htmlContent, existingDoc?.id || null);
          gdocId = gdoc.id;
        }

        return new Response(JSON.stringify({ ok: true, fileId: file.id, filename, status: report.status, gdocId }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: 'Drive error: ' + e.message }), { status: 500, headers });
      }
    }

    // ─── get_dcr ─────────────────────────────────────────────
    if (action === 'get_dcr') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      try {
        const token = await getDriveToken(env);
        const files = await driveListFiles(token, env.DRIVE_JSON_FOLDER_ID || env.DRIVE_FOLDER_ID);

        // Read each file's content in parallel (cap at 50)
        const toFetch = files.slice(0, 50);
        const reports = await Promise.all(
          toFetch.map(async f => {
            try {
              const content = await driveReadFile(token, f.id);
              return { ...content, _fileId: f.id, _filename: f.name, _modifiedTime: f.modifiedTime };
            } catch { return null; }
          })
        );

        const valid = reports.filter(Boolean);

        // Staff only see their own reports — match _key (login username)
        const selfUsername = user._key || body.username;
        const filtered = (user.role === 'staff')
          ? valid.filter(r => r.pdpUsername === selfUsername)
          : valid;

        return new Response(JSON.stringify({ ok: true, reports: filtered }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: 'Drive error: ' + e.message }), { status: 500, headers });
      }
    }

    // ─── delete_dcr ──────────────────────────────────────────
    if (action === 'delete_dcr') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      // Only admin can delete
      if (user.role !== 'admin')
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      const { fileId } = body;
      if (!fileId) return new Response(JSON.stringify({ error: 'Missing fileId' }), { status: 400, headers });

      try {
        const token = await getDriveToken(env);
        await driveTrashFile(token, fileId);
        return new Response(JSON.stringify({ ok: true }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: 'Drive error: ' + e.message }), { status: 500, headers });
      }
    }


    // ─── get_finance_data ─────────────────────────────────────
    if (action === 'get_finance_data') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      const allowed = ['admin', 'manager', 'finance'];
      if (!allowed.includes(user.role))
        return new Response(JSON.stringify({ error: 'Access denied' }), { status: 403, headers });

      const finSheetId = env.FINANCE_SPREADSHEET_ID;
      if (!finSheetId) return new Response(JSON.stringify({ error: 'FINANCE_SPREADSHEET_ID not configured' }), { status: 500, headers });

      try {
        // Use OAuth token — Finance sheet is private (API key only works on public sheets)
        const token = await getDriveToken(env);
        const finFetch = (sheet) => fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/${encodeURIComponent(sheet)}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );

        const [budgetRes, txRes, queueRes] = await Promise.all([
          finFetch('Budget'),
          finFetch('Transactions'),
          finFetch('Approval_Queue'),
        ]);

        const toObjects = async (res) => {
          const d = await res.json();
          if (d.error) throw new Error(`Sheets API: ${d.error.message}`);
          const rows = d.values || [];
          if (rows.length < 2) return [];
          const [headers, ...data] = rows;
          return data.map(r => Object.fromEntries(headers.map((h, i) => [h, r[i] || ''])));
        };

        const [budget, transactions, queue] = await Promise.all([
          toObjects(budgetRes), toObjects(txRes), toObjects(queueRes)
        ]);

        // Also fetch projects from main PDP sheet so finance.html can sync
        let projects = [];
        try {
          const mainRows = await fetchSheet(env.GOOGLE_API_KEY, env.SPREADSHEET_ID, 'Main Projects');
          const hIdx = mainRows.findIndex(r => r && r.some(c => String(c).trim() === 'Project ID'));
          if (hIdx >= 0) {
            const [hdrs, ...rows] = mainRows.slice(hIdx);
            projects = rows
              .map(r => Object.fromEntries(hdrs.map((h,i) => [h.trim(), r[i] || ''])))
              .filter(r => r['Project ID'] && !String(r['Project ID']).startsWith('#'))
              .map(r => ({ id: r['Project ID'].trim(), name: (r['Project Name '] || r['Project Name'] || r['Project ID']).trim() }));
          }
        } catch(_) {}

        return new Response(JSON.stringify({ ok: true, budget, transactions, queue, projects }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }), { status: 500, headers });
      }
    }

    // ─── log_expense ──────────────────────────────────────────
    if (action === 'log_expense') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      const allowed = ['admin', 'manager', 'finance'];
      if (!allowed.includes(user.role))
        return new Response(JSON.stringify({ error: 'Access denied' }), { status: 403, headers });

      const { project, category, amount, description } = body;
      if (!project || !category || !amount || !description)
        return new Response(JSON.stringify({ error: 'Missing fields: project, category, amount, description' }), { status: 400, headers });

      const finSheetId = env.FINANCE_SPREADSHEET_ID;
      const now = new Date().toISOString();
      const txId = 'TX-' + Date.now().toString(36).toUpperCase();

      try {
        const token = await getDriveToken(env);
        // Append to Transactions sheet
        const txRow = [txId, now.slice(0,10), project, category, amount, description,
                       user.name || user._key, 'pending', '', ''];
        await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Transactions:append?valueInputOption=USER_ENTERED`,
          {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ values: [txRow] })
          }
        );

        // Append to Approval_Queue
        const reqId = 'REQ-' + Date.now().toString(36).toUpperCase();
        const queueRow = [reqId, txId, user.name || user._key, now, amount, description, 'pending', '', '', ''];
        await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Approval_Queue:append?valueInputOption=USER_ENTERED`,
          {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ values: [queueRow] })
          }
        );

        return new Response(JSON.stringify({ ok: true, txId }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }), { status: 500, headers });
      }
    }

    // ─── approve_expense / reject_expense ─────────────────────
    if (action === 'approve_expense' || action === 'reject_expense') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      if (!['admin', 'manager'].includes(user.role))
        return new Response(JSON.stringify({ error: 'Manager or Admin only' }), { status: 403, headers });

      const { txId, notes } = body;
      if (!txId) return new Response(JSON.stringify({ error: 'Missing txId' }), { status: 400, headers });

      const finSheetId = env.FINANCE_SPREADSHEET_ID;
      const newStatus = action === 'approve_expense' ? 'approved' : 'rejected';
      const now = new Date().toISOString();

      try {
        const token = await getDriveToken(env);

        // Find and update Transactions row
        const txData = await (await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Transactions`,
          { headers: { Authorization: `Bearer ${token}` } }
        )).json();
        const txRows = txData.values || [];
        const txHeaderIdx = { id: 0, status: 7, approvedBy: 8, approvedAt: 9 };
        const txRowIdx = txRows.findIndex((r, i) => i > 0 && r[txHeaderIdx.id] === txId);
        if (txRowIdx > 0) {
          const sheetRow = txRowIdx + 1;
          await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Transactions!H${sheetRow}:J${sheetRow}?valueInputOption=USER_ENTERED`,
            {
              method: 'PUT',
              headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ values: [[newStatus, user.name || user._key, now]] })
            }
          );
        }

        // Find and update Approval_Queue row
        const qData = await (await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Approval_Queue`,
          { headers: { Authorization: `Bearer ${token}` } }
        )).json();
        const qRows = qData.values || [];
        const qRowIdx = qRows.findIndex((r, i) => i > 0 && r[1] === txId);
        if (qRowIdx > 0) {
          const sheetRow = qRowIdx + 1;
          await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Approval_Queue!G${sheetRow}:J${sheetRow}?valueInputOption=USER_ENTERED`,
            {
              method: 'PUT',
              headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ values: [[newStatus, user.name || user._key, now, notes || '']] })
            }
          );
        }

        return new Response(JSON.stringify({ ok: true, status: newStatus }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }), { status: 500, headers });
      }
    }

    // ─── update_budget ────────────────────────────────────────
    if (action === 'update_budget') {
      const user = validateUser(body.username, body.password);
      if (!user) return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401, headers });

      if (user.role !== 'admin')
        return new Response(JSON.stringify({ error: 'Admin only' }), { status: 403, headers });

      const { budgetId, allocated, notes } = body;
      if (!budgetId || !allocated)
        return new Response(JSON.stringify({ error: 'Missing budgetId or allocated' }), { status: 400, headers });

      const finSheetId = env.FINANCE_SPREADSHEET_ID;

      try {
        const token = await getDriveToken(env);

        const bData = await (await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Budget`,
          { headers: { Authorization: `Bearer ${token}` } }
        )).json();
        const bRows = bData.values || [];
        const bRowIdx = bRows.findIndex((r, i) => i > 0 && r[0] === budgetId);

        if (bRowIdx < 1)
          return new Response(JSON.stringify({ error: 'Budget ID not found' }), { status: 404, headers });

        const sheetRow = bRowIdx + 1;
        await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${finSheetId}/values/Budget!D${sheetRow}:H${sheetRow}?valueInputOption=USER_ENTERED`,
          {
            method: 'PUT',
            headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ values: [[allocated, bRows[bRowIdx][4] || 0,
              parseFloat(allocated) - parseFloat(bRows[bRowIdx][4] || 0),
              bRows[bRowIdx][6] || '', notes || bRows[bRowIdx][7] || '']] })
          }
        );

        return new Response(JSON.stringify({ ok: true }), { status: 200, headers });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }), { status: 500, headers });
      }
    }

      return new Response(JSON.stringify({ error: 'Unknown action' }), { status: 400, headers });

    } catch(e) {
      // Catch-all — always return CORS headers so browser doesn't show CORS error
      console.error('Worker unhandled error:', e.message);
      return new Response(JSON.stringify({ error: 'Internal error: ' + e.message }), { status: 500, headers });
    }
  }
};