const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'public');
const rooms = new Map();
const RECONNECT_GRACE_MS = 60000;
const ADMIN_KEY = String(process.env.ADMIN_KEY || '');
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const RATINGS_FILE = path.join(__dirname, 'ratings.json');
const ratingRateLimit = new Map();

let dbPool = null;
let ratings = [];
let storageBackend = DATABASE_URL ? 'postgresql' : 'file';

if (DATABASE_URL) {
  const { Pool } = require('pg');
  const dbConfig = {
    connectionString: DATABASE_URL,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  };
  if (/sslmode=(require|verify-ca|verify-full)/i.test(DATABASE_URL) || process.env.PGSSL === 'require') {
    dbConfig.ssl = { rejectUnauthorized: false };
  }
  dbPool = new Pool(dbConfig);
  dbPool.on('error', err => console.error('PostgreSQL pool error:', err.message));
}

function loadRatingsFile() {
  try {
    const raw = JSON.parse(fs.readFileSync(RATINGS_FILE, 'utf8'));
    return Array.isArray(raw) ? raw : [];
  } catch (_) { return []; }
}
function saveRatingsFile(items) {
  try { fs.writeFileSync(RATINGS_FILE, JSON.stringify(items, null, 2), 'utf8'); } catch (_) {}
}
ratings = loadRatingsFile();

async function initRatingsStorage() {
  if (!dbPool) {
    console.warn('Ratings storage: local file fallback (set DATABASE_URL for permanent PostgreSQL storage).');
    return;
  }
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS game_ratings (
      id TEXT PRIMARY KEY,
      rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
      comment VARCHAR(300) NOT NULL DEFAULT '',
      player_name VARCHAR(20) NOT NULL DEFAULT 'Игрок',
      mode VARCHAR(10) NOT NULL DEFAULT 'unknown',
      version VARCHAR(20) NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await dbPool.query('CREATE INDEX IF NOT EXISTS game_ratings_created_at_idx ON game_ratings (created_at DESC)');

  // One-time best-effort migration of any ratings.json bundled from an earlier version.
  if (ratings.length) {
    const countResult = await dbPool.query('SELECT COUNT(*)::int AS count FROM game_ratings');
    if ((countResult.rows[0]?.count || 0) === 0) {
      for (const item of ratings) {
        const n = Number(item.rating);
        if (!Number.isInteger(n) || n < 1 || n > 5) continue;
        await dbPool.query(
          `INSERT INTO game_ratings (id,rating,comment,player_name,mode,version,created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`,
          [String(item.id || crypto.randomBytes(8).toString('hex')), n,
           String(item.comment || '').slice(0,300), cleanName(item.playerName || 'Игрок'),
           ['online','bot','pvp'].includes(item.mode) ? item.mode : 'unknown',
           String(item.version || '').slice(0,20), item.createdAt || new Date().toISOString()]
        );
      }
      console.log(`Ratings migration: imported up to ${ratings.length} local ratings into PostgreSQL.`);
    }
  }
  console.log('Ratings storage: PostgreSQL (persistent).');
}

async function insertRating(item) {
  if (dbPool) {
    await dbPool.query(
      `INSERT INTO game_ratings (id,rating,comment,player_name,mode,version,created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [item.id,item.rating,item.comment,item.playerName,item.mode,item.version,item.createdAt]
    );
    return;
  }
  ratings.push(item);
  if (ratings.length > 5000) ratings = ratings.slice(-5000);
  saveRatingsFile(ratings);
}

async function getRatingsAdminData() {
  if (dbPool) {
    const summary = await dbPool.query(`
      SELECT COUNT(*)::int AS count, COALESCE(AVG(rating),0)::float AS average,
             COUNT(*) FILTER (WHERE rating=1)::int AS r1,
             COUNT(*) FILTER (WHERE rating=2)::int AS r2,
             COUNT(*) FILTER (WHERE rating=3)::int AS r3,
             COUNT(*) FILTER (WHERE rating=4)::int AS r4,
             COUNT(*) FILTER (WHERE rating=5)::int AS r5
      FROM game_ratings
    `);
    const latest = await dbPool.query(`
      SELECT id,rating,comment,player_name AS "playerName",mode,version,
             created_at AS "createdAt"
      FROM game_ratings ORDER BY created_at DESC LIMIT 200
    `);
    const r = summary.rows[0] || {};
    return {
      count: Number(r.count) || 0,
      average: Number(r.average) || 0,
      distribution: {1:Number(r.r1)||0,2:Number(r.r2)||0,3:Number(r.r3)||0,4:Number(r.r4)||0,5:Number(r.r5)||0},
      items: latest.rows,
      storage: 'postgresql'
    };
  }
  const distribution = {1:0,2:0,3:0,4:0,5:0};
  let sum = 0;
  for (const item of ratings) { distribution[item.rating] = (distribution[item.rating] || 0) + 1; sum += Number(item.rating) || 0; }
  const count = ratings.length;
  return {count, average:count ? sum/count : 0, distribution, items:[...ratings].reverse().slice(0,200), storage:'file'};
}

function readJsonBody(req, limit = 8192) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => { body += chunk; if (body.length > limit) { reject(new Error('too_large')); req.destroy(); } });
    req.on('end', () => { try { resolve(body ? JSON.parse(body) : {}); } catch (_) { reject(new Error('bad_json')); } });
    req.on('error', reject);
  });
}
function json(res, status, data) {
  res.writeHead(status, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store, no-cache, must-revalidate','Pragma':'no-cache'});
  res.end(JSON.stringify(data));
}
function adminAuthorized(req) {
  if (!ADMIN_KEY) return false;
  const provided = String(req.headers['x-admin-key'] || '');
  if (!provided || provided.length !== ADMIN_KEY.length) return false;
  try { return crypto.timingSafeEqual(Buffer.from(provided), Buffer.from(ADMIN_KEY)); } catch (_) { return false; }
}

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  do { code = Array.from({length: 5}, () => chars[Math.floor(Math.random()*chars.length)]).join(''); }
  while (rooms.has(code));
  return code;
}
function token() { return crypto.randomBytes(18).toString('hex'); }
function cleanName(value) { return String(value || 'Игрок').trim().replace(/\s+/g, ' ').slice(0, 20) || 'Игрок'; }
function send(ws, data) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
function playerByWs(room, ws) { return room?.players.find(p => p.ws === ws) || null; }
function otherPlayer(room, playerOrWs) {
  const me = playerOrWs && playerOrWs.token ? playerOrWs : playerByWs(room, playerOrWs);
  return room?.players.find(p => p !== me) || null;
}
function connectedCount(room) { return room.players.filter(p => p.ws && p.ws.readyState === WebSocket.OPEN).length; }
function bindSocket(ws, room, player) {
  if (player.disconnectTimer) { clearTimeout(player.disconnectTimer); player.disconnectTimer = null; }
  player.ws = ws;
  ws.room = room.code;
  ws.playerToken = player.token;
  ws.playerName = player.name;
}
function broadcastRoomState(room) {
  for (const p of room.players) {
    const opp = otherPlayer(room, p);
    send(p.ws, {type:'room_state', room:room.code, color:p.color, youName:p.name, opponentName:opp?.name || null, players:room.players.length, connected:connectedCount(room)});
  }
}
function removePlayer(room, player, notify=true) {
  if (player.disconnectTimer) { clearTimeout(player.disconnectTimer); player.disconnectTimer = null; }
  room.players = room.players.filter(p => p !== player);
  if (room.players.length === 0) { rooms.delete(room.code); return; }
  if (notify) {
    const peer = room.players[0];
    send(peer.ws, {type:'opponent_left', opponentName:player.name || 'Соперник'});
    broadcastRoomState(room);
  }
}
function cleanup(ws, immediate=false) {
  const code = ws.room;
  if (!code) return;
  const room = rooms.get(code);
  ws.room = null;
  if (!room) return;
  const player = playerByWs(room, ws);
  if (!player) return;
  if (player.ws === ws) player.ws = null;
  if (immediate) { removePlayer(room, player, true); return; }
  const peer = otherPlayer(room, player);
  send(peer?.ws, {type:'opponent_reconnecting', opponentName:player.name});
  if (player.disconnectTimer) clearTimeout(player.disconnectTimer);
  player.disconnectTimer = setTimeout(() => {
    if (player.ws) return;
    const currentRoom = rooms.get(code);
    if (!currentRoom || !currentRoom.players.includes(player)) return;
    removePlayer(currentRoom, player, true);
  }, RECONNECT_GRACE_MS);
  player.disconnectTimer.unref?.();
}

const server = http.createServer(async (req, res) => {
  let pathname = decodeURIComponent(req.url.split('?')[0]);

  if (pathname === '/api/ratings' && req.method === 'POST') {
    try {
      const now = Date.now();
      const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      const last = ratingRateLimit.get(ip) || 0;
      if (now - last < 15000) return json(res, 429, {error:'Подождите немного перед следующей оценкой.'});
      const body = await readJsonBody(req);
      const rating = Number(body.rating);
      if (!Number.isInteger(rating) || rating < 1 || rating > 5) return json(res, 400, {error:'Оценка должна быть от 1 до 5.'});
      const item = {
        id: crypto.randomBytes(8).toString('hex'),
        rating,
        comment: String(body.comment || '').trim().slice(0, 300),
        playerName: cleanName(body.playerName || 'Игрок'),
        mode: ['online','bot','pvp'].includes(body.mode) ? body.mode : 'unknown',
        version: String(body.version || '').slice(0, 20),
        createdAt: new Date().toISOString()
      };
      await insertRating(item);
      ratingRateLimit.set(ip, now);
      return json(res, 201, {ok:true});
    } catch (e) { return json(res, 400, {error:'Не удалось принять оценку.'}); }
  }

  if (pathname === '/api/admin/ratings' && req.method === 'GET') {
    if (!ADMIN_KEY) return json(res, 503, {error:'Ключ владельца ещё не настроен на сервере.'});
    if (!adminAuthorized(req)) return json(res, 401, {error:'Неверный ключ владельца.'});
    try {
      const data = await getRatingsAdminData();
      return json(res, 200, data);
    } catch (e) {
      console.error('Ratings read error:', e.message);
      return json(res, 500, {error:'Не удалось загрузить оценки.'});
    }
  }

  if (pathname === '/api/online-count') {
    const count = [...wss.clients].filter(ws => ws.readyState === WebSocket.OPEN).length;
    res.writeHead(200, {
      'Content-Type':'application/json; charset=utf-8',
      'Cache-Control':'no-store, no-cache, must-revalidate',
      'Pragma':'no-cache'
    });
    return res.end(JSON.stringify({online:count}));
  }

  if (pathname === '/') pathname = '/index.html';
  const file = path.normalize(path.join(ROOT, pathname));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    const types = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.svg':'image/svg+xml','.ico':'image/x-icon'};
    res.writeHead(200, {'Content-Type':types[ext] || 'application/octet-stream','Cache-Control':'no-store, no-cache, must-revalidate','Pragma':'no-cache'});
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });
const heartbeatInterval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { try { ws.terminate(); } catch (_) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 25000);
heartbeatInterval.unref?.();
wss.on('close', () => clearInterval(heartbeatInterval));

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    ws.isAlive = true;
    if (msg.type === 'ping') { send(ws,{type:'pong',ts:Date.now()}); return; }

    if (msg.type === 'create') {
      cleanup(ws, true);
      const code = roomCode();
      const name = cleanName(msg.name);
      const player = {ws:null,color:'white',name,token:token(),disconnectTimer:null};
      const room = {code,players:[player],rematchRequester:null,ply:0,turnColor:'white',started:false,gameState:null};
      rooms.set(code, room); bindSocket(ws, room, player);
      send(ws,{type:'created',room:code,color:'white',youName:name,token:player.token});
      broadcastRoomState(room); return;
    }

    if (msg.type === 'join') {
      cleanup(ws, true);
      const code = String(msg.room || '').toUpperCase().replace(/[^A-Z0-9]/g,'');
      const room = rooms.get(code);
      if (!room) return send(ws,{type:'error',message:'Комната не найдена'});
      if (room.players.length >= 2) return send(ws,{type:'error',message:'Комната уже заполнена'});
      const name = cleanName(msg.name);
      const player = {ws:null,color:'black',name,token:token(),disconnectTimer:null};
      room.players.push(player); bindSocket(ws, room, player); room.started = true;
      send(ws,{type:'joined',room:code,color:'black',youName:name,token:player.token});
      broadcastRoomState(room);
      for (const p of room.players) {
        const opp = otherPlayer(room,p);
        send(p.ws,{type:'start',room:code,color:p.color,youName:p.name,opponentName:opp?.name || 'Соперник',token:p.token});
      }
      return;
    }

    if (msg.type === 'resume') {
      const code = String(msg.room || '').toUpperCase().replace(/[^A-Z0-9]/g,'');
      const room = rooms.get(code);
      const resumeToken = String(msg.token || '');
      const player = room?.players.find(p => p.token === resumeToken);
      if (!room || !player) return send(ws,{type:'resume_failed'});
      if (player.ws && player.ws !== ws) { try { player.ws.close(4000,'replaced'); } catch (_) {} }
      bindSocket(ws,room,player);
      const opp = otherPlayer(room,player);
      send(ws,{type:'resumed',room:room.code,color:player.color,youName:player.name,opponentName:opp?.name || 'Соперник',token:player.token,started:room.started,ply:room.ply,state:room.gameState});
      send(opp?.ws,{type:'opponent_reconnected',opponentName:player.name});
      broadcastRoomState(room); return;
    }

    if (msg.type === 'rematch_request') {
      const room=rooms.get(ws.room), player=playerByWs(room,ws); if(!room||!player||room.players.length!==2)return;
      room.rematchRequester=player; const opponent=otherPlayer(room,player);
      send(ws,{type:'rematch_waiting'}); send(opponent?.ws,{type:'rematch_offer',fromName:player.name}); return;
    }
    if (msg.type === 'rematch_accept') {
      const room=rooms.get(ws.room), player=playerByWs(room,ws); if(!room||!player||room.players.length!==2)return;
      const requester=room.rematchRequester; if(!requester||requester===player)return;
      room.rematchRequester=null; room.ply=0; room.turnColor='white'; room.gameState=null;
      for(const p of room.players)p.color=p.color==='white'?'black':'white';
      for(const p of room.players){const opp=otherPlayer(room,p);send(p.ws,{type:'rematch_start',room:room.code,color:p.color,youName:p.name,opponentName:opp?.name||'Соперник'});} return;
    }
    if (msg.type === 'rematch_decline' || msg.type === 'rematch_cancel') {
      const room=rooms.get(ws.room), player=playerByWs(room,ws); if(!room||!player||room.players.length!==2)return;
      const opponent=otherPlayer(room,player); room.rematchRequester=null;
      send(opponent?.ws,{type:msg.type==='rematch_decline'?'rematch_declined':'rematch_cancelled',fromName:player.name}); return;
    }
    if (msg.type === 'move') {
      const room=rooms.get(ws.room), player=playerByWs(room,ws); if(!room||!player||room.players.length!==2)return;
      const opponent=otherPlayer(room,player); if(player.color!==room.turnColor)return;
      const nextPly=Number(msg.ply); if(!Number.isInteger(nextPly)||nextPly!==room.ply+1)return send(ws,{type:'sync_request',expectedPly:room.ply+1});
      if(!msg.state||!Array.isArray(msg.state.board)||msg.state.board.length!==8)return;
      room.ply=nextPly; room.gameState=msg.state;
      if(msg.state.currentPlayer==='white'||msg.state.currentPlayer==='black')room.turnColor=msg.state.currentPlayer;
      send(opponent?.ws,{...msg,fromName:player.name}); return;
    }
    if (msg.type === 'restart') {
      const room=rooms.get(ws.room), player=playerByWs(room,ws); if(!room||!player||room.players.length!==2)return;
      room.ply=0;room.turnColor='white';room.gameState=null;const opponent=otherPlayer(room,player);send(opponent?.ws,{...msg,ply:0,fromName:player.name});return;
    }
    if (msg.type === 'resign') {
      const room=rooms.get(ws.room), player=playerByWs(room,ws); if(!room||!player||room.players.length!==2)return;
      const opponent=otherPlayer(room,player);send(opponent?.ws,{...msg,fromName:player.name});return;
    }
    if (msg.type === 'leave') { cleanup(ws,true); return; }
  });
  ws.on('close',()=>cleanup(ws,false));
  ws.on('error',()=>cleanup(ws,false));
});

initRatingsStorage()
  .then(() => server.listen(PORT,()=>console.log(`Шашки v5.2.3 Online PRO: http://localhost:${PORT}`)))
  .catch(err => { console.error('Ratings storage initialization failed:', err.message); process.exit(1); });
