const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'public');
const rooms = new Map();

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  do {
    code = Array.from({length: 5}, () => chars[Math.floor(Math.random()*chars.length)]).join('');
  } while (rooms.has(code));
  return code;
}
function cleanName(value) {
  return String(value || 'Игрок').trim().replace(/\s+/g, ' ').slice(0, 20) || 'Игрок';
}
function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function otherPlayer(room, ws) {
  return room.players.find(p => p.ws !== ws) || null;
}
function broadcastRoomState(room) {
  for (const p of room.players) {
    const opp = room.players.find(x => x.ws !== p.ws);
    send(p.ws, {
      type: 'room_state',
      room: room.code,
      color: p.color,
      youName: p.name,
      opponentName: opp?.name || null,
      players: room.players.length
    });
  }
}
function cleanup(ws) {
  const code = ws.room;
  if (!code) return;
  const room = rooms.get(code);
  ws.room = null;
  if (!room) return;
  room.players = room.players.filter(p => p.ws !== ws);
  if (room.players.length === 0) {
    rooms.delete(code);
    return;
  }
  const peer = room.players[0];
  send(peer.ws, {type:'opponent_left', opponentName: ws.playerName || 'Соперник'});
  broadcastRoomState(room);
}

const server = http.createServer((req, res) => {
  let pathname = decodeURIComponent(req.url.split('?')[0]);
  if (pathname === '/') pathname = '/index.html';
  const file = path.normalize(path.join(ROOT, pathname));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    const types = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.svg':'image/svg+xml','.ico':'image/x-icon'};
    res.writeHead(200, {'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control':'no-store'});
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });

// Keep WebSocket connections alive through proxies/mobile networks.
// Browsers automatically answer protocol-level ping frames with pong.
const heartbeatInterval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (_) {}
      continue;
    }
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

    // Application-level heartbeat also keeps reverse proxies from treating
    // a waiting room as an idle connection.
    if (msg.type === 'ping') {
      ws.isAlive = true;
      send(ws, {type:'pong', ts: Date.now()});
      return;
    }

    if (msg.type === 'create') {
      cleanup(ws);
      const code = roomCode();
      const name = cleanName(msg.name);
      ws.playerName = name;
      ws.room = code;
      const room = {code, players:[{ws,color:'white',name}], rematchRequester:null, ply:0, turnColor:'white'};
      rooms.set(code, room);
      send(ws, {type:'created',room:code,color:'white',youName:name});
      broadcastRoomState(room);
      return;
    }

    if (msg.type === 'join') {
      cleanup(ws);
      const code = String(msg.room || '').toUpperCase().replace(/[^A-Z0-9]/g,'');
      const room = rooms.get(code);
      if (!room) return send(ws,{type:'error',message:'Комната не найдена'});
      if (room.players.length >= 2) return send(ws,{type:'error',message:'Комната уже заполнена'});
      const name = cleanName(msg.name);
      ws.playerName = name;
      ws.room = code;
      room.players.push({ws,color:'black',name});
      send(ws,{type:'joined',room:code,color:'black',youName:name});
      broadcastRoomState(room);
      for (const p of room.players) {
        const opp = room.players.find(x => x.ws !== p.ws);
        send(p.ws,{type:'start',room:code,color:p.color,youName:p.name,opponentName:opp?.name || 'Соперник'});
      }
      return;
    }

    if (msg.type === 'rematch_request') {
      const room = rooms.get(ws.room);
      if (!room || room.players.length !== 2) return;
      room.rematchRequester = ws;
      const opponent = otherPlayer(room, ws);
      send(ws, {type:'rematch_waiting'});
      send(opponent?.ws, {type:'rematch_offer', fromName: ws.playerName || 'Соперник'});
      return;
    }

    if (msg.type === 'rematch_accept') {
      const room = rooms.get(ws.room);
      if (!room || room.players.length !== 2) return;
      const requester = room.rematchRequester;
      if (!requester || requester === ws) return;
      room.rematchRequester = null;
      room.ply = 0;
      room.turnColor = 'white';
      for (const p of room.players) p.color = p.color === 'white' ? 'black' : 'white';
      for (const p of room.players) {
        const opp = room.players.find(x => x.ws !== p.ws);
        send(p.ws, {type:'rematch_start', room:room.code, color:p.color, youName:p.name, opponentName:opp?.name || 'Соперник'});
      }
      return;
    }

    if (msg.type === 'rematch_decline' || msg.type === 'rematch_cancel') {
      const room = rooms.get(ws.room);
      if (!room || room.players.length !== 2) return;
      const opponent = otherPlayer(room, ws);
      room.rematchRequester = null;
      send(opponent?.ws, {type: msg.type === 'rematch_decline' ? 'rematch_declined' : 'rematch_cancelled', fromName: ws.playerName || 'Соперник'});
      return;
    }

    if (msg.type === 'move') {
      const room = rooms.get(ws.room);
      if (!room || room.players.length !== 2) return;
      const player = room.players.find(p => p.ws === ws);
      const opponent = otherPlayer(room, ws);
      if (!player || player.color !== room.turnColor) return;
      const nextPly = Number(msg.ply);
      if (!Number.isInteger(nextPly) || nextPly !== room.ply + 1) {
        return send(ws, {type:'sync_request', expectedPly:room.ply + 1});
      }
      if (!msg.state || !Array.isArray(msg.state.board) || msg.state.board.length !== 8) return;
      room.ply = nextPly;
      if (msg.state.currentPlayer === 'white' || msg.state.currentPlayer === 'black') {
        room.turnColor = msg.state.currentPlayer;
      }
      send(opponent?.ws, {...msg, fromName: ws.playerName || 'Соперник'});
      return;
    }

    if (msg.type === 'restart') {
      const room = rooms.get(ws.room);
      if (!room || room.players.length !== 2) return;
      room.ply = 0;
      room.turnColor = 'white';
      const opponent = otherPlayer(room, ws);
      send(opponent?.ws, {...msg, ply:0, fromName: ws.playerName || 'Соперник'});
      return;
    }

    if (msg.type === 'resign') {
      const room = rooms.get(ws.room);
      if (!room || room.players.length !== 2) return;
      const opponent = otherPlayer(room, ws);
      send(opponent?.ws, {...msg, fromName: ws.playerName || 'Соперник'});
      return;
    }

    if (msg.type === 'leave') {
      cleanup(ws);
      return;
    }
  });

  ws.on('close', () => cleanup(ws));
  ws.on('error', () => cleanup(ws));
});

server.listen(PORT, () => {
  console.log(`Шашки v5.1.3 Online PRO: http://localhost:${PORT}`);
});
