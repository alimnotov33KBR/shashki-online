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
function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function other(room, ws) {
  return room.players.find(p => p.ws !== ws)?.ws || null;
}
function cleanup(ws) {
  const code = ws.room;
  if (!code) return;
  const room = rooms.get(code);
  if (!room) return;
  room.players = room.players.filter(p => p.ws !== ws);
  const peer = room.players[0]?.ws;
  if (peer) {
    peer.room = code;
    send(peer, {type:'opponent_left'});
  }
  if (room.players.length === 0) rooms.delete(code);
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

wss.on('connection', ws => {
  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'create') {
      cleanup(ws);
      const code = roomCode();
      rooms.set(code, {players:[{ws,color:'white'}]});
      ws.room = code;
      send(ws, {type:'created',room:code,color:'white'});
      return;
    }

    if (msg.type === 'join') {
      cleanup(ws);
      const code = String(msg.room || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws,{type:'error',message:'Комната не найдена'});
      if (room.players.length >= 2) return send(ws,{type:'error',message:'Комната уже заполнена'});
      room.players.push({ws,color:'black'});
      ws.room = code;
      send(ws,{type:'joined',room:code,color:'black'});
      for (const p of room.players) send(p.ws,{type:'start',room:code,color:p.color});
      return;
    }

    if (msg.type === 'move' || msg.type === 'restart') {
      const code = ws.room;
      const room = rooms.get(code);
      if (!room || room.players.length !== 2) return;
      send(other(room,ws), msg);
      return;
    }

    if (msg.type === 'leave') {
      cleanup(ws);
      ws.room = null;
      return;
    }
  });

  ws.on('close', () => cleanup(ws));
  ws.on('error', () => cleanup(ws));
});

server.listen(PORT, () => {
  console.log(`Шашки v5.0 Online: http://localhost:${PORT}`);
});
