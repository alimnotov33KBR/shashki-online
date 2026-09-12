const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'public');
const rooms = new Map();
const RECONNECT_GRACE_MS = 60000;

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

const server = http.createServer((req, res) => {
  let pathname = decodeURIComponent(req.url.split('?')[0]);
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

server.listen(PORT,()=>console.log(`Шашки v5.1.8 Online PRO: http://localhost:${PORT}`));
