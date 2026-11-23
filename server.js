/**
 * MONO co-op server
 * -----------------
 * Simple WebSocket lobby + host-authoritative relay.
 *
 * Run:  node server.js
 * Default port: 8765  (set PORT=xxxx to change)
 *
 * Protocol (JSON messages):
 *  Client → Server:
 *    { t:'hello', name, code }           // register identity
 *    { t:'create' }                      // create room (you are host)
 *    { t:'join', room }                  // join room by code
 *    { t:'leave' }
 *    { t:'ready', ready:true|false }
 *    { t:'start', map, mode }            // host starts the run
 *    { t:'pos', x,y,z,yaw,pitch,hp,w }   // player pose (~10–15 Hz)
 *    { t:'shot', x,y,z,dx,dy,dz,w }      // fired a bullet
 *    { t:'wave', wave, event, data }     // host: wave / boss / virus events
 *    { t:'enemy', op, data }             // host: spawn/kill/update enemies
 *    { t:'chat', text }
 *
 *  Server → Client:
 *    { t:'hello', id, code }
 *    { t:'room', room, host, players:[{id,name,ready,code}] }
 *    { t:'peer', id, name } / { t:'left', id }
 *    { t:'start', map, mode, host }
 *    { t:'pos', id, ... }  { t:'shot', id, ... }
 *    { t:'wave', ... }  { t:'enemy', ... }
 *    { t:'err', msg }
 */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT ? +process.env.PORT : 8765;

const rooms = new Map();   // roomCode → { hostId, players:Map<id, player> }
const clients = new Map(); // ws → { id, name, code, room, ready, ws }

function rid() {
  return Math.random().toString(36).slice(2, 10);
}
function roomCode() {
  const cs = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let s = '';
  for (let i = 0; i < 6; i++) s += cs[(Math.random() * cs.length) | 0];
  return s;
}
function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function roomPlayers(room) {
  const list = [];
  for (const p of room.players.values()) {
    list.push({ id: p.id, name: p.name, ready: !!p.ready, code: p.code, host: p.id === room.hostId });
  }
  return list;
}
function broadcast(room, obj, exceptId) {
  for (const p of room.players.values()) {
    if (exceptId && p.id === exceptId) continue;
    send(p.ws, obj);
  }
}
function leaveRoom(c) {
  if (!c.room) return;
  const room = rooms.get(c.room);
  if (!room) { c.room = null; return; }
  room.players.delete(c.id);
  broadcast(room, { t: 'left', id: c.id, name: c.name });
  if (room.players.size === 0) {
    rooms.delete(c.room);
  } else if (room.hostId === c.id) {
    // promote next player to host
    const next = room.players.values().next().value;
    room.hostId = next.id;
    broadcast(room, { t: 'room', room: c.room, host: room.hostId, players: roomPlayers(room) });
  } else {
    broadcast(room, { t: 'room', room: c.room, host: room.hostId, players: roomPlayers(room) });
  }
  c.room = null;
  c.ready = false;
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('MONO co-op server OK\nRooms: ' + rooms.size + '\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  const c = { id: rid(), name: 'Player', code: '', room: null, ready: false, ws };
  clients.set(ws, c);
  send(ws, { t: 'hello', id: c.id });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(String(raw)); } catch { return; }
    if (!msg || typeof msg.t !== 'string') return;

    switch (msg.t) {
      case 'hello': {
        c.name = String(msg.name || 'Player').slice(0, 16);
        c.code = String(msg.code || '').slice(0, 16);
        send(ws, { t: 'hello', id: c.id, code: c.code, name: c.name });
        break;
      }
      case 'create': {
        leaveRoom(c);
        let code = roomCode();
        while (rooms.has(code)) code = roomCode();
        const room = { hostId: c.id, players: new Map(), started: false };
        room.players.set(c.id, c);
        c.room = code;
        c.ready = false;
        rooms.set(code, room);
        send(ws, { t: 'room', room: code, host: c.id, players: roomPlayers(room) });
        break;
      }
      case 'join': {
        const code = String(msg.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
        const room = rooms.get(code);
        if (!room) { send(ws, { t: 'err', msg: 'Room not found' }); break; }
        if (room.players.size >= 4) { send(ws, { t: 'err', msg: 'Room full (max 4)' }); break; }
        if (room.started) { send(ws, { t: 'err', msg: 'Game already started' }); break; }
        leaveRoom(c);
        room.players.set(c.id, c);
        c.room = code;
        c.ready = false;
        broadcast(room, { t: 'peer', id: c.id, name: c.name });
        broadcast(room, { t: 'room', room: code, host: room.hostId, players: roomPlayers(room) });
        break;
      }
      case 'leave': {
        leaveRoom(c);
        send(ws, { t: 'room', room: null, host: null, players: [] });
        break;
      }
      case 'ready': {
        if (!c.room) break;
        c.ready = !!msg.ready;
        const room = rooms.get(c.room);
        if (room) broadcast(room, { t: 'room', room: c.room, host: room.hostId, players: roomPlayers(room) });
        break;
      }
      case 'start': {
        if (!c.room) break;
        const room = rooms.get(c.room);
        if (!room || room.hostId !== c.id) { send(ws, { t: 'err', msg: 'Only host can start' }); break; }
        room.started = true;
        broadcast(room, {
          t: 'start',
          map: msg.map || 1,
          mode: msg.mode || 'normal',
          host: room.hostId
        });
        break;
      }
      case 'pos':
      case 'shot':
      case 'wave':
      case 'enemy':
      case 'chat':
      case 'hit':
      case 'dead':
      case 'bonus': {
        if (!c.room) break;
        const room = rooms.get(c.room);
        if (!room) break;
        // relay to everyone else, tag with sender id
        const out = Object.assign({}, msg, { id: c.id, name: c.name });
        broadcast(room, out, c.id);
        break;
      }
      default:
        break;
    }
  });

  ws.on('close', () => {
    leaveRoom(c);
    clients.delete(ws);
  });
});

server.listen(PORT, () => {
  console.log('MONO co-op server listening on ws://0.0.0.0:' + PORT);
  console.log('Open the game, set server URL, create or join a room.');
});
