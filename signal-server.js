// Chatify signaling server — relays WebRTC handshakes, availability checks,
// Inner-Circle join requests, and global admin notices. Never sees or stores
// chat text/files (those travel peer-to-peer). All state below is in-memory
// only — it disappears the moment this process restarts. No database, ever.

const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 8080 });

const rooms = new Map();            // roomId -> Map(peerId -> ws)
const owners = new Map();           // roomId -> Set(ws) — devices proven as Admin via ADMIN_KEY
const pendingRequests = new Map();  // roomId -> Map(peerId -> ws) — people waiting for approval
const allSockets = new Set();       // every currently connected socket, for global notices
let globalNotice = null;            // { text, ts } or null — in-memory only, resets on restart

const ADMIN_KEY = process.env.INNER_CIRCLE_ADMIN_KEY || null;

console.log('Signaling server listening');

function broadcastToRoom(room, obj, exceptId) {
  const peersInRoom = rooms.get(room);
  if (!peersInRoom) return;
  for (const [id, peerWs] of peersInRoom) {
    if (id !== exceptId && peerWs.readyState === WebSocket.OPEN) {
      peerWs.send(JSON.stringify(obj));
    }
  }
}

wss.on('connection', (ws) => {
  let currentRoom = null;
  let myId = null;

  allSockets.add(ws);
  if (globalNotice) {
    ws.send(JSON.stringify({ type: 'notice-update', text: globalNotice.text, ts: globalNotice.ts }));
  }

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ---- global admin notice (visible across the whole app, all users) ----
    if (msg.type === 'post-notice') {
      if (ADMIN_KEY && msg.adminKey === ADMIN_KEY) {
        globalNotice = { text: msg.text, ts: Date.now() };
        allSockets.forEach((s) => { if (s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'notice-update', text: globalNotice.text, ts: globalNotice.ts })); });
      }
      return;
    }
    if (msg.type === 'clear-notice') {
      if (ADMIN_KEY && msg.adminKey === ADMIN_KEY) {
        globalNotice = null;
        allSockets.forEach((s) => { if (s.readyState === WebSocket.OPEN) s.send(JSON.stringify({ type: 'notice-update', text: null, ts: Date.now() })); });
      }
      return;
    }

    // ---- availability check (used before creating a room or chat ID) ----
    if (msg.type === 'check-availability') {
      const occupied = rooms.has(msg.code) && rooms.get(msg.code).size > 0;
      ws.send(JSON.stringify({ type: 'availability-result', code: msg.code, available: !occupied }));
      return;
    }

    // ---- normal room join (also used for Inner Circle once approved, and by the Admin) ----
    if (msg.type === 'join') {
      currentRoom = msg.room;
      myId = msg.from;
      if (!rooms.has(currentRoom)) rooms.set(currentRoom, new Map());
      const peersInRoom = rooms.get(currentRoom);
      const isFirst = peersInRoom.size === 0;

      let isOwner = false;
      if (ADMIN_KEY && msg.adminKey === ADMIN_KEY) {
        isOwner = true;
        if (!owners.has(currentRoom)) owners.set(currentRoom, new Set());
        owners.get(currentRoom).add(ws);
      }

      broadcastToRoom(currentRoom, { type: 'peer-joined', from: myId, name: msg.name }, myId);
      peersInRoom.set(myId, ws);

      ws.send(JSON.stringify({ type: 'joined', isFirst, isOwner }));
      return;
    }

    // ---- Inner Circle (or any owner-gated room) join request ----
    if (msg.type === 'join-request') {
      const room = msg.room;
      if (!pendingRequests.has(room)) pendingRequests.set(room, new Map());
      pendingRequests.get(room).set(msg.from, ws);

      const ownerSet = owners.get(room);
      if (ownerSet && ownerSet.size > 0) {
        ownerSet.forEach((ownerWs) => {
          if (ownerWs.readyState === WebSocket.OPEN) {
            ownerWs.send(JSON.stringify({ type: 'join-request-incoming', room, from: msg.from, name: msg.name }));
          }
        });
      } else {
        ws.send(JSON.stringify({ type: 'join-request-failed', room, reason: 'The Admin is currently offline. Please try again later.' }));
        pendingRequests.get(room).delete(msg.from);
      }
      return;
    }

    // ---- Admin approving/denying a pending request ----
    if (msg.type === 'join-decision') {
      const room = msg.room;
      const pending = pendingRequests.get(room);
      const targetWs = pending && pending.get(msg.to);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(JSON.stringify({ type: msg.decision === 'approve' ? 'join-approved' : 'join-denied', room }));
      }
      if (pending) pending.delete(msg.to);
      return;
    }

    // ---- targeted relay: offer / answer / ice all carry a "to" field ----
    if (msg.to && currentRoom && rooms.has(currentRoom)) {
      const target = rooms.get(currentRoom).get(msg.to);
      if (target && target.readyState === WebSocket.OPEN) {
        target.send(raw.toString());
      }
    }
  });

  ws.on('close', () => {
    allSockets.delete(ws);
    if (currentRoom && rooms.has(currentRoom)) {
      const peersInRoom = rooms.get(currentRoom);
      peersInRoom.delete(myId);
      broadcastToRoom(currentRoom, { type: 'peer-left', from: myId });
      if (peersInRoom.size === 0) rooms.delete(currentRoom);
    }
    owners.forEach((set) => set.delete(ws));
    pendingRequests.forEach((map) => {
      for (const [id, pendingWs] of map) if (pendingWs === ws) map.delete(id);
    });
  });
});
