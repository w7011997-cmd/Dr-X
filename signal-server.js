const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: 8080 });
const rooms = new Map();

console.log('Signaling server listening on port 8080');

wss.on('connection', (ws) => {
  let currentRoom = null;
  let myId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      currentRoom = msg.room;
      myId = msg.from;
      if (!rooms.has(currentRoom)) rooms.set(currentRoom, new Map());
      const peersInRoom = rooms.get(currentRoom);

      for (const [, peerWs] of peersInRoom) {
        if (peerWs.readyState === WebSocket.OPEN) {
          peerWs.send(JSON.stringify({ type: 'peer-joined', from: myId }));
        }
      }
      peersInRoom.set(myId, ws);
      return;
    }

    if (msg.to && currentRoom && rooms.has(currentRoom)) {
      const target = rooms.get(currentRoom).get(msg.to);
      if (target && target.readyState === WebSocket.OPEN) {
        target.send(raw.toString());
      }
    }
  });

  ws.on('close', () => {
    if (currentRoom && rooms.has(currentRoom)) {
      const peersInRoom = rooms.get(currentRoom);
      peersInRoom.delete(myId);
      for (const [, peerWs] of peersInRoom) {
        if (peerWs.readyState === WebSocket.OPEN) {
          peerWs.send(JSON.stringify({ type: 'peer-left', from: myId }));
        }
      }
      if (peersInRoom.size === 0) rooms.delete(currentRoom);
    }
  });
});
