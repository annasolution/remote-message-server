const express = require("express");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 10000;

const wss = new WebSocket.Server({
    server,
    path: "/ws"
});

const rooms = new Map();

app.get("/", (req, res) => {
    res.send("Remote message server is running.");
});

function send(ws, object) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(object));
    }
}

function getRoom(code) {
    if (!rooms.has(code)) {
        rooms.set(code, new Set());
    }

    return rooms.get(code);
}

wss.on("connection", (ws) => {

    let roomCode = null;

    ws.on("message", (raw) => {

        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch {
            send(ws, {
                type: "error",
                message: "Invalid message."
            });
            return;
        }

        if (data.type === "join") {

            if (
                typeof data.code !== "string" ||
                !/^[A-Z0-9]{6,20}$/.test(data.code)
            ) {
                send(ws, {
                    type: "error",
                    message: "Invalid connection code."
                });
                return;
            }

            roomCode = data.code;

            const room = getRoom(roomCode);
            room.add(ws);

            send(ws, {
                type: "joined",
                message: "Connected."
            });

            return;
        }

        if (data.type === "message") {

            if (!roomCode) {
                send(ws, {
                    type: "error",
                    message: "Join a connection first."
                });
                return;
            }

            if (
                typeof data.text !== "string" ||
                data.text.length === 0 ||
                data.text.length > 300
            ) {
                send(ws, {
                    type: "error",
                    message: "Message must be 1-300 characters."
                });
                return;
            }

            const room = rooms.get(roomCode);

            if (!room)
                return;

            for (const client of room) {

                if (client !== ws) {
                    send(client, {
                        type: "message",
                        text: data.text,
                        time: new Date().toISOString()
                    });
                }
            }

            return;
        }

        if (data.type === "clear") {

            if (!roomCode)
                return;

            const room = rooms.get(roomCode);

            if (!room)
                return;

            for (const client of room) {

                if (client !== ws) {
                    send(client, {
                        type: "clear"
                    });
                }
            }
        }
    });

    ws.on("close", () => {

        if (!roomCode)
            return;

        const room = rooms.get(roomCode);

        if (!room)
            return;

        room.delete(ws);

        if (room.size === 0) {
            rooms.delete(roomCode);
        }
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on port ${PORT}`);
});
