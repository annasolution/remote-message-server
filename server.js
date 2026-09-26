const express = require("express");
const http = require("http");
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

function send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function getRoom(code) {
    if (!rooms.has(code)) {
        rooms.set(code, new Set());
    }

    return rooms.get(code);
}

wss.on("connection", (ws) => {

    console.log("Client connected.");

    let roomCode = null;

    ws.on("message", (raw) => {

        console.log("Received:", raw.toString());

        let data;

        try {
            data = JSON.parse(raw.toString());
        }
        catch (error) {
            send(ws, {
                type: "error",
                message: "Invalid JSON."
            });
            return;
        }

        // JOIN
        if (data.type === "join") {

            const code =
                String(data.code || "")
                    .trim()
                    .toUpperCase();

            if (!/^[A-Z0-9]{6,20}$/.test(code)) {

                send(ws, {
                    type: "error",
                    message: "Invalid connection code."
                });

                return;
            }

            roomCode = code;

            const room = getRoom(roomCode);

            room.add(ws);

            console.log(
                `Client joined room ${roomCode}. Clients: ${room.size}`
            );

            send(ws, {
                type: "joined",
                message: "Connected.",
                clients: room.size
            });

            // Tell everyone in the room how many clients are connected.
            for (const client of room) {
                send(client, {
                    type: "room_status",
                    clients: room.size
                });
            }

            return;
        }

        // MESSAGE
        if (data.type === "message") {

            if (!roomCode) {

                send(ws, {
                    type: "error",
                    message: "You are not connected to a room."
                });

                return;
            }

            const text =
                String(data.text || "").trim();

            if (!text) {
                return;
            }

            if (text.length > 300) {

                send(ws, {
                    type: "error",
                    message: "Message is too long."
                });

                return;
            }

            const room = rooms.get(roomCode);

            if (!room) {
                return;
            }

            console.log(
                `Sending message to room ${roomCode}. Clients: ${room.size}`
            );

            let delivered = 0;

            for (const client of room) {

                if (client !== ws &&
                    client.readyState === WebSocket.OPEN) {

                    send(client, {
                        type: "message",
                        text: text,
                        time: new Date().toISOString()
                    });

                    delivered++;
                }
            }

            // Tell sender whether another client received it.
            send(ws, {
                type: "message_sent",
                delivered: delivered
            });

            return;
        }

        // CLEAR
        if (data.type === "clear") {

            if (!roomCode) {
                return;
            }

            const room = rooms.get(roomCode);

            if (!room) {
                return;
            }

            for (const client of room) {

                if (client !== ws &&
                    client.readyState === WebSocket.OPEN) {

                    send(client, {
                        type: "clear"
                    });
                }
            }

            send(ws, {
                type: "clear_sent"
            });

            return;
        }
    });

    ws.on("close", () => {

        console.log("Client disconnected.");

        if (!roomCode) {
            return;
        }

        const room = rooms.get(roomCode);

        if (!room) {
            return;
        }

        room.delete(ws);

        console.log(
            `Client left room ${roomCode}. Clients: ${room.size}`
        );

        for (const client of room) {
            send(client, {
                type: "room_status",
                clients: room.size
            });
        }

        if (room.size === 0) {
            rooms.delete(roomCode);
        }
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on port ${PORT}`);
});
