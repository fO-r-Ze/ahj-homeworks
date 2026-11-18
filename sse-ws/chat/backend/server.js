import { randomUUID } from "node:crypto";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import bodyParser from "body-parser";
import cors from "cors";
import express from "express";
import pino from "pino";
import pinoPretty from "pino-pretty";
import WebSocket, { WebSocketServer } from "ws";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const USERS_FILE = path.join(__dirname, "users.json");
const MESSAGES_FILE = path.join(__dirname, "messages.json");
const MAX_STORED_MESSAGES = 1000;
const MAX_HISTORY_ON_CONNECT = 100;
const HEARTBEAT_INTERVAL = 20000;

const app = express();
const logger = pino(pinoPretty());

app.use(cors());
app.use(
  bodyParser.json({
    type() {
      return true;
    },
  })
);
app.use((req, res, next) => {
  res.setHeader("Content-Type", "application/json");
  next();
});

// ---------- Persistence helpers ----------
function loadJSON(file, fallback) {
  try {
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, "utf8"));
    }
  } catch (e) {
    logger.error(`Failed to load ${file}: ${e.message}`);
  }
  return fallback;
}

function saveJSON(file, data) {
  try {
    fs.writeFileSync(file, JSON.stringify(data, null, 2), "utf8");
  } catch (e) {
    logger.error(`Failed to save ${file}: ${e.message}`);
  }
}

// ---------- State ----------
let registeredUsers = loadJSON(USERS_FILE, []);
let messages = loadJSON(MESSAGES_FILE, []);
const onlineUsers = [];

// ---------- HTTP server + WS ----------
const server = http.createServer(app);
const wsServer = new WebSocketServer({ server });

// ---------- Heartbeat ----------
const heartbeat = setInterval(() => {
  wsServer.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

server.on("close", () => clearInterval(heartbeat));

// ---------- Broadcast helpers ----------
function broadcast(payload) {
  const json = JSON.stringify(payload);
  [...wsServer.clients]
    .filter((c) => c.readyState === WebSocket.OPEN)
    .forEach((c) => c.send(json));
}

function broadcastUsers() {
  broadcast({ type: "users", users: onlineUsers });
}

// ---------- REST ----------
app.post("/new-user", (req, res) => {
  if (!req.body || Object.keys(req.body).length === 0) {
    res.status(400).send(
      JSON.stringify({ status: "error", message: "Empty request body" })
    );
    return;
  }

  const { name } = req.body;
  const isExist = registeredUsers.find((u) => u.name === name);

  if (isExist) {
    res.status(409).send(
      JSON.stringify({
        status: "error",
        message: "This name is already taken!",
      })
    );
    return;
  }

  const newUser = { id: randomUUID(), name };
  registeredUsers.push(newUser);
  saveJSON(USERS_FILE, registeredUsers);
  logger.info(`New user created: ${JSON.stringify(newUser)}`);
  res.send(JSON.stringify({ status: "ok", user: newUser }));
});

// ---------- WebSocket ----------
wsServer.on("connection", (ws, req) => {
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  const url = new URL(req.url, "http://localhost");
  const userId = url.searchParams.get("userId");
  const user = userId ? registeredUsers.find((u) => u.id === userId) : null;

  if (!user) {
    ws.close(4001, "Unauthorized");
    return;
  }

  ws.user = user;

  const alreadyOnline = onlineUsers.some((u) => u.id === user.id);
  if (!alreadyOnline) {
    onlineUsers.push({ id: user.id, name: user.name });
  }

  // История — только этому клиенту
  ws.send(
    JSON.stringify({
      type: "history",
      messages: messages.slice(-MAX_HISTORY_ON_CONNECT),
    })
  );

  // Список онлайн — всем
  broadcastUsers();

  logger.info(`User connected: ${user.name} (${user.id})`);

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      logger.error(`Invalid message: ${e.message}`);
      return;
    }

    // keepalive от клиента — игнорируем
    if (msg.type === "ping") {
      return;
    }

    if (
      msg.type === "send" &&
      typeof msg.message === "string" &&
      msg.message.trim()
    ) {
      const message = {
        type: "send",
        message: msg.message,
        user: { id: user.id, name: user.name },
        timestamp: new Date().toISOString(),
      };

      messages.push(message);
      if (messages.length > MAX_STORED_MESSAGES) {
        messages = messages.slice(-MAX_STORED_MESSAGES);
      }
      saveJSON(MESSAGES_FILE, messages);

      broadcast(message);
      logger.info(`Message from ${user.name}: ${message.message}`);
    }
  });

  ws.on("close", () => {
    // Не удаляем из онлайна, если у этого же пользователя осталось
    // другое открытое соединение (например, вторая вкладка)
    const stillConnected = [...wsServer.clients].some(
      (c) =>
        c !== ws &&
        c.user &&
        c.user.id === user.id &&
        c.readyState === WebSocket.OPEN
    );

    if (!stillConnected) {
      const idx = onlineUsers.findIndex((u) => u.id === user.id);
      if (idx !== -1) {
        onlineUsers.splice(idx, 1);
        broadcastUsers();
      }
    }
    logger.info(`User disconnected: ${user.name} (${user.id})`);
  });
});

const port = process.env.PORT || 3000;

const bootstrap = async () => {
  try {
    server.listen(port, () =>
      logger.info(`Server has been started on http://localhost:${port}`)
    );
  } catch (error) {
    logger.error(`Error: ${error.message}`);
  }
};

bootstrap();
