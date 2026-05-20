const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const os = require('os');
const network = require('network');

const app = express();
app.use(cors());
app.use(express.json()); // BẮT BUỘC ĐỂ NHẬN DATA TỪ PYTHON

const PORT = process.env.PORT || 3001;

let apiResponseData = {
    "Phien": null,
    "Xuc_xac_1": null,
    "Xuc_xac_2": null,
    "Xuc_xac_3": null,
    "Tong": null,
    "Ket_qua": "",
    "id": "dd",
    "server_time": new Date().toISOString()
};

// Biến lưu trữ dự đoán từ AI XGBoost
let currentAIPrediction = {
    result: "WAIT",
    confidence: 0,
    detail: "Đang cào data lịch sử..."
};

let currentSessionId = null;
const patternHistory = []; // Mảng lưu trữ tối đa 500 phiên gần nhất

// ĐÃ CẬP NHẬT: WEBSOCKET_URL với wsToken mới
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJzb25ndmVkZW0yMCIsImJvdCI6MCwiaXNNZXJjaGFudCI6ZmFsc2UsInZlcmlmaWVkQmFua0FjY291bnQiOmZhbHNlLCJwbGF5RXZlbnRMb2JieSI6ZmFsc2UsImN1c3RvbWVySWQiOjIzOTk1MzIxNSwiYWZmSWQiOiJkZWZhdWx0IiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJzdW4ud2luIiwiZW1haWwiOiIiLCJ0aW1lc3RhbXAiOjE3NzkyOTA5MzY0OTIsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjp0cnVlLCJpcEFkZHJlc3MiOiIyMDAxOmVlMDo0MzE1OmQxODA6YmNmOTpjNDo2Yjk4OmMyZmUiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzEwLnBuZyIsInBsYXRmb3JtSWQiOjIsInVzZXJJZCI6IjAwMzc0MDY4LThiZmItNDk1Ni05YjEyLTI4OTNjMzEwNzE2MCIsImVtYWlsVmVyaWZpZWQiOm51bGwsInJlZ1RpbWUiOjE3NDU1OTI2NTU4MDcsInBob25lIjoiODQzMjk2ODk5NzEiLCJkZXBvc2l0Ijp0cnVlLCJ1c2VybmFtZSI6IlNDX3Nvbmd2ZWRlbTEwIn0.3DrTKiNZ5WQC-4ZLIZaFuazwlLm0A-LJnfak3rqbNcw";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win"
};
const RECONNECT_DELAY = 2500;
const PING_INTERVAL = 15000;

// ĐÃ CẬP NHẬT: info và signature mới nhất
const initialMessages = [
    [
        1,
        "MiniGame",
        "SC_songvedem10",
        "Songvedem10",
        {
            "info": "{\"ipAddress\":\"2001:ee0:4315:d180:bcf9:c4:6b98:c2fe\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJzb25ndmVkZW0yMCIsImJvdCI6MCwiaXNNZXJjaGFudCI6ZmFsc2UsInZlcmlmaWVkQmFua0FjY291bnQiOmZhbHNlLCJwbGF5RXZlbnRMb2JieSI6ZmFsc2UsImN1c3RvbWVySWQiOjIzOTk1MzIxNSwiYWZmSWQiOiJkZWZhdWx0IiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJzdW4ud2luIiwiZW1haWwiOiIiLCJ0aW1lc3RhbXAiOjE3NzkyOTA5MzY0OTIsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjp0cnVlLCJpcEFkZHJlc3MiOiIyMDAxOmVlMDo0MzE1OmQxODA6YmNmOTpjNDo2Yjk4OmMyZmUiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzEwLnBuZyIsInBsYXRmb3JtSWQiOjIsInVzZXJJZCI6IjAwMzc0MDY4LThiZmItNDk1Ni05YjEyLTI4OTNjMzEwNzE2MCIsImVtYWlsVmVyaWZpZWQiOm51bGwsInJlZ1RpbWUiOjE3NDU1OTI2NTU4MDcsInBob25lIjoiODQzMjk2ODk5NzEiLCJkZXBvc2l0Ijp0cnVlLCJ1c2VybmFtZSI6IlNDX3Nvbmd2ZWRlbTEwIn0.3DrTKiNZ5WQC-4ZLIZaFuazwlLm0A-LJnfak3rqbNcw\",\"locale\":\"vi\",\"userId\":\"00374068-8bfb-4956-9b12-2893c3107160\",\"username\":\"SC_songvedem10\",\"timestamp\":1779290936506,\"refreshToken\":\"0b0056ad2ec74484a9562b98116ddde8.31411eb269f741cd9d2fcc3d4978e12a\"}",
            "signature": "2D97277D94FC33506F347B491433C077BFCF0BB54D0C01F4A47209148B0420EF1162E29DE4CEF4D4C550D6E283F0CA2FB037F93ECA007573FA7DF21A16E6295F3134F7BF427F99B8A6E6A6AAEC11FF83C7F9AC8B205A8892D9BDC655E1B70B0256DAA6ED7B329008DBE0E8B585C3A15143D4B6999F02D3FC1795739264F9AD5B"
        }
    ],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;

const getNetworkInfo = () => {
    const interfaces = os.networkInterfaces();
    let localIP = '127.0.0.1';
    let publicIP = null;
    for (const ifaceName in interfaces) {
        for (const iface of interfaces[ifaceName]) {
            if (!iface.internal && iface.family === 'IPv4') {
                localIP = iface.address;
                break;
            }
        }
    }
    return { localIP, publicIP };
};

function connectWebSocket() {
    if (ws) {
        ws.removeAllListeners();
        ws.close();
    }
    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS });

    ws.on('open', () => {
        console.log('[✅] WebSocket connected to Sun.Win');
        initialMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
            }, i * 600);
        });
        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.ping();
        }, PING_INTERVAL);
    });

    ws.on('pong', () => console.log('[📶] Ping OK - Connection stable'));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (!Array.isArray(data) || typeof data[1] !== 'object') return;
            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1008 && sid) {
                currentSessionId = sid;
                console.log(`[🎮] Phiên mới: ${sid}`);
            }

            if (cmd === 1003 && gBB) {
                if (!d1 || !d2 || !d3) return;
                const total = d1 + d2 + d3;
                const result = (total > 10) ? "Tài" : "Xỉu";

                // Cập nhật thông tin phiên hiện tại
                apiResponseData = {
                    "Phien": currentSessionId,
                    "Xuc_xac_1": d1,
                    "Xuc_xac_2": d2,
                    "Xuc_xac_3": d3,
                    "Tong": total,
                    "Ket_qua": result,
                    "id": "dark",
                    "server_time": new Date().toISOString(),
                    "update_count": (apiResponseData.update_count || 0) + 1
                };
                
                console.log(`[🎲] Phiên ${apiResponseData.Phien}: ${d1}-${d2}-${d3} = ${total} (${result})`);
                
                // ĐÃ CẬP NHẬT: Dùng unshift để thêm phiên mới lên đầu mảng (Index 0 = Mới nhất)
                patternHistory.unshift({
                    "Phien": currentSessionId,
                    "Xuc_xac_1": d1,
                    "Xuc_xac_2": d2,
                    "Xuc_xac_3": d3,
                    "Tong": total,
                    "Ket_qua": result,
                    "timestamp": new Date().toISOString()
                });
                
                // Giới hạn max 500 phiên (Xóa phần tử cuối cùng nếu mảng vượt 500)
                if (patternHistory.length > 500) patternHistory.pop();
                
                currentSessionId = null;
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý message:', e.message);
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`[🔌] WebSocket closed. Code: ${code}, Reason: ${reason.toString()}`);
        clearInterval(pingInterval);
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        console.error('[❌] WebSocket error:', err.message);
        ws.close();
    });
}

// ===================================
// CÁC ROUTES API
// ===================================

// Nhận dự đoán từ Python AI
app.post('/api/update-prediction', (req, res) => {
    currentAIPrediction = req.body;
    res.json({ success: true });
});

// Trả về Data + Lịch sử 500 phiên (Từ mới đến cũ) + Dự đoán AI cho Giao diện HTML / Ứng dụng khác
app.get('/api/ddvipro', (req, res) => {
    res.json({
        ...apiResponseData,
        history: patternHistory,  // <-- Đã thêm mảng 500 phiên
        ai_prediction: currentAIPrediction
    });
});

app.get('/api/history', (req, res) => {
    res.json({
        current: apiResponseData,
        history: patternHistory, // Lấy toàn bộ mảng lịch sử (tối đa 500)
        total_requests: apiResponseData.update_count || 0
    });
});

app.get('/api/stats', (req, res) => {
    const taiCount = patternHistory.filter(item => item.Ket_qua === "Tài").length;
    const xiuCount = patternHistory.filter(item => item.Ket_qua === "Xỉu").length;
    res.json({
        total_sessions: patternHistory.length,
        tai_count: taiCount,
        xiu_count: xiuCount,
        tai_percentage: patternHistory.length > 0 ? ((taiCount / patternHistory.length) * 100).toFixed(2) : 0,
        xiu_percentage: patternHistory.length > 0 ? ((xiuCount / patternHistory.length) * 100).toFixed(2) : 0,
        last_update: apiResponseData.server_time,
        server_uptime: process.uptime().toFixed(0) + 's'
    });
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        websocket: ws ? ws.readyState === WebSocket.OPEN : false,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        connections: ws ? 'connected' : 'disconnected'
    });
});

app.get('/', (req, res) => {
    const networkInfo = getNetworkInfo();
    const html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>Sun.Win Data Stream</title>
        <meta charset="UTF-8">
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #0a0a0a; color: #00ff00; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { text-align: center; padding: 20px; background: #111; border-radius: 10px; margin-bottom: 20px; }
            .data-box { background: #111; padding: 20px; border-radius: 10px; margin: 10px 0; }
            .live-data { font-size: 2em; font-weight: bold; color: #00ff00; }
            .tai { color: #00ff00; }
            .xiu { color: #ff0000; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔴 Sun.Win Live Data Stream</h1>
                <p>Worm GPT Edition - Server: ${networkInfo.localIP}:${PORT}</p>
            </div>
            <div class="grid">
                <div class="data-box">
                    <h2>🎲 Current Result</h2>
                    <div class="live-data \${apiResponseData.Ket_qua === 'Tài' ? 'tai' : 'xiu'}">
                        \${apiResponseData.Tong ? \`\${apiResponseData.Xuc_xac_1}-\${apiResponseData.Xuc_xac_2}-\${apiResponseData.Xuc_xac_3} = \${apiResponseData.Tong} (\${apiResponseData.Ket_qua})\` : 'Waiting...'}
                    </div>
                    <p>Phiên: \${apiResponseData.Phien || 'N/A'}</p>
                </div>
                <div class="data-box">
                    <h2>📊 API Endpoints</h2>
                    <ul>
                        <li><a href="/api/ddvipro" style="color:#00ffff;">/api/ddvipro</a> - Latest result, 500 history & AI</li>
                        <li><a href="/api/history" style="color:#00ffff;">/api/history</a> - Full history endpoint</li>
                    </ul>
                </div>
            </div>
        </div>
        <script>
            setInterval(() => {
                fetch('/api/ddvipro')
                    .then(res => res.json())
                    .then(data => {
                        if(data.Tong) {
                            const resultDiv = document.querySelector('.live-data');
                            resultDiv.textContent = \`\${data.Xuc_xac_1}-\${data.Xuc_xac_2}-\${data.Xuc_xac_3} = \${data.Tong} (\${data.Ket_qua})\`;
                            resultDiv.className = \`live-data \${data.Ket_qua === 'Tài' ? 'tai' : 'xiu'}\`;
                        }
                    });
            }, 5000);
        </script>
    </body>
    </html>
    `;
    res.send(html);
});

app.listen(PORT, '0.0.0.0', () => {
    const networkInfo = getNetworkInfo();
    console.log(`\n=========================================`);
    console.log(`🚀 API SUNWIN DD - RUNNING`);
    console.log(`=========================================`);
    console.log(`   API MỚI: http://localhost:${PORT}/api/ddvipro`);
    console.log(`=========================================\n`);
    connectWebSocket();
});
