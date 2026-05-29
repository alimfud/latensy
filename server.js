const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const ping = require('ping');
const geoip = require('geoip-lite');
const axios = require('axios');
const cheerio = require('cheerio');

const PORT = process.env.PORT || 3000;
const PING_COUNT = 5;
const PING_INTERVAL = 3000; // 3 detik

// In-memory broker list
let brokers = [];

// Scrape broker dari MQL5 (fallback jika gagal)
async function scrapeBrokers() {
  try {
    const { data } = await axios.get('https://www.mql5.com/en/vps');
    const $ = cheerio.load(data);
    const temp = [];
    $('table.broker-table tbody tr').each((i, row) => {
      const cols = $(row).find('td');
      if (cols.length >= 3) {
        temp.push({
          brokerName: $(cols[0]).text().trim(),
          serverName: $(cols[1]).text().trim(),
          location: $(cols[2]).text().trim(),
          ip: null // bisa diisi manual
        });
      }
    });
    brokers = temp;
    console.log(`Scraped ${brokers.length} brokers`);
  } catch (err) {
    console.warn('Scraping failed, using fallback list');
    brokers = [
      { brokerName: 'ICMarkets', serverName: 'ICMarkets-Demo', location: 'London', ip: '51.75.145.200' },
      { brokerName: 'Exness', serverName: 'Exness-Real', location: 'Singapore', ip: '185.186.79.24' },
      { brokerName: 'Pepperstone', serverName: 'Pepperstone-Edge', location: 'New York', ip: '104.18.20.100' },
      { brokerName: 'XM', serverName: 'XM-Demo', location: 'Cyprus', ip: '82.102.16.2' },
      { brokerName: 'FXTM', serverName: 'FXTM-ECN', location: 'London', ip: '74.125.200.94' }
    ];
  }
}

// GeoIP lookup
async function getGeoIP(ip) {
  const geo = geoip.lookup(ip);
  if (geo) return {
    country: geo.country,
    region: geo.region,
    city: geo.city,
    ll: geo.ll, // [lat, lon]
    timezone: geo.timezone,
    org: geo.org
  };
  try {
    const { data } = await axios.get(`http://ip-api.com/json/${ip}`);
    return {
      country: data.country,
      region: data.regionName,
      city: data.city,
      ll: [data.lat, data.lon],
      timezone: data.timezone,
      org: data.org
    };
  } catch {
    return null;
  }
}

// Ping sessions
class PingService {
  constructor() {
    this.sessions = new Map();
  }
  start(socket, { vpsIp, targetIp }) {
    this.stop(socket);
    const intervalId = setInterval(async () => {
      try {
        const res = await ping.promise.probe(targetIp, {
          timeout: 10,
          min_reply: PING_COUNT
        });
        const jitter = res.times?.length > 1
          ? Math.sqrt(res.times.reduce((a,b) => a + Math.pow(b - res.avg, 2), 0) / res.times.length).toFixed(2)
          : 0;
        socket.emit('latency:update', {
          timestamp: Date.now(),
          avg: res.avg,
          min: res.min,
          max: res.max,
          packetLoss: parseFloat(res.packetLoss),
          jitter,
          alive: res.alive
        });
      } catch (err) {
        socket.emit('latency:error', { message: 'Ping failed', error: err.message });
      }
    }, PING_INTERVAL);
    this.sessions.set(socket.id, intervalId);
    socket.emit('latency:started', { target: targetIp });
  }
  stop(socket) {
    if (this.sessions.has(socket.id)) {
      clearInterval(this.sessions.get(socket.id));
      this.sessions.delete(socket.id);
      socket.emit('latency:stopped');
    }
  }
}

// Express + Socket.IO
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// API endpoints
app.get('/api/brokers', (_, res) => res.json(brokers));
app.post('/api/geoip', async (req, res) => {
  const { ip } = req.body;
  if (!ip) return res.status(400).json({ error: 'IP required' });
  const geo = await getGeoIP(ip);
  res.json(geo);
});

// WebSocket handling
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  socket.on('ping:start', (data) => pingService.start(socket, data));
  socket.on('ping:stop', () => pingService.stop(socket));
  socket.on('disconnect', () => pingService.stop(socket));
});

const pingService = new PingService();

// Jalankan scraper lalu server
scrapeBrokers().then(() => {
  server.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
});
