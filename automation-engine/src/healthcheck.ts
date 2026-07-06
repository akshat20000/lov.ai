import http from 'http';
import fs from 'fs';
import path from 'path';
import { env, prisma } from './config/index.js';
import { redisConnection } from './queue/connection.js';
import { applyQueue } from './queue/jobQueues.js';
import { register } from './monitoring/metrics.js';

function sendJson(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function servePdf(res: http.ServerResponse, filePath: string | null) {
  if (!filePath || !fs.existsSync(filePath)) {
    sendJson(res, 404, { error: 'File not found' });
    return;
  }
  res.writeHead(200, { 'Content-Type': 'application/pdf' });
  fs.createReadStream(filePath).pipe(res);
}

async function listJobs() {
  return prisma.job.findMany({
    include: { company: true, applications: true },
    orderBy: [{ score: 'desc' }, { createdAt: 'desc' }],
    take: 200,
  });
}

export function startHealthCheckServer() {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    const pathname = url.pathname;

    // --- Existing health/metrics endpoints, unchanged ---
    if (pathname === '/health') {
      let isDbConnected = false;
      let isRedisConnected = false;

      try {
        await prisma.$queryRaw`SELECT 1`;
        isDbConnected = true;
      } catch (dbError) {
        console.error('❌ [Healthcheck] Database check failure:', dbError);
      }

      try {
        await redisConnection.ping();
        isRedisConnected = true;
      } catch (redisError) {
        console.error('❌ [Healthcheck] Redis check failure:', redisError);
      }

      const isHealthy = isDbConnected && isRedisConnected;
      sendJson(res, isHealthy ? 200 : 500, {
        status: isHealthy ? 'healthy' : 'unhealthy',
        database: isDbConnected ? 'connected' : 'disconnected',
        redis: isRedisConnected ? 'connected' : 'disconnected',
      });
      return;
    }

    if (pathname === '/metrics') {
      res.writeHead(200, { 'Content-Type': register.contentType });
      res.end(await register.metrics());
      return;
    }

    // --- Dashboard API ---
    if (pathname === '/api/jobs' && req.method === 'GET') {
      try {
        const jobs = await listJobs();
        sendJson(res, 200, jobs);
      } catch (err: any) {
        sendJson(res, 500, { error: err.message });
      }
      return;
    }

    const approveMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/approve$/);
    if (approveMatch && req.method === 'POST') {
      const jobId = approveMatch[1];
      try {
        const job = await prisma.job.findUnique({ where: { id: jobId } });
        if (!job) {
          sendJson(res, 404, { error: 'Job not found' });
          return;
        }
        if (job.status !== 'READY') {
          sendJson(res, 400, { error: `Job is in status "${job.status}", not READY.` });
          return;
        }
        await applyQueue.add(`apply-submission-${jobId}`, { jobId });
        sendJson(res, 200, { success: true });
      } catch (err: any) {
        sendJson(res, 500, { error: err.message });
      }
      return;
    }

    const resumeMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/resume$/);
    if (resumeMatch && req.method === 'GET') {
      const app = await prisma.application.findUnique({ where: { jobId: resumeMatch[1] } });
      servePdf(res, app?.resumePath ?? null);
      return;
    }

    const coverMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/cover-letter$/);
    if (coverMatch && req.method === 'GET') {
      const app = await prisma.application.findUnique({ where: { jobId: coverMatch[1] } });
      servePdf(res, app?.coverLetterPath ?? null);
      return;
    }

    // --- Static dashboard page ---
    if (pathname === '/' || pathname === '/dashboard') {
      const filePath = path.join(process.cwd(), 'public', 'dashboard.html');
      if (fs.existsSync(filePath)) {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        fs.createReadStream(filePath).pipe(res);
      } else {
        sendJson(res, 500, { error: 'dashboard.html not found in public/' });
      }
      return;
    }

    res.writeHead(404);
    res.end();
  });

  const port = env.PORT || 3000;
  server.listen(port, () => {
    console.log(`🏥 [Healthcheck] Server successfully booted on port ${port}`);
    console.log(`📊 [Dashboard] Available at http://localhost:${port}/`);
  });

  return server;
}

export default startHealthCheckServer;