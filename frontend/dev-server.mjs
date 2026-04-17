import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FRONTEND_ROOT = __dirname;
const PORT = Number.parseInt(process.env.FRONTEND_PORT || "5173", 10);
const BACKEND_URL = new URL(process.env.BACKEND_URL || "http://127.0.0.1:8000");

const API_PREFIXES = [
  "/access-policies",
  "/admin",
  "/answer",
  "/chart",
  "/chat",
  "/data",
  "/dimensions",
  "/exports",
  "/feedback",
  "/glossary",
  "/governance",
  "/health",
  "/intent",
  "/lineage",
  "/metadata",
  "/metrics",
  "/query",
  "/semantic",
  "/sql",
];

const CONTENT_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
};

const server = http.createServer((request, response) => {
  const requestUrl = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);

  if (isApiRequest(requestUrl.pathname)) {
    proxyToBackend(request, response, requestUrl);
    return;
  }

  serveStatic(response, requestUrl.pathname);
});

server.listen(PORT, () => {
  console.log(`Frontend dev server: http://127.0.0.1:${PORT}/`);
  console.log(`Proxying API requests to: ${BACKEND_URL.origin}`);
});

function isApiRequest(pathname) {
  return API_PREFIXES.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
}

function proxyToBackend(clientRequest, clientResponse, requestUrl) {
  const targetUrl = new URL(`${requestUrl.pathname}${requestUrl.search}`, BACKEND_URL);
  const transport = targetUrl.protocol === "https:" ? https : http;
  const headers = { ...clientRequest.headers, host: targetUrl.host };

  const proxyRequest = transport.request(
    targetUrl,
    {
      method: clientRequest.method,
      headers,
    },
    (proxyResponse) => {
      clientResponse.writeHead(proxyResponse.statusCode || 502, proxyResponse.headers);
      proxyResponse.pipe(clientResponse);
    },
  );

  proxyRequest.on("error", (error) => {
    clientResponse.writeHead(502, { "Content-Type": "application/json; charset=utf-8" });
    clientResponse.end(
      JSON.stringify({
        detail: `Backend proxy failed: ${error.message}`,
        backend_url: BACKEND_URL.origin,
      }),
    );
  });

  clientRequest.pipe(proxyRequest);
}

function serveStatic(response, pathname) {
  const relativePath = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
  const resolvedPath = path.resolve(FRONTEND_ROOT, relativePath);

  if (!resolvedPath.startsWith(FRONTEND_ROOT)) {
    response.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Forbidden");
    return;
  }

  fs.stat(resolvedPath, (statError, stats) => {
    if (statError || !stats.isFile()) {
      serveStatic(response, "/");
      return;
    }

    const extension = path.extname(resolvedPath);
    response.writeHead(200, {
      "Content-Type": CONTENT_TYPES[extension] || "application/octet-stream",
      "Cache-Control": "no-store",
    });
    fs.createReadStream(resolvedPath).pipe(response);
  });
}
