"""
TikTok Downloader API · fuente única: ttdownloader

Recibe un link de TikTok y devuelve el .mp4 directamente a quien lo pidió.
No recodifica nada: entrega el archivo tal cual lo sirve ttdownloader.
API pública: cualquiera que conozca el dominio puede usarla (sin API key).

Variables de entorno (todas opcionales):
  API_KEY          si la defines, los clientes deben mandarla en el header X-API-Key (o ?key=).
                   Vacía o sin definir = API pública (por defecto).
  MAX_CONCURRENT   descargas simultáneas máximas (default 2)
  ATTEMPTS         reintentos con ttdownloader por cada link (default 3)
  TOTAL_TIMEOUT    segundos máximos por petición, contando reintentos (default 90)
  ALLOWED_ORIGINS  orígenes CORS separados por coma (default *)
"""
import os
import re
import json
import time
import types
import shutil
import secrets
import logging
import tempfile
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tiktok-api")

# ───────── Configuración ─────────
API_KEY = os.getenv("API_KEY", "").strip()  # vacío = pública
MAX_CONCURRENT = max(1, int(os.getenv("MAX_CONCURRENT", "2")))
ATTEMPTS = max(1, int(os.getenv("ATTEMPTS", "3")))
TOTAL_TIMEOUT = int(os.getenv("TOTAL_TIMEOUT", "90"))
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]

SOURCE_NAME = "ttdownloader"
MIN_BYTES = 10_000  # por debajo de esto no puede ser un video real
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
EXPOSED = ["X-Source", "X-Resolution", "X-FPS", "X-Bitrate-Kbps", "X-Watermark", "X-Video-Id"]


# ───────── Fuente: solo ttdownloader ─────────
def _load_source():
    try:
        import tiktok_downloader as td
        fn = getattr(td, "ttdownloader", None)
        if isinstance(fn, types.ModuleType):  # por si el paquete expone el módulo y no la función
            fn = getattr(fn, "ttdownloader", None)
        if not callable(fn):
            raise ImportError("tiktok_downloader no expone 'ttdownloader'")
        return fn, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


ttdownloader, IMPORT_ERROR = _load_source()
if IMPORT_ERROR:
    log.error("⚠️ No pude cargar ttdownloader: %s", IMPORT_ERROR)
else:
    log.info("✓ ttdownloader cargado · API %s", "pública" if not API_KEY else "protegida con API key")


# ───────── Utilidades ─────────
def stream_to(link, path):
    """Guarda el archivo tal cual lo sirve el enlace (sin recodificar)."""
    headers = dict(UA, Referer="https://ttdownloader.com/")
    timeout = httpx.Timeout(30.0, connect=10.0)
    with httpx.stream("GET", link, headers=headers, follow_redirects=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_bytes(1 << 16):
                f.write(chunk)


def video_id(url):
    m = re.search(r"/(?:video|photo)/(\d+)", url)
    if m:
        return m.group(1)
    try:  # links cortos (vt.tiktok.com / vm.tiktok.com): hay que seguir la redirección
        r = httpx.get(url, headers=UA, follow_redirects=True, timeout=15)
        m = re.search(r"/(?:video|photo)/(\d+)", str(r.url))
        if m:
            return m.group(1)
    except Exception:
        pass
    return str(int(time.time()))


def _looks_like_mp4(path):
    try:
        with open(path, "rb") as f:
            head = f.read(12)
        return len(head) >= 8 and head[4:8] == b"ftyp"
    except OSError:
        return False


def probe(path):
    """Lee resolución/fps/bitrate con ffprobe. 'ok' = el archivo realmente tiene video."""
    size = os.path.getsize(path)
    info = dict(ok=False, w=0, h=0, codec="?", fps=0, size=size, kbps=0)
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name,width,height,r_frame_rate:format=duration",
             "-of", "json", path],
            capture_output=True, text=True, check=True, timeout=30).stdout
        j = json.loads(out)
    except FileNotFoundError:  # sin ffprobe (solo pasa fuera de Docker): valida por la firma del mp4
        info["ok"] = _looks_like_mp4(path)
        return info
    except Exception:
        return info

    streams = j.get("streams") or []
    if not streams:  # p. ej. si ttdownloader devolvió el audio (.mp3) en vez del video
        return info
    s = streams[0]
    try:
        dur = float((j.get("format") or {}).get("duration") or 0)
    except ValueError:
        dur = 0
    try:
        a, b = s["r_frame_rate"].split("/")
        fps = round(int(a) / int(b), 2) if int(b) else 0
    except Exception:
        fps = 0
    info.update(ok=True, w=int(s.get("width") or 0), h=int(s.get("height") or 0),
                codec=s.get("codec_name", "?"), fps=fps,
                kbps=int(size * 8 / dur / 1000) if dur else 0)
    return info


def is_tiktok_url(url):
    """Solo acepta links de TikTok (evita que usen la API para otras webs)."""
    try:
        u = urlparse(url)
        h = (u.hostname or "").lower()
        return u.scheme in ("http", "https") and (h == "tiktok.com" or h.endswith(".tiktok.com"))
    except Exception:
        return False


def _remove(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


# ───────── Descarga con ttdownloader ─────────
def _download(url, path):
    """Un intento: pide los enlaces a ttdownloader y se queda con el primer video válido."""
    result = ttdownloader(url)
    try:
        items = list(result)
    except TypeError:
        items = [result]
    if not items:
        raise RuntimeError("ttdownloader no devolvió enlaces")

    # sin marca de agua primero (sort estable: si la librería no informa, se respeta su orden)
    items.sort(key=lambda i: getattr(i, "watermark", None) is True)

    last = RuntimeError("ttdownloader no devolvió ningún video válido")
    for it in items:
        link = getattr(it, "url", None)
        for mode in ("lib", "direct"):  # 1) descarga de la librería, 2) descarga directa del enlace
            try:
                _remove(path)
                if mode == "lib":
                    it.download(path)
                elif link:
                    stream_to(link, path)
                else:
                    continue
                if not (os.path.exists(path) and os.path.getsize(path) >= MIN_BYTES):
                    raise RuntimeError("archivo vacío")
                info = probe(path)
                if not info["ok"]:
                    raise RuntimeError("el archivo descargado no es un video")
                return dict(info, wm=getattr(it, "watermark", None))
            except Exception as e:
                last = e
    _remove(path)
    raise last


def run_source(url, workdir, base):
    path = os.path.join(workdir, f"{base}__{SOURCE_NAME}.mp4")
    try:
        c = _download(url, path)
        c.update(name=SOURCE_NAME, path=path)
        log.info("✓ %s: %sx%s · %.1f MB · marca=%s", SOURCE_NAME, c["w"], c["h"], c["size"] / 1e6,
                 {True: "sí", False: "no", None: "?"}[c["wm"]])
        return c
    except Exception as e:
        log.info("✗ %s: %s", SOURCE_NAME, str(e)[:120])
        _remove(path)
        return None


def fetch_best(url, workdir):
    """Pide el video a ttdownloader con reintentos y un tiempo máximo total."""
    if ttdownloader is None:
        raise RuntimeError(f"ttdownloader no está disponible: {IMPORT_ERROR}")

    base = f"tiktok_{video_id(url)}"
    deadline = time.monotonic() + TOTAL_TIMEOUT
    pool = ThreadPoolExecutor(max_workers=1)  # así un intento colgado no bloquea la petición
    try:
        for attempt in range(1, ATTEMPTS + 1):
            left = deadline - time.monotonic()
            if left <= 3:
                break
            fut = pool.submit(run_source, url, workdir, base)
            try:
                c = fut.result(timeout=left)
            except FutTimeout:
                log.warning("ttdownloader no respondió a tiempo")
                break
            if c:
                return _finalize(c, workdir, base)
            if attempt < ATTEMPTS:
                log.info("reintento %d/%d", attempt + 1, ATTEMPTS)
                time.sleep(min(1.5 * attempt, max(0.0, deadline - time.monotonic() - 3)))
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return None


def _finalize(best, workdir, base):
    final = os.path.join(workdir, f"{base}.mp4")
    os.replace(best["path"], final)
    best["path"] = final
    best["base"] = base
    return best


# ───────── API ─────────
app = FastAPI(title="TikTok Downloader API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key"],
    expose_headers=EXPOSED,
)
gate = threading.BoundedSemaphore(MAX_CONCURRENT)


class DownloadBody(BaseModel):
    url: str


def _auth(x_api_key, key):
    if not API_KEY:  # pública: no se pide nada
        return
    given = (x_api_key or key or "").encode()
    if not secrets.compare_digest(given, API_KEY.encode()):
        raise HTTPException(401, "API key inválida")


def _serve(url: str):
    url = url.strip()
    if not is_tiktok_url(url):
        raise HTTPException(400, "Manda un link válido de TikTok")
    if ttdownloader is None:
        raise HTTPException(503, "El descargador no está disponible en el servidor")
    if not gate.acquire(timeout=30):
        raise HTTPException(503, "Servidor ocupado, intenta de nuevo en unos segundos")

    workdir = tempfile.mkdtemp(prefix="tt_")
    try:
        best = fetch_best(url, workdir)
    except Exception:
        shutil.rmtree(workdir, ignore_errors=True)
        log.exception("fallo inesperado descargando %s", url)
        raise HTTPException(500, "Error interno al descargar el video")
    finally:
        gate.release()

    if not best:
        shutil.rmtree(workdir, ignore_errors=True)
        raise HTTPException(502, "ttdownloader no pudo con ese link. Revisa que el video sea público e inténtalo otra vez.")

    wm = best["wm"]
    headers = {
        "X-Source": best["name"],
        "X-Resolution": f"{best['w']}x{best['h']}" if best["w"] and best["h"] else "unknown",
        "X-FPS": str(best["fps"]),
        "X-Bitrate-Kbps": str(best["kbps"]),
        "X-Watermark": "unknown" if wm is None else ("yes" if wm else "no"),
        "X-Video-Id": best["base"].replace("tiktok_", ""),
    }
    return FileResponse(
        best["path"],
        media_type="video/mp4",
        filename=f"{best['base']}.mp4",
        headers=headers,
        background=BackgroundTask(shutil.rmtree, workdir, True),  # borra el temporal al terminar de enviar
    )


@app.get("/")
def home():
    return {
        "status": "ok" if ttdownloader else "error",
        "uso": {
            "GET": "/download?url=<link de TikTok>",
            "POST": '/download con JSON {"url": "<link de TikTok>"}',
            "auth": "pública, no necesita API key" if not API_KEY else "header X-API-Key (o ?key=)",
            "respuesta": "archivo .mp4 + headers X-Source, X-Resolution, X-FPS, X-Bitrate-Kbps, X-Watermark",
        },
        "fuentes": [SOURCE_NAME],
        "docs": "/docs",
    }


@app.get("/health")
def health():
    if ttdownloader is None:
        return JSONResponse({"status": "error", "detail": IMPORT_ERROR}, status_code=503)
    return {"status": "ok", "source": SOURCE_NAME}


@app.get("/download")
def download_get(
    url: str = Query(..., description="Link del video de TikTok"),
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    return _serve(url)


@app.post("/download")
def download_post(
    body: DownloadBody,
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    return _serve(body.url)
