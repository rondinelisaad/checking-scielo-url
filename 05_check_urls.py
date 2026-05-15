#!/usr/bin/env python3
"""
SCIELO CHECKER - Passo 5: Verifica se todas as URLs estão funcionando

ARQUITETURA: asyncio.Queue + N workers compartilhando UMA session/connector.
Isso garante progresso real desde o primeiro segundo.

Verifica:
  - HTML  (cada idioma)
  - PDF   (confirma %PDF nos primeiros bytes)
  - DOI   (segue redirect, detecta DOI not found)

Status: OK | NOT_FOUND | DOI_ERROR | DOI_UNRESOLVED | PDF_INVALID |
        TIMEOUT | RATE_LIMITED | SERVER_ERROR | ERROR

RESILIÊNCIA:
  - Retry com backoff exponencial (30s → 60s → 120s)
  - Checkpoint a cada BATCH_SAVE artigos
  - Reiniciar pula artigos já verificados

Uso:
  python 05_check_urls.py                  # roda completo
  python 05_check_urls.py --workers 20     # menos concorrência
  python 05_check_urls.py --limit 200      # teste rápido
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path

import aiohttp
from tqdm import tqdm

# ─── Configuração ─────────────────────────────────────────────────────────────
DATA_DIR     = Path("data")
N_WORKERS    = 40    # workers paralelos
SEM_DOI      = 15    # subconcorrência para doi.org (mais lento)
TIMEOUT_BASE = 30    # segundos — dobra a cada retry
MAX_RETRIES  = 3
BATCH_SAVE   = 500   # checkpoint a cada N artigos concluídos

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

PDF_HEADER = b"%PDF"
SCIELO_PREFIX = "10.1590"  # prefixo proprio do SciELO

DOI_ERROR_PATTERNS = [
    re.compile(r"DOI\s+Not\s+Found",  re.IGNORECASE),
    re.compile(r"Resource not found", re.IGNORECASE),
    re.compile(r"unable to provide",  re.IGNORECASE),
]
ARTICLE_PATTERNS = [
    re.compile(r"scielo\.br",    re.IGNORECASE),
    re.compile(r"\babstract\b",  re.IGNORECASE),
    re.compile(r"doi\.org/10\.", re.IGNORECASE),
]


# ─── Fetch com retry + backoff ────────────────────────────────────────────────

async def fetch_url(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    read_bytes: int = 0,
) -> dict:
    out = {
        "status_code":  None,
        "final_url":    url,
        "body":         b"",
        "content_type": "",
        "error":        None,
        "attempts":     0,
    }
    async with sem:
        for attempt in range(MAX_RETRIES):
            out["attempts"] = attempt + 1
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_BASE * (2 ** attempt))
            try:
                async with session.get(
                    url,
                    timeout=timeout,
                    ssl=False,
                    allow_redirects=True,
                    max_redirects=10,
                ) as r:
                    out["status_code"]  = r.status
                    out["final_url"]    = str(r.url)
                    out["content_type"] = r.headers.get("Content-Type", "")
                    if read_bytes > 0:
                        out["body"] = await r.content.read(read_bytes)
                    return out   # qualquer HTTP status = sucesso do fetch

            except asyncio.TimeoutError:
                out["error"] = f"Timeout({TIMEOUT_BASE * (2**attempt)}s)"
            except aiohttp.TooManyRedirects:
                out["error"] = "TooManyRedirects"
                return out       # não tenta de novo
            except aiohttp.ClientConnectorError as e:
                out["error"] = f"ConnError:{str(e)[:80]}"
            except aiohttp.ClientError as e:
                out["error"] = str(e)[:100]
            except Exception as e:
                out["error"] = f"Unexpected:{str(e)[:80]}"
                return out       # erro desconhecido, não tenta de novo

            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** (attempt + 1))

    return out


# ─── Classificadores ──────────────────────────────────────────────────────────

def classify_html(r: dict) -> str:
    if r["status_code"] is None:
        return "TIMEOUT" if "Timeout" in (r["error"] or "") else "ERROR"
    sc = r["status_code"]
    if sc == 200:        return "OK"
    if sc in (404, 410): return "NOT_FOUND"
    if sc == 429:        return "RATE_LIMITED"
    if sc >= 500:        return "SERVER_ERROR"
    return "ERROR"


def classify_pdf(r: dict) -> str:
    if r["status_code"] is None:
        return "TIMEOUT" if "Timeout" in (r["error"] or "") else "ERROR"
    sc = r["status_code"]
    if sc == 200:
        # Estratégia em camadas para detectar PDF válido:
        # 1. %PDF nos primeiros 128 bytes (cobre bytes de lixo antes do header)
        # 2. Content-Type application/pdf (fallback quando corpo não chegou completo)
        body = r.get("body", b"") or b""
        ct   = r.get("content_type", "") or ""
        if PDF_HEADER in body[:128]:
            return "OK"
        if "application/pdf" in ct.lower():
            return "OK"
        return "PDF_INVALID"
    if sc in (404, 410): return "NOT_FOUND"
    if sc == 429:        return "RATE_LIMITED"
    return "ERROR"



def classify_doi(r: dict, doi: str = "") -> str:
    if r["status_code"] is None:
        return "TIMEOUT" if "Timeout" in (r["error"] or "") else "ERROR"
    sc    = r["status_code"]
    final = r["final_url"]

    # Extrai prefixo do DOI
    import re as _re
    m      = _re.match(r"(10\.\d{4,9})/", (doi or "").strip())
    prefix = m.group(1) if m else ""

    # DOI com prefixo externo (nao 10.1590):
    # Redireciona para o publisher proprio — aceitavel como OK
    # desde que nao fique preso no doi.org com 404.
    if prefix and prefix != SCIELO_PREFIX:
        in_doi_org = "doi.org" in final
        if not in_doi_org and sc and sc < 500:
            return "OK"   # chegou no publisher externo
        if in_doi_org and sc in (404, 410):
            return "NOT_FOUND"  # doi.org disse que nao existe
        if sc and sc >= 500:
            return "SERVER_ERROR"
        if sc in (404, 410):
            return "NOT_FOUND"
        if "Timeout" in (r["error"] or ""):
            return "TIMEOUT"
        return "EXTERNAL_OK"  # prefix externo, status ambiguo

    # Prefixo 10.1590 (SciELO): validacao completa
    if sc in (404, 410): return "NOT_FOUND"
    if sc >= 400:        return "ERROR"

    text = r["body"].decode("utf-8", errors="replace")
    if any(p.search(text) for p in DOI_ERROR_PATTERNS):
        return "DOI_ERROR"
    if "doi.org" in final and "scielo" not in final:
        if not any(p.search(text) for p in ARTICLE_PATTERNS):
            return "DOI_UNRESOLVED"
    return "OK"

# ─── Verificação de um artigo ─────────────────────────────────────────────────

async def check_article(
    session:    aiohttp.ClientSession,
    article:    dict,
    sem_main:   asyncio.Semaphore,
    sem_doi:    asyncio.Semaphore,
) -> dict:
    ts = time.time()

    # HTML
    html_checks = []
    for url in article.get("html_urls", []):
        r = await fetch_url(session, url, sem_main)
        html_checks.append({
            "url": url, "status": classify_html(r),
            "http_code": r["status_code"], "final_url": r["final_url"],
            "error": r["error"], "attempts": r["attempts"], "ts": ts,
        })

    # PDF
    base_url = article.get("base_url") or (
        "https://www.scielo.br/j/" + article["journal_acronym"]
        + "/a/" + article["article_id"] + "/"
    )
    langs = article.get("langs") or ["pt", "en", "es"]
    seen: set[str] = set()
    pdf_urls: list[str] = []
    for url in article.get("pdf_urls", []):
        if "/abstract/" in url or "format=pdf" not in url:
            import re as _re2
            m = _re2.search(r"lang=([a-z]{2})", url)
            lang = m.group(1) if m else langs[0]
            url = base_url + "?format=pdf&lang=" + lang
        if url not in seen:
            seen.add(url)
            pdf_urls.append(url)
    if not pdf_urls:
        pdf_urls = [base_url + "?format=pdf&lang=" + langs[0]]
    pdf_urls = pdf_urls[:2]

    pdf_checks = []
    for url in pdf_urls:
        r = await fetch_url(session, url, sem_main, read_bytes=128)
        pdf_checks.append({
            "url": url, "status": classify_pdf(r),
            "http_code": r["status_code"],
            "content_type": r.get("content_type", ""),
            "error": r["error"], "attempts": r["attempts"], "ts": ts,
        })

    # DOI
    doi_checks = []
    if article.get("doi_url"):
        r = await fetch_url(session, article["doi_url"], sem_doi, read_bytes=8192)
        doi_checks.append({
            "url": article["doi_url"],
            "status": classify_doi(r, doi=article.get("doi", "")),
            "http_code": r["status_code"], "final_url": r["final_url"],
            "error": r["error"], "attempts": r["attempts"], "ts": ts,
        })

    html_ok = all(c["status"] == "OK" for c in html_checks) if html_checks else None
    pdf_ok  = all(c["status"] == "OK" for c in pdf_checks)  if pdf_checks  else None
    doi_ok  = (doi_checks[0]["status"] in ("OK", "EXTERNAL_OK")) if doi_checks else None

    return {
        "article_id":      article["article_id"],
        "journal_acronym": article["journal_acronym"],
        "issue_id":        article.get("issue_id"),
        "year":            article.get("year"),
        "pid":             article.get("pid"),
        "doi":             article.get("doi"),
        "doi_url":         article.get("doi_url"),
        "html_ok":         html_ok,
        "pdf_ok":          pdf_ok,
        "doi_ok":          doi_ok,
        "all_ok":          all(x is not False for x in [html_ok, pdf_ok, doi_ok]),
        "html_checks":     html_checks,
        "pdf_checks":      pdf_checks,
        "doi_checks":      doi_checks,
    }


# ─── Worker pool ──────────────────────────────────────────────────────────────

async def run_pool(
    pending:   list[dict],
    results:   list[dict],
    out_file:  Path,
    n_workers: int,
) -> int:
    queue: asyncio.Queue = asyncio.Queue()
    for a in pending:
        await queue.put(a)

    sem_main   = asyncio.Semaphore(n_workers)
    sem_doi    = asyncio.Semaphore(SEM_DOI)
    connector  = aiohttp.TCPConnector(
        limit=n_workers + SEM_DOI, ssl=False, ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    pbar     = tqdm(total=len(pending), desc="Verificando", unit="art")
    errors   = 0
    counter  = 0
    lock     = asyncio.Lock()

    async with aiohttp.ClientSession(
        headers=HEADERS, connector=connector,
        cookie_jar=aiohttp.DummyCookieJar(),
    ) as session:

        async def worker():
            nonlocal errors, counter
            while True:
                try:
                    article = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    result = await check_article(session, article, sem_main, sem_doi)
                except Exception as e:
                    errors += 1
                    print("\n  WORKER ERR [" + article.get("article_id","") + "]: " + str(e), file=sys.stderr)
                    queue.task_done()
                    pbar.update(1)
                    continue

                async with lock:
                    results.append(result)
                    counter += 1
                    pbar.set_postfix(
                        ok=sum(1 for r in results[-500:] if r["all_ok"]),
                        err=errors
                    )
                    pbar.update(1)
                    if counter % BATCH_SAVE == 0:
                        out_file.write_text(
                            json.dumps(results, ensure_ascii=False), encoding="utf-8"
                        )
                queue.task_done()

        worker_tasks = [asyncio.create_task(worker()) for _ in range(n_workers)]
        await asyncio.gather(*worker_tasks)

    pbar.close()
    return errors



def load_existing(out_file: Path) -> tuple[list[dict], set[str]]:
    if not out_file.exists():
        return [], set()
    try:
        data = json.loads(out_file.read_text(encoding="utf-8"))
        return data, {r["article_id"] for r in data}
    except Exception as e:
        print(f"  ⚠️  Erro ao ler resultados existentes: {e}", file=sys.stderr)
        return [], set()


def _http_code_bd(results: list[dict]) -> dict:
    def tally(key: str) -> dict:
        codes: dict[str, int] = {}
        total = 0
        for r in results:
            for c in r.get(key, []):
                code = str(c.get("http_code") or "sem_resposta")
                codes[code] = codes.get(code, 0) + 1
                total += 1
        pct_map = {
            code: {"count": n, "pct": f"{100*n/max(1,total):.2f}%"}
            for code, n in sorted(codes.items(), key=lambda x: -x[1])
        }
        return {
            "total_checks": total,
            "http_200":     pct_map.get("200", {"count": 0, "pct": "0.00%"}),
            "non_200":      {k: v for k, v in pct_map.items() if k != "200"},
        }
    return {"html": tally("html_checks"), "pdf": tally("pdf_checks"), "doi": tally("doi_checks")}


def _status_bd(results: list[dict], key: str) -> dict[str, int]:
    bd: dict[str, int] = {}
    for r in results:
        for c in r.get(key, []):
            s = c.get("status", "UNKNOWN")
            bd[s] = bd.get(s, 0) + 1
    return dict(sorted(bd.items(), key=lambda x: -x[1]))


def _journals_breakdown(results: list[dict]) -> dict:
    """Por periódico com erros: total de artigos, erros por tipo e HTTP codes != 200."""
    by_journal: dict[str, list[dict]] = {}
    for r in results:
        acr = r.get("journal_acronym", "UNKNOWN")
        by_journal.setdefault(acr, []).append(r)

    breakdown: dict[str, dict] = {}
    for acr, arts in by_journal.items():
        html_f = sum(1 for a in arts if a.get("html_ok") is False)
        pdf_f  = sum(1 for a in arts if a.get("pdf_ok")  is False)
        doi_f  = sum(1 for a in arts if a.get("doi_ok")  is False)
        if html_f + pdf_f + doi_f == 0:
            continue

        def non200(key: str) -> dict[str, int]:
            codes: dict[str, int] = {}
            for a in arts:
                for c in a.get(key, []):
                    code = str(c.get("http_code") or "sem_resposta")
                    if code != "200":
                        codes[code] = codes.get(code, 0) + 1
            return dict(sorted(codes.items(), key=lambda x: -x[1]))

        breakdown[acr] = {
            "total_articles": len(arts),
            "errors": {"html": html_f, "pdf": pdf_f, "doi": doi_f,
                       "total": html_f + pdf_f + doi_f},
            "http_codes_non200": {
                "html": non200("html_checks"),
                "pdf":  non200("pdf_checks"),
                "doi":  non200("doi_checks"),
            },
        }
    return dict(sorted(breakdown.items(), key=lambda x: -x[1]["errors"]["total"]))


def build_summary(results: list[dict]) -> dict:
    total = len(results)
    pct   = lambda a, b: f"{100*a/max(1,a+b):.1f}%"
    def ct(field, val): return sum(1 for r in results if r.get(field) is val)

    html_ok = ct("html_ok", True);  html_f = ct("html_ok", False)
    pdf_ok  = ct("pdf_ok",  True);  pdf_f  = ct("pdf_ok",  False)
    doi_ok  = ct("doi_ok",  True);  doi_f  = ct("doi_ok",  False)
    doi_na  = ct("doi_ok",  None)
    all_ok  = ct("all_ok",  True)
    hcbd    = _http_code_bd(results)

    return {
        "total_articles": total, "all_ok": all_ok,
        "html": {
            "ok": html_ok, "fail": html_f, "pct": pct(html_ok, html_f),
            "status_breakdown":    _status_bd(results, "html_checks"),
            "http_code_breakdown": hcbd["html"],
        },
        "pdf": {
            "ok": pdf_ok, "fail": pdf_f, "pct": pct(pdf_ok, pdf_f),
            "status_breakdown":    _status_bd(results, "pdf_checks"),
            "http_code_breakdown": hcbd["pdf"],
        },
        "doi": {
            "ok": doi_ok, "fail": doi_f, "na": doi_na, "pct": pct(doi_ok, doi_f),
            "status_breakdown":    _status_bd(results, "doi_checks"),
            "http_code_breakdown": hcbd["doi"],
        },
        "journals_with_errors": _journals_breakdown(results),
    }


def save_all(results: list[dict], out_file: Path) -> tuple[dict, list]:
    summary = build_summary(results)
    broken  = [r for r in results
               if r.get("html_ok") is False
               or r.get("pdf_ok")  is False
               or r.get("doi_ok")  is False]
    out_file.write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "check_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "broken_urls.json").write_text(
        json.dumps(broken, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary, broken


def print_summary(summary: dict, broken: list) -> None:
    s    = summary
    html = s["html"]
    pdf  = s["pdf"]
    doi  = s["doi"]
    sep  = "=" * 60
    print("")
    print(sep)
    print("RESULTADO FINAL")
    print(sep)
    print("  Total:  " + str(s["total_articles"]) + "  |  100% OK: " + str(s["all_ok"]))
    print("  HTML:   " + str(html["ok"]) + " OK / " + str(html["fail"]) + " falhas (" + html["pct"] + ")")
    print("  PDF:    " + str(pdf["ok"])  + " OK / " + str(pdf["fail"])  + " falhas (" + pdf["pct"]  + ")")
    print("  DOI:    " + str(doi["ok"])  + " OK / " + str(doi["fail"])  + " falhas (" + doi["pct"]  + ") | " + str(doi["na"]) + " sem DOI")
    for tipo, section in (("HTML", html), ("PDF", pdf), ("DOI", doi)):
        non200 = section["http_code_breakdown"].get("non_200", {})
        if non200:
            print("")
            print("  HTTP codes != 200 [" + tipo + "]:")
            for code, info in non200.items():
                print("    " + code.rjust(15) + ": " + str(info["count"]).rjust(7) + "  (" + info["pct"] + ")")
    total = s["total_articles"]
    print("")
    print("  data/check_results.json  (" + str(total) + " artigos)")
    print("  data/broken_urls.json    (" + str(len(broken)) + " com problema)")
    print("  data/check_summary.json")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    # Argumentos opcionais
    limit   = None
    workers = N_WORKERS
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    if "--workers" in sys.argv:
        workers = int(sys.argv[sys.argv.index("--workers") + 1])

    # Arquivo de entrada
    in_file = DATA_DIR / "articles_with_doi.json"
    if not in_file.exists():
        in_file = DATA_DIR / "articles.json"
    if not in_file.exists():
        print("❌ Execute os passos 1-4 primeiro")
        sys.exit(1)

    articles = json.loads(in_file.read_text(encoding="utf-8"))
    if limit:
        articles = articles[:limit]
        print(f"  ⚠️  Modo teste: {limit} artigos")

    out_file = DATA_DIR / "check_results.json"
    results, done_ids = load_existing(out_file)
    pending = [a for a in articles if a["article_id"] not in done_ids]

    print("=" * 60)
    print("SCIELO CHECKER - Passo 5: Verificando URLs")
    print("=" * 60)
    if done_ids:
        print(f"  ♻️  Retomando: {len(done_ids)} artigos já verificados")
    print(f"  Total artigos:  {len(articles)}")
    print(f"  Já verificados: {len(done_ids)}")
    print(f"  Pendentes:      {len(pending)}")
    print(f"  Workers:        {workers}")

    if not pending:
        print("\n✅ Todos os artigos já foram verificados!")
        summary, broken = save_all(results, out_file)
        print_summary(summary, broken)
        return

    errors = await run_pool(pending, results, out_file, workers)

    summary, broken = save_all(results, out_file)
    print_summary(summary, broken)
    if errors:
        print(f"  ⚠️  Erros de worker: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
