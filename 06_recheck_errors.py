#!/usr/bin/env python3
"""
SCIELO RECHECKER - Revisita todos os artigos que tiveram qualquer erro.

Lê:   data/check_results.json  +  data/articles_with_doi.json
Salva: data/check_results.json  (atualizado, mesmos artigos OK + novos resultados)

Reprocessa qualquer artigo que tenha:
  - html_ok  = False
  - pdf_ok   = False  (incluindo PDF_INVALID de detecção bugada)
  - doi_ok   = False
  - qualquer check com status != OK e != NOT_FOUND
    (TIMEOUT, ERROR, RATE_LIMITED, SERVER_ERROR, PDF_INVALID)

Artigos com status NOT_FOUND são mantidos como estão (404 é definitivo).

LÓGICA DE PDF:
  - URL sempre reconstruída no padrão /?format=pdf&lang=xx
  - %PDF buscado nos primeiros 128 bytes (cobre bytes de lixo antes do header)
  - Content-Type application/pdf aceito como fallback

Uso:
  python 06_recheck_errors.py              # reprocessa todos os erros
  python 06_recheck_errors.py --limit 100  # teste rápido
  python 06_recheck_errors.py --workers 20 # menos concorrência
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path

import aiohttp
from tqdm import tqdm

DATA_DIR     = Path("data")
N_WORKERS    = 40
SEM_DOI      = 15
TIMEOUT_BASE = 30
MAX_RETRIES  = 3
BATCH_SAVE   = 500

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

PDF_HEADER = b"%PDF"
SCIELO_PREFIX = "10.1590"  # prefixo proprio do SciELO

# Status que são definitivos (não reprocessar)
FINAL_STATUS = {"OK", "NOT_FOUND"}

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


# ─── Fetch ────────────────────────────────────────────────────────────────────

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
                    url, timeout=timeout, ssl=False,
                    allow_redirects=True, max_redirects=10,
                ) as r:
                    out["status_code"]  = r.status
                    out["final_url"]    = str(r.url)
                    out["content_type"] = r.headers.get("Content-Type", "")
                    if read_bytes > 0:
                        out["body"] = await r.content.read(read_bytes)
                    return out
            except asyncio.TimeoutError:
                out["error"] = f"Timeout({TIMEOUT_BASE*(2**attempt)}s)"
            except aiohttp.TooManyRedirects:
                out["error"] = "TooManyRedirects"
                return out
            except aiohttp.ClientError as e:
                out["error"] = str(e)[:100]
            except Exception as e:
                out["error"] = f"Unexpected:{str(e)[:80]}"
                return out
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

def needs_recheck(result: dict) -> dict:
    """
    Retorna dict indicando quais checks precisam ser refeitos.
    {recheck_html: bool, recheck_pdf: bool, recheck_doi: bool}
    NOT_FOUND é definitivo e não é reprocessado.
    """
    def check_needs_redo(checks: list) -> bool:
        if not checks:
            return False
        return any(c.get("status") not in FINAL_STATUS for c in checks)

    recheck_html = check_needs_redo(result.get("html_checks", []))
    recheck_pdf  = check_needs_redo(result.get("pdf_checks", []))
    recheck_doi  = check_needs_redo(result.get("doi_checks", []))

    # Também reprocessa se o resultado agregado é False mas os checks individuais
    # têm algum NOT_FOUND misturado com erros (situação ambígua)
    if result.get("html_ok") is False and not recheck_html:
        recheck_html = True
    if result.get("pdf_ok") is False and not recheck_pdf:
        recheck_pdf = True
    if result.get("doi_ok") is False and not recheck_doi:
        recheck_doi = True

    return {
        "recheck_html": recheck_html,
        "recheck_pdf":  recheck_pdf,
        "recheck_doi":  recheck_doi,
    }


# ─── Reverificação de um artigo ───────────────────────────────────────────────

async def recheck_article(
    session:    aiohttp.ClientSession,
    result:     dict,
    article:    dict,
    flags:      dict,
    sem_main:   asyncio.Semaphore,
    sem_doi:    asyncio.Semaphore,
) -> dict:
    ts = time.time()
    result = result.copy()

    # ── HTML ──
    if flags["recheck_html"]:
        html_checks = []
        for url in article.get("html_urls", []):
            r = await fetch_url(session, url, sem_main)
            html_checks.append({
                "url": url, "status": classify_html(r),
                "http_code": r["status_code"], "final_url": r["final_url"],
                "error": r["error"], "attempts": r["attempts"], "ts": ts,
            })
        result["html_checks"] = html_checks
        result["html_ok"] = (
            all(c["status"] == "OK" for c in html_checks) if html_checks else None
        )

    # ── PDF ──
    if flags["recheck_pdf"]:
        base_url = article.get("base_url") or (
            f"https://www.scielo.br/j/{article['journal_acronym']}"
            f"/a/{article['article_id']}/"
        )
        langs = article.get("langs") or ["pt", "en", "es"]

        # Reconstrói URLs canônicas sem duplicatas
        seen: set[str] = set()
        pdf_urls: list[str] = []
        for url in article.get("pdf_urls", []):
            if "/abstract/" in url or "format=pdf" not in url:
                m = re.search(r"lang=([a-z]{2})", url)
                lang = m.group(1) if m else langs[0]
                url = f"{base_url}?format=pdf&lang={lang}"
            if url not in seen:
                seen.add(url)
                pdf_urls.append(url)
        if not pdf_urls:
            pdf_urls = [f"{base_url}?format=pdf&lang={langs[0]}"]
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
        result["pdf_checks"] = pdf_checks
        result["pdf_ok"] = (
            all(c["status"] == "OK" for c in pdf_checks) if pdf_checks else None
        )

    # ── DOI ──
    if flags["recheck_doi"] and article.get("doi_url"):
        r = await fetch_url(session, article["doi_url"], sem_doi, read_bytes=8192)
        doi_checks = [{
            "url": article["doi_url"], "status": classify_doi(r, doi=article.get("doi", "")),
            "http_code": r["status_code"], "final_url": r["final_url"],
            "error": r["error"], "attempts": r["attempts"], "ts": ts,
        }]
        result["doi_checks"] = doi_checks
        result["doi_ok"] = doi_checks[0]["status"] in ("OK", "EXTERNAL_OK")

    result["all_ok"] = all(
        x is not False for x in [result["html_ok"], result["pdf_ok"], result["doi_ok"]]
    )
    return result


# ─── Worker pool ──────────────────────────────────────────────────────────────

async def run_recheck(
    pending:      list[tuple],   # (result, article, flags)
    results_map:  dict,          # article_id → result (atualizado in-place)
    out_file:     Path,
    n_workers:    int,
) -> int:
    queue: asyncio.Queue = asyncio.Queue()
    for item in pending:
        await queue.put(item)

    sem_main   = asyncio.Semaphore(n_workers)
    sem_doi    = asyncio.Semaphore(SEM_DOI)
    connector  = aiohttp.TCPConnector(
        limit=n_workers + SEM_DOI, ssl=False, ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    pbar     = tqdm(total=len(pending), desc="Rechecando", unit="art")
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
                    result, article, flags = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    updated = await recheck_article(
                        session, result, article, flags, sem_main, sem_doi
                    )
                except Exception as e:
                    errors += 1
                    print(f"\n  ERRO [{result.get('article_id','')}]: {e}", file=sys.stderr)
                    queue.task_done()
                    pbar.update(1)
                    continue

                async with lock:
                    results_map[updated["article_id"]] = updated
                    counter += 1
                    still_broken = sum(
                        1 for r in results_map.values() if not r["all_ok"]
                    )
                    pbar.set_postfix(ainda_erros=still_broken, worker_erros=errors)
                    pbar.update(1)
                    if counter % BATCH_SAVE == 0:
                        out_file.write_text(
                            json.dumps(list(results_map.values()), ensure_ascii=False),
                            encoding="utf-8",
                        )
                queue.task_done()

        worker_tasks = [asyncio.create_task(worker()) for _ in range(n_workers)]
        await asyncio.gather(*worker_tasks)

    pbar.close()
    return errors


# ─── Sumário ──────────────────────────────────────────────────────────────────

def _http_code_bd(results: list[dict]) -> dict:
    """Breakdown de HTTP codes por tipo de check, separando 200 dos demais."""
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
    return {
        "html": tally("html_checks"),
        "pdf":  tally("pdf_checks"),
        "doi":  tally("doi_checks"),
    }


def _status_bd(results: list[dict], key: str) -> dict[str, int]:
    bd: dict[str, int] = {}
    for r in results:
        for c in r.get(key, []):
            s = c.get("status", "UNKNOWN")
            bd[s] = bd.get(s, 0) + 1
    return dict(sorted(bd.items(), key=lambda x: -x[1]))


def _journals_breakdown(results: list[dict]) -> dict:
    """
    Para cada acrônimo de periódico com pelo menos 1 erro, retorna:
      - total_articles: quantos artigos do periódico foram verificados
      - errors: {html: N, pdf: N, doi: N, total: N}
      - http_codes: {html: {code: count, ...}, pdf: {...}, doi: {...}}
        listando apenas códigos != 200
    Ordenado por total de erros decrescente.
    """
    # Agrupa por acrônimo
    by_journal: dict[str, list[dict]] = {}
    for r in results:
        acr = r.get("journal_acronym", "UNKNOWN")
        by_journal.setdefault(acr, []).append(r)

    breakdown: dict[str, dict] = {}
    for acr, arts in by_journal.items():
        html_f = sum(1 for a in arts if a.get("html_ok") is False)
        pdf_f  = sum(1 for a in arts if a.get("pdf_ok")  is False)
        doi_f  = sum(1 for a in arts if a.get("doi_ok")  is False)
        total_errors = html_f + pdf_f + doi_f

        if total_errors == 0:
            continue

        # HTTP codes != 200 por tipo
        def non200_for(check_key: str) -> dict[str, int]:
            codes: dict[str, int] = {}
            for a in arts:
                for c in a.get(check_key, []):
                    code = str(c.get("http_code") or "sem_resposta")
                    if code != "200":
                        codes[code] = codes.get(code, 0) + 1
            return dict(sorted(codes.items(), key=lambda x: -x[1]))

        breakdown[acr] = {
            "total_articles": len(arts),
            "errors": {
                "html":  html_f,
                "pdf":   pdf_f,
                "doi":   doi_f,
                "total": total_errors,
            },
            "http_codes_non200": {
                "html": non200_for("html_checks"),
                "pdf":  non200_for("pdf_checks"),
                "doi":  non200_for("doi_checks"),
            },
        }

    # Ordena por total de erros decrescente
    return dict(
        sorted(breakdown.items(), key=lambda x: -x[1]["errors"]["total"])
    )


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
        "total_articles": total,
        "all_ok":         all_ok,
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
            "ok": doi_ok, "fail": doi_f, "na": doi_na,
            "pct": pct(doi_ok, doi_f),
            "status_breakdown":    _status_bd(results, "doi_checks"),
            "http_code_breakdown": hcbd["doi"],
        },
        # Breakdown por periódico — só aparece se há erros
        "journals_with_errors": _journals_breakdown(results),
    }


def save_and_summarize(results: list[dict], out_file: Path) -> None:
    broken = [r for r in results
              if r.get("html_ok") is False
              or r.get("pdf_ok")  is False
              or r.get("doi_ok")  is False]

    out_file.write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "broken_urls.json").write_text(
        json.dumps(broken, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    summary = build_summary(results)
    (DATA_DIR / "check_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    s = summary
    print(f"\n{'='*60}")
    print("RESULTADO FINAL")
    print(f"{'='*60}")
    print(f"  Total:  {s['total_articles']}  |  100% OK: {s['all_ok']}")
    print(f"  HTML:   {s['html']['ok']} OK / {s['html']['fail']} falhas ({s['html']['pct']})")
    print(f"  PDF:    {s['pdf']['ok']} OK / {s['pdf']['fail']} falhas ({s['pdf']['pct']})")
    print(f"  DOI:    {s['doi']['ok']} OK / {s['doi']['fail']} falhas ({s['doi']['pct']}) | {s['doi']['na']} sem DOI")
    for tipo in ("html", "pdf", "doi"):
        non200 = s[tipo]["http_code_breakdown"].get("non_200", {})
        if non200:
            print(f"\n  HTTP codes != 200 [{tipo.upper()}]:")
            for code, info in non200.items():
                print(f"    {code:>15}: {info['count']:>7}  ({info['pct']})")
    print(f"\n  data/check_results.json  ({s['total_articles']} artigos)")
    print(f"  data/broken_urls.json    ({len(broken)} com problema)")
    print(f"  data/check_summary.json")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    limit   = None
    workers = N_WORKERS
    if "--limit"   in sys.argv: limit   = int(sys.argv[sys.argv.index("--limit")   + 1])
    if "--workers" in sys.argv: workers = int(sys.argv[sys.argv.index("--workers") + 1])

    # Carrega resultados existentes
    results_file = DATA_DIR / "check_results.json"
    if not results_file.exists():
        print("❌ data/check_results.json não encontrado. Execute 05_check_urls.py primeiro.")
        sys.exit(1)
    results = json.loads(results_file.read_text(encoding="utf-8"))

    # Carrega artigos com metadados completos (para reconstruir URLs)
    art_file = DATA_DIR / "articles_with_doi.json"
    if not art_file.exists():
        art_file = DATA_DIR / "articles.json"
    if not art_file.exists():
        print("❌ articles.json não encontrado.")
        sys.exit(1)
    articles_list = json.loads(art_file.read_text(encoding="utf-8"))
    articles_map  = {a["article_id"]: a for a in articles_list}

    # Identifica quais precisam de recheck
    pending = []
    for r in results:
        flags = needs_recheck(r)
        if any(flags.values()):
            article = articles_map.get(r["article_id"])
            if article:
                pending.append((r, article, flags))

    if limit:
        pending = pending[:limit]
        print(f"  ⚠️  Modo teste: {limit} artigos")

    total_broken = sum(1 for r in results if not r["all_ok"])

    print("=" * 60)
    print("SCIELO RECHECKER - Revisitando artigos com erro")
    print("=" * 60)
    print(f"  Total no check_results:  {len(results)}")
    print(f"  Com algum erro (all_ok=False): {total_broken}")
    print(f"  Precisam recheck:        {len(pending)}")
    print(f"  Workers:                 {workers}")

    # Breakdown dos erros
    html_erros = sum(1 for r in results if r.get("html_ok") is False)
    pdf_erros  = sum(1 for r in results if r.get("pdf_ok")  is False)
    doi_erros  = sum(1 for r in results if r.get("doi_ok")  is False)
    print(f"\n  Breakdown:")
    print(f"    HTML erros: {html_erros}")
    print(f"    PDF erros:  {pdf_erros}")
    print(f"    DOI erros:  {doi_erros}")

    if not pending:
        print("\n✅ Nenhum artigo precisa de recheck!")
        save_and_summarize(results, results_file)
        return

    # Mapa para atualização in-place
    results_map = {r["article_id"]: r for r in results}

    errors = await run_recheck(pending, results_map, results_file, workers)

    final_results = list(results_map.values())
    save_and_summarize(final_results, results_file)
    if errors:
        print(f"  ⚠️  Erros de worker: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
