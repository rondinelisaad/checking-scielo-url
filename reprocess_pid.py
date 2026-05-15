#!/usr/bin/env python3
"""
UTILITÁRIO: Reprocessa apenas os artigos com pid=null no articles_with_doi.json.

Não toca nos artigos que já têm PID — só busca os que estão faltando.
Tenta todos os idiomas disponíveis do artigo até encontrar o PID.

Uso:
  python reprocess_pid.py              # reprocessa todos com pid=null
  python reprocess_pid.py --limit 100  # teste rápido
"""

import asyncio
import json
import re
import sys
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

DATA_DIR     = Path("data")
SEM_LIMIT    = 20
TIMEOUT_BASE = 30
MAX_RETRIES  = 3
BATCH_SAVE   = 200

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

PID_RE = re.compile(r"S\d{4}-\d{3}[\dX]\d{13}")
DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"'<>\]\[]+", re.IGNORECASE)


async def fetch(session, url, sem):
    async with sem:
        for attempt in range(MAX_RETRIES):
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_BASE * (2 ** attempt))
            try:
                async with session.get(
                    url, timeout=timeout, ssl=False, allow_redirects=True
                ) as r:
                    if r.status == 200:
                        return await r.text()
                    elif r.status in (404, 410):
                        return None
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** (attempt + 1))
    return None


def extract_pid(html: str) -> str | None:
    P = PID_RE.pattern
    patterns = [
        re.compile(r"<!--\s*PID:\s*(" + P + r")\s*-->"),
        re.compile(r'<meta[^>]+name=["\']citation_pid["\'][^>]+content=["\'](' + P + r')["\']', re.I),
        re.compile(r'<meta[^>]+content=["\'](' + P + r')["\'][^>]+name=["\']citation_pid["\']', re.I),
        re.compile(r'["\']pid["\']\s*:\s*["\'](' + P + r')["\']'),
        re.compile(r'data-pid=["\'](' + P + r')["\']'),
        re.compile(r'[?&]pid=(' + P + r')'),
    ]
    for pat in patterns:
        m = pat.search(html)
        if m:
            return m.group(1)
    m = PID_RE.search(html)
    return m.group(0) if m else None


def extract_doi(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    meta = soup.find("meta", attrs={"name": "citation_doi"})
    if meta and meta.get("content"):
        return meta["content"].strip().rstrip(".")
    for a in soup.find_all("a", href=re.compile(r"doi\.org/10\.", re.I)):
        m = DOI_RE.search(a["href"])
        if m:
            return m.group(0).rstrip(".")
    return None


async def process_one(session, article, sem):
    urls = list(article.get("html_urls") or [article["base_url"]])
    article = article.copy()

    for url in urls:
        html = await fetch(session, url, sem)
        if not html:
            continue
        if not article.get("pid"):
            article["pid"] = extract_pid(html)
        if not article.get("doi"):
            doi = extract_doi(html)
            if doi:
                article["doi"]     = doi
                article["doi_url"] = f"https://doi.org/{doi}"
        if article.get("pid") and article.get("doi"):
            break

    return article


async def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])

    in_file = DATA_DIR / "articles_with_doi.json"
    if not in_file.exists():
        in_file = DATA_DIR / "articles.json"
    if not in_file.exists():
        print("❌ Arquivo de artigos não encontrado.")
        sys.exit(1)

    articles = json.loads(in_file.read_text(encoding="utf-8"))

    # Separa os que precisam de PID
    need_pid  = [a for a in articles if a.get("pid") is None]
    have_pid  = [a for a in articles if a.get("pid") is not None]

    if limit:
        need_pid = need_pid[:limit]

    print("=" * 60)
    print("REPROCESS PID: Buscando PIDs ausentes")
    print("=" * 60)
    print(f"  Total artigos:     {len(articles)}")
    print(f"  Com PID:           {len(have_pid)}")
    print(f"  Sem PID (null):    {len([a for a in articles if a.get('pid') is None])}")
    print(f"  A processar agora: {len(need_pid)}")

    if not need_pid:
        print("\n✅ Todos os artigos já têm PID!")
        return

    sem       = asyncio.Semaphore(SEM_LIMIT)
    connector = aiohttp.TCPConnector(limit=SEM_LIMIT, ssl=False)

    article_map = {a["article_id"]: a for a in articles}
    processed   = 0
    found       = 0

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        queue: asyncio.Queue = asyncio.Queue()
        for a in need_pid:
            await queue.put(a)

        lock = asyncio.Lock()
        pbar = tqdm(total=len(need_pid), desc="Buscando PID", unit="art")

        async def worker():
            nonlocal processed, found
            while True:
                try:
                    article = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    result = await process_one(session, article, sem)
                except Exception:
                    result = article
                async with lock:
                    article_map[result["article_id"]] = result
                    processed += 1
                    if result.get("pid"):
                        found += 1
                    pbar.set_postfix(encontrados=found, sem_pid=processed - found)
                    pbar.update(1)
                    if processed % BATCH_SAVE == 0:
                        final = list(article_map.values())
                        in_file.write_text(
                            json.dumps(final, ensure_ascii=False), encoding="utf-8"
                        )
                queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(SEM_LIMIT)]
        await asyncio.gather(*workers)
        pbar.close()

    # Salva final
    final = list(article_map.values())
    out_file = DATA_DIR / "articles_with_doi.json"
    out_file.write_text(
        json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    still_null = sum(1 for a in final if a.get("pid") is None)
    print(f"\n✅ Concluído")
    print(f"   PIDs encontrados:  {found}/{len(need_pid)}")
    print(f"   Ainda sem PID:     {still_null}")
    print(f"   Salvo em: {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
