#!/usr/bin/env python3
"""
SCIELO MAPPER - Passo 4: Extrai DOI e PID de cada artigo
Lê:    data/articles.json
Salva: data/articles_with_doi.json  (incrementalmente — nunca perde progresso)

RESILIÊNCIA:
  - Retry com backoff exponencial (3 tentativas: 2s, 4s, 8s)
  - Timeout progressivo (30s base, dobra a cada falha)
  - Checkpoint a cada BATCH_SAVE artigos — ao reiniciar, pula os já feitos
  - Nunca sobrescreve artigos já processados
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
BATCH_SAVE   = 200   # salva a cada N artigos processados
TIMEOUT_BASE = 30    # segundos — dobra a cada tentativa
MAX_RETRIES  = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"'<>\]\[]+", re.IGNORECASE)
PID_RE = re.compile(r"S\d{4}-\d{3}[\dX]\d{13}")


# ─── Fetch com retry + backoff exponencial ────────────────────────────────────

async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> str | None:
    async with sem:
        for attempt in range(MAX_RETRIES):
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_BASE * (2 ** attempt))
            try:
                async with session.get(
                    url,
                    timeout=timeout,
                    ssl=False,
                    allow_redirects=True,
                ) as r:
                    if r.status == 200:
                        return await r.text()
                    elif r.status in (404, 410):
                        return None          # não existe, não tenta de novo
                    elif r.status == 429:
                        # Rate limit: espera mais
                        wait = 10 * (attempt + 1)
                        print(f"\n  RATE LIMIT {url} — aguardando {wait}s", file=sys.stderr)
                        await asyncio.sleep(wait)
                        continue
                    # Outros erros HTTP: tenta de novo
            except asyncio.TimeoutError:
                wait = 2 ** (attempt + 1)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(wait)
                else:
                    print(f"\n  TIMEOUT definitivo {url}", file=sys.stderr)
            except aiohttp.ClientError as e:
                wait = 2 ** (attempt + 1)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(wait)
                else:
                    print(f"\n  ERRO definitivo {url}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"\n  ERRO inesperado {url}: {e}", file=sys.stderr)
                break
    return None


# ─── Extração de DOI ──────────────────────────────────────────────────────────

def extract_doi(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")

    # 1. <meta name="citation_doi">
    meta = soup.find("meta", attrs={"name": "citation_doi"})
    if meta and meta.get("content"):
        return meta["content"].strip().rstrip(".")

    # 2. <meta name="DC.Identifier">
    meta2 = soup.find("meta", attrs={"name": re.compile(r"DC\.Identifier", re.I)})
    if meta2 and meta2.get("content", "").startswith("10."):
        return meta2["content"].strip().rstrip(".")

    # 3. <a href="https://doi.org/10....">
    for a in soup.find_all("a", href=re.compile(r"doi\.org/10\.", re.I)):
        m = DOI_RE.search(a["href"])
        if m:
            return m.group(0).rstrip(".")

    # 4. Texto: "DOI: 10.xxx"
    text = soup.get_text()
    m = re.search(r"DOI[:\s]+(" + DOI_RE.pattern + ")", text, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".")

    # 5. JSON-LD schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                id_ = data.get("@id", "")
                if "doi.org/" in id_:
                    return id_.split("doi.org/")[-1].rstrip(".")
        except Exception:
            pass

    return None


# ─── Extração de PID ──────────────────────────────────────────────────────────

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
    # Fallback geral
    m = PID_RE.search(html)
    return m.group(0) if m else None


# ─── Processamento de um artigo ───────────────────────────────────────────────

async def process_article(
    session: aiohttp.ClientSession,
    article: dict,
    sem: asyncio.Semaphore,
) -> dict:
    # Pula se já tem tudo
    if article.get("doi") and article.get("pid"):
        return article

    # Tenta todos os idiomas disponíveis até obter DOI e PID
    # O PID aparece no comentário <!-- PID: S... --> presente em qualquer idioma
    urls_to_try = list(article.get("html_urls") or [article["base_url"]])

    # Garante pelo menos uma URL válida
    if not urls_to_try:
        return article

    article = article.copy()
    for url in urls_to_try:
        html = await fetch(session, url, sem)
        if not html:
            continue

        # Extrai DOI se ainda não tem
        if not article.get("doi"):
            doi = extract_doi(html)
            if doi:
                article["doi"]     = doi
                article["doi_url"] = f"https://doi.org/{doi}"

        # Extrai PID se ainda não tem
        if not article.get("pid"):
            article["pid"] = extract_pid(html)

        # Se já temos os dois, para de tentar
        if article.get("doi") and article.get("pid"):
            break

    return article


# ─── Checkpoint ───────────────────────────────────────────────────────────────

def load_existing(out_file: Path) -> tuple[list[dict], set[str]]:
    if not out_file.exists():
        return [], set()
    try:
        arts = json.loads(out_file.read_text(encoding="utf-8"))
        # Considera "feito" artigos que já têm doi OU pid preenchido
        done = {a["article_id"] for a in arts if a.get("doi") or a.get("pid")}
        return arts, done
    except Exception as e:
        print(f"  ⚠️  Erro ao ler arquivo existente: {e}", file=sys.stderr)
        return [], set()


def save(articles: list[dict], out_file: Path, indent: bool = False) -> None:
    out_file.write_text(
        json.dumps(articles, ensure_ascii=False, indent=2 if indent else None),
        encoding="utf-8",
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    # Aceita --limit N para testes rápidos
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])

    # Lê input: articles_with_doi.json > articles.json
    in_file = DATA_DIR / "articles_with_doi.json"
    if not in_file.exists():
        in_file = DATA_DIR / "articles.json"
    if not in_file.exists():
        print("❌ Execute primeiro: python 03_collect_articles.py")
        sys.exit(1)

    out_file = DATA_DIR / "articles_with_doi.json"

    print("=" * 60)
    print("SCIELO MAPPER - Passo 4: Extraindo DOI e PID")
    print("=" * 60)

    # Carrega state atual
    all_articles, done_ids = load_existing(out_file)

    # Se out_file não existe ainda, carrega do in_file
    if not out_file.exists():
        all_articles = json.loads(in_file.read_text(encoding="utf-8"))
        done_ids = {a["article_id"] for a in all_articles if a.get("doi") and a.get("pid") is not None}

    if limit:
        all_articles = all_articles[:limit]
        print(f"  ⚠️  Modo teste: {limit} artigos")

    need = [a for a in all_articles if a["article_id"] not in done_ids]
    print(f"  Total artigos:     {len(all_articles)}")
    print(f"  Já processados:    {len(done_ids)}")
    print(f"  Pendentes:         {len(need)}")

    if not need:
        print("\n✅ Todos os artigos já têm DOI/PID!")
        _print_stats(all_articles)
        return

    # Índice para merge rápido
    article_map = {a["article_id"]: a for a in all_articles}

    sem       = asyncio.Semaphore(SEM_LIMIT)
    connector = aiohttp.TCPConnector(limit=SEM_LIMIT, ssl=False)
    processed = 0
    errors    = 0

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        tasks = [asyncio.create_task(process_article(session, a, sem)) for a in need]
        pbar  = tqdm(total=len(tasks), desc="Artigos", unit="art")

        for task in asyncio.as_completed(tasks):
            try:
                result = await task
            except Exception as e:
                errors += 1
                print(f"\n  TASK ERRO: {e}", file=sys.stderr)
                pbar.update(1)
                continue

            article_map[result["article_id"]] = result
            processed += 1
            pbar.update(1)
            pbar.set_postfix(
                doi=sum(1 for a in article_map.values() if a.get("doi")),
                pid=sum(1 for a in article_map.values() if a.get("pid")),
                err=errors,
            )

            # Checkpoint frequente
            if processed % BATCH_SAVE == 0:
                save(list(article_map.values()), out_file)

        pbar.close()

    # Salva final
    final = list(article_map.values())
    save(final, out_file, indent=True)

    print(f"\n✅ Concluído")
    _print_stats(final)
    print(f"  Erros:   {errors}")
    print(f"  Salvo em: {out_file}")

    # Exemplos
    exemplos = [a for a in final if a.get("pid")][:3]
    if exemplos:
        print("\n  Exemplos:")
        for e in exemplos:
            print(f"    [{e['article_id']}] PID={e['pid']}  DOI={e.get('doi','—')}")


def _print_stats(articles: list[dict]) -> None:
    total    = len(articles)
    with_doi = sum(1 for a in articles if a.get("doi"))
    with_pid = sum(1 for a in articles if a.get("pid"))
    print(f"  Com DOI: {with_doi}/{total} ({100*with_doi/max(1,total):.1f}%)")
    print(f"  Com PID: {with_pid}/{total} ({100*with_pid/max(1,total):.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())
