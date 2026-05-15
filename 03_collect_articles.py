#!/usr/bin/env python3
"""
SCIELO MAPPER - Passo 3: Coleta todos os artigos de cada número
Lê:   data/issues.json
Salva: data/articles.json  ->  lista de artigos com URLs HTML, PDF, idiomas

CHECKPOINT ROBUSTO:
  - Salva progresso a cada BATCH_SAVE números processados
  - Ao reiniciar, lê data/articles.json existente e pula issues já feitos
  - Nunca apaga o que já foi coletado
  - Mesmo sem checkpoint explícito, se articles.json existir ele é preservado
"""

import asyncio
import json
import re
import sys
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE = "https://www.scielo.br"
DATA_DIR = Path("data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SEM_LIMIT    = 15   # concorrência (reduzido para evitar timeouts)
BATCH_SAVE   = 100  # salva checkpoint a cada N números processados
TIMEOUT_SECS = 45


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> str | None:
    async with sem:
        for attempt in range(3):
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS),
                    ssl=False,
                    allow_redirects=True,
                ) as r:
                    if r.status == 200:
                        return await r.text()
                    elif r.status in (404, 410):
                        return None
            except asyncio.TimeoutError:
                if attempt == 2:
                    print(f"\n  TIMEOUT {url}", file=sys.stderr)
            except Exception as e:
                if attempt == 2:
                    print(f"\n  ERRO {url}: {e}", file=sys.stderr)
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


def parse_articles_from_issue(html: str, issue: dict) -> list[dict]:
    """Extrai artigos da página de um número."""
    soup = BeautifulSoup(html, "lxml")
    acronym = issue["journal_acronym"]
    articles = []

    art_pattern = re.compile(rf"/j/{re.escape(acronym)}/a/([A-Za-z0-9]+)")
    seen_ids: set[str] = set()

    for a_tag in soup.find_all("a", href=art_pattern):
        href = a_tag.get("href", "")
        m = art_pattern.search(href)
        if not m:
            continue

        art_id = m.group(1)
        if art_id in seen_ids:
            continue
        seen_ids.add(art_id)

        base_url = f"{BASE}/j/{acronym}/a/{art_id}/"

        # Idiomas disponíveis
        langs: set[str] = set()
        lang_pat = re.compile(
            rf"/j/{re.escape(acronym)}/a/{re.escape(art_id)}/\?lang=([a-z]{{2}})"
        )
        for la in soup.find_all("a", href=lang_pat):
            lm = lang_pat.search(la["href"])
            if lm:
                langs.add(lm.group(1))

        # URLs de PDF: padrão SciELO (?format=pdf&lang=XX)
        pdf_urls: list[str] = []
        for lang in (sorted(langs) or ["pt", "en", "es"]):
            pdf_urls.append(f"{base_url}?format=pdf&lang={lang}")

        articles.append(
            {
                "journal_acronym": acronym,
                "issue_id":        issue["issue_id"],
                "year":            issue["year"],
                "volume":          issue["volume"],
                "number":          issue["number"],
                "article_id":      art_id,
                "base_url":        base_url,
                "langs":           sorted(langs) or ["pt"],
                "html_urls":       [f"{base_url}?lang={l}" for l in (sorted(langs) or ["pt"])],
                "pdf_urls":        pdf_urls[:3],
                "pid":             None,   # preenchido no step 4
                "doi":             None,   # preenchido no step 4
                "doi_url":         None,
            }
        )

    return articles


def load_existing(out_file: Path) -> tuple[list[dict], set[str]]:
    """
    Carrega artigos já coletados de execução anterior.
    Retorna (lista_artigos, set_de_issue_ids_processados).
    """
    if not out_file.exists():
        return [], set()
    try:
        articles = json.loads(out_file.read_text(encoding="utf-8"))
        done = {a["issue_id"] for a in articles}
        return articles, done
    except Exception as e:
        print(f"  ⚠️  Erro ao ler arquivo existente: {e}", file=sys.stderr)
        return [], set()


async def main():
    issues_file = DATA_DIR / "issues.json"
    if not issues_file.exists():
        print("❌ Execute primeiro: python 02_collect_issues.py")
        sys.exit(1)

    issues = json.loads(issues_file.read_text(encoding="utf-8"))
    out_file = DATA_DIR / "articles.json"

    print("=" * 60)
    print(f"SCIELO MAPPER - Passo 3: Coletando artigos")
    print("=" * 60)

    # ── Carrega progresso anterior (se existir) ──
    all_articles, done_issues = load_existing(out_file)
    if done_issues:
        print(f"   ♻️  Retomando: {len(done_issues)} números já processados "
              f"({len(all_articles)} artigos coletados até agora)")

    pending = [i for i in issues if i["issue_id"] not in done_issues]
    print(f"   Total issues:   {len(issues)}")
    print(f"   Já processados: {len(done_issues)}")
    print(f"   Pendentes:      {len(pending)}")

    if not pending:
        print(f"\n✅ Todos os números já foram processados!")
        print(f"   Total artigos: {len(all_articles)}")
        return

    sem       = asyncio.Semaphore(SEM_LIMIT)
    connector = aiohttp.TCPConnector(limit=SEM_LIMIT, ssl=False)
    batch_count = 0
    errors      = 0

    async def process_one(issue: dict) -> list[dict]:
        html = await fetch(session, issue["url"], sem)
        if html is None:
            return []
        return parse_articles_from_issue(html, issue)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        tasks = [asyncio.create_task(process_one(iss)) for iss in pending]
        pbar  = tqdm(total=len(tasks), desc="Números", unit="nº")

        for task in asyncio.as_completed(tasks):
            try:
                arts = await task
            except Exception as e:
                arts   = []
                errors += 1
                print(f"\n  TASK ERRO: {e}", file=sys.stderr)

            all_articles.extend(arts)
            batch_count += 1
            pbar.update(1)
            pbar.set_postfix(artigos=len(all_articles), erros=errors)

            # ── Checkpoint frequente: salva a cada BATCH_SAVE números ──
            if batch_count % BATCH_SAVE == 0:
                out_file.write_text(
                    json.dumps(all_articles, ensure_ascii=False),
                    encoding="utf-8",
                )

        pbar.close()

    # ── Salva resultado final com indentação ──
    out_file.write_text(
        json.dumps(all_articles, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"\n✅ Concluído")
    print(f"   Total artigos coletados: {len(all_articles)}")
    print(f"   Erros de fetch:          {errors}")
    print(f"   Salvo em: {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
