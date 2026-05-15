#!/usr/bin/env python3
"""
SCIELO MAPPER - Passo 2: Coleta todos os números (issues) de cada periódico
Lê:   data/journals.json
Salva: data/issues.json  ->  lista de {journal_acronym, year, volume, number, url}
"""

import asyncio
import json
import re
import sys
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

BASE = "https://www.scielo.br"
DATA_DIR = Path("data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SciELO-Mapper/1.0)"
    )
}

# Limita concorrência para não sobrecarregar o servidor
SEM_LIMIT = 20


async def fetch(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> str | None:
    async with sem:
        for attempt in range(3):
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=30),
                    allow_redirects=True,
                    ssl=False,
                ) as r:
                    if r.status == 200:
                        return await r.text()
                    elif r.status == 404:
                        return None
            except Exception as e:
                if attempt == 2:
                    print(f"\n  ERRO {url}: {e}", file=sys.stderr)
                await asyncio.sleep(1 * (attempt + 1))
    return None


def parse_issues_from_grid(html: str, acronym: str) -> list[dict]:
    """Extrai issues da página /j/<acronym>/grid"""
    soup = BeautifulSoup(html, "lxml")
    issues = []

    # Links de issues: /j/<acronym>/i/<year>.v<vol>n<num>/
    pattern = re.compile(rf"/j/{re.escape(acronym)}/i/([^/]+)/?")

    seen = set()
    for a in soup.find_all("a", href=pattern):
        href = a["href"]
        m = pattern.search(href)
        if not m:
            continue
        issue_id = m.group(1)
        full_url = f"{BASE}/j/{acronym}/i/{issue_id}/"

        if full_url in seen:
            continue
        seen.add(full_url)

        # Parse do issue_id: 2026.v39n1 ou variações
        year = volume = number = None
        ym = re.match(r"(\d{4})", issue_id)
        if ym:
            year = ym.group(1)
        vm = re.search(r"[vV](\d+)", issue_id)
        if vm:
            volume = vm.group(1)
        nm = re.search(r"[nN](\w+)", issue_id)
        if nm:
            number = nm.group(1)

        issues.append(
            {
                "journal_acronym": acronym,
                "issue_id": issue_id,
                "year": year,
                "volume": volume,
                "number": number,
                "url": full_url,
            }
        )

    return issues


async def process_journal(
    session: aiohttp.ClientSession,
    journal: dict,
    sem: asyncio.Semaphore,
) -> list[dict]:
    acronym = journal["acronym"]
    grid_url = journal["grid_url"]
    html = await fetch(session, grid_url, sem)
    if not html:
        return []
    return parse_issues_from_grid(html, acronym)


async def main():
    journals_file = DATA_DIR / "journals.json"
    if not journals_file.exists():
        print("❌ Execute primeiro: python 01_collect_journals.py")
        sys.exit(1)

    journals = json.loads(journals_file.read_text())
    print("=" * 60)
    print(f"SCIELO MAPPER - Passo 2: Coletando números de {len(journals)} periódicos")
    print("=" * 60)

    sem = asyncio.Semaphore(SEM_LIMIT)
    connector = aiohttp.TCPConnector(limit=SEM_LIMIT, ssl=False)

    all_issues = []
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        tasks = [process_journal(session, j, sem) for j in journals]
        for coro in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Periódicos",
            unit="periódico",
        ):
            issues = await coro
            all_issues.extend(issues)

    out_file = DATA_DIR / "issues.json"
    out_file.write_text(json.dumps(all_issues, ensure_ascii=False, indent=2))

    print(f"\n✅ {len(all_issues)} números encontrados em {len(journals)} periódicos")
    print(f"   Salvo em: {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
