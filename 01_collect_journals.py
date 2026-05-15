#!/usr/bin/env python3
"""
SCIELO MAPPER - Passo 1: Coleta todos os periódicos
Salva: data/journals.json  ->  lista de {name, acronym, url}

Estratégia: a página /journals/alpha tem ~864KB e contém todos os periódicos
de uma vez só. Fazemos UMA requisição e extraímos os acrônimos via regex,
depois buscamos os nomes visitando cada /j/<acronym>/ individualmente
(ou extraímos do próprio HTML da listagem).
"""

import asyncio
import json
import re
import sys
import urllib.request
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

BASE = "https://www.scielo.br"
OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SEM_LIMIT = 20


def fetch_with_urllib(url: str) -> str:
    """Usa urllib para buscar o HTML (funciona quando aiohttp é bloqueado)."""
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def extract_journals_from_html(html: str) -> list[dict]:
    """
    Extrai TODOS os periódicos (ativos + descontinuados) do HTML da
    página /journals/alpha.

    A página é uma SPA renderizada por JS (Underscore templates), mas o
    HTML estático entregue pelo servidor já contém todos os periódicos
    renderizados de uma vez (~864KB). O filtro Ativos/Descontinuados
    é feito apenas no frontend — basta parsear todos os links /j/<acronym>/
    sem se preocupar com o status.

    Padrão observado:
      <a href="/j/abcic/">ABC Imagem Cardiovascular</a>   <- nome do periódico
      <a href="/j/abcic/grid">5 números</a>               <- grid (ignorar)
      <a href="/j/abcic/">                                <- ícone home (sem texto útil)

    Estratégia:
      1. Encontra todos os links /j/<acronym>/ com texto >= 4 chars
         que não sejam palavras de navegação → extrai nome + acrônimo
      2. Para acrônimos sem nome (apenas ícone), usa o acrônimo como
         fallback — depois o enrich_journal_name corrige
    """
    soup = BeautifulSoup(html, "lxml")

    # Padrão estrito: /j/<acronym>/ sem nenhum subpath
    pattern = re.compile(r"^/j/([A-Za-z0-9_-]+)/?$")

    # Palavras que indicam link de navegação, não nome do periódico
    NAV_WORDS = {"grid", "about", "rss", "more", "ver", "home", "submit",
                 "contact", "instructions", "editorial", "submission"}

    seen = set()
    journals = []

    for a in soup.find_all("a", href=pattern):
        href = a.get("href", "")
        m = pattern.match(href)
        if not m:
            continue
        acronym = m.group(1)
        if acronym in seen:
            continue

        name = a.get_text(strip=True)

        # Pula links sem texto útil ou com texto de navegação
        if not name or len(name) < 4:
            continue
        if name.lower() in NAV_WORDS:
            continue
        # Pula se for só dígitos ou só o próprio acrônimo
        if name.lower() == acronym.lower():
            continue

        seen.add(acronym)
        journals.append(
            {
                "name": name,
                "acronym": acronym,
                "url": f"{BASE}/j/{acronym}/",
                "grid_url": f"{BASE}/j/{acronym}/grid",
                "status": None,  # será "active" ou "discontinued" se necessário
            }
        )

    # Captura também acrônimos que só aparecem como ícones (sem texto),
    # usando links /j/<acronym>/grid como referência
    grid_pattern = re.compile(r"^/j/([A-Za-z0-9_-]+)/grid/?$")
    for a in soup.find_all("a", href=grid_pattern):
        m = grid_pattern.match(a.get("href", ""))
        if not m:
            continue
        acronym = m.group(1)
        if acronym not in seen:
            seen.add(acronym)
            journals.append(
                {
                    "name": acronym,   # será enriquecido depois
                    "acronym": acronym,
                    "url": f"{BASE}/j/{acronym}/",
                    "grid_url": f"{BASE}/j/{acronym}/grid",
                    "status": None,
                }
            )

    return journals


async def enrich_journal_name(
    session: aiohttp.ClientSession,
    journal: dict,
    sem: asyncio.Semaphore,
) -> dict:
    """
    Se o nome ficou em branco ou suspeito, visita /j/<acronym>/ para
    pegar o título correto do <title> ou <h1>.
    """
    name = journal.get("name", "").strip()
    # Nome parece ok se tem mais de 5 chars e não é só o acrônimo
    if len(name) > 5 and name.lower() != journal["acronym"].lower():
        return journal

    async with sem:
        try:
            async with session.get(
                journal["url"], timeout=aiohttp.ClientTimeout(total=20)
            ) as r:
                if r.status != 200:
                    return journal
                html = await r.text()
            soup = BeautifulSoup(html, "lxml")
            # Tenta <h1>, depois <title>
            h1 = soup.find("h1")
            if h1 and len(h1.get_text(strip=True)) > 5:
                journal = journal.copy()
                journal["name"] = h1.get_text(strip=True)
            else:
                title = soup.find("title")
                if title:
                    t = title.get_text(strip=True).split("|")[0].strip()
                    if t:
                        journal = journal.copy()
                        journal["name"] = t
        except Exception:
            pass
    return journal


async def main():
    print("=" * 60)
    print("SCIELO MAPPER - Passo 1: Coletando periódicos")
    print("=" * 60)

    all_journals = []
    seen_acronyms = set()

    # Busca ativos E descontinuados separadamente para marcar o status
    sources = [
        (f"{BASE}/journals/alpha",                  "active"),
        (f"{BASE}/journals/alpha?status=no-current","discontinued"),
    ]

    for url, status in sources:
        print(f"\n   Buscando {url} ...")
        try:
            html = fetch_with_urllib(url)
            print(f"   HTML recebido: {len(html):,} chars")
        except Exception as e:
            print(f"   ERRO ao buscar {url}: {e}", file=sys.stderr)
            continue

        journals = extract_journals_from_html(html)
        print(f"   Periódicos extraídos ({status}): {len(journals)}")

        if not journals:
            print(f"   ⚠️  Nenhum periódico encontrado nesta URL!")
            (OUT_DIR / f"debug_{status}.html").write_text(html, encoding="utf-8")
            continue

        for j in journals:
            if j["acronym"] not in seen_acronyms:
                seen_acronyms.add(j["acronym"])
                j["status"] = status
                all_journals.append(j)

    if not all_journals:
        print("\n❌ Nenhum periódico encontrado em nenhuma URL!")
        sys.exit(1)

    print(f"\n   Total antes de enriquecer: {len(all_journals)} periódicos")

    # Enriquece nomes que ainda são só o acrônimo
    sem = asyncio.Semaphore(SEM_LIMIT)
    connector = aiohttp.TCPConnector(limit=SEM_LIMIT, ssl=False)
    needs_enrichment = [j for j in all_journals if len(j.get("name", "")) <= 5
                        or j["name"] == j["acronym"]]

    if needs_enrichment:
        print(f"   Enriquecendo {len(needs_enrichment)} nomes incompletos...")
        async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
            tasks = [enrich_journal_name(session, j, sem) for j in needs_enrichment]
            enriched = await asyncio.gather(*tasks)

        enriched_map = {j["acronym"]: j for j in enriched}
        all_journals = [enriched_map.get(j["acronym"], j) for j in all_journals]

    all_journals.sort(key=lambda x: x["acronym"])

    out_file = OUT_DIR / "journals.json"
    out_file.write_text(json.dumps(all_journals, ensure_ascii=False, indent=2))

    active = sum(1 for j in all_journals if j.get("status") == "active")
    discontinued = sum(1 for j in all_journals if j.get("status") == "discontinued")

    print(f"\n✅ {len(all_journals)} periódicos salvos em {out_file}")
    print(f"   Ativos:         {active}")
    print(f"   Descontinuados: {discontinued}")
    print("\n   Primeiros 10:")
    for j in all_journals[:10]:
        print(f"   - [{j['acronym']}] {j['name']} ({j.get('status','')})")
    if len(all_journals) > 10:
        print(f"   ... e mais {len(all_journals) - 10}")


if __name__ == "__main__":
    asyncio.run(main())
