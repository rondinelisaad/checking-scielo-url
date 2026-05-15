#!/usr/bin/env python3
"""
DIAGNÓSTICO: Inspeciona o HTML real do SciELO e mostra a estrutura
para que possamos corrigir os seletores.

Execute: python diagnose.py
"""

import asyncio
import re
import sys
from bs4 import BeautifulSoup
import aiohttp

BASE = "https://www.scielo.br"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0"}


async def main():
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        # ─── 1. Página de periódicos /journals/alpha ───
        print("=" * 60)
        print("1. Analisando https://www.scielo.br/journals/alpha")
        print("=" * 60)

        async with session.get(f"{BASE}/journals/alpha", timeout=aiohttp.ClientTimeout(total=30)) as r:
            print(f"   Status HTTP: {r.status}")
            print(f"   URL final:   {r.url}")
            html = await r.text()

        print(f"   Tamanho HTML: {len(html)} chars")

        # Salva HTML completo para inspeção manual
        with open("debug_journals_alpha.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("   HTML salvo em: debug_journals_alpha.html")

        soup = BeautifulSoup(html, "lxml")

        # Todos os links da página
        all_links = [a.get("href", "") for a in soup.find_all("a", href=True)]
        j_links = [l for l in all_links if re.match(r"^/j/[^/]+/?$", l)]
        print(f"\n   Total de links na página: {len(all_links)}")
        print(f"   Links com padrão /j/<acronym>/: {len(j_links)}")

        if j_links:
            print("\n   Primeiros 10 links /j/:")
            for l in j_links[:10]:
                print(f"     {l}")
        else:
            print("\n   ⚠️  NENHUM link /j/ encontrado!")
            print("\n   Todos os href únicos da página (primeiros 30):")
            unique = list(dict.fromkeys(all_links))[:30]
            for l in unique:
                print(f"     {l}")

        # Tenta padrões alternativos
        print("\n   Buscando padrões alternativos de links de periódicos...")
        patterns = [
            r"/j/",
            r"journal",
            r"/journals/",
            r"scielo\.php",
            r"pid=",
            r"issn",
        ]
        for pat in patterns:
            matches = [l for l in all_links if re.search(pat, l, re.IGNORECASE)]
            if matches:
                print(f"     Padrão '{pat}': {len(matches)} links | ex: {matches[0]}")

        # Mostra estrutura de elementos principais
        print("\n   Tags principais e suas classes/ids:")
        for tag in ["ul", "ol", "div", "section", "article", "table"]:
            elements = soup.find_all(tag, limit=5)
            for el in elements:
                cls = el.get("class", [])
                eid = el.get("id", "")
                if cls or eid:
                    print(f"     <{tag} class='{' '.join(cls)}' id='{eid}'>")

        # ─── 2. Testa uma URL alternativa de listagem ───
        print("\n" + "=" * 60)
        print("2. Testando URLs alternativas")
        print("=" * 60)

        alt_urls = [
            f"{BASE}/journals/",
            f"{BASE}/journals/alpha/",
            f"{BASE}/journals/thematic/",
        ]
        for url in alt_urls:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as r:
                    h = await r.text()
                    j = len(re.findall(r'href="(/j/[^/]+/?)"', h))
                    print(f"   {url} -> HTTP {r.status} | links /j/: {j} | final: {r.url}")
            except Exception as e:
                print(f"   {url} -> ERRO: {e}")

        # ─── 3. Testa acesso direto a um periódico ───
        print("\n" + "=" * 60)
        print("3. Testando acesso direto a https://www.scielo.br/j/abcic/grid")
        print("=" * 60)

        async with session.get(
            f"{BASE}/j/abcic/grid", timeout=aiohttp.ClientTimeout(total=30)
        ) as r:
            print(f"   Status HTTP: {r.status}")
            h2 = await r.text()
            with open("debug_abcic_grid.html", "w", encoding="utf-8") as f:
                f.write(h2)
            print(f"   HTML salvo em: debug_abcic_grid.html")

            soup2 = BeautifulSoup(h2, "lxml")
            issue_links = [
                a["href"]
                for a in soup2.find_all("a", href=re.compile(r"/j/abcic/i/"))
            ]
            print(f"   Links de números encontrados: {len(issue_links)}")
            for l in issue_links[:5]:
                print(f"     {l}")

        # ─── 4. Testa acesso a número específico ───
        print("\n" + "=" * 60)
        print("4. Testando https://www.scielo.br/j/abcic/i/2026.v39n1/")
        print("=" * 60)

        async with session.get(
            f"{BASE}/j/abcic/i/2026.v39n1/", timeout=aiohttp.ClientTimeout(total=30)
        ) as r:
            print(f"   Status HTTP: {r.status}")
            h3 = await r.text()
            with open("debug_abcic_issue.html", "w", encoding="utf-8") as f:
                f.write(h3)
            print(f"   HTML salvo em: debug_abcic_issue.html")

            soup3 = BeautifulSoup(h3, "lxml")
            art_links = [
                a["href"]
                for a in soup3.find_all("a", href=re.compile(r"/j/abcic/a/"))
            ]
            print(f"   Links de artigos encontrados: {len(art_links)}")
            for l in art_links[:10]:
                print(f"     {l}")

        print("\n" + "=" * 60)
        print("✅ Diagnóstico concluído. Arquivos gerados:")
        print("   debug_journals_alpha.html  — página de listagem de periódicos")
        print("   debug_abcic_grid.html      — grid de issues")
        print("   debug_abcic_issue.html     — página de um número")
        print("=" * 60)
        print("\nEnvie o output acima para o Claude corrigir os seletores.")


if __name__ == "__main__":
    asyncio.run(main())
