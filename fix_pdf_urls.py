#!/usr/bin/env python3
"""
FIX: Corrige URLs de PDF erradas nos arquivos já gerados e limpa
resultados inválidos do check_results.json para reprocessamento.

Problema: versões antigas do pipeline geravam pdf_urls com:
  - /abstract/?lang=xx  (página HTML, não PDF)
  - /?lang=xx           (sem format=pdf)
  - Duplicatas após correção

Solução: reconstrói pdf_urls no padrão canônico /?format=pdf&lang=xx
e remove do check_results.json os artigos cujos pdf_checks
tinham URL errada, para o 05 reprocessar apenas esses.

Uso:
  python fix_pdf_urls.py
  python 05_check_urls.py   # reprocessa só os pendentes
"""

import json
import re
from pathlib import Path

DATA_DIR = Path("data")


def is_bad_pdf_url(url: str) -> bool:
    return "/abstract/" in url or "format=pdf" not in url


def build_correct_pdf_urls(article: dict) -> list[str]:
    """Reconstrói pdf_urls canônicas sem duplicatas."""
    base_url = article.get("base_url") or (
        f"https://www.scielo.br/j/{article['journal_acronym']}"
        f"/a/{article['article_id']}/"
    )
    langs = article.get("langs") or ["pt", "en", "es"]

    seen: set[str] = set()
    result: list[str] = []
    for url in article.get("pdf_urls", []):
        if is_bad_pdf_url(url):
            m = re.search(r"lang=([a-z]{2})", url)
            lang = m.group(1) if m else langs[0]
            url = f"{base_url}?format=pdf&lang={lang}"
        if url not in seen:
            seen.add(url)
            result.append(url)

    if not result:
        result = [f"{base_url}?format=pdf&lang={l}" for l in langs[:2]]

    return result[:3]


def fix_articles_file(path: Path) -> int:
    if not path.exists():
        return 0
    articles = json.loads(path.read_text(encoding="utf-8"))
    fixed = 0
    for a in articles:
        old = a.get("pdf_urls", [])
        new = build_correct_pdf_urls(a)
        if old != new:
            a["pdf_urls"] = new
            fixed += 1
    if fixed:
        path.write_text(json.dumps(articles, ensure_ascii=False, indent=2), encoding="utf-8")
    return fixed


def fix_check_results(check_file: Path, articles_map: dict) -> int:
    """
    Remove do check_results.json os artigos com pdf_checks ruins para reprocessar:
      - URL errada (sem format=pdf, com /abstract/)
      - URL correta ainda não testada
      - pdf_ok=False com status PDF_INVALID (pode ser falso positivo de detecção bugada)
    O artigo é removido por completo → 05 reprocessa só esses.
    """
    if not check_file.exists():
        return 0

    results = json.loads(check_file.read_text(encoding="utf-8"))
    clean   = []
    removed = 0

    for r in results:
        pdf_checks = r.get("pdf_checks", [])
        has_bad_url = any(is_bad_pdf_url(c["url"]) for c in pdf_checks)

        # URL correta ainda não testada
        art = articles_map.get(r["article_id"], {})
        correct_urls = {u for u in build_correct_pdf_urls(art)} if art else set()
        tested_urls  = {c["url"] for c in pdf_checks}
        missing_correct = correct_urls - tested_urls

        # PDF_INVALID pode ser falso positivo da versão antiga (read_bytes=8, sem Content-Type)
        has_invalid = any(c.get("status") == "PDF_INVALID" for c in pdf_checks)

        if has_bad_url or missing_correct or has_invalid:
            removed += 1
        else:
            clean.append(r)

    if removed:
        check_file.write_text(
            json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # Atualiza broken_urls
        broken = [r for r in clean
                  if r.get("html_ok") is False
                  or r.get("pdf_ok")  is False
                  or r.get("doi_ok")  is False]
        (DATA_DIR / "broken_urls.json").write_text(
            json.dumps(broken, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return removed


def main():
    print("=" * 60)
    print("FIX: Corrigindo URLs de PDF erradas")
    print("=" * 60)

    # 1. Corrige articles.json
    n1 = fix_articles_file(DATA_DIR / "articles.json")
    print(f"  articles.json:          {n1} artigos corrigidos" if n1
          else "  articles.json:          ✅ sem URLs erradas")

    # 2. Corrige articles_with_doi.json
    n2 = fix_articles_file(DATA_DIR / "articles_with_doi.json")
    print(f"  articles_with_doi.json: {n2} artigos corrigidos" if n2
          else "  articles_with_doi.json: ✅ sem URLs erradas")

    # 3. Limpa check_results.json
    art_file = DATA_DIR / "articles_with_doi.json"
    if not art_file.exists():
        art_file = DATA_DIR / "articles.json"

    if art_file.exists():
        arts = json.loads(art_file.read_text(encoding="utf-8"))
        arts_map = {a["article_id"]: a for a in arts}
        n3 = fix_check_results(DATA_DIR / "check_results.json", arts_map)
        if n3:
            print(f"  check_results.json:     {n3} artigos removidos para reprocessar")
        else:
            print(f"  check_results.json:     ✅ sem entradas inválidas")
    else:
        print("  check_results.json:     ℹ️  artigos não encontrados para referência")

    print()
    if n1 or n2:
        print("  ✅ JSONs de artigos corrigidos.")
    print("  Próximo passo: python 05_check_urls.py")
    print("  (reprocessa apenas os artigos pendentes/removidos)")


if __name__ == "__main__":
    main()
