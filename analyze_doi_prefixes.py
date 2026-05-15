#!/usr/bin/env python3
"""
ANÁLISE: Erros NOT_FOUND de DOI separados por prefixo.

Lê:   data/broken_urls.json
Gera: data/doi_prefix_analysis.json
      data/doi_prefix_analysis.txt  (relatório legível)

O prefixo de um DOI é a parte antes da barra: 10.1590/xxx -> prefixo = 10.1590
O SciELO usa principalmente o prefixo 10.1590, mas alguns periódicos
têm prefixos próprios.

Uso:
  python analyze_doi_prefixes.py
"""

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path("data")


def extract_prefix(doi: str) -> str:
    """
    Extrai o prefixo do DOI (parte antes da segunda barra).
    Ex: 10.1590/s0102-09352009000100001 -> 10.1590
        10.1590/abc.2020.001            -> 10.1590
        10.15252/embj.201694800         -> 10.15252
    """
    if not doi:
        return "sem_doi"
    # DOI formato: 10.XXXX/sufixo
    m = re.match(r"(10\.\d{4,9})/", doi.strip())
    return m.group(1) if m else "prefixo_invalido"


def analyze(broken: list[dict]) -> dict:
    """
    Analisa erros NOT_FOUND de DOI separados por prefixo.
    Retorna estrutura completa com contagens e porcentagens.
    """
    # Filtra apenas artigos com doi_ok = False
    doi_errors = [r for r in broken if r.get("doi_ok") is False]

    # Dentro desses, pega os que têm status NOT_FOUND
    not_found = []
    other_doi_errors = []
    for r in doi_errors:
        checks = r.get("doi_checks", [])
        statuses = [c.get("status") for c in checks]
        if "NOT_FOUND" in statuses:
            not_found.append(r)
        else:
            other_doi_errors.append(r)

    # Agrupa NOT_FOUND por prefixo
    prefix_counts: dict[str, int]          = defaultdict(int)
    prefix_articles: dict[str, list[dict]] = defaultdict(list)
    prefix_journals: dict[str, set]        = defaultdict(set)

    for r in not_found:
        doi    = r.get("doi") or ""
        prefix = extract_prefix(doi)
        prefix_counts[prefix] += 1
        prefix_journals[prefix].add(r.get("journal_acronym", "?"))
        prefix_articles[prefix].append({
            "article_id":      r.get("article_id"),
            "journal_acronym": r.get("journal_acronym"),
            "year":            r.get("year"),
            "pid":             r.get("pid"),
            "doi":             doi,
            "doi_url":         r.get("doi_url"),
        })

    total_not_found = len(not_found)

    # Monta resultado ordenado por quantidade decrescente
    prefixes = []
    for prefix, count in sorted(prefix_counts.items(), key=lambda x: -x[1]):
        pct      = f"{100 * count / max(1, total_not_found):.2f}%"
        journals = sorted(prefix_journals[prefix])
        prefixes.append({
            "prefix":           prefix,
            "count":            count,
            "pct_of_not_found": pct,
            "journals":         journals,
            "journals_count":   len(journals),
            "articles":         prefix_articles[prefix],
        })

    # Também faz breakdown de TODOS os status de DOI (não só NOT_FOUND)
    all_doi_status: dict[str, int] = defaultdict(int)
    for r in broken:
        for c in r.get("doi_checks", []):
            all_doi_status[c.get("status", "UNKNOWN")] += 1

    return {
        "total_broken_articles":    len(broken),
        "total_doi_errors":         len(doi_errors),
        "total_doi_not_found":      total_not_found,
        "total_doi_other_errors":   len(other_doi_errors),
        "doi_status_breakdown":     dict(sorted(all_doi_status.items(), key=lambda x: -x[1])),
        "not_found_by_prefix":      prefixes,
    }


def write_text_report(result: dict, path: Path) -> None:
    """Gera relatório .txt legível."""
    lines = []
    sep   = "=" * 60

    lines.append(sep)
    lines.append("ANÁLISE DE ERROS DOI NOT_FOUND POR PREFIXO")
    lines.append(sep)
    lines.append("")
    lines.append(f"  Artigos com algum erro (broken_urls.json): {result['total_broken_articles']}")
    lines.append(f"  Artigos com doi_ok=False:                  {result['total_doi_errors']}")
    lines.append(f"  Desses com status NOT_FOUND:               {result['total_doi_not_found']}")
    lines.append(f"  Desses com outro erro (TIMEOUT, etc):      {result['total_doi_other_errors']}")
    lines.append("")

    lines.append("─" * 60)
    lines.append("STATUS DE DOI (todos os erros)")
    lines.append("─" * 60)
    for status, count in result["doi_status_breakdown"].items():
        lines.append(f"  {status:<20}: {count:>7}")
    lines.append("")

    lines.append("─" * 60)
    lines.append("NOT_FOUND POR PREFIXO DE DOI")
    lines.append("─" * 60)
    lines.append(
        f"  {'Prefixo':<18} {'Qtd':>7}  {'% do NOT_FOUND':>14}  "
        f"{'Periódicos':>10}  Exemplos de periódico"
    )
    lines.append("  " + "-" * 78)

    for p in result["not_found_by_prefix"]:
        journal_sample = ", ".join(p["journals"][:5])
        if p["journals_count"] > 5:
            journal_sample += f" ... +{p['journals_count']-5}"
        lines.append(
            f"  {p['prefix']:<18} {p['count']:>7}  {p['pct_of_not_found']:>14}  "
            f"{p['journals_count']:>10}  {journal_sample}"
        )

    lines.append("  " + "-" * 78)
    lines.append(f"  {'TOTAL':<18} {result['total_doi_not_found']:>7}")
    lines.append("")

    # Detalhe por prefixo (artigos individuais)
    for p in result["not_found_by_prefix"]:
        lines.append("")
        lines.append(f"PREFIXO: {p['prefix']}  ({p['count']} artigos, {p['pct_of_not_found']})")
        lines.append(f"  Periódicos afetados ({p['journals_count']}): {', '.join(p['journals'])}")
        lines.append(f"  {'Periódico':<12}  {'Ano':<6}  {'PID':<25}  DOI")
        lines.append("  " + "-" * 80)
        for art in p["articles"][:50]:  # limita 50 por prefixo
            pid = art.get("pid") or "—"
            doi = art.get("doi") or "—"
            acr = art.get("journal_acronym") or "—"
            yr  = art.get("year") or "—"
            lines.append(f"  {acr:<12}  {yr:<6}  {pid:<25}  {doi}")
        if len(p["articles"]) > 50:
            lines.append(f"  ... e mais {len(p['articles'])-50} artigos")

    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    broken_file = DATA_DIR / "broken_urls.json"
    if not broken_file.exists():
        print("❌ data/broken_urls.json não encontrado.")
        print("   Execute primeiro: python 05_check_urls.py")
        sys.exit(1)

    print("Lendo broken_urls.json...", end=" ", flush=True)
    broken = json.loads(broken_file.read_text(encoding="utf-8"))
    print(f"{len(broken):,} artigos com erro")

    print("Analisando prefixos de DOI...", end=" ", flush=True)
    result = analyze(broken)
    print("OK")

    # Salva JSON
    json_file = DATA_DIR / "doi_prefix_analysis.json"
    json_file.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Salva TXT
    txt_file = DATA_DIR / "doi_prefix_analysis.txt"
    write_text_report(result, txt_file)

    # Exibe resumo no terminal
    sep = "=" * 60
    print()
    print(sep)
    print("RESULTADO")
    print(sep)
    print(f"  Total DOI NOT_FOUND: {result['total_doi_not_found']}")
    print()
    print(f"  {'Prefixo':<18} {'Qtd':>7}  {'%':>8}  Periódicos")
    print("  " + "-" * 55)
    for p in result["not_found_by_prefix"]:
        print(f"  {p['prefix']:<18} {p['count']:>7}  {p['pct_of_not_found']:>8}  {p['journals_count']}")
    print("  " + "-" * 55)
    print(f"  {'TOTAL':<18} {result['total_doi_not_found']:>7}")
    print()
    print(f"  Arquivos gerados:")
    print(f"    data/doi_prefix_analysis.json  (dados completos)")
    print(f"    data/doi_prefix_analysis.txt   (relatório legível)")


if __name__ == "__main__":
    main()
