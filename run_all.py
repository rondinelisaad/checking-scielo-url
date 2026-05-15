#!/usr/bin/env python3
"""
SCIELO FULL PIPELINE — Executa todos os passos em sequência

Uso:
  python run_all.py                    # pipeline completo
  python run_all.py --start 3          # começa do passo 3
  python run_all.py --only 5           # só o passo 5
  python run_all.py --test --limit 100 # testa com 100 artigos
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

STEPS = [
    (1, "01_collect_journals.py",  "Coletar periódicos"),
    (2, "02_collect_issues.py",    "Coletar números (issues)"),
    (3, "03_collect_articles.py",  "Coletar artigos"),
    (4, "04_extract_dois.py",      "Extrair DOIs e PIDs"),
    (5, "05_check_urls.py",        "Verificar URLs"),
    (6, "06_recheck_errors.py",    "Reverificar erros"),
    (7, "07_generate_report.py",   "Gerar relatório HTML"),
]

SCIELO_DIR = Path(__file__).parent


def run_step(script: str, extra_args: list[str] = []):
    cmd = [sys.executable, SCIELO_DIR / script] + extra_args
    start = time.time()
    result = subprocess.run(cmd, cwd=SCIELO_DIR)
    elapsed = time.time() - start
    return result.returncode, elapsed


def main():
    parser = argparse.ArgumentParser(description="SciELO Full Pipeline")
    parser.add_argument("--start", type=int, default=1, help="Começa do passo N")
    parser.add_argument("--only",  type=int, default=None, help="Executa só o passo N")
    parser.add_argument("--test",  action="store_true", help="Modo teste rápido")
    parser.add_argument("--limit", type=int, default=50, help="Limite de artigos no modo teste")
    args = parser.parse_args()

    print("=" * 60)
    print("🔬 SCIELO BRASIL — AUDITORIA COMPLETA DE URLs")
    print("=" * 60)
    print()

    steps_to_run = STEPS
    if args.only:
        steps_to_run = [s for s in STEPS if s[0] == args.only]
    else:
        steps_to_run = [s for s in STEPS if s[0] >= args.start]

    total_start = time.time()

    for step_num, script, desc in steps_to_run:
        print(f"\n{'─'*60}")
        print(f"PASSO {step_num}: {desc}")
        print(f"{'─'*60}")

        extra = []
        if args.test and step_num == 5:
            extra = ["--limit", str(args.limit)]

        code, elapsed = run_step(script, extra)
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)

        if code != 0:
            print(f"\n❌ PASSO {step_num} FALHOU (código {code}) após {mins}m{secs}s")
            print("   Corrija o erro e execute novamente com --start", step_num)
            sys.exit(code)
        else:
            print(f"\n✅ PASSO {step_num} concluído em {mins}m{secs}s")

    total = time.time() - total_start
    total_mins = int(total // 60)

    print(f"\n{'='*60}")
    print(f"🎉 PIPELINE COMPLETO em {total_mins} minutos")
    print(f"{'='*60}")
    print()
    print("  Arquivos gerados em ./data/:")
    print("    journals.json          — lista de periódicos")
    print("    issues.json            — lista de números")
    print("    articles_with_doi.json — todos os artigos com DOIs")
    print("    check_results.json     — resultado de cada verificação")
    print("    broken_urls.json       — URLs com problema")
    print("    check_summary.json     — resumo estatístico")
    print()
    print("  Relatório: relatorio_scielo.html")
    print("  → Abra no browser para ver o painel interativo")


if __name__ == "__main__":
    main()
