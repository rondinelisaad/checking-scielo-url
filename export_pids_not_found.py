#!/usr/bin/env python3
"""
Gera arquivo com um PID por linha de todos os artigos com:
  - prefixo de DOI = 10.1590
  - status de DOI = NOT_FOUND

Lê:   data/broken_urls.json
Gera: data/pids_doi_not_found_10.1590.txt
"""

import json
import re
import sys
from pathlib import Path

DATA_DIR = Path("data")
PREFIXO  = "10.1590"
OUT_FILE = DATA_DIR / f"pids_doi_not_found_{PREFIXO}.txt"


def main():
    broken_file = DATA_DIR / "broken_urls.json"
    if not broken_file.exists():
        print("❌ data/broken_urls.json não encontrado.")
        sys.exit(1)

    broken = json.loads(broken_file.read_text(encoding="utf-8"))

    pids = []
    sem_pid = []

    for r in broken:
        # Só erros NOT_FOUND no DOI
        doi_checks = r.get("doi_checks", [])
        if not any(c.get("status") == "NOT_FOUND" for c in doi_checks):
            continue

        # Só DOIs com prefixo 10.1590
        doi = r.get("doi") or ""
        if not doi.startswith(PREFIXO + "/"):
            continue

        pid = r.get("pid")
        if pid:
            pids.append(pid)
        else:
            sem_pid.append(r.get("article_id", "?"))

    # Salva um PID por linha
    OUT_FILE.write_text("\n".join(pids) + "\n", encoding="utf-8")

    print(f"PIDs com DOI NOT_FOUND (prefixo {PREFIXO}): {len(pids)}")
    if sem_pid:
        print(f"Artigos sem PID registrado (não incluídos): {len(sem_pid)}")
    print(f"Arquivo gerado: {OUT_FILE}")


if __name__ == "__main__":
    main()
