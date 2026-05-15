#!/usr/bin/env python3
"""
UTILITÁRIO: Regenera check_summary.json a partir do check_results.json existente.

Não faz nenhuma requisição HTTP — apenas relê os dados já coletados
e recalcula o sumário com a estrutura atualizada (incluindo breakdown
por periódico e HTTP codes != 200).

Uso:
  python regenerate_summary.py
"""

import json
import sys
from pathlib import Path

DATA_DIR = Path("data")


def _http_code_bd(results: list[dict]) -> dict:
    def tally(key: str) -> dict:
        codes: dict[str, int] = {}
        total = 0
        for r in results:
            for c in r.get(key, []):
                code = str(c.get("http_code") or "sem_resposta")
                codes[code] = codes.get(code, 0) + 1
                total += 1
        pct_map = {
            code: {"count": n, "pct": f"{100*n/max(1,total):.2f}%"}
            for code, n in sorted(codes.items(), key=lambda x: -x[1])
        }
        return {
            "total_checks": total,
            "http_200":     pct_map.get("200", {"count": 0, "pct": "0.00%"}),
            "non_200":      {k: v for k, v in pct_map.items() if k != "200"},
        }
    return {
        "html": tally("html_checks"),
        "pdf":  tally("pdf_checks"),
        "doi":  tally("doi_checks"),
    }


def _status_bd(results: list[dict], key: str) -> dict[str, int]:
    bd: dict[str, int] = {}
    for r in results:
        for c in r.get(key, []):
            s = c.get("status", "UNKNOWN")
            bd[s] = bd.get(s, 0) + 1
    return dict(sorted(bd.items(), key=lambda x: -x[1]))


def _journals_breakdown(results: list[dict]) -> dict:
    """
    Por periódico com erros: total de artigos, erros por tipo e HTTP codes != 200.
    Ordenado por total de erros decrescente.
    """
    by_journal: dict[str, list[dict]] = {}
    for r in results:
        acr = r.get("journal_acronym", "UNKNOWN")
        by_journal.setdefault(acr, []).append(r)

    breakdown: dict[str, dict] = {}
    for acr, arts in by_journal.items():
        html_f = sum(1 for a in arts if a.get("html_ok") is False)
        pdf_f  = sum(1 for a in arts if a.get("pdf_ok")  is False)
        doi_f  = sum(1 for a in arts if a.get("doi_ok")  is False)
        if html_f + pdf_f + doi_f == 0:
            continue

        def non200(key: str) -> dict[str, int]:
            codes: dict[str, int] = {}
            for a in arts:
                for c in a.get(key, []):
                    code = str(c.get("http_code") or "sem_resposta")
                    if code != "200":
                        codes[code] = codes.get(code, 0) + 1
            return dict(sorted(codes.items(), key=lambda x: -x[1]))

        breakdown[acr] = {
            "total_articles": len(arts),
            "errors": {
                "html":  html_f,
                "pdf":   pdf_f,
                "doi":   doi_f,
                "total": html_f + pdf_f + doi_f,
            },
            "http_codes_non200": {
                "html": non200("html_checks"),
                "pdf":  non200("pdf_checks"),
                "doi":  non200("doi_checks"),
            },
        }

    return dict(sorted(breakdown.items(), key=lambda x: -x[1]["errors"]["total"]))


def build_summary(results: list[dict]) -> dict:
    total = len(results)
    pct   = lambda a, b: f"{100*a/max(1,a+b):.1f}%"
    def ct(field, val): return sum(1 for r in results if r.get(field) is val)

    html_ok = ct("html_ok", True);  html_f = ct("html_ok", False)
    pdf_ok  = ct("pdf_ok",  True);  pdf_f  = ct("pdf_ok",  False)
    doi_ok  = ct("doi_ok",  True);  doi_f  = ct("doi_ok",  False)
    doi_na  = ct("doi_ok",  None)
    all_ok  = ct("all_ok",  True)
    hcbd    = _http_code_bd(results)

    return {
        "total_articles": total,
        "all_ok":         all_ok,
        "html": {
            "ok": html_ok, "fail": html_f, "pct": pct(html_ok, html_f),
            "status_breakdown":    _status_bd(results, "html_checks"),
            "http_code_breakdown": hcbd["html"],
        },
        "pdf": {
            "ok": pdf_ok, "fail": pdf_f, "pct": pct(pdf_ok, pdf_f),
            "status_breakdown":    _status_bd(results, "pdf_checks"),
            "http_code_breakdown": hcbd["pdf"],
        },
        "doi": {
            "ok": doi_ok, "fail": doi_f, "na": doi_na,
            "pct": pct(doi_ok, doi_f),
            "status_breakdown":    _status_bd(results, "doi_checks"),
            "http_code_breakdown": hcbd["doi"],
        },
        "journals_with_errors": _journals_breakdown(results),
    }


def main():
    results_file = DATA_DIR / "check_results.json"
    if not results_file.exists():
        print("❌ data/check_results.json não encontrado.")
        print("   Execute primeiro: python 05_check_urls.py")
        sys.exit(1)

    print("Lendo check_results.json...", end=" ", flush=True)
    results = json.loads(results_file.read_text(encoding="utf-8"))
    print(f"{len(results):,} artigos")

    print("Calculando sumário...", end=" ", flush=True)
    summary = build_summary(results)
    print("OK")

    # Salva check_summary.json
    summary_file = DATA_DIR / "check_summary.json"
    summary_file.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Recalcula broken_urls.json também
    broken = [r for r in results
              if r.get("html_ok") is False
              or r.get("pdf_ok")  is False
              or r.get("doi_ok")  is False]
    broken_file = DATA_DIR / "broken_urls.json"
    broken_file.write_text(
        json.dumps(broken, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Exibe resultado
    s    = summary
    html = s["html"]
    pdf  = s["pdf"]
    doi  = s["doi"]
    jwe  = s["journals_with_errors"]
    sep  = "=" * 60

    print()
    print(sep)
    print("SUMÁRIO")
    print(sep)
    print("  Total artigos:  " + str(s["total_articles"]))
    print("  100% OK:        " + str(s["all_ok"]))
    print()
    print("  HTML:  " + str(html["ok"]) + " OK / " + str(html["fail"]) + " falhas (" + html["pct"] + ")")
    print("  PDF:   " + str(pdf["ok"])  + " OK / " + str(pdf["fail"])  + " falhas (" + pdf["pct"]  + ")")
    print("  DOI:   " + str(doi["ok"])  + " OK / " + str(doi["fail"])  + " falhas (" + doi["pct"]  + ") | " + str(doi["na"]) + " sem DOI")

    # HTTP codes != 200
    for tipo, section in (("HTML", html), ("PDF", pdf), ("DOI", doi)):
        non200 = section["http_code_breakdown"].get("non_200", {})
        if non200:
            print()
            print("  HTTP codes != 200 [" + tipo + "]:")
            for code, info in non200.items():
                print("    " + code.rjust(15) + ": " + str(info["count"]).rjust(7) + "  (" + info["pct"] + ")")

    # Periódicos com erros
    print()
    print("  Periódicos com erros: " + str(len(jwe)))
    if jwe:
        print()
        print("  " + "-" * 56)
        print("  " + "Periódico".ljust(12) + "Total art.".rjust(10) +
              "HTML err".rjust(10) + "PDF err".rjust(9) + "DOI err".rjust(9) + "TOTAL".rjust(8))
        print("  " + "-" * 56)
        for acr, info in list(jwe.items())[:30]:   # top 30
            e = info["errors"]
            print("  " + acr.ljust(12) +
                  str(info["total_articles"]).rjust(10) +
                  str(e["html"]).rjust(10) +
                  str(e["pdf"]).rjust(9) +
                  str(e["doi"]).rjust(9) +
                  str(e["total"]).rjust(8))
        if len(jwe) > 30:
            print("  ... e mais " + str(len(jwe) - 30) + " periódicos")
        print("  " + "-" * 56)

    print()
    print("  Arquivos atualizados:")
    print("    data/check_summary.json  (" + str(s["total_articles"]) + " artigos)")
    print("    data/broken_urls.json    (" + str(len(broken)) + " com problema)")


if __name__ == "__main__":
    main()
