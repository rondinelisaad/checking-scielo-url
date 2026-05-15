#!/usr/bin/env python3
"""
SCIELO REPORTER - Passo 6: Gera relatório HTML interativo
Lê:   data/check_results.json, data/check_summary.json, data/broken_urls.json
Gera: relatorio_scielo.html
"""

import json
import sys
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data")


def load_json(path):
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def generate_report():
    summary = load_json(DATA_DIR / "check_summary.json")
    broken = load_json(DATA_DIR / "broken_urls.json") or []
    results = load_json(DATA_DIR / "check_results.json") or []

    if not summary:
        print("❌ Execute o passo 5 primeiro: python 05_check_urls.py")
        sys.exit(1)

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    total = summary["total_articles"]
    all_ok = summary["all_ok"]

    html_ok = summary["html"]["ok"]
    html_fail = summary["html"]["fail"]
    html_pct = summary["html"]["pct"]

    pdf_ok = summary["pdf"]["ok"]
    pdf_fail = summary["pdf"]["fail"]
    pdf_pct = summary["pdf"]["pct"]

    doi_ok = summary["doi"]["ok"]
    doi_fail = summary["doi"]["fail"]
    doi_na = summary["doi"]["na"]
    doi_pct = summary["doi"]["pct"]

    # Por periódico
    journal_stats = {}
    for r in results:
        acr = r["journal_acronym"]
        if acr not in journal_stats:
            journal_stats[acr] = {
                "acronym": acr, "total": 0, "html_fail": 0,
                "pdf_fail": 0, "doi_fail": 0
            }
        s = journal_stats[acr]
        s["total"] += 1
        if r["html_ok"] is False:
            s["html_fail"] += 1
        if r["pdf_ok"] is False:
            s["pdf_fail"] += 1
        if r["doi_ok"] is False:
            s["doi_fail"] += 1

    journals_with_issues = sorted(
        [j for j in journal_stats.values() if j["html_fail"] + j["pdf_fail"] + j["doi_fail"] > 0],
        key=lambda x: x["html_fail"] + x["pdf_fail"] + x["doi_fail"],
        reverse=True
    )

    # Constrói tabela de URLs quebradas
    broken_rows = ""
    for b in broken[:5000]:  # limita para não explodir o HTML
        article_url = b.get("html_checks", [{}])[0].get("url", "#") if b.get("html_checks") else "#"
        doi_status = b.get("doi_checks", [{}])[0].get("status", "N/A") if b.get("doi_checks") else "N/A"

        html_badge = _badge(b["html_ok"])
        pdf_badge = _badge(b["pdf_ok"])
        doi_badge = _badge(b["doi_ok"])

        doi_link = f'<a href="{b["doi_url"]}" target="_blank">{b["doi"] or "—"}</a>' if b.get("doi_url") else "—"
        art_link = f'<a href="{article_url}" target="_blank">{b["article_id"]}</a>'

        pid_val = b.get("pid") or "—"
        broken_rows += f"""<tr>
          <td>{b["journal_acronym"]}</td>
          <td>{b.get("year", "—")}</td>
          <td>{art_link}</td>
          <td><code style="font-size:.75rem">{pid_val}</code></td>
          <td>{doi_link}</td>
          <td>{html_badge}</td>
          <td>{pdf_badge}</td>
          <td>{doi_badge} <small>{doi_status}</small></td>
        </tr>\n"""

    # Periódicos com problemas
    journal_rows = ""
    for j in journals_with_issues[:200]:
        total_issues = j["html_fail"] + j["pdf_fail"] + j["doi_fail"]
        journal_rows += f"""<tr>
          <td><a href="https://www.scielo.br/j/{j['acronym']}/grid" target="_blank">{j['acronym']}</a></td>
          <td>{j['total']}</td>
          <td class="{'fail' if j['html_fail'] else ''}">{j['html_fail']}</td>
          <td class="{'fail' if j['pdf_fail'] else ''}">{j['pdf_fail']}</td>
          <td class="{'fail' if j['doi_fail'] else ''}">{j['doi_fail']}</td>
          <td><strong>{total_issues}</strong></td>
        </tr>\n"""

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SciELO Brasil — Relatório de Auditoria de URLs</title>
<style>
  :root {{
    --green: #22c55e; --red: #ef4444; --yellow: #f59e0b;
    --gray: #6b7280; --blue: #3b82f6; --dark: #1e293b;
    --light: #f8fafc; --border: #e2e8f0;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, sans-serif; background: var(--light); color: var(--dark); }}
  header {{ background: var(--dark); color: white; padding: 2rem; }}
  header h1 {{ font-size: 1.8rem; margin-bottom: 0.25rem; }}
  header p {{ opacity: 0.7; font-size: 0.9rem; }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 2rem; }}

  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; margin: 2rem 0; }}
  .card {{ background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 4px rgba(0,0,0,.08); border-left: 4px solid var(--blue); }}
  .card.green {{ border-color: var(--green); }}
  .card.red {{ border-color: var(--red); }}
  .card.yellow {{ border-color: var(--yellow); }}
  .card h3 {{ font-size: 0.8rem; text-transform: uppercase; letter-spacing: .05em; color: var(--gray); margin-bottom: .5rem; }}
  .card .num {{ font-size: 2.2rem; font-weight: 700; line-height: 1; }}
  .card .sub {{ font-size: 0.8rem; color: var(--gray); margin-top: .4rem; }}

  .section {{ background: white; border-radius: 12px; padding: 1.5rem; margin-bottom: 2rem; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  .section h2 {{ font-size: 1.1rem; margin-bottom: 1rem; padding-bottom: .5rem; border-bottom: 2px solid var(--border); }}

  .progress-bar {{ background: var(--border); border-radius: 99px; height: 8px; overflow: hidden; margin: .4rem 0; }}
  .progress-fill {{ height: 100%; border-radius: 99px; }}
  .progress-fill.green {{ background: var(--green); }}
  .progress-fill.red {{ background: var(--red); }}

  .metric-row {{ display: flex; align-items: center; gap: 1rem; margin: .8rem 0; }}
  .metric-label {{ width: 60px; font-weight: 600; font-size: .85rem; }}
  .metric-bar {{ flex: 1; }}
  .metric-pct {{ width: 55px; text-align: right; font-size: .85rem; color: var(--gray); }}
  .metric-counts {{ font-size: .8rem; color: var(--gray); }}

  table {{ width: 100%; border-collapse: collapse; font-size: .85rem; }}
  th {{ background: var(--light); padding: .6rem .8rem; text-align: left; font-weight: 600; border-bottom: 2px solid var(--border); white-space: nowrap; }}
  td {{ padding: .5rem .8rem; border-bottom: 1px solid var(--border); }}
  tr:hover td {{ background: #f0f9ff; }}
  td.fail {{ color: var(--red); font-weight: 600; }}

  .badge {{ display: inline-block; padding: .15rem .5rem; border-radius: 4px; font-size: .75rem; font-weight: 600; }}
  .badge.ok {{ background: #dcfce7; color: #166534; }}
  .badge.fail {{ background: #fee2e2; color: #991b1b; }}
  .badge.na {{ background: #f3f4f6; color: #6b7280; }}

  .search-box {{ width: 100%; padding: .6rem 1rem; border: 1px solid var(--border); border-radius: 8px; font-size: .9rem; margin-bottom: 1rem; }}
  .tabs {{ display: flex; gap: .5rem; margin-bottom: 1.5rem; flex-wrap: wrap; }}
  .tab {{ padding: .5rem 1rem; border-radius: 8px; border: 1px solid var(--border); cursor: pointer; font-size: .85rem; background: white; }}
  .tab.active {{ background: var(--dark); color: white; border-color: var(--dark); }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}
  .overflow {{ overflow-x: auto; }}

  @media (max-width: 768px) {{
    .cards {{ grid-template-columns: repeat(2, 1fr); }}
  }}
</style>
</head>
<body>
<header>
  <h1>🔬 SciELO Brasil — Auditoria de URLs</h1>
  <p>Gerado em {now} &nbsp;|&nbsp; {total:,} artigos verificados</p>
</header>

<div class="container">

  <!-- Cards de resumo -->
  <div class="cards">
    <div class="card green">
      <h3>Artigos verificados</h3>
      <div class="num">{total:,}</div>
    </div>
    <div class="card {'green' if all_ok == total else 'red'}">
      <h3>100% funcionando</h3>
      <div class="num">{all_ok:,}</div>
      <div class="sub">{100*all_ok/max(1,total):.1f}% do total</div>
    </div>
    <div class="card {'green' if html_fail==0 else 'red'}">
      <h3>HTML com problema</h3>
      <div class="num">{html_fail:,}</div>
      <div class="sub">{html_pct} OK</div>
    </div>
    <div class="card {'green' if pdf_fail==0 else 'red'}">
      <h3>PDF com problema</h3>
      <div class="num">{pdf_fail:,}</div>
      <div class="sub">{pdf_pct} OK</div>
    </div>
    <div class="card {'green' if doi_fail==0 else 'red'}">
      <h3>DOI com problema</h3>
      <div class="num">{doi_fail:,}</div>
      <div class="sub">{doi_pct} OK &nbsp;|&nbsp; {doi_na} sem DOI</div>
    </div>
  </div>

  <!-- Barras de progresso -->
  <div class="section">
    <h2>Taxa de funcionamento por tipo</h2>
    {_metric_row("HTML", html_ok, html_fail, html_pct)}
    {_metric_row("PDF", pdf_ok, pdf_fail, pdf_pct)}
    {_metric_row("DOI", doi_ok, doi_fail, doi_pct)}
  </div>

  <!-- Abas principais -->
  <div class="tabs">
    <button class="tab active" onclick="showTab('broken')">
      ⚠️ URLs com problema ({len(broken):,})
    </button>
    <button class="tab" onclick="showTab('journals')">
      📰 Periódicos afetados ({len(journals_with_issues):,})
    </button>
    <button class="tab" onclick="showTab('doi-status')">
      🔗 Status DOI detalhado
    </button>
  </div>

  <!-- Tab: broken -->
  <div id="tab-broken" class="tab-content active section">
    <h2>URLs com problema</h2>
    <input class="search-box" type="text" placeholder="Filtrar por ID, periódico, DOI..." oninput="filterTable('broken-table', this.value)">
    <div class="overflow">
    <table id="broken-table">
      <thead><tr>
        <th>Periódico</th><th>Ano</th><th>Artigo</th>
        <th>PID</th><th>DOI</th><th>HTML</th><th>PDF</th><th>DOI status</th>
      </tr></thead>
      <tbody>
        {broken_rows if broken_rows else '<tr><td colspan="7" style="text-align:center;padding:2rem;color:#22c55e">✅ Nenhuma URL com problema!</td></tr>'}
      </tbody>
    </table>
    </div>
    {f'<p style="margin-top:.8rem;color:#6b7280;font-size:.8rem">Mostrando até 5.000 de {len(broken):,} URLs com problema</p>' if len(broken) > 5000 else ''}
  </div>

  <!-- Tab: journals -->
  <div id="tab-journals" class="tab-content section">
    <h2>Periódicos com URLs problemáticas</h2>
    <div class="overflow">
    <table>
      <thead><tr>
        <th>Periódico</th><th>Total artigos</th>
        <th>HTML falhas</th><th>PDF falhas</th><th>DOI falhas</th><th>Total falhas</th>
      </tr></thead>
      <tbody>
        {journal_rows if journal_rows else '<tr><td colspan="6" style="text-align:center;padding:2rem;color:#22c55e">✅ Nenhum periódico com problemas!</td></tr>'}
      </tbody>
    </table>
    </div>
  </div>

  <!-- Tab: doi-status -->
  <div id="tab-doi-status" class="tab-content section">
    <h2>Breakdown de status dos DOIs</h2>
    <table style="max-width:600px">
      <thead><tr><th>Status</th><th>Quantidade</th><th>Descrição</th></tr></thead>
      <tbody>
        {_doi_breakdown_rows(summary['doi']['status_breakdown'])}
      </tbody>
    </table>
    <br>
    <p style="font-size:.85rem;color:#6b7280">
      <strong>OK</strong> — DOI resolve e chega no artigo<br>
      <strong>DOI_ERROR</strong> — doi.org retornou "DOI Not Found"<br>
      <strong>DOI_UNRESOLVED</strong> — DOI não redirecionou para o artigo<br>
      <strong>NOT_FOUND</strong> — HTTP 404<br>
      <strong>TIMEOUT</strong> — sem resposta em 30s<br>
      <strong>ERROR</strong> — outro erro HTTP
    </p>
  </div>

</div><!-- /container -->

<script>
function showTab(name) {{
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
}}

function filterTable(tableId, query) {{
  const q = query.toLowerCase();
  document.querySelectorAll('#' + tableId + ' tbody tr').forEach(row => {{
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  }});
}}
</script>
</body>
</html>"""

    out_file = Path("relatorio_scielo.html")
    out_file.write_text(html, encoding="utf-8")
    print(f"✅ Relatório gerado: {out_file}")
    print(f"   Abra no browser: file://{out_file.absolute()}")


def _badge(status):
    if status is True:
        return '<span class="badge ok">OK</span>'
    elif status is False:
        return '<span class="badge fail">FALHA</span>'
    else:
        return '<span class="badge na">N/A</span>'


def _metric_row(label, ok, fail, pct):
    total = ok + fail
    bar_width = int(100 * ok / max(1, total))
    color = "green" if fail == 0 else "red"
    return f"""<div class="metric-row">
      <div class="metric-label">{label}</div>
      <div class="metric-bar">
        <div class="progress-bar">
          <div class="progress-fill {color}" style="width:{bar_width}%"></div>
        </div>
        <div class="metric-counts">{ok:,} OK &nbsp;·&nbsp; {fail:,} falhas</div>
      </div>
      <div class="metric-pct">{pct}</div>
    </div>"""


def _doi_breakdown_rows(breakdown: dict) -> str:
    descriptions = {
        "OK": "DOI resolve corretamente para o artigo",
        "DOI_ERROR": "doi.org retornou 'DOI Not Found'",
        "DOI_UNRESOLVED": "DOI não redirecionou para conteúdo do artigo",
        "NOT_FOUND": "Resposta HTTP 404",
        "TIMEOUT": "Timeout (sem resposta em 30s)",
        "ERROR": "Erro HTTP genérico",
    }
    rows = ""
    for status, count in sorted(breakdown.items(), key=lambda x: -x[1]):
        cls = "ok" if status == "OK" else "fail"
        badge = f'<span class="badge {cls}">{status}</span>'
        desc = descriptions.get(status, "")
        rows += f"<tr><td>{badge}</td><td>{count:,}</td><td>{desc}</td></tr>\n"
    return rows


if __name__ == "__main__":
    generate_report()
