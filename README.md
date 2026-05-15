# SciELO Brasil — Auditoria Completa de URLs

Ferramenta para mapear **todos os artigos** do SciELO Brasil e verificar se:
- ✅ O HTML do artigo está acessível (todos os idiomas)
- ✅ O PDF está disponível e válido
- ✅ O DOI resolve corretamente (não retorna "DOI Not Found")

---

## Pré-requisitos

```bash
pip install -r requirements.txt
```

---

## Uso rápido

### Pipeline completo (recomendado)

```bash
python run_all.py
```

Isso executa os 6 passos automaticamente e gera `relatorio_scielo.html`.

**Tempo estimado:** 4–12 horas (depende da velocidade da conexão e do SciELO)
- ~500 periódicos × ~50 números × ~10 artigos = ~250.000 artigos

---

### Teste rápido (50 artigos)

```bash
python run_all.py --test --limit 50
```

Completa em ~2 minutos. Bom para testar que tudo funciona.

---

### Passos individuais

Cada passo pode ser executado separadamente:

```bash
python 01_collect_journals.py   # Coleta lista de periódicos → data/journals.json
python 02_collect_issues.py     # Coleta números (issues)   → data/issues.json
python 03_collect_articles.py   # Coleta artigos            → data/articles.json
python 04_extract_dois.py       # Extrai DOIs               → data/articles_with_doi.json
python 05_check_urls.py         # Verifica todas as URLs    → data/check_results.json
python 06_generate_report.py    # Gera relatório HTML       → relatorio_scielo.html
```

Se a execução for interrompida, os passos 3 e 5 têm **checkpoint automático**
e podem ser retomados de onde pararam.

### Retomar de um passo específico

```bash
python run_all.py --start 5   # Começa a verificação (pulo os passos 1-4)
```

---

## Estrutura de saída

```
data/
  journals.json           Lista de periódicos com acrônimos
  issues.json             Todos os números de todos os periódicos
  articles.json           Artigos sem DOI
  articles_with_doi.json  Artigos com DOIs extraídos
  check_results.json      Resultado detalhado de cada verificação
  broken_urls.json        Apenas artigos com algum problema
  check_summary.json      Resumo estatístico

relatorio_scielo.html     Painel interativo com filtros
```

---

## Como o DOI é verificado

O script segue o redirect do DOI e verifica se:

1. **OK** → O redirect chegou em uma página do artigo (scielo.br ou outra plataforma)
2. **DOI_ERROR** → A página contém "DOI Not Found" ou equivalente
3. **DOI_UNRESOLVED** → O redirect ficou na página do doi.org sem resolver
4. **NOT_FOUND** → HTTP 404
5. **TIMEOUT** → Sem resposta em 30 segundos
6. **ERROR** → Outro erro HTTP

---

## Configurações avançadas

Edite as constantes no topo de cada script:

| Script | Variável | Padrão | Descrição |
|--------|----------|--------|-----------|
| 02 | `SEM_LIMIT` | 20 | Concorrência ao buscar issues |
| 03 | `SEM_LIMIT` | 25 | Concorrência ao buscar artigos |
| 04 | `SEM_LIMIT` | 30 | Concorrência ao extrair DOIs |
| 05 | `SEM_SCIELO` | 40 | Concorrência para scielo.br |
| 05 | `SEM_DOI` | 20 | Concorrência para doi.org |

Reduza se receber muitos erros de timeout ou rate limiting (HTTP 429).

---

## Notas

- O SciELO pode bloquear requisições muito rápidas. Se isso acontecer,
  reduza `SEM_LIMIT` e aguarde alguns minutos antes de retomar.
- O relatório HTML funciona offline, sem servidor web.
- Os arquivos JSON podem ser importados em Excel/Power BI para análise adicional.
