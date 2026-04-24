# BroindexAnalise
Análise de dados de temperatura e umidade de sensores em galpão avícola.
Scripts em R. Saídas em `resultados/` e `data/tratados/`.

## Estrutura
```
scripts/
  config.R          # configuração central — lote_id fica aqui
  faixas_ideais.R   # faixas ideais de temp/umid por dia do ciclo (1–42)
  1_leitura.R       # lê XLSX bruto e salva RDS
  2_analise.R       # gera gráficos e tabelas
  3_comparativo.R   # tabela comparativa entre lotes
  testes/           # scripts de teste e exploração — não são pipeline
data/
  brutos/           # XLSX originais (não versionados)
  tratados/         # RDS gerados por 1_leitura.R
resultados/         # saídas por lote
docs/
  arquitetura.md
.claude/
  rules/
    visualizacao.md
    modelagem.md
```

## Estilo de código R

- Comentários e nomes de variáveis em português
- Seções separadas com `# - - - Nome Da Secao - - -`
- Atribuição com `=` (não `<-`)
- Snake_case para variáveis: `dados_sensor`, `banco_periodos`
- Sem pipes (`%>%` ou `|>`) — preferir `$` e colchetes
- Sem `purrr`, sem `map()` — usar `for` e `ifelse` quando necessário
- Sem comentários óbvios — só onde a lógica não é evidente
- Não adicionar comentários em código que não foi alterado
- Funções de agregação: preferir `aggregate()` ao invés de `dplyr::summarise()`
- Leitura de arquivos: `readRDS()` para RDS, `readxl::read_excel()` para XLSX
- Exportação: `readr::write_csv()` para CSV

## Cabeçalho obrigatório em todo script

```r
# ============================================================
# scripts/nome_do_script.R
# O que esse script faz em uma linha.
# Saída: caminho/do/arquivo/gerado.csv
# Dependências: scripts/outro.R
# ============================================================
```

## Regras do projeto

- `lote_id` e `pasta_base` sempre vêm de `config.R` — não duplicar em outros scripts
- Em scripts de teste, caminhos podem ser definidos diretamente (sem `pasta_base`)
- Não alterar lógica fora do escopo pedido
- Não usar `library(dplyr)` isolado — carregar só o necessário
- Consultar `.claude/rules/visualizacao.md` antes de propor gráficos
- Consultar `.claude/rules/modelagem.md` antes de propor modelos

## Contexto do projeto

- 30 sensores (S1–S30) em 3 linhas (L1, L2, L3) e 10 colunas (C1–C10)
- Sensores com intervalo de 5min ou 10min (ver `config.R`)
- Ciclo de 42 dias por lote (Lote1, Lote2, Lote3)
- Dia 1 = primeiro dia de alojamento (data definida em `config.R`)
- Faixas ideais de temperatura e umidade por dia: `scripts/faixas_ideais.R`
- Sensores 4, 5, 6 e 10 apresentam leituras anomalamente baixas — tratar com cautela

## Métricas validadas pela literatura

Usar APENAS estas — não propor outras sem consultar a bibliografia:

- `rmse`        = sqrt(sum((referencia - combinacao)^2) / n)
- `mape`        = (100/n) * sum(abs((referencia - combinacao) / referencia))
- `entropia`    = -sum(prob * log2(prob))
- `correlacao`  = cov(a, b) / (sd(a) * sd(b))
- `mad_robusto` = median(abs(x - median(x)))
- `z_score_mad` = 0.6745 * (x - median(x)) / mad_robusto

## Parâmetros de referência (não alterar sem justificativa bibliográfica)

- `tol_temp_resolucao`     = 0.1    # resolução típica de sensores avícolas
- `incerteza_hardware`     = 0.5    # margem de erro do datasheet
- `limiar_z_score_outlier` = 3.0    # leitura anômala acima deste z-score
- `tol_gap_comunicacao`    = 0.6    # minutos de tolerância ao redor do intervalo
- `limiar_gap_p95_critico` = 30.0   # P95 acima disso = hardware comprometido
- `gap_anomalia_minimo`    = 15.0   # mínimo de minutos contínuos para confirmar falha estrutural
- `gap_critico`            = 120.0  # maior gap acima disso = sensor crítico

## Critérios de exclusão de sensores

- Excluir ou penalizar se maior gap > 120 min (sem justificativa sistêmica)
- Excluir se P95 do gap > 30 min
- Excluir se RMSE ou viés constante em relação à mediana global (descalibração)
- Filtrar zeros consecutivos do datalogger antes de qualquer análise
- Eventos sistêmicos simultâneos em toda a malha NÃO penalizam sensor individual

## Janelas temporais de referência

- Intervalo base de coleta: 5 a 10 min (conforme sensor)
- Agregação para correlação: janelas de 12h (dia = 06:00–18:00, noite = 18:00–06:00)
- Janelas de detecção de anomalia: 1h, 2h, 4h, 8h e 16h
- Mínimo de 3 steps contínuos (15 min) para confirmar anomalia estrutural

## Regras científicas de tratamento de dados

- Usar mediana e MAD no lugar de média e desvio padrão para filtragem de outliers
  (evita a "Ilusão da Média" — sensor com bom RMSE mas falhas constantes)
- Diferenciar Additive Outliers (pico falso pontual, ex: 50°C) de
  Innovation Outliers (queda prolongada ou falha estrutural de comunicação)
- Eventos sistêmicos simultâneos em toda a malha NÃO penalizam sensor individual
- Correlação de Pearson >= 0.99 entre dois sensores = redundância espacial confirmada
  → um dos dois deve ser excluído antes da modelagem

## O que NÃO fazer

- Não usar `setwd()` — caminhos sempre relativos ao `pasta_base` ou `getwd()`
- Não hardcodar datas de início de lote fora do `config.R`
- Não misturar leitura + análise + exportação num mesmo script
- Não usar loops linha a linha (`for i in 1:nrow`) quando vetorização resolve
- Não usar média simples para detecção de outliers — sempre MAD
- Não classificar falha sistêmica como falha individual do sensor
- Não selecionar dois sensores com correlação >= 0.99 como independentes
- Não propor gráficos ou modelos sem consultar `.claude/rules/`
- Não substituir RMSE por outra métrica de erro sem justificativa bibliográfica