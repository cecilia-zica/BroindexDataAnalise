# PLANO DE REFATORAÇÃO — Projeto BroindexAnalise (R)

## Contexto do Projeto

Este é um projeto de análise de dados de **sensores de temperatura e umidade** em galpões de frangos de corte (avicultura de precisão). São **30 sensores** distribuídos em **3 linhas × 10 colunas** ao longo de um galpão, coletando dados de Temperatura (°C) e Umidade Relativa (%) ao longo de **42 dias por lote** (ciclo de criação). Os dados cobrem **3 lotes** com datas diferentes.

### Layout Físico do Galpão

```
                    Placas evap.          Exaustores (E)
                    ↓                     ↓
        L1:  S1  S4  | S7  S10  S13  S16  S19  [S22]  S25  S28
        L2:  S2  S5  | S8  S11  S14  S17  S20  [S23]  S26  S29
        L3:  S3  S6  | S9  S12  S15  S18  S21  [S24]  S27  S30
             C1  C2    C3   C4   C5   C6   C7    C8    C9   C10
                                                  ↑
                                            (S22-24 em amarelo/vermelho
                                             = região problemática)
```

- **Linha** = posição vertical (L1, L2, L3)
- **Coluna** = posição horizontal (C1–C10)
- Sensores próximos às **placas evaporativas** (C1-C2) e aos **exaustores** (C9-C10) têm comportamento diferente

### Problema Crítico: Intervalos de Medição Misturados

Os 30 sensores NÃO medem no mesmo intervalo:
- **15 sensores (ímpares de coluna: S1-S3, S7-S9, S13-S15, S19-S21, S25-S27)** → medição a cada **5 minutos** → esperado ~12.069 medidas em 42 dias
- **15 sensores (pares de coluna: S4-S6, S10-S12, S16-S18, S22-S24, S28-S30)** → medição a cada **10 minutos** → esperado ~6.048 medidas em 42 dias

> **Contexto**: Os primeiros 15 sensores comprados foram programados para 5min. Os outros 15 vieram pré-configurados de fábrica em 10min e não foram reconfigurados. O pesquisador (Leo) organizou no galpão alternando colunas de 5min e 10min.

### Períodos Operacionais (42 dias por lote)

| Período | Dias | Dias Acum. | Temp. (°C) | UR |
|---------|------|-----------|-----------|-----|
| Alojamento | 3 | 3 | 32–35 | 60–70% |
| Abertura 1 | 2 | 5 | 32–35 | 60–70% |
| Abertura 2 | 2 | 7 | 32–35 | 60–70% |
| Abertura 3 | 5 | 12 | 29–32 | 60–70% |
| Abertura 4 | 7 | 19 | 26–29 | 50–60% |
| Abertura 5.1 | 8 | 27 | 23–26 | 50–60% |
| Abertura 5.2 | 15 | 42 | 20–23 | 50–60% |

### Datas por Lote

| | VS (Vazio Sanitário) | Alojamento | Saída (Ab5.2 fim) |
|---|---|---|---|
| **Lote1** | 23/07/25 | 24/07/25–26/07/25 | 03/09/25 (VS: 04/09/25) |
| **Lote2** | 23/09/25 | 24/09/25–26/09/25 | 04/11/25 (VS: 05/11/25) |
| **Lote3** | 22/11/25 | 23/11/25–25/11/25 | 03/01/26 (VS: 04/01/26) |

**Sensores faltantes**: Lote2 não tem S23, S24, S25/09, S05/11. Lote3 não tem S22/11, S04/01.

### Dados no Excel de Referência (totalDadosBroindex.xlsx)

Cada aba (LOTE1, LOTE2, LOTE3) contém 30 linhas (sensores) com:
- `Total`: número total esperado de medições (12.096 para todos)
- `Ideal`: número observado real de medições por sensor

---

## FASE 0: Auditoria e Diagnóstico

### 0.1 — Inventário de scripts existentes

Mapear TODOS os scripts `.R` no projeto e documentar:

```
scripts/
├── 0_periodos_lotes.R        # fonte única de datas/períodos
├── 1_leitura_dados.R          # leitura dos dados brutos → banco_periodos.rds
├── 2_main_analise.R           # análise principal (~1500 linhas, MONOLÍTICO)
└── ...outros?
```

**Ação**: Listar o conteúdo de `scripts/` e `data/` para mapear tudo que existe.

### 0.2 — Verificação do banco de dados Lote3

> "Cecilia, verifica se o banco de dados do lote3 tem uma planilha em branco"

**Ação**: No script de leitura (`1_leitura_dados.R`), adicionar verificação automática:
```r
# Checar se há abas vazias ou corrompidas no Excel do Lote3
verificar_integridade_excel <- function(caminho_xlsx) {
  abas <- readxl::excel_sheets(caminho_xlsx)
  for (aba in abas) {
    df <- readxl::read_excel(caminho_xlsx, sheet = aba)
    if (nrow(df) == 0) warning("⚠️ Aba vazia detectada: ", aba, " em ", caminho_xlsx)
  }
}
```

### 0.3 — Mapeamento de intervalo por sensor

Criar um arquivo de configuração `config/sensor_intervalos.R` com a informação de qual sensor mede em qual intervalo:

```r
sensor_config <- tibble::tibble(
  Sensor = 1:30,
  intervalo_min = c(
    # C1(5): S1,S2,S3   C2(10): S4,S5,S6
    5, 5, 5,  10, 10, 10,
    # C3(5): S7,S8,S9   C4(10): S10,S11,S12
    5, 5, 5,  10, 10, 10,
    # C5(5): S13,S14,S15  C6(10): S16,S17,S18
    5, 5, 5,  10, 10, 10,
    # C7(5): S19,S20,S21  C8(10): S22,S23,S24
    5, 5, 5,  10, 10, 10,
    # C9(5): S25,S26,S27  C10(10): S28,S29,S30
    5, 5, 5,  10, 10, 10
  ),
  Linha = rep(c("L1", "L2", "L3"), 10),
  Coluna = rep(paste0("C", 1:10), each = 3)
)

# Valores esperados em 42 dias (do Alojamento até Ab5.2)
sensor_config <- sensor_config |>
  mutate(
    esperado_42d = case_when(
      intervalo_min == 5  ~ 42 * 24 * 60 / 5  + 1,  # ~12069
      intervalo_min == 10 ~ 42 * 24 * 60 / 10 + 1   # ~6048
    )
  )
```

> **IMPORTANTE**: Confirmar esses intervalos quando os sensores físicos chegarem. Alguns sensores mediram MAIS do que o esperado (vide Sensor 26 do Lote1 com 11.954 vs esperado ~11.250).

---

## FASE 1: Reestruturação da Arquitetura

### 1.1 — Nova Estrutura de Diretórios

```
BroindexAnalise/
├── config/
│   ├── sensor_intervalos.R      # NOVO: config de intervalo por sensor
│   ├── periodos_lotes.R         # já existe (renomear de 0_periodos_lotes.R)
│   ├── faixas_ideais.R          # NOVO: extrair de main_analise
│   ├── sensores_periodo.R       # NOVO: extrair de main_analise
│   └── visual_config.R          # NOVO: cores, rótulos, tema padrão
│
├── R/                           # Funções puras (sem side-effects)
│   ├── dados/
│   │   ├── carregar_banco.R
│   │   ├── add_linha_coluna.R
│   │   └── limpeza.R            # NOVO: verificações de integridade
│   ├── falhas/
│   │   ├── calc_gaps.R
│   │   ├── calcular_falhas.R
│   │   ├── calcular_gap5.R
│   │   └── diagnostico_intervalo.R  # NOVO
│   ├── plots/
│   │   ├── plot_calendario.R
│   │   ├── plot_geral.R
│   │   ├── plot_por_dia.R
│   │   ├── plot_painel_periodo.R
│   │   ├── plot_por_linha.R
│   │   ├── plot_por_coluna.R
│   │   ├── plot_gaps.R
│   │   ├── plot_falhas_sensor.R
│   │   ├── plot_mapas_temporal.R
│   │   └── plot_diagnostico.R   # NOVO: tabela freq sensor×dia
│   └── utils/
│       ├── save_plot.R
│       ├── dir_helpers.R
│       └── sensor_palettes.R
│
├── scripts/                     # Orquestradores (chamam funções de R/)
│   ├── 01_leitura_dados.R
│   ├── 02_diagnostico_falhas.R  # NOVO: gera tabela sensor×dia (42 linhas × 30 colunas)
│   ├── 03_analise_serie.R       # temperatura + umidade
│   ├── 04_analise_gaps.R        # maior gap, gap5, Pareto
│   ├── 05_relatorio_falhas.R    # NOVO: relatório consolidado
│   └── run_all.R                # roda tudo para um ou todos os lotes
│
├── data/
│   ├── brutos/                  # Excel originais
│   ├── tratados/                # .rds limpos por lote
│   └── referencia/              # totalDadosBroindex.xlsx
│
├── resultados/                  # outputs por lote
│   ├── lote1/
│   ├── lote2/
│   └── lote3/
│
└── tests/                       # NOVO
    ├── test_leitura.R
    └── test_contagem_sensor.R
```

### 1.2 — Quebra do Monolito `2_main_analise.R` (1500 linhas)

O script atual (`2_main_analise.R`) faz TUDO num arquivo só. A refatoração divide em:

| Bloco atual (linhas) | Destino |
|---|---|
| 1–38: config + pacotes | `config/` + cada script carrega o que precisa |
| 39–101: períodos, rótulos, faixas, sensores | `config/periodos_lotes.R`, `config/faixas_ideais.R`, `config/sensores_periodo.R`, `config/visual_config.R` |
| 102–170: funções de diretório + calendário | `R/utils/dir_helpers.R`, `R/plots/plot_calendario.R` |
| 171–248: carregar banco + add_linha_coluna | `R/dados/carregar_banco.R`, `R/dados/add_linha_coluna.R` |
| 249–467: MAIOR GAP por período | `R/falhas/calc_gaps.R`, `R/plots/plot_gaps.R`, `scripts/04_analise_gaps.R` |
| 468–746: funções genéricas de série (T/UR) | `R/plots/plot_geral.R`, `R/plots/plot_por_dia.R`, `R/plots/plot_painel_periodo.R` |
| 747–973: plots por linha/coluna | `R/plots/plot_por_linha.R`, `R/plots/plot_por_coluna.R` |
| 974–1145: entregáveis de série + mapas | `scripts/03_analise_serie.R`, `R/plots/plot_mapas_temporal.R` |
| 1146–1200: falhas por limites | `R/falhas/calcular_falhas.R`, `scripts/05_relatorio_falhas.R` |
| 1200–1403: gap5 (regra dos 5 min) | `R/falhas/calcular_gap5.R` |
| 1404–1499: falhas por sensor (gráficos) | `R/plots/plot_falhas_sensor.R` |

---

## FASE 2: Novo Módulo de Diagnóstico de Falhas (prioridade da Profa. Simone)

### 2.1 — Tabela de Frequência: Sensor × Dia (42 linhas × 30 colunas)

> Pedido da professora: "Pegue só uma planilha de um lote (1 sensor), separe as observações por dia, conte quantas observações por dia esse sensor tem. Depois faça isso para todos os sensores por dia e monte uma tabela para podermos conferir onde estão exatamente essas falhas."

Criar `R/falhas/diagnostico_intervalo.R`:

```r
#' Tabela de contagem de observações por sensor × dia
#'
#' @param banco data.frame com colunas: Sensor, data_hora
#' @param sensor_config tibble com colunas: Sensor, intervalo_min, esperado_42d
#' @return tibble com: Dia (linhas) × Sensor (colunas) + colunas de referência
tabela_freq_sensor_dia <- function(banco, sensor_config) {
  # 1. Contar observações por sensor por dia
  contagem <- banco |>
    filter(!is.na(data_hora), !is.na(Sensor)) |>
    mutate(Dia = as.Date(data_hora)) |>
    count(Dia, Sensor, name = "obs") |>
    tidyr::pivot_wider(
      names_from = Sensor,
      values_from = obs,
      values_fill = 0,
      names_prefix = "S"
    ) |>
    arrange(Dia)

  # 2. Adicionar coluna de esperado por dia para cada sensor
  esperado_dia <- sensor_config |>
    mutate(esperado_dia = 24 * 60 / intervalo_min) |>
    select(Sensor, esperado_dia)

  # 3. Retornar contagem + metadado
  list(
    contagem = contagem,
    esperado_dia = esperado_dia
  )
}

#' Heatmap de "% do esperado" por sensor × dia
#' Vermelho = abaixo do esperado, Verde = ok, Azul = acima do esperado
tabela_freq_pct <- function(contagem, esperado_dia) {
  # Para cada sensor-dia, calcular: obs / esperado_dia * 100
  # Sensor 5min: esperado_dia = 288
  # Sensor 10min: esperado_dia = 144
  ...
}
```

### 2.2 — Diagnóstico: Por que alguns sensores têm MAIS do que o esperado?

Na tabela da imagem 3 (Image 3), vemos valores em **negrito** que excedem o esperado:
- Sensor 4 (10min): Lote1 tem 6280 vs esperado 6048 → 232 a mais
- Sensor 9 (5min): Lote3 tem 13308 vs esperado 12069 → 1239 a mais!
- Sensor 15 (5min): Lote2 tem 12228 vs esperado 12069

**Hipótese**: Esses sensores podem ter mudado de intervalo durante o ciclo, ou os dados incluem o período de Vazio Sanitário.

Criar checagem:
```r
# Para cada sensor com obs > esperado, investigar:
# 1. Distribuição do dif_min → tem picos em 5 E em 10?
# 2. Verificar se há dados fora do período Alojamento–Ab5.2
# 3. Verificar se houve reset/reinício do sensor
```

### 2.3 — Correção da Regra de Gap

O script atual (`calcular_gap5`) assume que TODOS os sensores deveriam medir a cada 5 min. Isso está **errado** para metade dos sensores.

**Refatorar para**: cada sensor ter sua própria regra baseada no `sensor_config`:

```r
calcular_gap_dinamico <- function(df, sensor_config) {
  df |>
    left_join(sensor_config |> select(Sensor, intervalo_min), by = "Sensor") |>
    arrange(Sensor, data_hora) |>
    group_by(Sensor) |>
    mutate(
      dif_min = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      # Classificar gap com base no intervalo REAL do sensor
      classe_gap = case_when(
        is.na(dif_min)                           ~ NA_character_,
        dif_min < intervalo_min                   ~ "falha_comunicacao",
        near(dif_min, intervalo_min, tol = 0.5)  ~ "ok",
        dif_min > intervalo_min                   ~ "falha_gap"
      )
    ) |>
    ungroup()
}
```

### 2.4 — Freq. Relativa de Falha por Sensor (Imagem 1)

A Imagem 1 mostra a frequência relativa de falha por sensor para cada lote, separando os dois tipos de intervalo (Esp.5_5min vs Esp.10_10min). As linhas tracejadas verticais indicam os limiares aceitáveis.

**Ação**: Recalcular esses gráficos usando o `sensor_config` correto, não assumindo 5min para todos.

```r
# freq. relativa de falha = (obs - esperado) / esperado
# Se positivo: sensor mediu MAIS que o esperado
# Se negativo: sensor mediu MENOS que o esperado
freq_relativa_falha <- function(banco, sensor_config, lote_id) {
  banco |>
    filter(!is.na(data_hora)) |>
    count(Sensor, name = "obs") |>
    left_join(sensor_config |> select(Sensor, esperado_42d, intervalo_min), by = "Sensor") |>
    mutate(
      freq_rel = (obs - esperado_42d) / esperado_42d,
      tipo = paste0("Esp.", intervalo_min, "_", intervalo_min, "min")
    )
}
```

---

## FASE 3: Leitura Robusta dos Dados

### 3.1 — Parsing de data/hora

> "Leitura dos dados corretamente por conta do formato dos dados: data e hora (SEMPRE conferir!!!)"

Criar `R/dados/limpeza.R` com validações:

```r
validar_data_hora <- function(df, col_data_hora = "data_hora") {
  # 1. Checar NAs
  n_na <- sum(is.na(df[[col_data_hora]]))
  if (n_na > 0) warning(glue("⚠️ {n_na} registros com data_hora NA"))

  # 2. Checar se está no timezone correto
  tz_atual <- attr(df[[col_data_hora]], "tzone")
  message(glue("Timezone detectado: {tz_atual}"))

  # 3. Checar se datas estão dentro do período esperado
  range_dt <- range(df[[col_data_hora]], na.rm = TRUE)
  message(glue("Range: {range_dt[1]} até {range_dt[2]}"))

  # 4. Checar se há datas duplicadas por sensor
  dups <- df |>
    group_by(Sensor) |>
    summarise(n_dup = sum(duplicated(data_hora)), .groups = "drop") |>
    filter(n_dup > 0)
  if (nrow(dups) > 0) warning("⚠️ Datas duplicadas detectadas: ", paste(dups$Sensor, collapse=", "))

  invisible(df)
}
```

### 3.2 — Limpeza: Definir Janela de Trabalho

> "Definir o intervalo (data e hora) que vamos trabalhar (sem ou com vazio sanitário). Para Maringá: Lote1 tem VS, mas Lote2 e Lote3 não. Retiramos o VS de todos."

```r
limpar_janela <- function(banco, periodos, incluir_vs = FALSE) {
  if (incluir_vs) {
    inicio <- periodos |> filter(Periodo == "VazioSanitario_Ini") |> pull(inicio)
    fim    <- periodos |> filter(Periodo == "VazioSanitario_Fim") |> pull(fim)
  } else {
    inicio <- periodos |> filter(Periodo == "Alojamento") |> pull(inicio)
    fim    <- periodos |> filter(Periodo == "Abertura5_15d") |> pull(fim)
  }

  banco |>
    filter(data_hora >= inicio, data_hora <= fim)
}
```

---

## FASE 4: Refatoração de Plots (DRY)

### 4.1 — Tema Global Reutilizável

Muitas funções de plot repetem o mesmo bloco de `theme(...)`. Criar:

```r
# R/utils/tema_broindex.R
tema_broindex <- function(base_size = 12) {
  theme_minimal(base_size = base_size) %+replace%
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5),
      plot.subtitle = element_text(hjust = 0.5),
      plot.margin = margin(10, 20, 10, 10),
      panel.grid.minor = element_blank()
    )
}

tema_broindex_facet <- function(base_size = 11) {
  theme_bw(base_size = base_size) %+replace%
    theme(
      panel.grid = element_blank(),
      strip.text = element_text(face = "bold", size = 8),
      axis.text = element_text(size = 7),
      plot.title = element_text(face = "bold", hjust = 0.5)
    )
}
```

### 4.2 — Overlay de Eventos Reutilizável

O padrão `geom_vline + geom_text(angle=90)` para eventos se repete em >6 funções. Extrair:

```r
# R/utils/overlay_eventos.R
add_eventos <- function(g, datas_eventos, y_label) {
  if (is.null(datas_eventos) || nrow(datas_eventos) == 0) return(g)
  g +
    geom_vline(xintercept = datas_eventos$data_hora, color = "red", linetype = "dashed", linewidth = 0.4) +
    geom_text(data = datas_eventos, aes(x = data_hora, y = y_label, label = evento),
              angle = 90, vjust = 0, hjust = 0, size = 3, color = "red", inherit.aes = FALSE)
}
```

### 4.3 — Faixas Ideais como Layer

O bloco de "faixas ideais" (geom_rect verde) se repete. Extrair:

```r
add_faixa_ideal <- function(g, faixa_dias, usar_faixa) {
  if (!usar_faixa || is.null(faixa_dias)) return(g)
  g + geom_rect(data = faixa_dias,
    aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
    fill = "springgreen3", alpha = 0.15, inherit.aes = FALSE)
}
```

---

## FASE 5: Testes e Validação

### 5.1 — Teste de Contagem por Sensor

```r
# tests/test_contagem_sensor.R
# Para cada lote, verificar que a contagem de obs por sensor bate com o
# esperado no totalDadosBroindex.xlsx (coluna "Ideal")
test_contagem <- function(banco, lote_id, caminho_xlsx) {
  referencia <- readxl::read_excel(caminho_xlsx, sheet = toupper(lote_id))
  contagem <- banco |> count(Sensor, name = "obs")
  comparacao <- contagem |>
    left_join(referencia, by = "Sensor") |>
    mutate(diff = obs - Ideal)
  
  sensores_divergentes <- comparacao |> filter(diff != 0)
  if (nrow(sensores_divergentes) > 0) {
    warning("⚠️ Sensores com contagem diferente do Excel de referência:")
    print(sensores_divergentes)
  }
}
```

### 5.2 — Teste de Leitura de Datas

```r
# Verificar que nenhum sensor tem dados fora da janela do lote
# Verificar que o parsing não inverteu dia/mês
test_datas <- function(banco, periodos) {
  range_esperado <- c(
    min(periodos$inicio),
    max(periodos$fim)
  )
  range_real <- range(banco$data_hora, na.rm = TRUE)
  
  if (range_real[1] < range_esperado[1] - days(1)) {
    warning("⚠️ Dados ANTES do período esperado: ", range_real[1])
  }
  if (range_real[2] > range_esperado[2] + days(1)) {
    warning("⚠️ Dados DEPOIS do período esperado: ", range_real[2])
  }
}
```

---

## FASE 6: Pipeline Unificado

### 6.1 — `run_all.R`

```r
#!/usr/bin/env Rscript
# scripts/run_all.R — Executa todo o pipeline para um ou mais lotes

args <- commandArgs(trailingOnly = TRUE)
lotes <- if (length(args) > 0) args else c("lote1", "lote2", "lote3")

source("config/sensor_intervalos.R")

for (lote_id in lotes) {
  message("\n", strrep("=", 60))
  message("PROCESSANDO: ", toupper(lote_id))
  message(strrep("=", 60))
  
  # 1. Leitura
  source("scripts/01_leitura_dados.R")
  
  # 2. Diagnóstico de falhas (tabela sensor×dia)
  source("scripts/02_diagnostico_falhas.R")
  
  # 3. Análise de séries (T + UR)
  source("scripts/03_analise_serie.R")
  
  # 4. Análise de gaps
  source("scripts/04_analise_gaps.R")
  
  # 5. Relatório consolidado
  source("scripts/05_relatorio_falhas.R")
}
```

### 6.2 — Comparação entre Lotes

Criar `scripts/06_comparacao_lotes.R` para gerar:
- Gráfico da Imagem 1 (freq. relativa de falha × sensor, por lote, colorido por tipo de intervalo)
- Gráfico da Imagem 4 (N. de medidas observadas × sensor, por lote)
- Tabela consolidada como a da Imagem 3

---

## Ordem de Execução Recomendada para o Claude Code

1. **Primeiro**: Criar `config/sensor_intervalos.R` — é a base de tudo
2. **Segundo**: Criar a estrutura de diretórios `R/`, `config/`, `scripts/`, `tests/`
3. **Terceiro**: Extrair funções puras do `2_main_analise.R` → `R/dados/`, `R/falhas/`, `R/plots/`, `R/utils/`
4. **Quarto**: Criar `R/falhas/diagnostico_intervalo.R` (tabela sensor×dia — pedido principal da Simone)
5. **Quinto**: Reescrever `calcular_gap5` → `calcular_gap_dinamico` (corrigir regra 5min vs 10min)
6. **Sexto**: Criar os scripts orquestradores em `scripts/`
7. **Sétimo**: Criar testes em `tests/`
8. **Oitavo**: Rodar para todos os 3 lotes e validar contra o Excel de referência

---

## Checklist de Validação Final

- [ ] Cada sensor é classificado com seu intervalo correto (5 ou 10 min)
- [ ] Contagem por sensor bate com `totalDadosBroindex.xlsx`
- [ ] Nenhum dado fora da janela Alojamento–Ab5.2 (VS removido)
- [ ] Tabela sensor×dia (42 linhas × 30 colunas) gerada para cada lote
- [ ] Gap/falha calculado com base no intervalo REAL do sensor
- [ ] Sensores com obs > esperado investigados (Sensor 26/Lote1, Sensor 9/Lote3, etc.)
- [ ] Gráficos de freq. relativa de falha reproduzidos corretamente
- [ ] Pipeline roda para os 3 lotes sem erro
- [ ] Nenhum `pasta_base` hardcoded com caminho Windows
