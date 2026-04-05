# ============================================================
# scripts/3_comparativo.R
#
# Tabela comparativa de observações por sensor entre lotes.
# Detecta automaticamente quais lotes têm RDS disponível
# (independente do flag `lotes` no run_lote.R).
#
# Contexto: alguns sensores mediram de 10 em 10 min em vez de
# 5 em 5 min. Esta tabela permite conferir se o número de
# observações por sensor está dentro do esperado para cada lote.
#
# Saída em: data/referencia/
#   contagem_obs_por_sensor.csv   — CSV simples
#   contagem_obs_por_sensor.xlsx  — Excel formatado com cores por zona do galpão
#
# Chamado por: run_lote.R (Etapa 3, rodar_comparativo = TRUE)
# ============================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(readr)
  library(duckdb)
  library(DBI)
  library(openxlsx)
})

if (!exists("pasta_base")) pasta_base <- normalizePath(getwd(), winslash = "/")

# Carrega sensor_config (intervalos e esperados por sensor)
source(file.path(pasta_base, "scripts", "config.R"))

# ── Detecta lotes disponíveis ─────────────────────────────────
todos_lotes <- c("lote1", "lote2", "lote3")

lotes_disponiveis <- todos_lotes[sapply(todos_lotes, function(l) {
  file.exists(file.path(pasta_base, "data", "tratados", l, "banco_periodos.rds"))
})]

if (length(lotes_disponiveis) == 0)
  stop("Nenhum RDS encontrado em data/tratados/. Rode a Etapa 1 primeiro.")

cat("Lotes disponíveis:", paste(lotes_disponiveis, collapse = ", "), "\n")

# ── Carrega e filtra pelo intervalo ativo (exclui Vazio Sanitário por data) ──
# Filtra por data_hora em vez de Periodo para não incluir linhas com Periodo = NA
# (dados fora dos períodos definidos que passariam pelo filtro de Periodo).
vs_excluir <- c("VazioSanitario_Ini", "VazioSanitario_Fim")

carregar_lote <- function(lote_id) {
  periodos_ativos <- pegar_periodos_lote(lote_id) |>
    dplyr::filter(!Periodo %in% vs_excluir)

  readr::read_rds(file.path(pasta_base, "data", "tratados", lote_id, "banco_periodos.rds")) |>
    filter(
      data_hora >= min(periodos_ativos$inicio),
      data_hora <= max(periodos_ativos$fim)
    ) |>
    mutate(Sensor = as.integer(Sensor))
}

bancos <- lapply(setNames(lotes_disponiveis, lotes_disponiveis), carregar_lote)

# ── Conta obs por sensor via SQL (DuckDB — eficiente para RDS grandes) ──
con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
for (l in lotes_disponiveis) duckdb::duckdb_register(con, l, bancos[[l]])

contar_lote <- function(lote_id) {
  dbGetQuery(con, sprintf(
    "SELECT Sensor, COUNT(*) AS obs FROM \"%s\" GROUP BY Sensor ORDER BY Sensor", lote_id
  )) |>
    rename(!!paste0(lote_id, "_obs") := obs) |>
    mutate(Sensor = as.integer(Sensor))
}

obs_lista <- lapply(lotes_disponiveis, contar_lote)
dbDisconnect(con)
cat("Contagem concluída.\n")

# ── Monta tabela final ────────────────────────────────────────
tabela <- sensor_config |>
  select(Sensor, Coluna, intervalo_min, esperado_42d) |>
  mutate(intervalo_tempo = paste0(intervalo_min, "_", intervalo_min), Esperado = esperado_42d)

for (obs_df in obs_lista) tabela <- left_join(tabela, obs_df, by = "Sensor")

cols_obs <- paste0(lotes_disponiveis, "_obs")

# ── Salva CSV ─────────────────────────────────────────────────
dir_ref <- file.path(pasta_base, "data", "referencia")
dir.create(dir_ref, recursive = TRUE, showWarnings = FALSE)

arq_csv <- file.path(dir_ref, "contagem_obs_por_sensor.csv")
readr::write_csv(
  tabela |> select(intervalo_tempo, Sensor, Esperado, all_of(cols_obs)),
  arq_csv
)
cat("✔ CSV salvo em:", arq_csv, "\n")

# ── Excel formatado (erros aqui não interrompem o restante) ──
tryCatch({

# Paleta por zona do galpão (cada cor representa uma região de expansão dos pintos)
cor_zona <- c(
  C1 = "#FFFFFF", C2 = "#FFFFFF",
  C3 = "#D9D9D9",
  C4 = "#FFFFFF", C5 = "#FFFFFF",
  C6 = "#92D050",
  C7 = "#FFFF00",
  C8 = "#FF6666",
  C9 = "#FAC090",
  C10 = "#E26B0A"
)

wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "contagem_obs")

header_cols <- c("intervalo_tempo", "Sensor", "Esperado", cols_obs)
sty_header  <- openxlsx::createStyle(fontName = "Calibri", fontSize = 11, textDecoration = "bold",
                                      fgFill = "#D9D9D9", halign = "CENTER", valign = "CENTER",
                                      border = "TopBottomLeftRight", borderStyle = "thin")
openxlsx::writeData(wb, "contagem_obs", as.data.frame(t(header_cols)),
                    startRow = 1, startCol = 1, colNames = FALSE)
openxlsx::addStyle(wb, "contagem_obs", sty_header,
                   rows = 1, cols = seq_along(header_cols), gridExpand = TRUE)

make_sty <- function(fill, bold = FALSE) {
  openxlsx::createStyle(fontName = "Calibri", fontSize = 11, fgFill = fill,
                        textDecoration = if (bold) "bold" else "none",
                        halign = "CENTER", valign = "CENTER",
                        border = "TopBottomLeftRight", borderStyle = "thin")
}

n_linhas <- nrow(tabela)
for (i in seq_len(n_linhas)) {
  row_excel <- i + 1L
  linha     <- tabela[i, ]
  fill      <- cor_zona[[ linha$Coluna ]]

  vals <- data.frame(intervalo_tempo = linha$intervalo_tempo,
                     Sensor = linha$Sensor, Esperado = linha$Esperado)
  for (co in cols_obs) vals[[co]] <- linha[[co]]

  openxlsx::writeData(wb, "contagem_obs", vals, startRow = row_excel, startCol = 1, colNames = FALSE)
  openxlsx::addStyle(wb, "contagem_obs", make_sty(fill),
                     rows = row_excel, cols = 1:length(header_cols), gridExpand = TRUE, stack = FALSE)

  # Negrito para sensores com obs acima do esperado
  for (j in seq_along(cols_obs)) {
    val_obs <- linha[[ cols_obs[j] ]]
    if (!is.na(val_obs) && val_obs > linha$Esperado)
      openxlsx::addStyle(wb, "contagem_obs", make_sty(fill, bold = TRUE),
                         rows = row_excel, cols = j + 3L, stack = FALSE)
  }
}

# Mescla células de intervalo_tempo por bloco
blocos <- tabela |>
  mutate(row_excel = row_number() + 1L) |>
  group_by(intervalo_tempo) |>
  summarise(r_ini = min(row_excel), r_fim = max(row_excel), .groups = "drop")

for (i in seq_len(nrow(blocos))) {
  b <- blocos[i, ]
  if (b$r_ini < b$r_fim) {
    openxlsx::mergeCells(wb, "contagem_obs", cols = 1, rows = b$r_ini:b$r_fim)
    openxlsx::addStyle(wb, "contagem_obs", make_sty("#FFFFFF", bold = TRUE),
                       rows = b$r_ini, cols = 1, stack = FALSE)
  }
}

openxlsx::setColWidths(wb, "contagem_obs", cols = seq_along(header_cols),
                       widths = c(16, 9, 11, rep(11, length(cols_obs))))
openxlsx::freezePane(wb, "contagem_obs", firstRow = TRUE)

arq_xlsx <- file.path(dir_ref, "contagem_obs_por_sensor.xlsx")
openxlsx::saveWorkbook(wb, arq_xlsx, overwrite = TRUE)
cat("✔ Excel salvo em:", arq_xlsx, "\n")

}, error = function(e) {
  cat("⚠ Excel não gerado (openxlsx):", conditionMessage(e), "\n")
  cat("  O CSV já foi salvo normalmente.\n")
})

# ── Resumo no console ─────────────────────────────────────────
cat("\n── Contagem completa ────────────────────────────────────\n")
print(tabela |> select(intervalo_tempo, Sensor, Esperado, all_of(cols_obs)) |> as.data.frame())

acima <- tabela |>
  filter(if_any(all_of(cols_obs), ~ . > Esperado)) |>
  select(Sensor, Coluna, Esperado, all_of(cols_obs))

if (nrow(acima) > 0) {
  cat("\n── Sensores com obs > Esperado (negrito no Excel) ───────\n")
  print(acima)
}

cat("\n── Arquivos gerados em:", dir_ref, "────────────────────\n")
cat("   contagem_obs_por_sensor.csv\n")
cat("   contagem_obs_por_sensor.xlsx  (se openxlsx ok)\n")

# ============================================================
# VALIDAÇÃO DIÁRIA — Sensor 21
#
# Para cada lote disponível, compara:
#   s21_obs      → contagem por dia usando a coluna Data (data original do Excel)
#   s21_leitura  → contagem por dia usando data_hora (data após parse do R)
#
# Se o R estiver lendo as datas corretamente, as duas colunas devem
# ser idênticas em todos os dias. Qualquer diferença indica problema
# de parse (ex.: virada de meia-noite mudando o dia por fuso horário).
#
# Saída: data/referencia/validacao_s21_por_dia.csv
# ============================================================

sensor_alvo <- 21L

cat("\n── Validação diária sensor 21 ───────────────────────────\n")
for (l in lotes_disponiveis) {

  df <- readr::read_rds(
    file.path(pasta_base, "data", "tratados", l, "banco_periodos.rds")
  ) |>
    dplyr::filter(as.integer(Sensor) == sensor_alvo) |>
    dplyr::mutate(
      Dia_obs     = as.Date(Data),                          # data original do Excel
      Dia_leitura = as.Date(data_hora, tz = tz_local)       # data após parse do R
    )

  por_obs <- df |> dplyr::count(Dia = Dia_obs,     name = "s21_obs")
  por_lei <- df |> dplyr::count(Dia = Dia_leitura, name = "s21_leitura")

  tabela_s21 <- dplyr::full_join(por_obs, por_lei, by = "Dia") |>
    dplyr::arrange(Dia)

  arq_s21 <- file.path(dir_ref, paste0("validacao_s21_", l, ".csv"))
  readr::write_csv(tabela_s21, arq_s21)
  cat("✔ Salvo em:", arq_s21,
      "| dias:", nrow(tabela_s21),
      "| total obs:", sum(tabela_s21$s21_obs, na.rm = TRUE), "\n")
}
