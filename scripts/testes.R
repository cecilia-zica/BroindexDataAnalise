# ============================================================
# scripts/testes.R
# Scores de faixa ideal de T e UR — todos os lotes
# Saída: resultados/<lote>/scores_faixa_<lote>.csv
# ============================================================

library(readr)

if (!exists("pasta_base")) pasta_base = normalizePath(getwd(), winslash = "/")

source(file.path(pasta_base, "scripts", "config.R"))
source(file.path(pasta_base, "scripts", "faixas_ideais.R"))

# - - - Configuracao Dos Lotes - - -

lotes = list(
  lote1 = list(id = "lote1", lote_label = "I",   data_inicio = as.Date("2025-07-24")),
  lote2 = list(id = "lote2", lote_label = "II",  data_inicio = as.Date("2025-09-24")),
  lote3 = list(id = "lote3", lote_label = "III", data_inicio = as.Date("2025-11-23"))
)

cols = c("dia", "sensor", "lote", "temp_d", "temp_f", "umid_d", "umid_f")

# - - - Loop Por Lote - - -

for (cfg in lotes) {

  rds = file.path(pasta_base, "data", "tratados", cfg$id, "banco_periodos.rds")

  if (!file.exists(rds)) {
    message("Arquivo não encontrado, pulando: ", rds)
    next
  }

  banco = readRDS(rds)

  banco$sensor      = paste0("s", banco$Sensor)
  banco$lote        = cfg$lote_label
  banco$dia         = as.integer(as.Date(banco$data_hora) - cfg$data_inicio) + 1L
  banco$temperatura = banco$T
  banco$umidade     = banco$UR

  banco = banco[banco$dia >= 1L & banco$dia <= 42L, ]
  banco = banco[!is.na(banco$temperatura) & !is.na(banco$umidade), ]

  scores = calcular_scores_diarios(banco)

  dir_saida = file.path(pasta_base, "resultados", cfg$id)
  dir.create(dir_saida, recursive = TRUE, showWarnings = FALSE)

  saida = file.path(dir_saida, paste0("scores_faixa_", cfg$id, ".csv"))
  write_csv(scores[, cols], saida)
  message("✓ Exportado: resultados/", cfg$id, "/scores_faixa_", cfg$id, ".csv")
}
