# ============================================================
# scripts/testes.R
# Monitora faixa de temperatura e umidade — Lote 1
# Saída: resultados/lote1/monitorando_faixa_temp_lote1.csv
#        resultados/lote1/monitorando_faixa_umd_lote1.csv
# ============================================================

library(readr)

source("scripts/faixas_ideais.R")

# - - - Leitura - - -

banco = readRDS("data/tratados/lote1/banco_periodos.rds")

# - - - Preparo - - -

DATA_INICIO = as.Date("2025-07-24")

banco$sensor      = paste0("s", banco$Sensor)
banco$lote        = "I"
banco$dia         = as.integer(as.Date(banco$data_hora) - DATA_INICIO) + 1L
banco$temperatura = banco$T
banco$umidade     = banco$UR

banco = banco[banco$dia >= 1L & banco$dia <= 42L, ]
banco = banco[!is.na(banco$temperatura) & !is.na(banco$umidade), ]

# - - - Scores - - -

scores = calcular_scores_diarios(banco)

# - - - Exportacao - - -

dir_saida = "resultados/lote1"
dir.create(dir_saida, recursive = TRUE, showWarnings = FALSE)

cols = c("dia", "sensor", "lote", "temp_d", "temp_f", "umid_d", "umid_f")
write_csv(scores[, cols], file.path(dir_saida, "scores_faixa_lote1.csv"))

message("✓ Exportado: ", file.path(dir_saida, "scores_faixa_lote1.csv"))