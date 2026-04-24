# ============================================================
# scripts/config.R — Configuração central do projeto
# Chamado por: 1_leitura.R, 2_analise.R, 3_comparativo.R
# ============================================================

suppressPackageStartupMessages({
  library(tibble)
  library(dplyr)
  library(lubridate)
})

# - - - Mude Aqui - - -
lote_id   <- "lote1"   # "lote1" | "lote2" | "lote3"
tz_local  <- "America/Sao_Paulo"

# ── Períodos operacionais por lote ───────────────────────────
# Cada linha define um período: nome, data início e data fim.
# A professora edita aqui quando as datas mudarem.
periodos_por_lote <- list(
  lote1 = tibble::tribble(
    ~Periodo,             ~inicio_date, ~fim_date,
    "VazioSanitario_Ini", "2025-07-23", "2025-07-23",
    "Alojamento",         "2025-07-24", "2025-07-26",
    "Abertura1",          "2025-07-27", "2025-07-28",
    "Abertura2",          "2025-07-29", "2025-07-30",
    "Abertura3",          "2025-07-31", "2025-08-04",
    "Abertura4",          "2025-08-05", "2025-08-11",
    "Abertura5_8d",       "2025-08-12", "2025-08-19",
    "Abertura5_15d",      "2025-08-20", "2025-09-03",
    "VazioSanitario_Fim", "2025-09-04", "2025-09-04"
  ),
  lote2 = tibble::tribble(
    ~Periodo,             ~inicio_date, ~fim_date,
    "VazioSanitario_Ini", "2025-09-23", "2025-09-23",
    "Alojamento",         "2025-09-24", "2025-09-26",
    "Abertura1",          "2025-09-27", "2025-09-28",
    "Abertura2",          "2025-09-29", "2025-09-30",
    "Abertura3",          "2025-10-01", "2025-10-05",
    "Abertura4",          "2025-10-06", "2025-10-12",
    "Abertura5_8d",       "2025-10-13", "2025-10-20",
    "Abertura5_15d",      "2025-10-21", "2025-11-04",
    "VazioSanitario_Fim", "2025-11-05", "2025-11-05"
  ),
  lote3 = tibble::tribble(
    ~Periodo,             ~inicio_date, ~fim_date,
    "VazioSanitario_Ini", "2025-11-22", "2025-11-22",
    "Alojamento",         "2025-11-23", "2025-11-25",
    "Abertura1",          "2025-11-26", "2025-11-27",
    "Abertura2",          "2025-11-28", "2025-11-29",
    "Abertura3",          "2025-11-30", "2025-12-04",
    "Abertura4",          "2025-12-05", "2025-12-11",
    "Abertura5_8d",       "2025-12-12", "2025-12-19",
    "Abertura5_15d",      "2025-12-20", "2026-01-03",
    "VazioSanitario_Fim", "2026-01-04", "2026-01-04"
  )
)

# Converte strings de data para POSIXct no fuso correto.
# lote_id pode vir como "lote3" ou "lote03".
pegar_periodos_lote <- function(lote_id, tz = tz_local) {
  key <- gsub("^lote0+", "lote", as.character(lote_id))

  if (!key %in% names(periodos_por_lote))
    stop("Sem períodos para: ", lote_id)

  periodos_por_lote[[key]] |>
    dplyr::mutate(
      inicio = lubridate::ymd_hms(paste(inicio_date, "00:00:00"), tz = tz),
      fim    = lubridate::ymd_hms(paste(fim_date,    "23:59:59"), tz = tz)
    ) |>
    dplyr::select(Periodo, inicio, fim)
}

# ── Configuração dos sensores ─────────────────────────────────
# Layout do galpão: 3 linhas × 10 colunas = 30 sensores
#   L1: S1  S4  S7  S10 S13 S16 S19 S22 S25 S28
#   L2: S2  S5  S8  S11 S14 S17 S20 S23 S26 S29
#   L3: S3  S6  S9  S12 S15 S18 S21 S24 S27 S30
#       C1  C2  C3   C4  C5  C6  C7  C8  C9  C10
#
# Regra de intervalo (IMPORTANTE — confirmar com sensores físicos):
#   Colunas ÍMPARES (C1,C3,C5,C7,C9)  → 5 min  (sensores configurados)
#   Colunas PARES   (C2,C4,C6,C8,C10) → 10 min (fábrica, não reconfigurados)
sensor_config <- tibble::tibble(
  Sensor      = 1:30,
  Coluna_num  = ((Sensor - 1L) %/% 3L) + 1L,
  Linha       = rep(c("L1", "L2", "L3"), times = 10),
  Coluna      = paste0("C", Coluna_num),
  intervalo_min = dplyr::if_else(Coluna_num %% 2L == 1L, 5L, 10L),
  esperado_42d  = 42L * 24L * 60L %/% intervalo_min,  # 12096 (5min) | 6048 (10min)
  esperado_dia  = 24L * 60L %/% intervalo_min          # 288 (5min)   | 144 (10min)
) |>
  dplyr::select(-Coluna_num)

stopifnot(
  "sensor_config deve ter 15 sensores de 5 min"  = sum(sensor_config$intervalo_min == 5L)  == 15L,
  "sensor_config deve ter 15 sensores de 10 min" = sum(sensor_config$intervalo_min == 10L) == 15L,
  "sensor_config deve ter 30 sensores"           = nrow(sensor_config) == 30L
)

# Vetores de sensores por intervalo (útil para filtros)
sensores_5min  <- sensor_config$Sensor[sensor_config$intervalo_min == 5L]
sensores_10min <- sensor_config$Sensor[sensor_config$intervalo_min == 10L]

# ── Classificação de períodos ─────────────────────────────────
# Adiciona a coluna Periodo ao banco conforme as datas de cada período.
# Leituras fora de qualquer período ficam com Periodo = NA (factor).
# Uso: banco <- classificar_periodos(banco, periodos)
classificar_periodos <- function(banco, periodos) {
  banco <- dplyr::mutate(banco, Periodo = NA_character_)
  for (i in seq_len(nrow(periodos))) {
    banco <- dplyr::mutate(banco,
      Periodo = dplyr::if_else(
        is.na(Periodo) &
          data_hora >= periodos$inicio[i] &
          data_hora <= periodos$fim[i],
        periodos$Periodo[i],
        Periodo
      )
    )
  }
  dplyr::mutate(banco, Periodo = factor(Periodo, levels = periodos$Periodo))
}

# ── Filtro de período de análise ──────────────────────────────
# Mantém apenas as leituras que caem dentro da janela definida
# para o lote (início do primeiro período até fim do último).
# Uso: banco <- filtrar_periodo_analise(banco, periodos)
filtrar_periodo_analise <- function(banco, periodos) {
  ini <- min(periodos$inicio)
  fim <- max(periodos$fim)
  n_antes  <- nrow(banco)
  banco_f  <- dplyr::filter(banco, data_hora >= ini & data_hora <= fim)
  n_depois <- nrow(banco_f)
  if (n_antes != n_depois)
    message(sprintf(
      "filtrar_periodo_analise: %d → %d linhas (removeu %d fora da janela %s a %s)",
      n_antes, n_depois, n_antes - n_depois,
      format(ini, "%Y-%m-%d"), format(fim, "%Y-%m-%d")
    ))
  banco_f
}
