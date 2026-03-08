# ============================================================
# scripts/0_periodos_lotes.R
# ============================================================

tz_local <- "America/Sao_Paulo"

# Garante tribble + funções usadas mesmo antes do tidyverse
suppressPackageStartupMessages({
  library(tibble)
  library(dplyr)
  library(lubridate)
})

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

# lote_id pode vir como "lote3" ou "lote03" etc.
pegar_periodos_lote <- function(lote_id, tz = tz_local) {
  key <- gsub("^lote0+", "lote", as.character(lote_id))
  
  if (!key %in% names(periodos_por_lote)) {
    stop("Sem períodos para: ", lote_id)
  }
  
  periodos_por_lote[[key]] |>
    dplyr::mutate(
      # dia civil completo no fuso do projeto
      inicio = lubridate::ymd_hms(paste(inicio_date, "00:00:00"), tz = tz),
      fim    = lubridate::ymd_hms(paste(fim_date,    "23:59:59"), tz = tz)
    ) |>
    dplyr::select(Periodo, inicio, fim)
}