# ============================================================
# scripts/1_leitura.R
#
# Lê o arquivo XLSX bruto de um lote e salva o RDS tratado em:
#   data/tratados/<lote_id>/banco_periodos.rds
#
# Quando rodar: apenas na primeira vez, ou se o XLSX mudar.
# Chamado por: run_lote.R (Etapa 1) via ler_e_tratar_lote()
# ============================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(readxl)
  library(glue)
  library(readr)
})

# Carrega períodos e fuso (usa dir_scripts se disponível, senão pasta_base)
if (exists("dir_scripts")) {
  source(file.path(dir_scripts, "config.R"))
} else {
  source(file.path(pasta_base, "scripts", "config.R"))
}

if (!exists("tz_local")) tz_local <- "America/Sao_Paulo"

# ── Parsers robustos de Data e Hora ──────────────────────────
# Lida com os formatos que o Excel exporta (serial, string, POSIXt)

parse_data_excel <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (is.numeric(x)) return(as.Date(x, origin = "1899-12-30"))
  x <- stringr::str_trim(as.character(x))
  x[x == "" | is.na(x)] <- NA_character_
  out  <- as.Date(x, format = "%d/%m/%Y")
  out2 <- as.Date(x, format = "%Y-%m-%d")
  as.Date(ifelse(is.na(out), out2, out), origin = "1970-01-01")
}

parse_hora_excel <- function(x) {
  if (inherits(x, "POSIXt")) return(format(x, "%H:%M:%S"))
  if (inherits(x, "hms"))    return(as.character(x))
  if (is.numeric(x)) {
    secs <- round((x %% 1) * 86400)
    return(sprintf("%02d:%02d:%02d", secs %/% 3600, (secs %% 3600) %/% 60, secs %% 60))
  }
  x <- stringr::str_trim(as.character(x))
  x[x == "" | is.na(x)] <- NA_character_
  x <- dplyr::if_else(!is.na(x) & stringr::str_detect(x, "^\\d{1,2}:\\d{2}$"), paste0(x, ":00"), x)
  x <- dplyr::if_else(!is.na(x) & stringr::str_detect(x, "^\\d:"),             paste0("0", x),   x)
  x
}

# ── Função principal ──────────────────────────────────────────
ler_e_tratar_lote <- function(pasta_base, lote_id, arquivo_xlsx = NULL, tz = tz_local) {

  pasta_brutos   <- file.path(pasta_base, "data", "brutos")
  pasta_tratados <- file.path(pasta_base, "data", "tratados")

  if (is.null(arquivo_xlsx))
    arquivo_xlsx <- glue("dados_PRJT_UFSC_{lote_id}.xlsx")

  arquivo_lote <- file.path(pasta_brutos, arquivo_xlsx)
  if (!file.exists(arquivo_lote))
    stop("Arquivo não encontrado: ", arquivo_lote, "\nConfirme o nome e se está em data/brutos.")

  periodos <- pegar_periodos_lote(lote_id, tz = tz)

  dir.create(file.path(pasta_tratados, lote_id), recursive = TRUE, showWarnings = FALSE)
  saida_rds <- file.path(pasta_tratados, lote_id, "banco_periodos.rds")

  message("Lendo arquivo: ", arquivo_lote)

  abas  <- excel_sheets(arquivo_lote)
  banco <- purrr::map2_dfr(abas, seq_along(abas), function(nome_aba, idx_aba) {

    message("  Aba ", idx_aba, ": ", nome_aba)
    df <- read_excel(arquivo_lote, sheet = nome_aba, col_names = TRUE)
    names(df) <- stringr::str_trim(names(df))

    if (!"Sensor" %in% names(df)) {
      s <- readr::parse_number(nome_aba)
      df <- df |> mutate(Sensor = if (is.na(s)) idx_aba else s)
    }
    if (!"T"  %in% names(df)) df <- df |> mutate(T  = NA_real_)
    if (!"UR" %in% names(df)) df <- df |> mutate(UR = NA_real_)
    if (!"Data" %in% names(df)) stop("Aba sem coluna Data: ", nome_aba)
    if (!"Hora" %in% names(df)) stop("Aba sem coluna Hora: ", nome_aba)

    df |> mutate(
      T  = suppressWarnings(as.numeric(stringr::str_replace(as.character(T),  ",", "."))),
      UR = suppressWarnings(as.numeric(stringr::str_replace(as.character(UR), ",", "."))),
      Data      = parse_data_excel(Data),
      Hora      = parse_hora_excel(Hora),
      data_hora = as.POSIXct(paste(Data, Hora), format = "%Y-%m-%d %H:%M:%S", tz = tz)
    )
  })

  n_total <- nrow(banco)
  n_ok    <- sum(!is.na(banco$data_hora))
  message("Linhas lidas: ", n_total, " | com data_hora válida: ", n_ok)

  banco <- banco |> filter(!is.na(data_hora), !is.na(Sensor))

  if (nrow(banco) == 0)
    stop(
      "Banco ficou com 0 linhas após montar data_hora.\n",
      "Confira no Excel se as colunas se chamam exatamente: Data e Hora."
    )

  readr::write_rds(banco, saida_rds, compress = "xz")
  message("✅ ", nrow(banco), " linhas salvas em: ", saida_rds)
  invisible(banco)
}
