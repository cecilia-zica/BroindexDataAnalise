# ============================================================
# scripts/1_leitura_dados.R
# ============================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(readxl)
  library(glue)
  library(readr)
})

# ------------------------------------------------------------
# Fonte única dos períodos + tz_local
# (usa dir_scripts se existir; senão cai para pasta_base/scripts)
# ------------------------------------------------------------
if (exists("dir_scripts")) {
  source(file.path(dir_scripts, "0_periodos_lotes.R"))
} else {
  source(file.path(pasta_base, "scripts", "0_periodos_lotes.R"))
}

# fallback blindado
if (!exists("tz_local")) tz_local <- "America/Sao_Paulo"

# -------------------------------
# PARSERS ROBUSTOS
# -------------------------------
parse_data_excel <- function(x) {
  if (inherits(x, "Date")) return(x)
  
  # Excel serial date
  if (is.numeric(x)) return(as.Date(x, origin = "1899-12-30"))
  
  x <- as.character(x)
  x <- stringr::str_trim(x)
  x[x == "" | is.na(x)] <- NA_character_
  
  # tenta dd/mm/yyyy
  out <- as.Date(x, format = "%d/%m/%Y")
  # se falhar, tenta yyyy-mm-dd
  out2 <- as.Date(x, format = "%Y-%m-%d")
  
  out_final <- ifelse(is.na(out), out2, out)
  as.Date(out_final, origin = "1970-01-01")
}

parse_hora_excel <- function(x) {
  # Se vier como POSIXt
  if (inherits(x, "POSIXt")) return(format(x, "%H:%M:%S"))
  
  # Se vier como hms
  if (inherits(x, "hms")) return(as.character(x))
  
  # Excel serial time (fração do dia)
  if (is.numeric(x)) {
    secs <- round((x %% 1) * 86400)
    h <- secs %/% 3600
    m <- (secs %% 3600) %/% 60
    s <- secs %% 60
    return(sprintf("%02d:%02d:%02d", h, m, s))
  }
  
  x <- as.character(x)
  x <- stringr::str_trim(x)
  x[x == "" | is.na(x)] <- NA_character_
  
  # "HH:MM" -> "HH:MM:00"
  x <- dplyr::if_else(
    !is.na(x) & stringr::str_detect(x, "^\\d{1,2}:\\d{2}$"),
    paste0(x, ":00"),
    x
  )
  # "7:05:00" -> "07:05:00"
  x <- dplyr::if_else(
    !is.na(x) & stringr::str_detect(x, "^\\d:"),
    paste0("0", x),
    x
  )
  
  x
}

# -------------------------------
# FUNÇÃO PRINCIPAL
# -------------------------------
ler_e_tratar_lote <- function(pasta_base, lote_id, arquivo_xlsx = NULL, tz = tz_local) {
  
  pasta_brutos   <- file.path(pasta_base, "data", "brutos")
  pasta_tratados <- file.path(pasta_base, "data", "tratados")
  
  if (is.null(arquivo_xlsx)) {
    arquivo_xlsx <- glue("dados_PRJT_UFSC_{lote_id}.xlsx")
  }
  
  arquivo_lote <- file.path(pasta_brutos, arquivo_xlsx)
  if (!file.exists(arquivo_lote)) {
    stop(
      "Arquivo não encontrado: ", arquivo_lote,
      "\nConfirme o nome e se está em data/brutos."
    )
  }
  
  periodos <- pegar_periodos_lote(lote_id, tz = tz)
  
  dir.create(file.path(pasta_tratados, lote_id), recursive = TRUE, showWarnings = FALSE)
  saida_rds <- file.path(pasta_tratados, lote_id, "banco_periodos.rds")
  
  message("📂 Lendo arquivo: ", arquivo_lote)
  
  abas <- excel_sheets(arquivo_lote)
  
  banco <- purrr::map2_dfr(
    abas, seq_along(abas),
    function(nome_aba, idx_aba) {
      
      message("➡️ Lendo aba: ", idx_aba, " | ", nome_aba)
      
      df <- read_excel(arquivo_lote, sheet = nome_aba, col_names = TRUE)
      
      # padroniza nomes (remove espaços nas pontas)
      names(df) <- stringr::str_trim(names(df))
      
      # garante Sensor
      if (!"Sensor" %in% names(df)) {
        s <- readr::parse_number(nome_aba)
        if (is.na(s)) s <- idx_aba
        df <- df |> mutate(Sensor = s)
      }
      
      # garante colunas esperadas (se faltar, cria como NA)
      if (!"T"  %in% names(df)) df <- df |> mutate(T  = NA_real_)
      if (!"UR" %in% names(df)) df <- df |> mutate(UR = NA_real_)
      
      # Data/Hora são obrigatórias
      if (!"Data" %in% names(df)) stop("Aba sem coluna Data: ", nome_aba)
      if (!"Hora" %in% names(df)) stop("Aba sem coluna Hora: ", nome_aba)
      
      df |>
        mutate(
          T  = suppressWarnings(as.numeric(stringr::str_replace(as.character(T),  ",", "."))),
          UR = suppressWarnings(as.numeric(stringr::str_replace(as.character(UR), ",", "."))),
          Data = parse_data_excel(Data),
          Hora = parse_hora_excel(Hora),
          data_hora = as.POSIXct(
            paste(Data, Hora),
            format = "%Y-%m-%d %H:%M:%S",
            tz = tz
          )
        )
    }
  )
  
  n_total <- nrow(banco)
  n_ok_ts <- sum(!is.na(banco$data_hora))
  message("ℹ️ Linhas lidas: ", n_total, " | com data_hora válida: ", n_ok_ts)
  
  banco <- banco |> filter(!is.na(data_hora), !is.na(Sensor))
  
  if (nrow(banco) == 0) {
    stop(
      "❌ Banco ficou com 0 linhas após montar data_hora.\n",
      "Isso significa que Data/Hora não estão sendo parseados.\n",
      "Confira no Excel se as colunas se chamam exatamente: Data e Hora,\n",
      "e se Hora não está em formato estranho."
    )
  }
  
  # classifica período
  banco <- banco |> mutate(Periodo = NA_character_)
  
  for (i in seq_len(nrow(periodos))) {
    banco <- banco |>
      mutate(
        Periodo = if_else(
          is.na(Periodo) & data_hora >= periodos$inicio[i] & data_hora <= periodos$fim[i],
          periodos$Periodo[i],
          Periodo
        )
      )
  }
  
  banco <- banco |> mutate(Periodo = factor(Periodo, levels = periodos$Periodo))
  
  # salva
  readr::write_rds(banco, saida_rds, compress = "xz")
  message("✅ ", nrow(banco), " linhas salvas em: ", saida_rds)
  
  invisible(banco)
}

# Exemplo:
# ler_e_tratar_lote("C:/Users/LENOVO/Documents/BroindexAnalise", "lote3", "dados_PRJT_UFSC_lote3.xlsx")
