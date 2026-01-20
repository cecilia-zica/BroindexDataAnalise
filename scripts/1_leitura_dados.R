# ============================================================
# scripts/01_leitura_tratamento.R
# Leitura + tratamento do lote bruto -> banco_periodos.rds
# ============================================================

# -------------------------------
# 0) CONFIGURACOES
# -------------------------------
pasta_base <- "C:/Users/LENOVO/Documents/BroindexAnalise"
lote_id    <- "lote2"
arquivo_xlsx <- "dados_PRJT_UFSC_lote1.xlsx"

# -------------------------------
# 1) pacotes
# -------------------------------
library(tidyverse)
library(lubridate)
library(readxl)
library(future)
library(furrr)
library(glue)

# -------------------------------
# 2) caminhos
# -------------------------------
pasta_brutos   <- file.path(pasta_base, "data", "brutos")
pasta_tratados <- file.path(pasta_base, "data", "tratados")

dir_tratado_lote <- function(lote_id) {
  d <- file.path(pasta_tratados, lote_id)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

# -------------------------------
# 3) tabela global de periodos POR LOTE
# -------------------------------
periodos <- tibble(
  Periodo = c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4","Abertura5"),
  inicio  = ymd_hm(c("2025-07-21 10:10","2025-07-23 07:00","2025-07-26 07:00",
                     "2025-07-28 07:00","2025-07-30 07:00","2025-08-04 07:00")),
  fim     = ymd_hm(c("2025-07-23 07:00","2025-07-26 07:00","2025-07-28 07:00",
                     "2025-07-30 07:00","2025-08-04 07:00","2025-09-03 00:00"))
)

# -------------------------------
# 4) FUNÇÃO PRINCIPAL
# -------------------------------
ler_e_tratar_lote <- function(lote_id, arquivo_xlsx = NULL) {
  
  if (is.null(arquivo_xlsx)) {
    arquivo_xlsx <- glue("dados_PRJT_UFSC_{lote_id}.xlsx")
  }
  
  arquivo_lote <- file.path(pasta_brutos, arquivo_xlsx)
  
  if (!file.exists(arquivo_lote)) {
    stop("Arquivo não encontrado: ", arquivo_lote,
         "\nConfirme o nome e se está em data/brutos.")
  }
  
  message("📂 Lendo arquivo: ", arquivo_lote)
  
  # Paralelismo
  plan(multisession, workers = max(1, parallel::detectCores() - 1))
  
  banco <- excel_sheets(arquivo_lote) |>
    future_map_dfr(
      function(nome_aba) {
        read_excel(arquivo_lote, sheet = nome_aba, col_names = TRUE) %>%
          mutate(
            # padroniza decimais
            T  = as.numeric(gsub(",", ".", T)),
            UR = as.numeric(gsub(",", ".", UR)),
            
            # Data e Hora
            Data = as.Date(Data, format = "%d/%m/%Y"),
            Hora = format(as.POSIXct(Hora, format = "%H:%M:%S"), "%H:%M:%S"),
            
            # junta
            data_hora = as.POSIXct(
              paste(Data, Hora),
              format = "%Y-%m-%d %H:%M:%S"
            )
          )
      },
      .progress = TRUE
    )
  
  plan(sequential)
  
  # Cria Periodo usando tabela global `periodos`
  banco <- banco %>%
    mutate(
      Periodo = case_when(
        data_hora >= periodos$inicio[1] & data_hora < periodos$fim[1] ~ "Alojamento",
        data_hora >= periodos$inicio[2] & data_hora < periodos$fim[2] ~ "Abertura1",
        data_hora >= periodos$inicio[3] & data_hora < periodos$fim[3] ~ "Abertura2",
        data_hora >= periodos$inicio[4] & data_hora < periodos$fim[4] ~ "Abertura3",
        data_hora >= periodos$inicio[5] & data_hora < periodos$fim[5] ~ "Abertura4",
        data_hora >= periodos$inicio[6] & data_hora <= periodos$fim[6] ~ "Abertura5",
        TRUE ~ NA_character_
      ),
      Periodo = factor(
        Periodo,
        levels = c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4","Abertura5")
      )
    )
  
  # Salva tratado
  pasta_saida <- dir_tratado_lote(lote_id)
  saida_rds <- file.path(pasta_saida, "banco_periodos.rds")
  
  write_rds(banco, saida_rds, compress = "xz")
  
  message("✅ ", nrow(banco), " linhas salvas em: ", saida_rds)
  
  invisible(banco)
}

# -------------------------------
# 5) EXECUÇÃO
# -------------------------------
ler_e_tratar_lote(lote_id = lote_id, arquivo_xlsx = arquivo_xlsx)

