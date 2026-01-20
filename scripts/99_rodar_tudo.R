# scripts/99_run_lote.R
source("scripts/00_config.R")  # se você quiser centralizar libs/paths aqui

lote_id <- "lote01"
limites_falha <- c(15, 30)

# 01 é leitura/tratamento (se você quiser rodar)
# source("scripts/01_leitura_tratamento.R")

# 02 é tudo em 1 (menos leitura)
source("scripts/02_analise_graficos_unico.R")
