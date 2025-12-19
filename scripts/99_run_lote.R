# scripts/99_run_lote.R ----------------------------------------

# 1) Defina apenas UMA VEZ o lote aqui:
lote_id <- "lote1"   # <- sem zero à esquerda porque seu arquivo é "_lote1.xlsx"

# 2) carregue configurações globais
source("scripts/00_config.R")

# 3) carregue o script que DEFINE a função ler_e_tratar_lote()
source("scripts/01_leitura_tratamento.R")

# 4) execute a função para gerar banco_periodos.rds
banco <- ler_e_tratar_lote(lote_id)

# 5) agora rode as próximas etapas do pipeline:
source("scripts/02_metricas_falhas.R")
source("scripts/03_graficos_temperatura.R")
source("scripts/04_graficos_falhas_pareto.R")
source("scripts/05_paineis_periodos.R")

message("✅ Pipeline completo executado para o lote: ", lote_id)
