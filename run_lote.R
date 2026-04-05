# ============================================================
# run_lote.R — Ponto de entrada único do projeto BroindexAnalise
#
# FLUXO DO PIPELINE:
#
#   Etapa 1 — scripts/1_leitura.R
#     Lê o XLSX bruto e salva o RDS tratado em
#     data/tratados/<lote_id>/banco_periodos.rds.
#     Necessário apenas na primeira vez ou se o XLSX mudar.
#
#   Etapa 2 — scripts/2_analise.R
#     Define todas as funções (gaps, falhas, plots) e executa
#     o pipeline completo. Saída em: resultados/<lote_id>/
#
#   Etapa 3 — scripts/3_comparativo.R
#     Tabela comparativa de observações por sensor entre todos
#     os lotes que já têm RDS disponível (independente de `lotes`).
#     Saída em: data/referencia/contagem_obs_por_sensor.csv e .xlsx
#
# Como usar:
#   1. Ajuste as variáveis na seção CONFIGURAÇÕES abaixo.
#   2. Dê source() neste arquivo (Ctrl+Shift+Enter no RStudio).
# ============================================================

# ============================================================
# CONFIGURAÇÕES — edite apenas esta seção
# ============================================================

# Lotes a processar nas Etapas 1 e 2
lotes <- c("lote1")
# Exemplos:
#   lotes <- c("lote2")
#   lotes <- c("lote1", "lote2", "lote3")

# Mapeamento lote → arquivo XLSX bruto (em data/brutos/)
xlsx_por_lote <- list(
  lote1 = "dados_PRJT_UFSC_lote1.xlsx",
  lote2 = "dados_PRJT_UFSC_lote2.xlsx",
  lote3 = "dados_PRJT_UFSC_lote3.xlsx"
)

# ── Flags de execução ────────────────────────────────────────
# FALSE = pula a etapa   TRUE = executa

# Etapa 1: lê o XLSX bruto e gera o RDS tratado.
# Necessário apenas na primeira vez ou se o dado bruto mudar.
rodar_leitura <- TRUE

# Etapa 2: todas as análises e gráficos por lote.
# Saída em resultados/<lote_id>/
rodar_analises <- FALSE

# Etapa 3: tabela comparativa de obs por sensor entre lotes.
# Detecta automaticamente quais lotes têm RDS disponível.
# Saída em data/referencia/contagem_obs_por_sensor.csv e .xlsx
rodar_comparativo <- FALSE

# ============================================================
# SETUP — não edite abaixo desta linha
# ============================================================

# Detecta a raiz do projeto automaticamente.
# Funciona tanto com source() quanto ao abrir via .Rproj no RStudio.
pasta_base <- normalizePath(
  tryCatch(dirname(sys.frame(1)$ofile), error = function(e) getwd()),
  winslash = "/"
)

# Trava anti-recursão (evita loop se algum script der source() neste arquivo)
if (isTRUE(getOption("broindex.run_lote.running"))) {
  stop(
    "run_lote.R foi chamado enquanto já estava rodando. ",
    "Verifique se algum script está dando source() no run_lote.R."
  )
}
options(broindex.run_lote.running = TRUE)
on.exit(options(broindex.run_lote.running = FALSE), add = TRUE)

dir_scripts <- file.path(pasta_base, "scripts")

# Validações iniciais
if (!dir.exists(dir_scripts))
  stop("Pasta scripts não encontrada: ", dir_scripts)

arqs_necessarios <- c(
  "config.R",
  "1_leitura.R",
  "2_analise.R",
  "3_comparativo.R"
)
faltando <- arqs_necessarios[!file.exists(file.path(dir_scripts, arqs_necessarios))]
if (length(faltando) > 0)
  stop("Arquivos faltando em scripts/: ", paste(faltando, collapse = ", "))

lotes_invalidos <- lotes[!lotes %in% names(xlsx_por_lote)]
if (length(lotes_invalidos) > 0)
  stop("Lotes sem xlsx mapeado: ", paste(lotes_invalidos, collapse = ", "))

# Log com timestamp
log_msg <- function(...) {
  cat(format(Sys.time(), "%H:%M:%S"), "|", ..., "\n", sep = " ")
  flush.console()
}

# Tabela de status por lote
status <- data.frame(
  lote         = lotes,
  leitura_ok   = NA,
  analise_ok   = NA,
  erro_leitura = NA_character_,
  erro_analise = NA_character_,
  stringsAsFactors = FALSE
)
set_status <- function(lote_id, campo, valor) {
  i <- which(status$lote == lote_id)
  status[i, campo] <<- valor
}

# ============================================================
# ETAPAS 1 E 2: LOOP DOS LOTES
# ============================================================
for (lote_id in lotes) {

  log_msg("========================================")
  log_msg("INICIANDO:", lote_id)
  log_msg("========================================")

  local({

    lote_id_local     <- lote_id
    pasta_base_local  <- pasta_base
    dir_scripts_local <- dir_scripts

    arq_xlsx <- file.path(
      pasta_base_local, "data", "brutos", xlsx_por_lote[[lote_id_local]]
    )
    arq_rds <- file.path(
      pasta_base_local, "data", "tratados", lote_id_local, "banco_periodos.rds"
    )

    # ── Etapa 1: Leitura ────────────────────────────────────
    if (isTRUE(rodar_leitura)) {
      log_msg("Etapa 1: leitura/tratamento do XLSX...")

      if (!file.exists(arq_xlsx)) {
        msg <- paste0("XLSX não encontrado: ", arq_xlsx)
        log_msg(msg)
        set_status(lote_id_local, "erro_leitura", msg)
        set_status(lote_id_local, "leitura_ok", FALSE)

      } else {
        ok <- tryCatch({
          pasta_base  <- pasta_base_local
          dir_scripts <- dir_scripts_local
          lote_id     <- lote_id_local
          source(file.path(dir_scripts_local, "1_leitura.R"), local = TRUE)
          ler_e_tratar_lote(
            pasta_base   = pasta_base_local,
            lote_id      = lote_id_local,
            arquivo_xlsx = xlsx_por_lote[[lote_id_local]]
          )
          TRUE
        }, error = function(e) {
          msg <- paste0("ERRO LEITURA (", lote_id_local, "): ", conditionMessage(e))
          log_msg(msg)
          set_status(lote_id_local, "erro_leitura", msg)
          FALSE
        })
        set_status(lote_id_local, "leitura_ok", ok)
      }

    } else {
      log_msg("Etapa 1: pulada (rodar_leitura = FALSE).")
      set_status(lote_id_local, "leitura_ok", NA)
    }

    # ── Etapa 2: Análises / Gráficos ────────────────────────
    if (isTRUE(rodar_analises)) {
      log_msg("Etapa 2: análises e gráficos...")

      if (!file.exists(arq_rds)) {
        msg <- paste0(
          "RDS não encontrado: ", arq_rds,
          " (ative rodar_leitura = TRUE para gerá-lo)"
        )
        log_msg(msg)
        set_status(lote_id_local, "erro_analise", msg)
        set_status(lote_id_local, "analise_ok", FALSE)

      } else {
        ok <- tryCatch({
          pasta_base  <- pasta_base_local
          dir_scripts <- dir_scripts_local
          lote_id     <- lote_id_local
          source(file.path(dir_scripts_local, "2_analise.R"), local = TRUE)
          TRUE
        }, error = function(e) {
          msg <- paste0("ERRO ANALISE (", lote_id_local, "): ", conditionMessage(e))
          log_msg(msg)
          set_status(lote_id_local, "erro_analise", msg)
          FALSE
        })
        set_status(lote_id_local, "analise_ok", ok)
      }

    } else {
      log_msg("Etapa 2: pulada (rodar_analises = FALSE).")
      set_status(lote_id_local, "analise_ok", NA)
    }

    gc()
    log_msg("Concluido:", lote_id_local)
  })
}

# ============================================================
# ETAPA 3: COMPARATIVO CROSS-LOTE (roda após todos os lotes)
# ============================================================
if (isTRUE(rodar_comparativo)) {
  log_msg("========================================")
  log_msg("Etapa 3: tabela comparativa cross-lote...")
  log_msg("========================================")

  tryCatch({
    source(file.path(dir_scripts, "3_comparativo.R"), local = TRUE)
    log_msg("Etapa 3 concluida. Resultado em: data/referencia/")
  }, error = function(e) {
    log_msg("ERRO comparativo:", conditionMessage(e))
  })
}

# ============================================================
# RESUMO FINAL
# ============================================================
log_msg("========================================")
log_msg("TODOS OS LOTES FINALIZADOS")
log_msg("========================================")
print(status)
log_msg("Resultados por lote em:  resultados/<lote>/")
log_msg("Tabelas comparativas em: data/referencia/")
