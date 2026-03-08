# ============================================================
# run_lote.R
# Rodar LEITURA + ANÁLISES (GRÁFICOS) por lote (isolado)
# - NÃO roda otimização
# - Não para o loop se um lote falhar (tryCatch por lote)
# - Proteção contra recursão / loop infinito
# ============================================================

# -------------------------------
# TRAVA ANTI-RECURSÃO
# -------------------------------
if (isTRUE(getOption("broindex.run_lote.running"))) {
  stop(
    "⚠️ run_lote.R foi chamado novamente enquanto já estava rodando.\n",
    "Isso indica recursão. Verifique se algum script está dando source() no run_lote.R."
  )
}
options(broindex.run_lote.running = TRUE)
on.exit(options(broindex.run_lote.running = FALSE), add = TRUE)

rm(list = ls())
gc()

# -------------------------------
# 0) CONFIGURAÇÕES GERAIS
# -------------------------------
pasta_base  <- "C:/Users/LENOVO/Documents/BroindexAnalise"
dir_scripts <- file.path(pasta_base, "scripts")

rodar_leitura    <- TRUE
rodar_analises   <- TRUE
rodar_otimizacao <- FALSE

lotes <- c("lote1", "lote2", "lote3")

xlsx_por_lote <- list(
  lote1 = "dados_PRJT_UFSC_lote1.xlsx",
  lote2 = "dados_PRJT_UFSC_lote2.xlsx",
  lote3 = "dados_PRJT_UFSC_lote3.xlsx"
)

# -------------------------------
# 0b) CHECAGENS RÁPIDAS
# -------------------------------
if (!dir.exists(dir_scripts)) stop("Pasta scripts não encontrada: ", dir_scripts)

arqs_necessarios <- c(
  "0_periodos_lotes.R",
  "1_leitura_dados.R",
  "2_main_analise.R"
)

faltando <- arqs_necessarios[!file.exists(file.path(dir_scripts, arqs_necessarios))]
if (length(faltando) > 0) {
  stop("Arquivos faltando em /scripts: ", paste(faltando, collapse = ", "))
}

lotes_ok <- lotes %in% names(xlsx_por_lote)
if (any(!lotes_ok)) {
  stop("Há lotes sem xlsx mapeado: ", paste(lotes[!lotes_ok], collapse = ", "))
}

# -------------------------------
# 0c) LOGS + STATUS
# -------------------------------
log_msg <- function(...) {
  cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " | ", ..., "\n", sep = "")
  flush.console()
}

status <- data.frame(
  lote = lotes,
  leitura_ok   = rep(NA, length(lotes)),
  analise_ok   = rep(NA, length(lotes)),
  erro_leitura = rep(NA_character_, length(lotes)),
  erro_analise = rep(NA_character_, length(lotes)),
  stringsAsFactors = FALSE
)

set_status <- function(lote_id, campo, valor) {
  i <- which(status$lote == lote_id)
  if (length(i) != 1) stop("Status: lote_id não encontrado/duplicado: ", lote_id)
  status[i, campo] <<- valor
}

# -------------------------------
# 1) LOOP DOS LOTES (ISOLADO)
# -------------------------------
for (lote_id in lotes) {
  
  log_msg("========================================")
  log_msg("🚀 INICIANDO: ", lote_id)
  log_msg("========================================")
  
  local({
    
    pasta_base_local  <- pasta_base
    dir_scripts_local <- dir_scripts
    lote_id_local     <- lote_id
    
    arq_xlsx <- file.path(
      pasta_base_local, "data", "brutos", xlsx_por_lote[[lote_id_local]]
    )
    
    arq_rds <- file.path(
      pasta_base_local, "data", "tratados", lote_id_local, "banco_periodos.rds"
    )
    
    # -------------------------
    # 2) LEITURA + TRATAMENTO
    # -------------------------
    if (isTRUE(rodar_leitura)) {
      log_msg("▶️ Rodando leitura/tratamento...")
      
      if (!file.exists(arq_xlsx)) {
        msg <- paste0("❌ XLSX não encontrado: ", arq_xlsx)
        log_msg(msg)
        set_status(lote_id_local, "erro_leitura", msg)
        set_status(lote_id_local, "leitura_ok", FALSE)
        
      } else {
        ok_leitura <- tryCatch({
          pasta_base <- pasta_base_local
          dir_scripts <- dir_scripts_local
          lote_id <- lote_id_local
          
          source(file.path(dir_scripts_local, "1_leitura_dados.R"), local = TRUE)
          
          ler_e_tratar_lote(
            pasta_base   = pasta_base_local,
            lote_id      = lote_id_local,
            arquivo_xlsx = xlsx_por_lote[[lote_id_local]]
          )
          
          TRUE
        }, error = function(e) {
          msg <- paste0("❌ ERRO LEITURA (", lote_id_local, "): ", conditionMessage(e))
          log_msg(msg)
          set_status(lote_id_local, "erro_leitura", msg)
          FALSE
        })
        
        set_status(lote_id_local, "leitura_ok", ok_leitura)
      }
      
    } else {
      log_msg("⏭️ Pulando leitura/tratamento (rodar_leitura = FALSE).")
      set_status(lote_id_local, "leitura_ok", NA)
    }
    
    # -------------------------
    # 3) ANÁLISES / GRÁFICOS
    # -------------------------
    if (isTRUE(rodar_analises)) {
      log_msg("▶️ Rodando análises/gráficos (2_main_analise.R)...")
      
      if (!file.exists(arq_rds)) {
        msg <- paste0(
          "❌ Não achei o RDS do lote: ", arq_rds,
          " (rode leitura antes ou ative rodar_leitura)"
        )
        log_msg(msg)
        set_status(lote_id_local, "erro_analise", msg)
        set_status(lote_id_local, "analise_ok", FALSE)
        
      } else {
        ok_analise <- tryCatch({
          pasta_base <- pasta_base_local
          dir_scripts <- dir_scripts_local
          lote_id <- lote_id_local
          
          source(file.path(dir_scripts_local, "2_main_analise.R"), local = TRUE)
          
          TRUE
        }, error = function(e) {
          msg <- paste0("❌ ERRO ANÁLISE (", lote_id_local, "): ", conditionMessage(e))
          log_msg(msg)
          set_status(lote_id_local, "erro_analise", msg)
          FALSE
        })
        
        set_status(lote_id_local, "analise_ok", ok_analise)
      }
      
    } else {
      log_msg("⏭️ Pulando análises (rodar_analises = FALSE).")
      set_status(lote_id_local, "analise_ok", NA)
    }
    
    # -------------------------
    # 4) OTIMIZAÇÃO (NÃO RODA)
    # -------------------------
    if (isTRUE(rodar_otimizacao)) {
      log_msg("⚠️ rodar_otimizacao = TRUE, mas este run_lote.R está configurado para NÃO rodar otimização.")
    }
    
    gc()
  })
  
  log_msg("✅ FINALIZADO (loop continua): ", lote_id)
}

log_msg("🏁 TODOS OS LOTES PROCESSADOS (com ou sem erro).")
log_msg("📌 RESUMO:")
print(status)