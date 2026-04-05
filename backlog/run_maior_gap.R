# ============================================================
# scripts/run_maior_gap_todos_lotes.R
# MAIOR GAP (tempo sem registro) + métricas por período
# Roda para: lote1, lote2 e lote3
# ============================================================
#
#
# Objetivo:
# avaliar falhas de registro e qualidade do sinal dos sensores
# ao longo dos diferentes períodos de cada lote.
#
# O script gera, por período:
#
# - métricas de gaps entre leituras
# - contagem de falhas acima de limites definidos
# - tempo excedente sem registro
# - comparação do sinal de cada sensor com a mediana global
# - análise de correlação e agrupamento entre sensores
# - seleção de sensores mais representativos
#
# Também são salvos gráficos e tabelas-resumo para apoiar a
# interpretação dos resultados por sensor e por período.
#
# Referência adotada:
# a mediana global dos sensores em cada instante.
#
# Tolerância padrão:
# 1 °C para temperatura.
#
# Observação:
#    Todas as análises são feitas separadamente por período.
# ==================================================================


rm(list = ls())
gc()

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(readr)
  library(glue)
  library(viridis)
})

# ----------------------------
# 0) CONFIGURAÇÕES PRINCIPAIS
# ----------------------------

pasta_base <- "C:/Users/LENOVO/Documents/BroindexAnalise"
lotes <- c("lote1") 

# limites (min) para linhas de referência + métricas de falha
padrao_min <- c(10, 15, 30, 60)

# timezone
tz_local <- "America/Sao_Paulo"

# remove duplicatas por (Sensor, Periodo, data_hora)
remover_duplicatas <- TRUE

# filtrar sensores (opcional)
sensores_alvo <- 7:15 # ex: 1:30

# ----------------------------
# 0a) CONFIGURAÇÕES DO “PACOTE ML”
# ----------------------------

# qual variável analisar no ML (T ou UR)
# - seu objetivo principal costuma ser T, então deixo T como default.
var_ml <- "T"       # "T" ou "UR"

# bin de tempo para alinhar sensores no ML (5 min é coerente com “ideal 5 em 5”)
bin_ml <- "5 min"

# tolerância do outlier (você pediu 1 grau; para UR você pode trocar)
tol_abs_T  <- 1.0   # 1°C
tol_abs_UR <- 5.0   # exemplo (ajuste se quiser)

# seleção de sensores (k) para “reconstrução”
k_max <- 5         # quantos sensores selecionar no máximo (ex.: 10)
k_min <- 1

# para não ficar pesado em bases enormes:
# - limita quantos bins (linhas) entram na parte ML
max_bins_ml <- 1500  # se tiver mais bins, amostra aleatoriamente

# ----------------------------
# 1) FUNÇÕES DE DIRETÓRIO / SALVAR
# ----------------------------

dir_tratado_lote <- function(pasta_base, lote_id) {
  file.path(pasta_base, "data", "tratados", lote_id)
}

dir_result_lote <- function(pasta_base, lote_id, ...) {
  d <- file.path(pasta_base, "resultados", lote_id, ...)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

save_plot <- function(plot, path, w = 12, h = 7, dpi = 300) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  ggsave(filename = path, plot = plot, width = w, height = h, dpi = dpi)
  message("✅ salvo: ", path)
}

# ----------------------------
# 1b) SALVAR CSV “AMIGÁVEL” PARA EXCEL pt-BR
# ----------------------------
# Por que existe:
# - Excel pt-BR costuma interpretar errado CSV padrão (decimal = ".")
# - CSV2 usa ";" e decimal ",", então abre certo no Excel
salvar_csv_excel <- function(df, path_sem_extensao) {
  # CSV padrão (bom para R / Python)
  readr::write_csv(df, paste0(path_sem_extensao, ".csv"))
  
  # CSV2 (bom para Excel pt-BR)
  utils::write.csv2(df, paste0(path_sem_extensao, "_excel.csv"),
                    row.names = FALSE, fileEncoding = "UTF-8")
}
# ----------------------------
# 2) FUNÇÕES DE DADOS (banco + layout)
# ----------------------------

add_linha_coluna <- function(df) {
  df |>
    mutate(
      Sensor = as.integer(Sensor),
      Linha = case_when(
        Sensor %in% c(1, 4, 7, 10, 13, 16, 19, 22, 25, 28) ~ "Linha 1",
        Sensor %in% c(2, 5, 8, 11, 14, 17, 20, 23, 26, 29) ~ "Linha 2",
        Sensor %in% c(3, 6, 9, 12, 15, 18, 21, 24, 27, 30) ~ "Linha 3",
        TRUE ~ NA_character_
      ),
      Coluna = paste("Coluna", ((Sensor - 1) %/% 3) + 1)
    )
}

carregar_banco <- function(pasta_base, lote_id) {
  arq <- file.path(dir_tratado_lote(pasta_base, lote_id), "banco_periodos.rds")
  if (!file.exists(arq)) stop("Não achei o banco tratado: ", arq)
  readr::read_rds(arq)
}

preparar_banco_maior_gap <- function(banco, tz_local, remover_duplicatas = TRUE, sensores_alvo = NULL) {
  banco2 <- banco |>
    add_linha_coluna() |>
    mutate(
      data_hora = as.POSIXct(data_hora, tz = tz_local),
      Periodo   = as.character(Periodo),
      Sensor    = as.integer(Sensor)
    ) |>
    filter(!is.na(data_hora), !is.na(Sensor), !is.na(Periodo))
  
  if (!is.null(sensores_alvo)) {
    banco2 <- banco2 |> filter(Sensor %in% sensores_alvo)
  }
  
  if (isTRUE(remover_duplicatas)) {
    banco2 <- banco2 |>
      distinct(Sensor, Periodo, data_hora, .keep_all = TRUE)
  }
  
  banco2
}

# ----------------------------
# 3) GAP: cálculo base
# ----------------------------

# gap = tempo entre duas leituras consecutivas do MESMO sensor no MESMO período
calc_gaps <- function(df) {
  df |>
    arrange(Sensor, data_hora) |>
    group_by(Sensor, Linha, Coluna, Periodo) |>
    mutate(
      dif_min     = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      inicio_gap  = lag(data_hora),
      fim_gap     = data_hora,
      Dia         = as.Date(fim_gap)
    ) |>
    ungroup() |>
    filter(is.finite(dif_min), dif_min >= 0)
}

# ----------------------------
# 4) GAP: métricas robustas por sensor
# ----------------------------

# >>> AQUI nascem:
# - MAIOR_GAP: max_gap_min
# - P95_GAP:   p95_gap_min
# - N_FALHAS:  n_gaps_acima_<lim>
# - TEMPO PERDIDO: excesso_total_<lim>
calc_metricas_gap_por_sensor <- function(gaps_df, limites = c(10, 15, 30, 60)) {
  
  # regra prática: dif_min == 0 costuma ser duplicata/ruído
  d <- gaps_df |> filter(is.finite(dif_min), dif_min > 0)
  
  if (nrow(d) == 0) {
    return(tibble(
      Sensor = integer(),
      Linha = character(),
      Coluna = character(),
      n_gaps = integer(),
      max_gap_min = numeric(),
      p95_gap_min = numeric(),
      inicio_max_gap = as.POSIXct(character()),
      fim_max_gap = as.POSIXct(character())
    ))
  }
  
  base <- d |>
    group_by(Sensor, Linha, Coluna) |>
    summarise(
      # MAIOR_GAP
      max_gap_min     = max(dif_min, na.rm = TRUE),
      inicio_max_gap  = inicio_gap[which.max(dif_min)],
      fim_max_gap     = fim_gap[which.max(dif_min)],
      
      # tamanho da amostra
      n_gaps          = n(),
      
      # P95_GAP
      p95_gap_min     = as.numeric(stats::quantile(dif_min, probs = 0.95, na.rm = TRUE, type = 7)),
      
      .groups = "drop"
    )
  
  # colunas por limite (contagem + excesso)
  for (lim in limites) {
    nm_n <- paste0("n_gaps_acima_", lim)
    nm_e <- paste0("excesso_total_", lim)
    
    aux <- d |>
      group_by(Sensor, Linha, Coluna) |>
      summarise(
        "{nm_n}" := sum(dif_min > lim, na.rm = TRUE),
        "{nm_e}" := sum(pmax(dif_min - lim, 0), na.rm = TRUE),
        .groups = "drop"
      )
    
    base <- base |> left_join(aux, by = c("Sensor", "Linha", "Coluna"))
  }
  
  base |> arrange(desc(max_gap_min))
}

# ----------------------------
# 4b) CLASSIFICAÇÃO DOS SENSORES (instável vs sistêmico)
# ----------------------------

# helper: normaliza para 0..1 (sem explodir com outliers)
rank01 <- function(x) {
  if (all(is.na(x))) return(x)
  r <- dplyr::min_rank(x)  # 1..n
  (r - 1) / (max(r) - 1 + 1e-9)
}

# Detecta evento sistêmico:
# - olha os "maiores gaps" de cada sensor
# - se muitos sensores começam o maior gap em janela de X minutos,
#   isso é cara de evento sistêmico (rede/captura/energia)
detectar_evento_sistemico <- function(tab_gap, janela_min = 20, min_sensores = 3) {
  if (!all(c("Sensor", "inicio_max_gap", "fim_max_gap") %in% names(tab_gap))) {
    return(tab_gap |> mutate(flag_evento_sistemico = FALSE))
  }
  
  t0 <- tab_gap |>
    mutate(
      inicio_max_gap = as.POSIXct(inicio_max_gap, tz = "UTC"),
      fim_max_gap    = as.POSIXct(fim_max_gap, tz = "UTC")
    ) |>
    filter(!is.na(inicio_max_gap), !is.na(fim_max_gap)) |>
    arrange(inicio_max_gap)
  
  if (nrow(t0) == 0) {
    return(tab_gap |> mutate(flag_evento_sistemico = FALSE))
  }
  
  # cria “clusters” por proximidade do horário de início
  t0 <- t0 |>
    mutate(
      delta_min = c(NA_real_, as.numeric(difftime(inicio_max_gap[-1], inicio_max_gap[-n()], units = "mins"))),
      novo_grupo = if_else(is.na(delta_min) | delta_min > janela_min, 1L, 0L),
      grupo = cumsum(novo_grupo)
    )
  
  grupos_grandes <- t0 |>
    count(grupo, name = "n_sens") |>
    filter(n_sens >= min_sensores) |>
    pull(grupo)
  
  t0 <- t0 |>
    mutate(flag_evento_sistemico = grupo %in% grupos_grandes) |>
    select(Sensor, flag_evento_sistemico)
  
  tab_gap |>
    left_join(t0, by = "Sensor") |>
    mutate(flag_evento_sistemico = tidyr::replace_na(flag_evento_sistemico, FALSE))
}

# Classifica “instável estrutural”:
# - usa P95 + excesso_30 + n_gaps>30
classificar_instabilidade <- function(tab_gap) {
  tab_gap2 <- tab_gap
  
  # garante colunas
  if (!"n_gaps_acima_30" %in% names(tab_gap2)) tab_gap2$n_gaps_acima_30 <- NA_integer_
  if (!"excesso_total_30" %in% names(tab_gap2)) tab_gap2$excesso_total_30 <- NA_real_
  
  # thresholds por quantil (bom porque se adapta ao período)
  thr_p95 <- stats::quantile(tab_gap2$p95_gap_min, 0.75, na.rm = TRUE)
  thr_n30 <- stats::quantile(tab_gap2$n_gaps_acima_30, 0.75, na.rm = TRUE)
  thr_ex  <- stats::quantile(tab_gap2$excesso_total_30, 0.75, na.rm = TRUE)
  
  tab_gap2 |>
    mutate(
      flag_instavel_estrutural = case_when(
        is.na(p95_gap_min) ~ FALSE,
        p95_gap_min >= thr_p95 & (n_gaps_acima_30 >= thr_n30 | excesso_total_30 >= thr_ex) ~ TRUE,
        TRUE ~ FALSE
      )
    )
}

# Índice final de confiabilidade (0..100):
# - penaliza falha recorrente (p95, excesso, n_falhas)
# - penaliza “qualidade do sinal” (rmse/outlier/bias) se existir
# - e reduz o peso do que foi “evento sistêmico” (porque não é culpa do sensor)
criar_indice_confiabilidade <- function(tab_master) {
  tab_master2 <- tab_master
  
  # faltas (se não tiver, vira NA -> não pesa)
  if (!"p95_gap_min" %in% names(tab_master2)) tab_master2$p95_gap_min <- NA_real_
  if (!"excesso_total_30" %in% names(tab_master2)) tab_master2$excesso_total_30 <- NA_real_
  if (!"n_gaps_acima_30" %in% names(tab_master2)) tab_master2$n_gaps_acima_30 <- NA_real_
  
  # qualidade (ML) pode não existir em alguns períodos
  if (!"rmse" %in% names(tab_master2)) tab_master2$rmse <- NA_real_
  if (!"outlier_rate" %in% names(tab_master2)) tab_master2$outlier_rate <- NA_real_
  if (!"bias" %in% names(tab_master2)) tab_master2$bias <- NA_real_
  
  # scores 0..1 (rank)
  s_p95 <- rank01(tab_master2$p95_gap_min)
  s_ex  <- rank01(tab_master2$excesso_total_30)
  s_n30 <- rank01(tab_master2$n_gaps_acima_30)
  
  s_rmse <- rank01(tab_master2$rmse)
  s_out  <- rank01(tab_master2$outlier_rate)
  s_bias <- rank01(abs(tab_master2$bias))
  
  # score de falha (0..1): maior = pior
  score_falha01 <- (s_p95 + s_ex + s_n30) / 3
  
  # score do sinal (0..1): maior = pior (se ML existir)
  score_sinal01 <- (s_rmse + s_out + s_bias) / 3
  
  # combina (peso pode ajustar)
  # regra:
  # - falha pesa mais (porque seu problema principal é registro)
  # - sinal pesa menos (mas entra)
  raw <- 0.65 * score_falha01 + 0.35 * score_sinal01
  
  # se for evento sistêmico, “alivia” (não zera, só reduz)
  if ("flag_evento_sistemico" %in% names(tab_master2)) {
    raw <- if_else(tab_master2$flag_evento_sistemico, raw * 0.6, raw)
  }
  
  # confiabilidade 0..100 (maior = melhor)
  tab_master2 |>
    mutate(
      score_ruim_01 = raw,
      confiabilidade_0_100 = pmax(0, pmin(100, round(100 * (1 - raw), 1)))
    )
}
# ----------------------------
# 5) GAP: visualizações
# ----------------------------

plot_ranking_metrica <- function(tab, lote_id, metrica, titulo_metrica, padrao_min = NULL, titulo_extra = NULL) {
  g <- ggplot(
    tab,
    aes(
      x    = reorder(as.factor(Sensor), -.data[[metrica]]),
      y    = .data[[metrica]],
      fill = Linha
    )
  ) +
    geom_col(alpha = 0.9) +
    geom_text(aes(label = round(.data[[metrica]], 1)), vjust = -0.25, size = 3) +
    scale_fill_viridis_d(option = "C") +
    labs(
      title = glue(
        "{titulo_metrica} por sensor — {lote_id}",
        "{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"
      ),
      x = "Sensor",
      y = titulo_metrica,
      fill = "Linha"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  if (!is.null(padrao_min) && length(padrao_min) > 0) {
    g <- g + geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey35")
  }
  
  g
}

plot_heatmap_maior_gap_por_dia <- function(df_day, lote_id, titulo_extra = NULL) {
  ggplot(df_day, aes(x = Dia, y = factor(Sensor), fill = max_gap_dia)) +
    geom_tile(height = 0.9) +
    scale_fill_viridis_c(option = "C", trans = "sqrt", na.value = "grey95") +
    labs(
      title = glue(
        "Heatmap — maior gap por dia (tempo × sensor) — {lote_id}",
        "{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"
      ),
      subtitle = "Cada célula = MAIOR intervalo (min) naquele dia para aquele sensor",
      x = "Dia",
      y = "Sensor",
      fill = "Maior gap\n(min)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"), axis.text.y = element_text(size = 7))
}

plot_gaps_sensor <- function(gaps_df, sensor_id, lote_id, padrao_min, titulo_extra = NULL) {
  d <- gaps_df |>
    filter(Sensor == sensor_id) |>
    filter(is.finite(dif_min), dif_min > 0) |>
    arrange(fim_gap)
  
  if (nrow(d) == 0) return(NULL)
  
  max_gap <- max(d$dif_min, na.rm = TRUE)
  d_max   <- d |> slice(which.max(dif_min))
  
  ggplot(d, aes(x = fim_gap, y = dif_min)) +
    geom_point(alpha = 0.35, size = 1.1, color = "grey35") +
    geom_point(data = d_max, color = "red", size = 2.5) +
    geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey55") +
    geom_hline(yintercept = max_gap, color = "red", linewidth = 0.8) +
    annotate(
      "text",
      x = d_max$fim_gap,
      y = max_gap,
      label = glue("MAX {round(max_gap, 1)} min"),
      vjust = -0.6,
      color = "red",
      size = 3.2
    ) +
    labs(
      title = glue(
        "Gaps ao longo do tempo — Sensor {sensor_id} — {lote_id}",
        "{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"
      ),
      subtitle = glue("Maior gap: {format(d_max$inicio_gap, '%d/%m %H:%M')} → {format(d_max$fim_gap, '%d/%m %H:%M')}"),
      x = "Tempo (fim do intervalo)",
      y = "Gap entre registros (min)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
}

plot_gaps_facets_6 <- function(gaps_df, sensors_6, lote_id, padrao_min, titulo_extra = NULL) {
  d <- gaps_df |>
    filter(Sensor %in% sensors_6) |>
    filter(is.finite(dif_min), dif_min > 0) |>
    mutate(Sensor_f = factor(Sensor, levels = sort(unique(Sensor))))
  
  if (nrow(d) == 0) return(NULL)
  
  ggplot(d, aes(x = fim_gap, y = dif_min)) +
    geom_point(alpha = 0.35, size = 1.0, color = "grey35") +
    geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey55") +
    facet_wrap(~Sensor_f, ncol = 3, scales = "free_x") +
    labs(
      title = glue(
        "Gaps — 6 sensores por página — {lote_id}",
        "{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"
      ),
      subtitle = "Y = gap (min). Linhas tracejadas = limites padrão",
      x = "Tempo (fim do intervalo)",
      y = "Gap (min)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
}

# ============================================================
# 6) “PACOTE ML” — funções (qualidade + correlação + seleção k)
# ============================================================

# ----------------------------
# 6.1) Monta matriz tempo x sensor (alinhada por bin)
# ----------------------------
# Retorna:
# - wide: dataframe com colunas: t_bin, s1, s2, ...
# - sensores: vetor de sensores (nomes das colunas)
make_matrix_por_bin <- function(df_periodo, var = "T", bin = "5 min") {
  
  if (!var %in% names(df_periodo)) {
    return(list(wide = NULL, sensores = character()))
  }
  
  # agrega no bin (mediana) -> robusto contra spikes
  df_bin <- df_periodo |>
    filter(!is.na(data_hora), !is.na(Sensor), !is.na(.data[[var]])) |>
    mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
    group_by(t_bin, Sensor) |>
    summarise(valor = median(.data[[var]], na.rm = TRUE), .groups = "drop")
  
  if (nrow(df_bin) == 0) {
    return(list(wide = NULL, sensores = character()))
  }
  
  wide <- df_bin |>
    mutate(Sensor = as.character(Sensor)) |>
    tidyr::pivot_wider(names_from = Sensor, values_from = valor)
  
  sensores <- setdiff(names(wide), "t_bin")
  list(wide = wide, sensores = sensores)
}

# ----------------------------
# 6.2) Referência = mediana global por tempo (por bin)
# ----------------------------
# Para cada t_bin:
# ref = mediana dos sensores disponíveis naquele t_bin
calc_referencia_mediana_global <- function(wide_df) {
  # wide_df: t_bin + colunas dos sensores
  sensores <- setdiff(names(wide_df), "t_bin")
  if (length(sensores) == 0) return(tibble(t_bin = wide_df$t_bin, ref_mediana = NA_real_))
  
  # pega só a parte numérica como matriz (sem dplyr dentro do apply)
  mat <- as.matrix(wide_df[, sensores, drop = FALSE])
  
  ref_med <- apply(mat, 1, median, na.rm = TRUE)
  
  tibble(
    t_bin = wide_df$t_bin,
    ref_mediana = as.numeric(ref_med)
  )
}

# ----------------------------
# 6.3) Qualidade do sinal por sensor vs mediana global
# ----------------------------
# Métricas:
# - bias: média(sensor - ref)
# - rmse: sqrt(média((sensor-ref)^2))
# - outlier_rate: %(|sensor-ref|>tolerância)
calc_qualidade_sinal_vs_ref <- function(wide_df, tol_abs = 1.0) {
  sensores <- setdiff(names(wide_df), "t_bin")
  if (length(sensores) == 0) return(tibble())
  
  ref <- calc_referencia_mediana_global(wide_df)
  df2 <- wide_df |> left_join(ref, by = "t_bin")
  
  purrr::map_dfr(sensores, function(sid) {
    v <- df2[[sid]]
    r <- df2$ref_mediana
    
    ok <- is.finite(v) & is.finite(r)
    if (!any(ok)) {
      return(tibble(
        Sensor = as.integer(sid),
        n_pts = 0L,
        bias = NA_real_,
        rmse = NA_real_,
        outlier_rate = NA_real_
      ))
    }
    
    err <- v[ok] - r[ok]
    
    tibble(
      Sensor = as.integer(sid),
      n_pts = sum(ok),
      bias = mean(err, na.rm = TRUE),
      rmse = sqrt(mean(err^2, na.rm = TRUE)),
      outlier_rate = mean(abs(err) > tol_abs, na.rm = TRUE)
    )
  })
}

# ----------------------------
# 6.4) Correlação + clustering (redundância)
# ----------------------------
# - corr matrix (pairwise complete)
# - hierárquico (hclust) com distância = 1 - corr
# - score de “unicidade” simples:
#     unicidade = 1 - max(corr_com_outros)
#     -> se max corr ~ 0.98 => unicidade ~ 0.02 (super redundante)
calc_correlacao_e_unicidade <- function(wide_df) {
  sensores <- setdiff(names(wide_df), "t_bin")
  if (length(sensores) < 2) {
    return(list(corr = NULL, unicidade = tibble()))
  }
  
  mat <- as.matrix(wide_df[, sensores, drop = FALSE])
  suppressWarnings({
    corr <- cor(mat, use = "pairwise.complete.obs")
  })
  
  # unicidade (redundância)
  # - define diagonal como NA pra não pegar ele mesmo
  corr2 <- corr
  diag(corr2) <- NA_real_
  max_corr <- apply(corr2, 1, function(x) max(x, na.rm = TRUE))
  unicidade <- tibble(
    Sensor = as.integer(rownames(corr)),
    max_corr_com_outros = as.numeric(max_corr),
    unicidade = 1 - as.numeric(max_corr)
  )
  
  list(corr = corr, unicidade = unicidade)
}

plot_heatmap_correlacao <- function(corr_mat, lote_id, pp, var) {
  if (is.null(corr_mat)) return(NULL)
  
  dfm <- as.data.frame(as.table(corr_mat)) |>
    transmute(
      s1 = Var1,
      s2 = Var2,
      corr = Freq
    )
  
  ggplot(dfm, aes(x = s1, y = s2, fill = corr)) +
    geom_tile() +
    scale_fill_viridis_c(option = "C", limits = c(-1, 1)) +
    labs(
      title = glue("Heatmap correlação — {lote_id} — {pp} — var={var}"),
      x = "Sensor",
      y = "Sensor",
      fill = "corr"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
      axis.text.y = element_text(size = 6)
    )
}

plot_dendrograma_correlacao <- function(corr_mat, lote_id, pp, var) {
  if (is.null(corr_mat) || nrow(corr_mat) < 2) return(NULL)
  
  # distância = 1 - corr (quanto mais correlacionado, mais perto)
  dist_mat <- as.dist(1 - corr_mat)
  hc <- hclust(dist_mat, method = "average")
  
  # plot “base” em PNG é o mais simples e robusto (sem depender de pacotes extras)
  # então aqui eu só devolvo o objeto hc; o salvamento é feito depois via png()
  hc
}

# ----------------------------
# 6.5) Seleção “k sensores” por capacidade de reconstrução (greedy)
# ----------------------------
# Ideia:
# - queremos escolher um subconjunto S de sensores tal que os outros
#   sejam “bem previstos” por S.
#
# Como medimos:
# - para cada sensor alvo j fora de S:
#     ajusta regressão linear: j ~ sensores de S
#     calcula RMSE de reconstrução (em dados disponíveis)
# - score(S) = média desses RMSEs nos sensores fora de S
# - greedy: começa vazio e vai adicionando o sensor que mais reduz score(S)
#
# Obs:
# - é “ML simples” (regressão) e funciona MUITO bem pra redundância/representatividade
# - não precisa de deep learning pra isso.
calc_recon_rmse_for_set <- function(mat, sensores, set_sel) {
  # mat: matrix [n_time x n_sensores] colnames = sensores
  # set_sel: sensores selecionados (character)
  # retorna: rmse médio de reconstrução dos sensores NÃO selecionados
  
  if (length(set_sel) == 0) return(Inf)
  
  rest <- setdiff(sensores, set_sel)
  if (length(rest) == 0) return(0)
  
  Xall <- mat[, set_sel, drop = FALSE]
  
  rmses <- c()
  for (target in rest) {
    yall <- mat[, target]
    
    # casos completos para (y e X)
    ok <- is.finite(yall)
    for (sx in set_sel) ok <- ok & is.finite(Xall[, sx])
    
    if (sum(ok) < 20) {
      # pouco dado => ignora esse target para não poluir
      next
    }
    
    X <- as.matrix(Xall[ok, , drop = FALSE])
    y <- yall[ok]
    
    # adiciona intercepto
    X <- cbind(1, X)
    
    fit <- tryCatch(
      lm.fit(x = X, y = y),
      error = function(e) NULL
    )
    if (is.null(fit)) next
    
    yhat <- as.numeric(X %*% fit$coefficients)
    rmse <- sqrt(mean((y - yhat)^2, na.rm = TRUE))
    rmses <- c(rmses, rmse)
  }
  
  if (length(rmses) == 0) return(Inf)
  mean(rmses, na.rm = TRUE)
}

selecionar_sensores_greedy <- function(wide_df, k_max = 10, max_bins = 6000) {
  sensores <- setdiff(names(wide_df), "t_bin")
  if (length(sensores) < 2) {
    return(list(selecionados = sensores, curva = tibble(k = 1, rmse = NA_real_)))
  }
  
  # matriz numérica
  mat <- as.matrix(wide_df[, sensores, drop = FALSE])
  
  # amostragem de linhas (bins) se estiver muito grande (pra não ficar lento)
  if (nrow(mat) > max_bins) {
    set.seed(123)
    idx <- sample(seq_len(nrow(mat)), size = max_bins)
    mat <- mat[idx, , drop = FALSE]
  }
  
  k_max <- min(k_max, length(sensores))
  
  selecionados <- character()
  curva <- tibble()
  
  # passo 1: escolha inicial
  # - como baseline simples: pega o sensor com maior “cobertura” (mais dados)
  cobertura <- colSums(is.finite(mat))
  s0 <- names(which.max(cobertura))
  selecionados <- c(selecionados, s0)
  
  rmse0 <- calc_recon_rmse_for_set(mat, sensores, selecionados)
  curva <- bind_rows(curva, tibble(k = length(selecionados), rmse = rmse0))
  
  # passos seguintes: greedy
  while (length(selecionados) < k_max) {
    candidatos <- setdiff(sensores, selecionados)
    if (length(candidatos) == 0) break
    
    scores <- purrr::map_dbl(candidatos, function(cand) {
      calc_recon_rmse_for_set(mat, sensores, c(selecionados, cand))
    })
    
    best <- candidatos[which.min(scores)]
    selecionados <- c(selecionados, best)
    
    curva <- bind_rows(curva, tibble(k = length(selecionados), rmse = min(scores)))
  }
  
  list(selecionados = selecionados, curva = curva)
}

plot_curva_recon <- function(curva, lote_id, pp, var) {
  if (nrow(curva) == 0) return(NULL)
  
  ggplot(curva, aes(x = k, y = rmse)) +
    geom_line(color = "grey30", linewidth = 0.9) +
    geom_point(color = "royalblue4", size = 2.2) +
    scale_x_continuous(breaks = curva$k) +
    labs(
      title = glue("Curva de reconstrução (greedy) — {lote_id} — {pp} — var={var}"),
      subtitle = "Y = RMSE médio ao reconstruir sensores NÃO selecionados (menor é melhor)",
      x = "k (nº de sensores escolhidos)",
      y = "RMSE médio"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
}

# ----------------------------
# 6.6) Junta tudo num “master” por sensor (gap + qualidade + redundância)
# ----------------------------
montar_master_por_sensor <- function(tab_gap, tab_qual, tab_uni, banco_pp) {
  # garante Linha/Coluna no master
  layout <- banco_pp |> distinct(Sensor, Linha, Coluna)

  out <- layout |>
    left_join(tab_gap,  by = c("Sensor", "Linha", "Coluna")) |>
    left_join(tab_qual, by = "Sensor") |>
    left_join(tab_uni,  by = "Sensor")

  # z-score seguro (se tudo NA, devolve NA)
  safe_scale <- function(x) {
    if (all(is.na(x))) return(x)
    as.numeric(scale(x))
  }

  out <- out |>
    mutate(
      # ----------------------------------------------------------
      # variáveis temporárias (NÃO use nomes começando com "_")
      # ----------------------------------------------------------
      tmp_p95  = if ("p95_gap_min" %in% names(out)) p95_gap_min else NA_real_,
      tmp_ex30 = if ("excesso_total_30" %in% names(out)) excesso_total_30 else NA_real_,
      tmp_n30  = if ("n_gaps_acima_30" %in% names(out)) n_gaps_acima_30 else NA_real_,

      # ----------------------------------------------------------
      # score de falha (gap/falta de dado)
      # maior = pior
      # ----------------------------------------------------------
      score_falha = safe_scale(tmp_p95) + safe_scale(tmp_ex30) + safe_scale(tmp_n30),

      # ----------------------------------------------------------
      # score do sinal (drift/defeito) vs mediana global
      # maior = pior
      # ----------------------------------------------------------
      score_sinal = safe_scale(rmse) + safe_scale(outlier_rate) + safe_scale(abs(bias)),

      # ----------------------------------------------------------
      # “valor por ser único”
      # maior = mais único (menos redundante)
      # ----------------------------------------------------------
      score_unico = safe_scale(unicidade)
    ) |>
    select(-starts_with("tmp_"))

  out
}
# ============================================================
# 7) LOOP PRINCIPAL (POR LOTE e POR PERÍODO)
# ============================================================

for (lote_id in lotes) {
  message("\n========================================")
  message("🚀 Rodando GAP + ML para: ", lote_id)
  message("========================================")
  
  banco <- carregar_banco(pasta_base, lote_id)
  banco <- preparar_banco_maior_gap(
    banco,
    tz_local = tz_local,
    remover_duplicatas = remover_duplicatas,
    sensores_alvo = sensores_alvo
  )
  
  dir_maior_gap <- dir_result_lote(pasta_base, lote_id, "falhas", "maior_gap")
  
  periodos_existentes <- banco |>
    distinct(Periodo) |>
    pull(Periodo) |>
    as.character()
  
  for (pp in periodos_existentes) {
    
    # ----------------------------
    # pasta do período
    # ----------------------------
    dir_pp <- file.path(dir_maior_gap, paste0("periodo_", pp))
    dir.create(dir_pp, recursive = TRUE, showWarnings = FALSE)
    
    banco_pp <- banco |> filter(Periodo == pp)
    
    # ============================================================
    # (A) BLOCO GAP (falhas de registro)
    # ============================================================
    gaps_pp <- calc_gaps(banco_pp)
    if (nrow(gaps_pp) == 0) next
    
    tab_gap <- calc_metricas_gap_por_sensor(gaps_pp, limites = padrao_min)
    
    # ------------------------------------------------------------
    # (A1) conserto “Excel” + classificação por GAP
    # ------------------------------------------------------------
    
    # adiciona flags (evento sistêmico + instável estrutural)
    tab_gap <- tab_gap |>
      detectar_evento_sistemico(janela_min = 20, min_sensores = 3) |>
      classificar_instabilidade()
    
    # salva (R + Excel)
    salvar_csv_excel(tab_gap, file.path(dir_pp, "gap_metricas_por_sensor"))
    write_rds(tab_gap, file.path(dir_pp, "gap_metricas_por_sensor.rds"))
    
    # Rankings GAP
    g_max <- plot_ranking_metrica(tab_gap, lote_id, "max_gap_min", "MAIOR GAP (min)", padrao_min, pp)
    save_plot(g_max, file.path(dir_pp, "01_ranking_maior_gap.png"), w = 16, h = 7)
    
    g_p95 <- plot_ranking_metrica(tab_gap, lote_id, "p95_gap_min", "P95 do GAP (min)", padrao_min, pp)
    save_plot(g_p95, file.path(dir_pp, "02_ranking_p95_gap.png"), w = 16, h = 7)
    
    if ("excesso_total_30" %in% names(tab_gap)) {
      g_ex30 <- plot_ranking_metrica(tab_gap, lote_id, "excesso_total_30", "Tempo perdido acima de 30 min (min)", NULL, pp)
      save_plot(g_ex30, file.path(dir_pp, "03_ranking_excesso_total_30.png"), w = 16, h = 7)
    }
    
    # Heatmap diário do MAIOR GAP do dia
    df_day <- gaps_pp |>
      filter(!is.na(Dia)) |>
      group_by(Dia, Sensor) |>
      summarise(max_gap_dia = max(dif_min, na.rm = TRUE), .groups = "drop")
    
    if (nrow(df_day) > 0) {
      dias <- seq(min(df_day$Dia), max(df_day$Dia), by = "day")
      sensores <- sort(unique(df_day$Sensor))
      df_day <- df_day |> tidyr::complete(Dia = dias, Sensor = sensores)
    }
    
    g_hm <- plot_heatmap_maior_gap_por_dia(df_day, lote_id, pp)
    save_plot(g_hm, file.path(dir_pp, "04_heatmap_maior_gap_por_dia.png"), w = 18, h = 9)
    
    # Séries individuais e facets
    dir_sens <- file.path(dir_pp, "sensores_individuais")
    dir.create(dir_sens, recursive = TRUE, showWarnings = FALSE)
    
    sensores_pp <- sort(unique(gaps_pp$Sensor))
    for (sid in sensores_pp) {
      g1 <- plot_gaps_sensor(gaps_pp, sid, lote_id, padrao_min, pp)
      if (is.null(g1)) next
      save_plot(g1, file.path(dir_sens, glue("sensor_{sid}_gaps_tempo.png")), w = 16, h = 6)
    }
    
    dir_facets <- file.path(dir_pp, "facets_6_sensores")
    dir.create(dir_facets, recursive = TRUE, showWarnings = FALSE)
    
    chunks <- split(sensores_pp, ceiling(seq_along(sensores_pp) / 6))
    for (i in seq_along(chunks)) {
      g6 <- plot_gaps_facets_6(gaps_pp, chunks[[i]], lote_id, padrao_min, pp)
      if (is.null(g6)) next
      save_plot(g6, file.path(dir_facets, glue("gaps_6sensores_pagina_{i}.png")), w = 18, h = 10)
    }
    
    # ============================================================
    # (B) BLOCO “PACOTE ML”
    # ============================================================
    
    # ----------------------------
    # B0) checa variável e define tolerância
    # ----------------------------
    if (!var_ml %in% names(banco_pp)) {
      message("⚠️ var_ml=", var_ml, " não existe no banco (", lote_id, " | ", pp, "). Pulando ML.")
      next
    }
    
    tol_abs <- if (var_ml == "T") tol_abs_T else tol_abs_UR
    
    # ----------------------------
    # B1) matriz por bin (tempo x sensor)
    # ----------------------------
    mx <- make_matrix_por_bin(banco_pp, var = var_ml, bin = bin_ml)
    wide <- mx$wide
    
    if (is.null(wide) || nrow(wide) < 50) {
      message("⚠️ poucos dados para ML (", lote_id, " | ", pp, "). Pulando ML.")
      next
    }
    
    # salva “wide” (opcional, mas bom pra auditoria)
    write_rds(wide, file.path(dir_pp, glue("ml_wide_{var_ml}_{str_replace_all(bin_ml,' ','')}.rds")))
    
    # ----------------------------
    # B2) qualidade do sinal vs mediana global (REF)
    # ----------------------------
    # >>> AQUI nascem: bias, rmse, outlier_rate <<<
    tab_qual <- calc_qualidade_sinal_vs_ref(wide, tol_abs = tol_abs)
    
    # adiciona Linha/Coluna para facilitar leitura
    tab_qual <- tab_qual |>
      left_join(banco_pp |> distinct(Sensor, Linha, Coluna), by = "Sensor") |>
      select(Sensor, Linha, Coluna, everything())
    
    write_csv(tab_qual, file.path(dir_pp, glue("ml_qualidade_{var_ml}_vs_mediana.csv")))
    write_rds(tab_qual, file.path(dir_pp, glue("ml_qualidade_{var_ml}_vs_mediana.rds")))
    
    # ranking do RMSE (quem mais “descola” da mediana global)
    g_rmse <- ggplot(tab_qual, aes(x = reorder(as.factor(Sensor), -rmse), y = rmse, fill = Linha)) +
      geom_col(alpha = 0.9) +
      geom_text(aes(label = round(rmse, 2)), vjust = -0.25, size = 3) +
      scale_fill_viridis_d(option = "C") +
      labs(
        title = glue("Qualidade do sinal — RMSE vs mediana global — {lote_id} — {pp} — var={var_ml}"),
        subtitle = glue("bin={bin_ml} | tolerância outlier = ±{tol_abs}"),
        x = "Sensor",
        y = "RMSE",
        fill = "Linha"
      ) +
      theme_minimal(base_size = 12) +
      theme(plot.title = element_text(face = "bold"), axis.text.x = element_text(angle = 45, hjust = 1))
    save_plot(g_rmse, file.path(dir_pp, glue("ML_01_ranking_rmse_{var_ml}.png")), w = 16, h = 7)
    
    # ranking do outlier_rate
    g_out <- ggplot(tab_qual, aes(x = reorder(as.factor(Sensor), -outlier_rate), y = outlier_rate, fill = Linha)) +
      geom_col(alpha = 0.9) +
      geom_text(aes(label = scales::percent(outlier_rate, accuracy = 0.1)), vjust = -0.25, size = 3) +
      scale_fill_viridis_d(option = "C") +
      scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
      labs(
        title = glue("Qualidade do sinal — Outlier rate vs mediana global — {lote_id} — {pp} — var={var_ml}"),
        subtitle = glue("outlier = |sensor - mediana| > {tol_abs} | bin={bin_ml}"),
        x = "Sensor",
        y = "Outlier rate",
        fill = "Linha"
      ) +
      theme_minimal(base_size = 12) +
      theme(plot.title = element_text(face = "bold"), axis.text.x = element_text(angle = 45, hjust = 1))
    save_plot(g_out, file.path(dir_pp, glue("ML_02_ranking_outlier_rate_{var_ml}.png")), w = 16, h = 7)
    
    # ----------------------------
    # B3) correlação + clustering (redundância)
    # ----------------------------
    corr_out <- calc_correlacao_e_unicidade(wide)
    corr_mat <- corr_out$corr
    tab_uni  <- corr_out$unicidade
    
    # salva redundância/unicidade
    write_csv(tab_uni, file.path(dir_pp, glue("ML_redundancia_unicidade_{var_ml}.csv")))
    write_rds(tab_uni, file.path(dir_pp, glue("ML_redundancia_unicidade_{var_ml}.rds")))
    
    # heatmap correlação
    g_corr <- plot_heatmap_correlacao(corr_mat, lote_id, pp, var_ml)
    if (!is.null(g_corr)) {
      save_plot(g_corr, file.path(dir_pp, glue("ML_03_heatmap_correlacao_{var_ml}.png")), w = 12, h = 10)
    }
    
    # dendrograma (salva via base plot)
    hc <- plot_dendrograma_correlacao(corr_mat, lote_id, pp, var_ml)
    if (!is.null(hc)) {
      png(file.path(dir_pp, glue("ML_04_dendrograma_correlacao_{var_ml}.png")), width = 1400, height = 900, res = 140)
      plot(hc, main = glue("Dendrograma (1-corr) — {lote_id} — {pp} — var={var_ml}"), xlab = "Sensor", sub = "")
      dev.off()
      message("✅ salvo: ", file.path(dir_pp, glue("ML_04_dendrograma_correlacao_{var_ml}.png")))
    }
    
    # ranking “unicidade” (quem é menos redundante)
    if (nrow(tab_uni) > 0) {
      tab_uni2 <- tab_uni |>
        left_join(banco_pp |> distinct(Sensor, Linha, Coluna), by = "Sensor")
      g_uni <- ggplot(tab_uni2, aes(x = reorder(as.factor(Sensor), -unicidade), y = unicidade, fill = Linha)) +
        geom_col(alpha = 0.9) +
        geom_text(aes(label = round(unicidade, 2)), vjust = -0.25, size = 3) +
        scale_fill_viridis_d(option = "C") +
        labs(
          title = glue("Unicidade (1 - max corr) — {lote_id} — {pp} — var={var_ml}"),
          subtitle = "Quanto maior, menos redundante (mais “único”/valioso)",
          x = "Sensor",
          y = "Unicidade",
          fill = "Linha"
        ) +
        theme_minimal(base_size = 12) +
        theme(plot.title = element_text(face = "bold"), axis.text.x = element_text(angle = 45, hjust = 1))
      save_plot(g_uni, file.path(dir_pp, glue("ML_05_ranking_unicidade_{var_ml}.png")), w = 16, h = 7)
    }
    
    # ----------------------------
    # B4) seleção de k sensores (reconstrução) — otimização
    # ----------------------------
    sel <- selecionar_sensores_greedy(wide, k_max = k_max, max_bins = max_bins_ml)
    
    # sensores escolhidos (ordem importa: é a ordem greedy)
    escolhidos <- tibble(
      ordem = seq_along(sel$selecionados),
      Sensor = as.integer(sel$selecionados)
    ) |>
      left_join(banco_pp |> distinct(Sensor, Linha, Coluna), by = "Sensor")
    
    write_csv(escolhidos, file.path(dir_pp, glue("ML_selecao_sensores_greedy_kmax{ k_max }_{var_ml}.csv")))
    write_rds(escolhidos, file.path(dir_pp, glue("ML_selecao_sensores_greedy_kmax{ k_max }_{var_ml}.rds")))
    
    # curva de erro vs k
    g_curva <- plot_curva_recon(sel$curva, lote_id, pp, var_ml)
    if (!is.null(g_curva)) {
      save_plot(g_curva, file.path(dir_pp, glue("ML_06_curva_reconstrucao_{var_ml}.png")), w = 12, h = 7)
    }
    
    # ============================================================
    # (C) TABELA MASTER (gap + ML)
    # ============================================================
    tab_master <- montar_master_por_sensor(
      tab_gap = tab_gap,
      tab_qual = tab_qual |> select(Sensor, bias, rmse, outlier_rate, n_pts),
      tab_uni = tab_uni,
      banco_pp = banco_pp
    )
    # ------------------------------------------------------------
    # (C1) Índice final de confiabilidade (0..100)
    # ------------------------------------------------------------
    # - usa GAP + ML (se tiver) + alívio para evento sistêmico
    tab_master <- criar_indice_confiabilidade(tab_master)
    
    # salva master (R + Excel)
    salvar_csv_excel(tab_master, file.path(dir_pp, glue("MASTER_gap_ml_por_sensor_{var_ml}")))
    write_rds(tab_master, file.path(dir_pp, glue("MASTER_gap_ml_por_sensor_{var_ml}.rds")))
    
    # Rankings finais práticos:
    # - ruim por falha
    g_falha <- tab_master |>
      ggplot(aes(x = reorder(as.factor(Sensor), -score_falha), y = score_falha, fill = Linha)) +
      geom_col(alpha = 0.9) +
      scale_fill_viridis_d(option = "C") +
      labs(
        title = glue("Ranking FINAL — RUIM por falha de registro — {lote_id} — {pp}"),
        subtitle = "score_falha = z(p95_gap) + z(excesso_30) + z(n_gaps>30) (maior = pior)",
        x = "Sensor", y = "score_falha", fill = "Linha"
      ) +
      theme_minimal(base_size = 12) +
      theme(plot.title = element_text(face = "bold"), axis.text.x = element_text(angle = 45, hjust = 1))
    save_plot(g_falha, file.path(dir_pp, glue("FINAL_01_ranking_ruim_por_falha_{var_ml}.png")), w = 16, h = 7)
    
    # - ruim por leitura (sinal)
    g_sinal <- tab_master |>
      ggplot(aes(x = reorder(as.factor(Sensor), -score_sinal), y = score_sinal, fill = Linha)) +
      geom_col(alpha = 0.9) +
      scale_fill_viridis_d(option = "C") +
      labs(
        title = glue("Ranking FINAL — RUIM por leitura inconsistente — {lote_id} — {pp} — var={var_ml}"),
        subtitle = "score_sinal = z(rmse) + z(outlier_rate) + z(|bias|) (maior = pior)",
        x = "Sensor", y = "score_sinal", fill = "Linha"
      ) +
      theme_minimal(base_size = 12) +
      theme(plot.title = element_text(face = "bold"), axis.text.x = element_text(angle = 45, hjust = 1))
    save_plot(g_sinal, file.path(dir_pp, glue("FINAL_02_ranking_ruim_por_sinal_{var_ml}.png")), w = 16, h = 7)
    
    # - valioso por ser único
    g_unico <- tab_master |>
      ggplot(aes(x = reorder(as.factor(Sensor), -score_unico), y = score_unico, fill = Linha)) +
      geom_col(alpha = 0.9) +
      scale_fill_viridis_d(option = "C") +
      labs(
        title = glue("Ranking FINAL — CRÍTICO/VALIOSO por ser único — {lote_id} — {pp} — var={var_ml}"),
        subtitle = "score_unico cresce com unicidade (1 - max corr). Maior = menos redundante",
        x = "Sensor", y = "score_unico", fill = "Linha"
      ) +
      theme_minimal(base_size = 12) +
      theme(plot.title = element_text(face = "bold"), axis.text.x = element_text(angle = 45, hjust = 1))
    save_plot(g_unico, file.path(dir_pp, glue("FINAL_03_ranking_unico_{var_ml}.png")), w = 16, h = 7)
    
    message("✅ Período ", pp, " finalizado (GAP + ML) em: ", dir_pp)
  }
  
  message("✅ FINALIZADO lote: ", lote_id, " | pasta: ", dir_maior_gap)
}

# ============================================================
# EXPLICAÇÃO (DIRETA) — O QUE FOI ADICIONADO E ONDE
# ============================================================
#
# 1) GAP / FALHAS (já era sua base, agora robusto e completo)
#    - calculado em: calc_metricas_gap_por_sensor()
#    - principais variáveis:
#        max_gap_min     = MAIOR_GAP
#        p95_gap_min     = P95_GAP
#        n_gaps_acima_X  = N_FALHAS acima do limite X
#        excesso_total_X = TEMPO perdido acima do limite X
#
# 2) ML / QUALIDADE DO SINAL vs MEDIANA GLOBAL (sua referência)
#    - matriz por bin: make_matrix_por_bin()
#    - referência (mediana global por instante): calc_referencia_mediana_global()
#    - métricas por sensor: calc_qualidade_sinal_vs_ref()
#        bias         = média(sensor - ref)
#        rmse         = sqrt(média((sensor - ref)^2))
#        outlier_rate = %(|sensor - ref| > tolerância)
#
# 3) ML / CLUSTERING DE CORRELAÇÃO (redundância)
#    - correlação + unicidade: calc_correlacao_e_unicidade()
#        max_corr_com_outros e unicidade = 1 - max_corr
#    - heatmap: plot_heatmap_correlacao()
#    - dendrograma: plot_dendrograma_correlacao()
#
# 4) ML / SELEÇÃO DE k SENSORES POR RECONSTRUÇÃO (otimização real)
#    - seleção greedy: selecionar_sensores_greedy()
#    - mede “quão bem k sensores explicam os outros” por regressão linear
#    - salva lista escolhida e curva RMSE vs k
#
# 5) TABELA MASTER (o “painel único” por sensor)
#    - monta em: montar_master_por_sensor()
#    - salva:
#        MASTER_gap_ml_por_sensor_<var>.csv / .rds
#
# DICA PRÁTICA (de operação):
# - “sensor ruim por falha de registro”  -> veja FINAL_01 (score_falha)
# - “sensor ruim por leitura”            -> veja FINAL_02 (score_sinal)
# - “sensor valioso por ser único”       -> veja FINAL_03 (score_unico)
# - “subconjunto representativo”         -> veja ML_selecao_sensores_greedy_...
# ============================================================