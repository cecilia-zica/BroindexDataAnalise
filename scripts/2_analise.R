# ============================================================
# scripts/2_analise.R
#
# Orquestrador principal de análise do projeto BroindexAnalise.
# Define todas as funções auxiliares e executa o pipeline completo.
#
# Saída em: resultados/<lote_id>/
#   calendario_operacional.png
#   temperatura/  e  umidade/   — série geral, por dia, por linha,
#                                 por coluna, painéis por período, mapas
#   falhas/maior_gap/           — maior gap por período e sensor
#   falhas/gap_dinamico/        — frequência de gaps (5min vs 10min)
#   falhas/por_sensor/          — série de T com marcação de falhas
#   falhas/diagnostico_frequencia/ — tabela obs × dia × sensor
#   falhas/tabelas/             — CSVs de contagem de falhas
#
# Chamado por: run_lote.R (Etapa 2)
# Depende de:  scripts/config.R (períodos + sensor_config)
# ============================================================

# ── CONFIGURAÇÕES (edite aqui se necessário) ──────────────────
if (!exists("pasta_base")) pasta_base <- normalizePath(getwd(), winslash = "/")

gerar_temperatura <- TRUE
gerar_umidade     <- TRUE

# Limites de gap para mapas e plots de falhas (em minutos absolutos)
padrao_min          <- c(10, 15, 30, 60)
bins_map_min        <- padrao_min
limites_plot_falhas <- padrao_min


# FALSE = salva direto (mais rápido); TRUE = exibe no Viewer do RStudio
mostrar_graficos <- FALSE
bin_minimo_mapas <- 10    # bins menores que este valor são pulados nos mapas
periodo_mapas    <- NULL  # ex: c(as.POSIXct("2025-10-01"), as.POSIXct("2025-10-10"))

# ── PACOTES ───────────────────────────────────────────────────
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(readr)
  library(glue)
  library(viridis)
  library(progress)
  library(ggtext)
  library(scales)
  library(tidyselect)
})

# ── CONFIGURAÇÃO (config.R) ───────────────────────────────────
source(file.path(pasta_base, "scripts", "config.R"))
periodos <- pegar_periodos_lote(lote_id, tz = tz_local)


# ══════════════════════════════════════════════════════════════
# FUNÇÕES: Utilitários
# ══════════════════════════════════════════════════════════════

# Retorna o caminho do banco tratado de um lote
dir_tratado_lote <- function(pasta_base, lote_id) {
  file.path(pasta_base, "data", "tratados", lote_id)
}

# Retorna o caminho de resultados e cria o diretório se não existir
dir_result_lote <- function(pasta_base, lote_id, ...) {
  d <- file.path(pasta_base, "resultados", lote_id, ...)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

# Salva um ggplot em disco
save_plot <- function(plot, path, w = 12, h = 7, dpi = 300) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  ggplot2::ggsave(filename = path, plot = plot, width = w, height = h, dpi = dpi)
  message("salvo: ", path)
}

# Retorna metadados da variável: nome de pasta, título do gráfico, label do eixo Y
get_spec <- function(var) {
  if (var == "T")  return(list(var = "T",  nome = "temperatura", titulo = "Temperatura",      ylab = "Temperatura (°C)"))
  if (var == "UR") return(list(var = "UR", nome = "umidade",     titulo = "Umidade Relativa", ylab = "Umidade Relativa (%)"))
  stop("Variável não suportada: ", var, ". Use 'T' ou 'UR'.")
}

# Paleta de cores fixa para os 30 sensores
sensor_levels <- as.character(1:30)
sensor_palette <- setNames(scales::hue_pal(l = 60, c = 100)(30), sensor_levels)

# Scale de cor para todos os 30 sensores
scale_cor_sensor_padrao <- function(legend_title = "Sensor") {
  ggplot2::scale_color_manual(values = sensor_palette, breaks = sensor_levels,
                               drop = TRUE, name = legend_title)
}

# Scale de cor para um subconjunto de sensores (paleta proporcional ao subconjunto)
scale_cor_sensor_subset <- function(levels_subset, legend_title = "Sensor") {
  lev <- as.character(levels_subset)
  pal <- setNames(scales::hue_pal(l = 60, c = 120)(length(lev)), lev)
  ggplot2::scale_color_manual(values = pal, breaks = lev, drop = TRUE, name = legend_title)
}


# ══════════════════════════════════════════════════════════════
# FUNÇÕES: Dados
# ══════════════════════════════════════════════════════════════

# Carrega o banco_periodos.rds de um lote
carregar_banco <- function(pasta_base, lote_id) {
  arq <- file.path(dir_tratado_lote(pasta_base, lote_id), "banco_periodos.rds")
  if (!file.exists(arq))
    stop("Banco tratado não encontrado: ", arq,
         "\nRode Etapa 1 (rodar_leitura = TRUE) para gerá-lo.")
  readr::read_rds(arq)
}

# Adiciona colunas Linha e Coluna com base no número do sensor
# Layout: L1 = S1,S4,S7... | L2 = S2,S5,S8... | L3 = S3,S6,S9...
add_linha_coluna <- function(df) {
  df |> dplyr::mutate(
    Sensor = as.integer(Sensor),
    Linha  = dplyr::case_when(
      Sensor %in% c(1, 4, 7, 10, 13, 16, 19, 22, 25, 28) ~ "L1",
      Sensor %in% c(2, 5, 8, 11, 14, 17, 20, 23, 26, 29) ~ "L2",
      Sensor %in% c(3, 6, 9, 12, 15, 18, 21, 24, 27, 30) ~ "L3",
      TRUE ~ NA_character_
    ),
    Coluna = paste0("C", ((Sensor - 1L) %/% 3L) + 1L)
  )
}


# ══════════════════════════════════════════════════════════════
# FUNÇÕES: Gaps e Falhas
# ══════════════════════════════════════════════════════════════

# Calcula dif_min (gap) entre leituras consecutivas por Sensor × Período.
# Base para a análise de maior gap.
calc_gaps <- function(df) {
  df |>
    dplyr::filter(!is.na(data_hora), !is.na(Sensor)) |>
    dplyr::arrange(Sensor, data_hora) |>
    dplyr::group_by(Sensor, Linha, Coluna, Periodo) |>
    dplyr::mutate(
      dif_min    = as.numeric(difftime(data_hora, dplyr::lag(data_hora), units = "mins")),
      inicio_gap = dplyr::lag(data_hora),
      fim_gap    = data_hora,
      Dia        = as.Date(fim_gap)
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.finite(dif_min), dif_min >= 0)
}

# Classifica cada intervalo entre leituras consecutivas em três estados:
#   "falha_comunicacao" — dif_min < intervalo_min − tol_min
#                         (dado chegou rápido demais: duplicata ou ruído de comunicação)
#   "ok"                — dif_min dentro de intervalo_min ± tol_min
#                         (leitura normal)
#   "falha_gap"         — dif_min > intervalo_min + tol_min
#                         (leitura perdida: gap real de monitoramento)
#
# excesso_min: minutos além do intervalo esperado (> 0 só para "falha_gap").
calcular_falhas <- function(df, sensor_config, tol_min = 0.6) {
  df |>
    dplyr::left_join(
      sensor_config |> dplyr::select(Sensor, intervalo_min),
      by = "Sensor"
    ) |>
    dplyr::arrange(Sensor, data_hora) |>
    dplyr::group_by(Sensor) |>
    dplyr::mutate(
      dif_min     = as.numeric(difftime(data_hora, dplyr::lag(data_hora), units = "mins")),
      tem_falha   = dplyr::case_when(
        is.na(dif_min)                        ~ NA_character_,
        dif_min <  (intervalo_min - tol_min)  ~ "falha_comunicacao",
        dif_min <= (intervalo_min + tol_min)  ~ "ok",
        dif_min >  (intervalo_min + tol_min)  ~ "falha_gap"
      ),
      excesso_min = dplyr::if_else(
        !is.na(tem_falha) & tem_falha == "falha_gap",
        dif_min - intervalo_min, 0
      )
    ) |>
    dplyr::ungroup()
}

# Classifica gaps usando o intervalo REAL de cada sensor (5 ou 10 min).
# Corrige o erro antigo que aplicava regra de 5 min para todos os sensores.
# tol_min: tolerância em minutos ao redor do intervalo esperado.
calcular_gap_dinamico <- function(df, sensor_config, tol_min = 0.6) {
  df |>
    dplyr::filter(!is.na(Sensor), !is.na(data_hora)) |>
    dplyr::left_join(sensor_config |> dplyr::select(Sensor, intervalo_min), by = "Sensor") |>
    dplyr::arrange(Sensor, data_hora) |>
    dplyr::group_by(Sensor, Linha, Coluna) |>
    dplyr::mutate(
      dif_min = as.numeric(difftime(data_hora, dplyr::lag(data_hora), units = "mins")),
      cod_gap = dplyr::case_when(
        is.na(dif_min)                         ~ NA_integer_,
        dif_min < (intervalo_min - tol_min)    ~ -1L,  # falha comunicação
        dif_min <= (intervalo_min + tol_min)   ~  0L,  # ok
        dif_min >  (intervalo_min + tol_min)   ~  1L   # falha gap
      ),
      classe_gap = dplyr::case_when(
        is.na(dif_min)                         ~ NA_character_,
        dif_min < (intervalo_min - tol_min)    ~ paste0("falha_comunicacao(<", intervalo_min, ")"),
        dif_min <= (intervalo_min + tol_min)   ~ paste0("ok(=",               intervalo_min, ")"),
        dif_min >  (intervalo_min + tol_min)   ~ paste0("falha_gap(>",        intervalo_min, ")")
      ),
      tipo_sensor = paste0(intervalo_min, "min")
    ) |>
    dplyr::ungroup()
}

# Conta observações por sensor por dia e calcula % do esperado.
# Retorna list com: contagem, pct_esperado, resumo, dias_seq, sensores.
# Janela: Alojamento até Abertura5_15d (Vazio Sanitário excluído).
tabela_freq_sensor_dia <- function(banco, sensor_config, periodos) {
  ini_row <- periodos[periodos$Periodo == "Alojamento",      ]
  fim_row <- periodos[periodos$Periodo == "Abertura5_15d",   ]

  if (nrow(ini_row) == 0 || nrow(fim_row) == 0)
    stop("periodos deve conter 'Alojamento' e 'Abertura5_15d'.")

  ini <- ini_row$inicio[[1]]
  fim <- fim_row$fim[[1]]

  banco_ok <- banco |>
    dplyr::filter(!is.na(data_hora), !is.na(Sensor),
                  data_hora >= ini, data_hora <= fim) |>
    dplyr::mutate(Dia = as.Date(data_hora), Sensor = as.integer(Sensor))

  if (nrow(banco_ok) == 0) { warning("Nenhum dado na janela Alojamento–Abertura5_15d."); return(NULL) }

  dias_seq <- seq(as.Date(ini), as.Date(fim), by = "day")
  sensores <- sort(unique(banco_ok$Sensor))

  grade <- tidyr::expand_grid(Dia = dias_seq, Sensor = sensores) |>
    dplyr::left_join(banco_ok |> dplyr::count(Dia, Sensor, name = "obs"), by = c("Dia", "Sensor")) |>
    dplyr::mutate(obs = dplyr::coalesce(obs, 0L))

  contagem <- grade |>
    tidyr::pivot_wider(names_from = Sensor, values_from = obs,
                       names_prefix = "S", names_sort = TRUE) |>
    dplyr::arrange(Dia)

  esperado_dia_tbl <- sensor_config |> dplyr::select(Sensor, intervalo_min, esperado_dia)

  pct_esperado <- grade |>
    dplyr::left_join(esperado_dia_tbl, by = "Sensor") |>
    dplyr::mutate(pct = round(obs / esperado_dia * 100, 1)) |>
    dplyr::select(Dia, Sensor, pct) |>
    tidyr::pivot_wider(names_from = Sensor, values_from = pct,
                       names_prefix = "S", names_sort = TRUE) |>
    dplyr::arrange(Dia)

  resumo <- grade |>
    dplyr::left_join(esperado_dia_tbl, by = "Sensor") |>
    dplyr::group_by(Sensor, intervalo_min, esperado_dia) |>
    dplyr::summarise(
      n_dias         = dplyr::n(),
      total_obs      = sum(obs),
      esperado_total = esperado_dia[1] * dplyr::n(),
      pct_cobertura  = round(total_obs / esperado_total * 100, 1),
      dias_zero      = sum(obs == 0),
      dias_baixo     = sum(obs < esperado_dia * 0.8 & obs > 0),
      dias_ok        = sum(obs >= esperado_dia * 0.8),
      .groups        = "drop"
    ) |>
    dplyr::arrange(Sensor)

  list(contagem = contagem, pct_esperado = pct_esperado,
       resumo = resumo, dias_seq = dias_seq, sensores = sensores)
}

# Salva tabelas CSV e heatmap de cobertura para um lote
diagnostico_frequencia_lote <- function(banco, sensor_config, periodos, lote_id, dir_out) {
  dir.create(dir_out, recursive = TRUE, showWarnings = FALSE)
  message("\n[", lote_id, "] Diagnóstico freq sensor × dia...")

  resultado <- tabela_freq_sensor_dia(banco, sensor_config, periodos)
  if (is.null(resultado)) { warning("Nenhum resultado para ", lote_id); return(invisible(NULL)) }

  readr::write_csv(resultado$contagem,     file.path(dir_out, "tabela_obs_sensor_dia.csv"))
  readr::write_csv(resultado$pct_esperado, file.path(dir_out, "tabela_pct_esperado_sensor_dia.csv"))
  readr::write_csv(resultado$resumo,       file.path(dir_out, "resumo_cobertura_por_sensor.csv"))

  g <- plot_heatmap_freq_dia(resultado, sensor_config, lote_id)
  if (!is.null(g))
    save_plot(g, file.path(dir_out, "heatmap_cobertura_sensor_dia.png"), w = 20, h = 10)

  n_prob <- sum(resultado$resumo$pct_cobertura < 80)
  message("  Sensores com cobertura < 80%: ", n_prob, " de ", nrow(resultado$resumo))
  message("✅ Diagnóstico salvo em: ", dir_out)
  invisible(resultado)
}


# ══════════════════════════════════════════════════════════════
# FUNÇÕES: Gráficos
# ══════════════════════════════════════════════════════════════

# ── Calendário operacional (Gantt) ────────────────────────────
plot_calendario <- function(periodos, lote_id, rotulos_eventos) {
  periodos_cal <- periodos |>
    dplyr::mutate(
      rotulo = dplyr::recode(Periodo, !!!rotulos_eventos),
      rotulo = factor(rotulo, levels = rev(unique(rotulo))),
      meio   = inicio + (fim - inicio) / 2
    )

  ggplot2::ggplot(periodos_cal) +
    ggplot2::geom_segment(
      ggplot2::aes(x = inicio, xend = fim, y = rotulo, yend = rotulo, color = rotulo),
      linewidth = 10, lineend = "butt"
    ) +
    ggplot2::geom_text(ggplot2::aes(x = meio, y = rotulo, label = rotulo),
                       color = "white", size = 4, fontface = "bold") +
    ggplot2::scale_x_datetime(date_breaks = "3 days", date_labels = "%d/%m",
                               expand = ggplot2::expansion(mult = c(0.01, 0.01))) +
    ggplot2::guides(color = "none") +
    ggplot2::labs(title = glue::glue("Calendário operacional — {lote_id}"), x = "Data", y = NULL) +
    ggplot2::theme_minimal(base_size = 13) +
    ggplot2::theme(plot.title = ggplot2::element_text(face = "bold"),
                   panel.grid.major.y = ggplot2::element_blank(),
                   panel.grid.minor   = ggplot2::element_blank(),
                   plot.margin        = ggplot2::margin(10, 20, 10, 20))
}

# ── Série temporal geral (todos sensores, período completo) ───
plot_geral <- function(df, spec, lote_id, datas_eventos) {
  df2 <- df |> dplyr::filter(!is.na(.data[[spec$var]]))
  if (nrow(df2) == 0) return(NULL)

  y_min <- min(df2[[spec$var]], na.rm = TRUE)
  y_max <- max(df2[[spec$var]], na.rm = TRUE)
  headroom <- max((y_max - y_min) * 0.10, 1)
  y_label  <- y_max + headroom

  ggplot2::ggplot(df2, ggplot2::aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor))) +
    ggplot2::geom_line(linewidth = 0.7, alpha = 0.8) +
    ggplot2::geom_vline(xintercept = datas_eventos$data_hora,
                        color = "red", linetype = "dashed", linewidth = 0.5) +
    ggplot2::geom_text(data = datas_eventos,
                       ggplot2::aes(x = data_hora, y = y_label, label = evento),
                       angle = 90, vjust = 0, hjust = 0, size = 3, color = "red", inherit.aes = FALSE) +
    ggplot2::coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
    ggplot2::labs(title = glue::glue("{spec$titulo} por sensor — visão geral ({lote_id})"),
                  x = "Tempo", y = spec$ylab, color = "Sensor") +
    ggplot2::theme_minimal(base_size = 13) +
    ggplot2::theme(legend.position = "bottom",
                   plot.margin     = ggplot2::margin(35, 30, 10, 10),
                   plot.title      = ggplot2::element_text(face = "bold", hjust = 0.5))
}

# ── Série por dia (facet_wrap de dias, hora no eixo X) ────────
layout_por_dias <- function(n_dias) {
  if (n_dias <= 1) return(list(ncol = 1))
  if (n_dias == 2) return(list(ncol = 2))
  if (n_dias == 3) return(list(ncol = 3))
  if (n_dias == 4) return(list(ncol = 2))
  if (n_dias <= 6) return(list(ncol = 3))
  list(ncol = 4)
}

plot_por_dia <- function(df, spec, lote_id) {
  df2 <- df |>
    dplyr::filter(!is.na(data_hora), !is.na(.data[[spec$var]])) |>
    dplyr::mutate(Data    = as.Date(data_hora),
                  hora_dia = as.POSIXct(paste("1970-01-01", format(data_hora, "%H:%M:%S")), tz = ""))

  if (nrow(df2) == 0) return(NULL)

  dias_seq <- seq(min(df2$Data), max(df2$Data), by = "day")
  df2 <- df2 |> dplyr::mutate(Data     = factor(Data, levels = dias_seq),
                               Sensor_f = factor(as.character(Sensor), levels = sensor_levels))

  ggplot2::ggplot(df2, ggplot2::aes(x = hora_dia, y = .data[[spec$var]],
                                     color = Sensor_f, group = Sensor_f)) +
    ggplot2::geom_line(linewidth = 0.5, alpha = 0.8, na.rm = TRUE) +
    ggplot2::facet_wrap(~Data, ncol = 5, drop = FALSE) +
    ggplot2::scale_x_datetime(date_labels = "%H:%M", date_breaks = "4 hours",
                               limits = as.POSIXct(c("1970-01-01 00:00:00", "1970-01-01 23:59:59"), tz = "")) +
    ggplot2::labs(title = glue::glue("{spec$titulo} — todos os dias ({lote_id})"),
                  x = "Hora (00:00–23:59)", y = spec$ylab) +
    ggplot2::theme_bw(base_size = 11) +
    ggplot2::theme(axis.text.x   = ggplot2::element_text(size = 6, angle = 45, hjust = 1),
                   axis.text.y   = ggplot2::element_text(size = 5),
                   panel.spacing = ggplot2::unit(0.4, "lines"),
                   strip.text    = ggplot2::element_text(size = 6, face = "bold",
                                                          margin = ggplot2::margin(1, 0, 1, 0)),
                   strip.background = ggplot2::element_rect(fill = "grey97", color = NA, linewidth = 0.1),
                   legend.position  = "none",
                   plot.title       = ggplot2::element_text(face = "bold", hjust = 0.5))
}

# ── Painel por período (facets de dias, faixa ideal) ──────────
# Usa variáveis do ambiente chamador: periodos, sensores_periodo,
# faixas_ideais_t, faixas_ideais_ur, sensor_levels, scale_cor_sensor_padrao
plot_painel_periodo <- function(df, spec, nome_periodo, quebra_horas = "4 hours") {
  p         <- periodos |> dplyr::filter(Periodo == nome_periodo)
  sens_range <- sensores_periodo |> dplyr::filter(Periodo == nome_periodo)

  if (nrow(p) == 0)         stop("Período não encontrado: ", nome_periodo)
  if (nrow(sens_range) == 0) stop("Regra de sensores não encontrada: ", nome_periodo)

  sensores_validos <- sens_range$sensor_min:sens_range$sensor_max

  dados_p <- df |>
    dplyr::filter(data_hora >= p$inicio, data_hora <= p$fim) |>
    dplyr::mutate(Data = as.Date(data_hora), Sensor = as.integer(Sensor)) |>
    dplyr::filter(Sensor %in% sensores_validos, !is.na(.data[[spec$var]]), !is.na(Data))

  if (nrow(dados_p) == 0 || dplyr::n_distinct(dados_p$Data) == 0) return(NULL)

  n_dias  <- dplyr::n_distinct(dados_p$Data)
  layout  <- layout_por_dias(n_dias)

  dados_p <- dados_p |>
    dplyr::mutate(hora_dia = as.POSIXct(paste("1970-01-01", format(data_hora, "%H:%M:%S")), tz = ""),
                  Sensor_f = factor(as.character(Sensor), levels = sensor_levels))

  usar_faixa <- FALSE
  faixa_dias <- NULL

  if (spec$var == "T") {
    faixa <- faixas_ideais_t |> dplyr::filter(Periodo == nome_periodo)
    usar_faixa <- nrow(faixa) == 1 && is.finite(faixa$Tmin) && is.finite(faixa$Tmax)
    if (usar_faixa) {
      faixa_dias <- dados_p |> dplyr::group_by(Data) |>
        dplyr::summarise(xmin = min(hora_dia), xmax = max(hora_dia), .groups = "drop") |>
        dplyr::mutate(ymin = faixa$Tmin, ymax = faixa$Tmax)
    }
  } else if (spec$var == "UR") {
    faixa <- faixas_ideais_ur |> dplyr::filter(Periodo == nome_periodo)
    usar_faixa <- nrow(faixa) == 1 && is.finite(faixa$URmin) && is.finite(faixa$URmax)
    if (usar_faixa) {
      faixa_dias <- dados_p |> dplyr::group_by(Data) |>
        dplyr::summarise(xmin = min(hora_dia), xmax = max(hora_dia), .groups = "drop") |>
        dplyr::mutate(ymin = faixa$URmin, ymax = faixa$URmax)
    }
  }

  faixa_txt <- if (usar_faixa) {
    if (spec$var == "T") glue::glue(" | Faixa ideal: {faixa$Tmin}–{faixa$Tmax}")
    else                  glue::glue(" | Faixa ideal: {faixa$URmin}–{faixa$URmax}")
  } else " | Faixa ideal: (não definida)"

  subtitle_txt <- glue::glue(
    "Sensores: {sens_range$sensor_min}–{sens_range$sensor_max} | ",
    "Período: {p$inicio} até {p$fim}", faixa_txt
  )

  dias_seq <- seq(as.Date(p$inicio), as.Date(p$fim), by = "day")
  dados_p  <- dados_p |> dplyr::mutate(Data = factor(as.Date(data_hora), levels = dias_seq))
  if (!is.null(faixa_dias))
    faixa_dias <- faixa_dias |> dplyr::mutate(Data = factor(Data, levels = dias_seq))

  g <- ggplot2::ggplot(dados_p,
    ggplot2::aes(x = hora_dia, y = .data[[spec$var]], color = Sensor_f, group = Sensor_f))

  if (usar_faixa)
    g <- g + ggplot2::geom_rect(data = faixa_dias,
      ggplot2::aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
      fill = "springgreen3", alpha = 0.15, inherit.aes = FALSE)

  g +
    ggplot2::geom_line(alpha = 0.85, linewidth = 0.55) +
    ggplot2::facet_wrap(~Data, ncol = layout$ncol, drop = FALSE) +
    ggplot2::scale_x_datetime(date_labels = "%H:%M", date_breaks = quebra_horas,
                               limits = as.POSIXct(c("1970-01-01 00:00:00", "1970-01-01 23:59:59"), tz = "")) +
    scale_cor_sensor_padrao("Sensor") +
    ggplot2::labs(title = glue::glue("{spec$titulo} — {nome_periodo}"),
                  subtitle = subtitle_txt, x = "Hora", y = spec$ylab) +
    ggplot2::theme_bw(base_size = 11)
}

# ── Série por linha do galpão (L1, L2, L3) ───────────────────
plot_por_linha <- function(df, spec, lote_id, datas_eventos = NULL) {
  df2 <- df |> dplyr::filter(!is.na(.data[[spec$var]]), !is.na(Linha))
  if (nrow(df2) == 0) return(NULL)

  y_min <- min(df2[[spec$var]], na.rm = TRUE)
  y_max <- max(df2[[spec$var]], na.rm = TRUE)
  headroom <- max((y_max - y_min) * 0.12, 1)
  y_label  <- y_max + headroom

  g <- ggplot2::ggplot(df2,
    ggplot2::aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
    ggplot2::geom_line(linewidth = 0.55, alpha = 0.85) +
    ggplot2::facet_wrap(~Linha, ncol = 1) +
    scale_cor_sensor_padrao("Sensor") +
    ggplot2::labs(title = glue::glue("Sensores por linha — {spec$titulo} — {lote_id}"),
                  x = "Tempo", y = spec$ylab) +
    ggplot2::coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(legend.position = "bottom",
                   plot.title      = ggplot2::element_text(face = "bold", hjust = 0.5),
                   plot.margin     = ggplot2::margin(35, 20, 10, 10))

  if (!is.null(datas_eventos) && nrow(datas_eventos) > 0)
    g <- g +
      ggplot2::geom_vline(xintercept = datas_eventos$data_hora,
                          color = "red", linetype = "dashed", linewidth = 0.4) +
      ggplot2::geom_text(data = datas_eventos,
                         ggplot2::aes(x = data_hora, y = y_label, label = evento),
                         angle = 90, vjust = 0, hjust = 0, size = 3, color = "red", inherit.aes = FALSE)
  g
}

# Salva um gráfico individual por linha (L1, L2, L3)
salvar_por_linha <- function(df, spec, lote_id, datas_eventos, dir_out) {
  linhas <- c("L1", "L2", "L3")
  pb <- progress::progress_bar$new(
    format = glue::glue("{spec$nome} linhas [:bar] :current/:total (:percent) ETA: :eta"),
    total  = length(linhas), clear = FALSE, width = 60)

  for (ll in linhas) {
    pb$tick()
    d <- df |> dplyr::filter(Linha == ll, !is.na(.data[[spec$var]]))
    if (nrow(d) == 0) next

    sensores_ll <- sort(unique(as.character(d$Sensor)))
    y_min <- min(d[[spec$var]], na.rm = TRUE)
    y_max <- max(d[[spec$var]], na.rm = TRUE)
    headroom <- max((y_max - y_min) * 0.12, 1)
    y_label  <- y_max + headroom

    g <- ggplot2::ggplot(d,
      ggplot2::aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
      ggplot2::geom_line(linewidth = 0.6, alpha = 0.85) +
      scale_cor_sensor_subset(sensores_ll, "Sensor") +
      ggplot2::labs(title = glue::glue("{ll} — {spec$titulo} — {lote_id}"), x = "Tempo", y = spec$ylab) +
      ggplot2::coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(legend.position = "bottom",
                     plot.title      = ggplot2::element_text(face = "bold", hjust = 0.5),
                     plot.margin     = ggplot2::margin(35, 20, 10, 10))

    if (!is.null(datas_eventos) && nrow(datas_eventos) > 0)
      g <- g +
        ggplot2::geom_vline(xintercept = datas_eventos$data_hora,
                            color = "red", linetype = "dashed", linewidth = 0.4) +
        ggplot2::geom_text(data = datas_eventos,
                           ggplot2::aes(x = data_hora, y = y_label, label = evento),
                           angle = 90, vjust = 0, hjust = 0, size = 3, color = "red", inherit.aes = FALSE)

    save_plot(g, file.path(dir_out, paste0("02_linha_", gsub(" ", "_", ll), ".png")), w = 16, h = 6)
  }
}

# ── Série por coluna do galpão (C1 a C10) ────────────────────
plot_por_coluna_5x2 <- function(df, spec, lote_id, datas_eventos = NULL) {
  df2 <- df |> dplyr::filter(!is.na(.data[[spec$var]]), !is.na(Coluna))
  if (nrow(df2) == 0) return(NULL)

  y_min <- min(df2[[spec$var]], na.rm = TRUE)
  y_max <- max(df2[[spec$var]], na.rm = TRUE)
  headroom <- max((y_max - y_min) * 0.12, 1)
  y_label  <- y_max + headroom

  g <- ggplot2::ggplot(df2,
    ggplot2::aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
    ggplot2::geom_line(linewidth = 0.45, alpha = 0.8) +
    ggplot2::facet_wrap(~Coluna, ncol = 2) +
    scale_cor_sensor_padrao("Sensor") +
    ggplot2::labs(title = glue::glue("Sensores por coluna — {spec$titulo} — {lote_id} (10 colunas | 5×2)"),
                  x = "Tempo", y = spec$ylab) +
    ggplot2::coord_cartesian(clip = "off") +
    ggplot2::theme_bw(base_size = 11) +
    ggplot2::theme(legend.position = "bottom",
                   axis.text.x     = ggplot2::element_text(angle = 45, hjust = 1),
                   strip.text      = ggplot2::element_text(face = "bold"),
                   plot.margin     = ggplot2::margin(35, 20, 10, 10))

  if (!is.null(datas_eventos) && nrow(datas_eventos) > 0)
    g <- g +
      ggplot2::geom_vline(xintercept = datas_eventos$data_hora,
                          color = "red", linetype = "dashed", linewidth = 0.35) +
      ggplot2::geom_text(data = datas_eventos,
                         ggplot2::aes(x = data_hora, y = y_label, label = evento),
                         angle = 90, vjust = 0, hjust = 0, size = 2.6, color = "red", inherit.aes = FALSE)
  g
}

# Salva um gráfico individual por coluna (Coluna 1 a Coluna 10)
salvar_por_coluna <- function(df, spec, lote_id, datas_eventos, dir_out) {
  colunas <- paste0("C", 1:10)
  pb <- progress::progress_bar$new(
    format = glue::glue("{spec$nome} colunas [:bar] :current/:total (:percent) ETA: :eta"),
    total  = length(colunas), clear = FALSE, width = 60)

  for (cc in colunas) {
    pb$tick()
    d <- df |> dplyr::filter(Coluna == cc, !is.na(.data[[spec$var]]))
    if (nrow(d) == 0) next

    sensores_cc <- sort(unique(as.character(d$Sensor)))
    y_min <- min(d[[spec$var]], na.rm = TRUE)
    y_max <- max(d[[spec$var]], na.rm = TRUE)
    headroom <- max((y_max - y_min) * 0.12, 1)
    y_label  <- y_max + headroom

    g <- ggplot2::ggplot(d,
      ggplot2::aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
      ggplot2::geom_line(linewidth = 0.6, alpha = 0.85) +
      scale_cor_sensor_subset(sensores_cc, "Sensor") +
      ggplot2::labs(title = glue::glue("{cc} — {spec$titulo} — {lote_id}"), x = "Tempo", y = spec$ylab) +
      ggplot2::coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(legend.position = "bottom",
                     plot.title      = ggplot2::element_text(face = "bold", hjust = 0.5),
                     plot.margin     = ggplot2::margin(35, 20, 10, 10))

    if (!is.null(datas_eventos) && nrow(datas_eventos) > 0)
      g <- g +
        ggplot2::geom_vline(xintercept = datas_eventos$data_hora,
                            color = "red", linetype = "dashed", linewidth = 0.4) +
        ggplot2::geom_text(data = datas_eventos,
                           ggplot2::aes(x = data_hora, y = y_label, label = evento),
                           angle = 90, vjust = 0, hjust = 0, size = 3, color = "red", inherit.aes = FALSE)

    save_plot(g, file.path(dir_out, paste0("03_coluna_", gsub(" ", "_", cc), ".png")), w = 16, h = 6)
  }
}

# ── Gráficos de gaps ─────────────────────────────────────────

# Ranking de maior gap por sensor (barplot ordenado)
# Usa padrao_min e lote_id do ambiente chamador
plot_ranking_por_sensor <- function(tab, titulo_extra = NULL) {
  ggplot2::ggplot(tab, ggplot2::aes(x = reorder(as.factor(Sensor), -max_gap_min),
                                     y = max_gap_min, fill = Linha)) +
    ggplot2::geom_col(alpha = 0.9) +
    ggplot2::geom_text(ggplot2::aes(label = max_gap_min), vjust = -0.25, size = 3) +
    ggplot2::scale_fill_viridis_d(option = "C") +
    ggplot2::geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey35") +
    ggplot2::labs(
      title    = glue::glue("Maior tempo sem registro por sensor — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = glue::glue("Linhas tracejadas: {paste(padrao_min, collapse = ', ')} min"),
      x = "Sensor", y = "Maior gap (min)", fill = "Linha"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(plot.title  = ggplot2::element_text(face = "bold"),
                   axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
}

# Heatmap de maior gap por dia (sensor × dia)
plot_heatmap_maior_gap_por_dia <- function(df_day, titulo_extra = NULL) {
  ggplot2::ggplot(df_day, ggplot2::aes(x = Dia, y = factor(Sensor), fill = max_gap_dia)) +
    ggplot2::geom_tile(height = 0.9) +
    ggplot2::scale_fill_viridis_c(option = "C", trans = "sqrt", na.value = "grey95") +
    ggplot2::labs(
      title    = glue::glue("Heatmap — maior gap por dia — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = "Cada célula = MAIOR intervalo (min) naquele dia para aquele sensor",
      x = "Dia", y = "Sensor", fill = "Maior gap\n(min)"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(plot.title  = ggplot2::element_text(face = "bold"),
                   axis.text.y = ggplot2::element_text(size = 7))
}

# Gaps ao longo do tempo para um sensor específico
plot_gaps_sensor <- function(gaps_df, sensor_id, titulo_extra = NULL) {
  d <- gaps_df |>
    dplyr::filter(Sensor == sensor_id, is.finite(dif_min), dif_min > 0) |>
    dplyr::arrange(fim_gap)
  if (nrow(d) == 0) return(NULL)

  max_gap <- max(d$dif_min, na.rm = TRUE)
  d_max   <- d |> dplyr::slice(which.max(dif_min))

  ggplot2::ggplot(d, ggplot2::aes(x = fim_gap, y = dif_min)) +
    ggplot2::geom_point(alpha = 0.35, size = 1.1, color = "grey35") +
    ggplot2::geom_point(data = d_max, color = "red", size = 2.5) +
    ggplot2::geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey55") +
    ggplot2::geom_hline(yintercept = max_gap, color = "red", linewidth = 0.8) +
    ggplot2::annotate("text", x = d_max$fim_gap, y = max_gap,
                      label = glue::glue("MAX {round(max_gap,1)} min"),
                      vjust = -0.6, hjust = 0.5, color = "red", size = 3.2) +
    ggplot2::labs(
      title    = glue::glue("Gaps ao longo do tempo — Sensor {sensor_id} — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = glue::glue("Maior gap: {format(d_max$inicio_gap, '%d/%m %H:%M')} → {format(d_max$fim_gap, '%d/%m %H:%M')}"),
      x = "Tempo (fim do intervalo)", y = "Gap entre registros (min)"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(plot.title = ggplot2::element_text(face = "bold"))
}

# Gaps em facets de 6 sensores por página
plot_gaps_facets_6 <- function(gaps_df, sensors_6, titulo_extra = NULL) {
  d <- gaps_df |>
    dplyr::filter(Sensor %in% sensors_6, is.finite(dif_min), dif_min > 0) |>
    dplyr::mutate(Sensor_f = factor(Sensor, levels = sort(unique(Sensor))))
  if (nrow(d) == 0) return(NULL)

  ggplot2::ggplot(d, ggplot2::aes(x = fim_gap, y = dif_min)) +
    ggplot2::geom_point(alpha = 0.35, size = 1.0, color = "grey35") +
    ggplot2::geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey55") +
    ggplot2::facet_wrap(~Sensor_f, ncol = 3, scales = "free_x") +
    ggplot2::labs(
      title    = glue::glue("Gaps — 6 sensores por página — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = "Y = gap (min). Linhas tracejadas = limites padrão",
      x = "Tempo (fim do intervalo)", y = "Gap (min)"
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(plot.title = ggplot2::element_text(face = "bold"))
}

# Histograma de dif_min por sensor, separado por tipo de intervalo (5min ou 10min).
# Mostra toda a distribuição (sem corte de cauda). Eixo Y em escala log10 para
# que gaps raros mas longos sejam visíveis junto com a massa principal de leituras.
plot_hist_dif_min_dinamico <- function(gap_df, lote_id, tipo = NULL) {
  df_hist <- gap_df |> dplyr::filter(!is.na(dif_min), is.finite(dif_min), dif_min >= 0)
  if (!is.null(tipo)) df_hist <- df_hist |> dplyr::filter(tipo_sensor == tipo)
  if (nrow(df_hist) == 0) return(NULL)

  df_hist    <- df_hist |>
    dplyr::mutate(Sensor = factor(Sensor, levels = sort(unique(Sensor))))
  intervalo  <- max(df_hist$intervalo_min, na.rm = TRUE)
  tipo_label <- if (!is.null(tipo)) paste0(" (sensores ", tipo, ")") else ""

  ggplot2::ggplot(df_hist, ggplot2::aes(x = dif_min)) +
    ggplot2::geom_histogram(binwidth = 1, boundary = 0, closed = "left",
                             fill = "#5B8FD9", color = "black", linewidth = 0.2) +
    ggplot2::facet_wrap(~Sensor, ncol = 6, scales = "free_x") +
    ggplot2::scale_x_continuous(
      breaks = function(x) {
        mx <- ceiling(max(x, na.rm = TRUE))
        unique(c(0, seq(intervalo, mx, by = intervalo)))
      },
      expand = c(0.02, 0)) +
    ggplot2::scale_y_log10(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.05))) +
    ggplot2::labs(
      title    = glue::glue("Histograma de dif_min por sensor{tipo_label} — {lote_id}"),
      subtitle = "dif_min = diferença entre leituras consecutivas | eixo Y: escala log10 | eixo X livre por sensor",
      x = "dif_min (min)", y = "Frequência (log10)") +
    ggplot2::theme_bw(base_size = 11) +
    ggplot2::theme(panel.grid = ggplot2::element_blank(),
                   panel.background = ggplot2::element_rect(fill = "white", color = "black", linewidth = 0.3),
                   strip.background = ggplot2::element_rect(fill = "grey92", color = "black", linewidth = 0.3),
                   strip.text   = ggplot2::element_text(face = "bold", size = 8),
                   axis.text.x  = ggplot2::element_text(size = 7),
                   axis.text.y  = ggplot2::element_text(size = 7),
                   axis.title   = ggplot2::element_text(face = "bold"),
                   plot.title   = ggplot2::element_text(face = "bold", hjust = 0.5),
                   plot.subtitle = ggplot2::element_text(hjust = 0.5),
                   panel.spacing = ggplot2::unit(0.35, "lines"),
                   plot.margin   = ggplot2::margin(8, 8, 8, 8))
}

# ── Plot A: histograma AGREGADO (todos sensores juntos), Y log10 ──────
# Mostra a distribuição global das diferenças entre leituras.
# Sem facet — permite enxergar o sinal principal (pico no intervalo esperado)
# e a cauda de gaps longos em escala logarítmica.
plot_hist_dif_min_agregado <- function(gap_df, lote_id, tipo = NULL) {
  df <- gap_df |> dplyr::filter(!is.na(dif_min), is.finite(dif_min), dif_min >= 0)
  if (!is.null(tipo)) df <- df |> dplyr::filter(tipo_sensor == tipo)
  if (nrow(df) == 0) return(NULL)

  intervalo  <- max(df$intervalo_min, na.rm = TRUE)
  mx         <- ceiling(max(df$dif_min, na.rm = TRUE))
  tipo_label <- if (!is.null(tipo)) paste0(" (sensores ", tipo, ")") else ""

  ggplot2::ggplot(df, ggplot2::aes(x = dif_min)) +
    ggplot2::geom_histogram(binwidth = 1, boundary = 0, closed = "left",
                             fill = "#5B8FD9", color = "black", linewidth = 0.2) +
    ggplot2::scale_x_continuous(
      breaks = unique(c(0, seq(intervalo, mx, by = intervalo))),
      expand = c(0.01, 0)) +
    ggplot2::scale_y_log10(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.05))) +
    ggplot2::labs(
      title    = glue::glue("Distribuição global de dif_min{tipo_label} — {lote_id}"),
      subtitle = "Todos os sensores agregados | eixo Y: escala log10",
      x = "dif_min (min)", y = "Frequência (log10)") +
    ggplot2::theme_bw(base_size = 13) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      axis.title       = ggplot2::element_text(face = "bold"),
      plot.title       = ggplot2::element_text(face = "bold", hjust = 0.5),
      plot.subtitle    = ggplot2::element_text(hjust = 0.5),
      plot.margin      = ggplot2::margin(8, 12, 8, 8))
}

# ── Plot B: frequência RELATIVA (%) por sensor, facet_wrap ────────────
# Normaliza por sensor — permite comparar o perfil de gaps entre sensores
# independentemente do volume total de leituras de cada um.
plot_hist_dif_min_relativo <- function(gap_df, lote_id, tipo = NULL) {
  df <- gap_df |> dplyr::filter(!is.na(dif_min), is.finite(dif_min), dif_min >= 0)
  if (!is.null(tipo)) df <- df |> dplyr::filter(tipo_sensor == tipo)
  if (nrow(df) == 0) return(NULL)

  intervalo  <- max(df$intervalo_min, na.rm = TRUE)
  tipo_label <- if (!is.null(tipo)) paste0(" (sensores ", tipo, ")") else ""

  df_bins <- df |>
    dplyr::mutate(
      Sensor = factor(Sensor, levels = sort(unique(Sensor))),
      bin    = floor(dif_min)
    ) |>
    dplyr::count(Sensor, bin) |>
    dplyr::group_by(Sensor) |>
    dplyr::mutate(freq_rel = n / sum(n)) |>
    dplyr::ungroup()

  ggplot2::ggplot(df_bins, ggplot2::aes(x = bin, y = freq_rel)) +
    ggplot2::geom_col(width = 1, fill = "#5B8FD9", color = "black", linewidth = 0.15) +
    ggplot2::facet_wrap(~Sensor, ncol = 6, scales = "free_x") +
    ggplot2::scale_x_continuous(
      breaks = function(x) {
        mx <- ceiling(max(x, na.rm = TRUE))
        unique(c(0, seq(intervalo, mx, by = intervalo)))
      },
      expand = c(0.02, 0)) +
    ggplot2::scale_y_continuous(
      labels = scales::label_percent(accuracy = 0.1),
      expand = ggplot2::expansion(mult = c(0, 0.05))) +
    ggplot2::labs(
      title    = glue::glue("Frequência relativa de dif_min por sensor{tipo_label} — {lote_id}"),
      subtitle = "Y = % de ocorrências por bin, normalizado por sensor | eixo X livre por sensor",
      x = "dif_min (min)", y = "Frequência relativa (%)") +
    ggplot2::theme_bw(base_size = 11) +
    ggplot2::theme(panel.grid       = ggplot2::element_blank(),
                   panel.background = ggplot2::element_rect(fill = "white", color = "black", linewidth = 0.3),
                   strip.background = ggplot2::element_rect(fill = "grey92", color = "black", linewidth = 0.3),
                   strip.text       = ggplot2::element_text(face = "bold", size = 8),
                   axis.text.x      = ggplot2::element_text(size = 7),
                   axis.text.y      = ggplot2::element_text(size = 7),
                   axis.title       = ggplot2::element_text(face = "bold"),
                   plot.title       = ggplot2::element_text(face = "bold", hjust = 0.5),
                   plot.subtitle    = ggplot2::element_text(hjust = 0.5),
                   panel.spacing    = ggplot2::unit(0.35, "lines"),
                   plot.margin      = ggplot2::margin(8, 8, 8, 8))
}

# ── Plot C: histograma COLORIDO por classe de gap, facet_wrap ─────────
# Cada barra é colorida conforme a classe do intervalo:
#   vermelho = falha_comunicacao (dif_min abaixo do esperado)
#   verde    = ok (dif_min dentro do intervalo esperado)
#   azul     = falha_gap (dif_min acima do esperado)
# Y em log10 para que as classes raras (comunicação, gaps longos) apareçam.
plot_hist_dif_min_colorido <- function(gap_df, lote_id, tipo = NULL) {
  df <- gap_df |> dplyr::filter(!is.na(dif_min), is.finite(dif_min), dif_min >= 0, !is.na(cod_gap))
  if (!is.null(tipo)) df <- df |> dplyr::filter(tipo_sensor == tipo)
  if (nrow(df) == 0) return(NULL)

  intervalo  <- max(df$intervalo_min, na.rm = TRUE)
  tipo_label <- if (!is.null(tipo)) paste0(" (sensores ", tipo, ")") else ""
  cores      <- c("falha comunicacao" = "#E05C52",
                  "ok"                = "#00BA38",
                  "falha gap"         = "#5B8FD9")

  df <- df |>
    dplyr::mutate(
      Sensor = factor(Sensor, levels = sort(unique(Sensor))),
      classe = dplyr::case_when(
        cod_gap == -1L ~ "falha comunicacao",
        cod_gap ==  0L ~ "ok",
        cod_gap ==  1L ~ "falha gap"
      ),
      classe = factor(classe, levels = names(cores))
    )

  ggplot2::ggplot(df, ggplot2::aes(x = dif_min, fill = classe)) +
    ggplot2::geom_histogram(binwidth = 1, boundary = 0, closed = "left",
                             color = "black", linewidth = 0.15, position = "stack") +
    ggplot2::facet_wrap(~Sensor, ncol = 6, scales = "free_x") +
    ggplot2::scale_fill_manual(values = cores, name = NULL) +
    ggplot2::scale_x_continuous(
      breaks = function(x) {
        mx <- ceiling(max(x, na.rm = TRUE))
        unique(c(0, seq(intervalo, mx, by = intervalo)))
      },
      expand = c(0.02, 0)) +
    ggplot2::scale_y_log10(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.05))) +
    ggplot2::labs(
      title    = glue::glue("dif_min por classe de gap{tipo_label} — {lote_id}"),
      subtitle = "Vermelho: falha comunicação | Verde: ok | Azul: falha gap | Y: log10 | X livre por sensor",
      x = "dif_min (min)", y = "Frequência (log10)") +
    ggplot2::theme_bw(base_size = 11) +
    ggplot2::theme(legend.position  = "bottom",
                   legend.text      = ggplot2::element_text(size = 10),
                   panel.grid       = ggplot2::element_blank(),
                   panel.background = ggplot2::element_rect(fill = "white", color = "black", linewidth = 0.3),
                   strip.background = ggplot2::element_rect(fill = "grey92", color = "black", linewidth = 0.3),
                   strip.text       = ggplot2::element_text(face = "bold", size = 8),
                   axis.text.x      = ggplot2::element_text(size = 7),
                   axis.text.y      = ggplot2::element_text(size = 7),
                   axis.title       = ggplot2::element_text(face = "bold"),
                   plot.title       = ggplot2::element_text(face = "bold", hjust = 0.5),
                   plot.subtitle    = ggplot2::element_text(hjust = 0.5),
                   panel.spacing    = ggplot2::unit(0.35, "lines"),
                   plot.margin      = ggplot2::margin(8, 8, 8, 8))
}

# Barras de frequência relativa das classes de gap (falha comunicação / ok / falha gap)
plot_barras_gap_classes_dinamico <- function(gap_df, lote_id, tipo = NULL) {
  df_use <- gap_df |> dplyr::filter(!is.na(classe_gap))
  if (!is.null(tipo)) df_use <- df_use |> dplyr::filter(tipo_sensor == tipo)
  if (nrow(df_use) == 0) return(NULL)

  int_val   <- unique(df_use$intervalo_min)
  int_label <- if (length(int_val) == 1) int_val[[1]] else "N"

  niveis_gap <- c(
    paste0("falha_comunicacao(<", int_label, ")"),
    paste0("ok(=",               int_label, ")"),
    paste0("falha_gap(>",        int_label, ")")
  )
  cores_gap <- setNames(
    c("#F07C73", "#00BA38", "#619CFF"),
    niveis_gap
  )

  df_cat <- df_use |>
    dplyr::count(Sensor, classe_gap, name = "n") |>
    dplyr::group_by(Sensor) |>
    dplyr::mutate(freq_rel = n / sum(n)) |>
    dplyr::ungroup() |>
    dplyr::mutate(Sensor    = factor(Sensor, levels = sort(unique(Sensor))),
                  classe_gap = factor(classe_gap, levels = niveis_gap))

  tipo_label <- if (!is.null(tipo)) paste0(" — sensores ", tipo) else ""

  ggplot2::ggplot(df_cat, ggplot2::aes(x = classe_gap, y = freq_rel, fill = classe_gap)) +
    ggplot2::geom_col(width = 0.72, color = "black", linewidth = 0.2) +
    ggplot2::facet_wrap(~Sensor, ncol = 6, scales = "fixed") +
    ggplot2::scale_fill_manual(values = cores_gap, na.value = "grey80") +
    ggplot2::scale_x_discrete(expand = c(0.08, 0.08)) +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                                 limits = c(0, 1), expand = c(0, 0)) +
    ggplot2::labs(
      title    = glue::glue("Classes de gap (regra dinâmica){tipo_label} — {lote_id}"),
      subtitle = glue::glue("< {int_label}min = falha comunicação | = {int_label}min = ok | > {int_label}min = falha gap"),
      x = NULL, y = "Frequência relativa") +
    ggplot2::theme_bw(base_size = 11) +
    ggplot2::theme(panel.grid = ggplot2::element_blank(),
                   panel.background = ggplot2::element_rect(fill = "white", color = "black", linewidth = 0.3),
                   strip.background = ggplot2::element_blank(),
                   strip.text   = ggplot2::element_text(face = "bold", size = 8),
                   axis.text.x  = ggplot2::element_text(size = 7, angle = 35, hjust = 1),
                   axis.text.y  = ggplot2::element_text(size = 7),
                   axis.title   = ggplot2::element_text(face = "bold"),
                   legend.position = "none",
                   plot.title   = ggplot2::element_text(face = "bold", hjust = 0.5),
                   plot.subtitle = ggplot2::element_text(hjust = 0.5),
                   panel.spacing = ggplot2::unit(0.35, "lines"),
                   plot.margin   = ggplot2::margin(8, 8, 8, 8))
}

# ── Série de temperatura com marcação de falhas (pontos vermelhos) ──
plot_falhas_sensor <- function(df, sensor_id, limite_min, lote_id, datas_eventos = NULL) {
  d <- df |>
    dplyr::filter(Sensor == sensor_id) |>
    dplyr::arrange(data_hora) |>
    dplyr::mutate(dif_min   = as.numeric(difftime(data_hora, dplyr::lag(data_hora), units = "mins")),
                  tem_falha = !is.na(dif_min) & dif_min > limite_min)

  if (nrow(d) == 0 || !"T" %in% names(d) || all(is.na(d$T))) return(NULL)

  y_min <- min(d$T, na.rm = TRUE)
  y_max <- max(d$T, na.rm = TRUE)
  headroom <- max((y_max - y_min) * 0.12, 1)
  y_label  <- y_max + headroom

  g <- ggplot2::ggplot(d, ggplot2::aes(x = data_hora, y = T)) +
    ggplot2::geom_line(color = "grey40", linewidth = 0.4) +
    ggplot2::geom_point(data = d |> dplyr::filter(tem_falha), color = "red", size = 2) +
    ggplot2::labs(title    = glue::glue("Falhas de registro — Sensor {sensor_id}"),
                  subtitle = glue::glue("{lote_id} | Intervalos > {limite_min} min sem dados"),
                  x = "Tempo", y = "Temperatura (°C)") +
    ggplot2::coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(plot.title  = ggplot2::element_text(face = "bold"),
                   plot.margin = ggplot2::margin(35, 20, 10, 10))

  if (!is.null(datas_eventos) && nrow(datas_eventos) > 0)
    g <- g +
      ggplot2::geom_vline(xintercept = datas_eventos$data_hora,
                          color = "red", linetype = "dashed", linewidth = 0.4) +
      ggplot2::geom_text(data = datas_eventos,
                         ggplot2::aes(x = data_hora, y = y_label, label = evento),
                         angle = 90, vjust = 0, hjust = 0, size = 3, color = "red", inherit.aes = FALSE)
  g
}

# ── Mapas temporais (heatmap sensor × bin de tempo) ───────────

# Mapa de contagem de registros por bin de tempo
plot_mapa_temporal_registros <- function(df, spec, lote_id, bin = "10 min") {
  df_ok <- df |> dplyr::filter(!is.na(data_hora), !is.na(.data[[spec$var]]))
  if (nrow(df_ok) == 0) return(NULL)

  df_bin <- df_ok |>
    dplyr::mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
    dplyr::filter(!is.na(t_bin)) |>
    dplyr::count(Sensor, t_bin, name = "n_reg") |>
    dplyr::mutate(Sensor_f = factor(as.character(Sensor), levels = sensor_levels))
  if (nrow(df_bin) == 0) return(NULL)

  ggplot2::ggplot(df_bin, ggplot2::aes(x = t_bin, y = Sensor_f, fill = n_reg)) +
    ggplot2::geom_tile(height = 0.8) +
    ggplot2::scale_fill_viridis_c(option = "C", trans = "sqrt") +
    ggplot2::labs(title    = glue::glue("Mapa temporal de registros por sensor — {spec$titulo}"),
                  subtitle = glue::glue("{lote_id} | bin = {bin}"),
                  x = "Tempo", y = "Sensor", fill = "Registros") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 8),
                   plot.title  = ggplot2::element_text(face = "bold"))
}

# Mapa de presença (0/1 por bin de tempo) — mostra onde há lacunas de dados
plot_mapa_temporal_presenca <- function(df, spec, lote_id, bin = "10 min") {
  df_ok <- df |> dplyr::filter(!is.na(data_hora), !is.na(.data[[spec$var]]))
  if (nrow(df_ok) == 0) return(NULL)

  base <- df_ok |>
    dplyr::mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
    dplyr::filter(!is.na(t_bin)) |>
    dplyr::distinct(Sensor, t_bin) |>
    dplyr::mutate(presente = 1L)
  if (nrow(base) == 0) return(NULL)

  tmin <- min(base$t_bin, na.rm = TRUE)
  tmax <- max(base$t_bin, na.rm = TRUE)
  if (!is.finite(as.numeric(tmin)) || !is.finite(as.numeric(tmax)) || tmin >= tmax) return(NULL)

  df_bin <- base |>
    tidyr::complete(Sensor = 1:30, t_bin = seq(from = tmin, to = tmax, by = bin),
                    fill = list(presente = 0L)) |>
    dplyr::mutate(Sensor_f = factor(as.character(Sensor), levels = sensor_levels))

  ggplot2::ggplot(df_bin, ggplot2::aes(x = t_bin, y = Sensor_f, fill = presente)) +
    ggplot2::geom_tile(height = 0.85) +
    ggplot2::scale_fill_viridis_c(option = "C", breaks = c(0, 1),
                                   labels = c("Sem dado", "Com dado")) +
    ggplot2::labs(title    = glue::glue("Mapa temporal de presença — {spec$titulo}"),
                  subtitle = glue::glue("{lote_id} | bin = {bin}"),
                  x = "Tempo", y = "Sensor", fill = "Presença") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 8),
                   plot.title  = ggplot2::element_text(face = "bold"))
}

# Heatmap de cobertura diária (% do esperado) — usado no diagnóstico de frequência
# Escala: Vermelho = 0% | Amarelo = ~50% | Verde = 100% | Ciano > 100%
plot_heatmap_freq_dia <- function(resultado, sensor_config, lote_id) {
  if (is.null(resultado)) return(NULL)

  config_5  <- sensor_config[sensor_config$intervalo_min == 5L, ]
  config_10 <- sensor_config[sensor_config$intervalo_min == 10L, ]

  dados_long <- resultado$contagem |>
    tidyr::pivot_longer(cols = tidyselect::starts_with("S"),
                        names_to = "Sensor_str", values_to = "obs") |>
    dplyr::mutate(Sensor = as.integer(sub("^S", "", Sensor_str))) |>
    dplyr::left_join(sensor_config |> dplyr::select(Sensor, intervalo_min, esperado_dia), by = "Sensor") |>
    dplyr::mutate(pct     = obs / esperado_dia * 100,
                  pct_cap = pmin(pct, 120),
                  Sensor_f = factor(Sensor, levels = sort(unique(Sensor))))

  ggplot2::ggplot(dados_long, ggplot2::aes(x = Dia, y = Sensor_f, fill = pct_cap)) +
    ggplot2::geom_tile(color = "white", linewidth = 0.04) +
    ggplot2::scale_fill_gradientn(
      colors = c("red3", "orangered", "orange", "yellow", "lightgreen", "green3", "cyan3"),
      values = scales::rescale(c(0, 10, 30, 60, 80, 100, 120)),
      limits = c(0, 120), na.value = "grey80", name = "% Esperado\n(cap. 120%)"
    ) +
    ggplot2::scale_x_date(date_breaks = "7 days", date_labels = "%d/%m") +
    ggplot2::labs(
      title    = glue::glue("Cobertura diária de registros por sensor — {lote_id}"),
      subtitle = glue::glue(
        "% das leituras esperadas no dia | Sensores 5min: {paste(config_5$Sensor, collapse=',')} | ",
        "10min: {paste(config_10$Sensor, collapse=',')} | Cap. 120%"),
      x = "Dia", y = "Sensor"
    ) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(plot.title    = ggplot2::element_text(face = "bold", hjust = 0.5),
                   plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 8),
                   axis.text.x   = ggplot2::element_text(angle = 45, hjust = 1, size = 8),
                   axis.text.y   = ggplot2::element_text(size = 8),
                   panel.grid    = ggplot2::element_blank(),
                   legend.position = "right")
}


# ══════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════

# ── Config visual (faixas ideais e sensores ativos por período) ──
rotulos_eventos <- c(
  "VazioSanitario_Ini" = "VS.Ini",
  "VazioSanitario_Fim" = "VS.Fim",
  "Alojamento"         = "Alojamento",
  "Abertura1"          = "Ab.1",
  "Abertura2"          = "Ab.2",
  "Abertura3"          = "Ab.3",
  "Abertura4"          = "Ab.4",
  "Abertura5_8d"       = "Ab.5_8d",
  "Abertura5_15d"      = "Ab.5_15d"
)

faixas_ideais_t <- tibble::tribble(
  ~Periodo,        ~Tmin, ~Tmax,
  "Alojamento",     32,    35,
  "Abertura1",      32,    35,
  "Abertura2",      32,    35,
  "Abertura3",      29,    32,
  "Abertura4",      26,    29,
  "Abertura5_8d",   23,    26,
  "Abertura5_15d",  20,    23
)

faixas_ideais_ur <- tibble::tribble(
  ~Periodo,        ~URmin, ~URmax,
  "Alojamento",     60,     70,
  "Abertura1",      60,     70,
  "Abertura2",      60,     70,
  "Abertura3",      60,     70,
  "Abertura4",      50,     60,
  "Abertura5_8d",   50,     60,
  "Abertura5_15d",  50,     60
)

# Sensores ativos por período (progressão de colunas ao longo do crescimento dos pintos)
sensores_periodo <- tibble::tribble(
  ~Periodo,             ~sensor_min, ~sensor_max,
  "VazioSanitario_Ini",     1,          30,
  "Alojamento",             7,          15,
  "Abertura1",              7,          18,
  "Abertura2",              7,          21,
  "Abertura3",              7,          24,
  "Abertura4",              7,          30,
  "Abertura5_8d",           1,          30,
  "Abertura5_15d",          1,          30,
  "VazioSanitario_Fim",     1,          30
)

# ── Calendário operacional ────────────────────────────────────
g_cal <- plot_calendario(periodos, lote_id, rotulos_eventos)
save_plot(g_cal, file.path(dir_result_lote(pasta_base, lote_id), "calendario_operacional.png"), w = 16, h = 5)

# ── Carrega banco e prepara datas de eventos ──────────────────
datas_eventos <- periodos |>
  filter(Periodo %in% names(rotulos_eventos)) |>
  transmute(
    evento    = recode(Periodo, !!!rotulos_eventos),
    data_hora = case_when(Periodo == "VazioSanitario_Fim" ~ fim, TRUE ~ inicio)
  ) |>
  arrange(data_hora)

banco <- carregar_banco(pasta_base, lote_id) |>
  add_linha_coluna() |>
  mutate(
    data_hora = lubridate::with_tz(data_hora, tzone = tz_local),
    Data      = as.Date(data_hora),
    hora      = lubridate::hour(data_hora),
    Linha     = factor(Linha, levels = c("L1", "L2", "L3")),
    Coluna    = factor(Coluna, levels = paste0("C", 1:10)),
    Sensor    = as.integer(Sensor)
  ) |>
  filter(!is.na(data_hora)) |>
  filtrar_periodo_analise(periodos) |>
  classificar_periodos(periodos)

# ── Maior gap por período ─────────────────────────────────────
dir_maior_gap <- dir_result_lote(pasta_base, lote_id, "falhas", "maior_gap")

periodos_existentes <- banco |>
  filter(!is.na(Periodo)) |>
  distinct(Periodo) |>
  pull(Periodo) |>
  as.character()

pb_gap_periodos <- progress::progress_bar$new(
  format = "Maior gap (periodos) [:bar] :current/:total (:percent) ETA: :eta",
  total  = max(1, length(periodos_existentes)), clear = FALSE, width = 60)

for (pp in periodos_existentes) {
  pb_gap_periodos$tick()

  dir_pp   <- file.path(dir_maior_gap, paste0("periodo_", pp))
  dir.create(dir_pp, recursive = TRUE, showWarnings = FALSE)
  banco_pp <- banco |> filter(as.character(Periodo) == pp)
  gaps_pp  <- calc_gaps(banco_pp)
  if (nrow(gaps_pp) == 0) next

  tab_pp <- gaps_pp |>
    group_by(Sensor, Linha, Coluna) |>
    summarise(
      max_gap_min    = as.integer(round(max(dif_min, na.rm = TRUE), 0)),
      p95_gap_min    = as.integer(round(quantile(dif_min, probs = 0.95, na.rm = TRUE), 0)),
      inicio_max_gap = inicio_gap[which.max(dif_min)],
      fim_max_gap    = fim_gap[which.max(dif_min)],
      .groups        = "drop"
    ) |>
    arrange(desc(max_gap_min))

  readr::write_csv(tab_pp, file.path(dir_pp, "maior_gap_por_sensor.csv"))
  readr::write_rds(tab_pp, file.path(dir_pp, "maior_gap_por_sensor.rds"))

  save_plot(plot_ranking_por_sensor(tab_pp, titulo_extra = pp),
            file.path(dir_pp, "01_ranking_maior_gap_por_sensor.png"), w = 16, h = 7)

  df_day <- gaps_pp |>
    filter(!is.na(Dia)) |>
    group_by(Dia, Sensor) |>
    summarise(max_gap_dia = max(dif_min, na.rm = TRUE), .groups = "drop")

  if (nrow(df_day) > 0) {
    dias <- seq(min(df_day$Dia), max(df_day$Dia), by = "day")
    df_day <- df_day |> tidyr::complete(Dia = dias, Sensor = sort(unique(df_day$Sensor)))
  }

  save_plot(plot_heatmap_maior_gap_por_dia(df_day, titulo_extra = pp),
            file.path(dir_pp, "02_heatmap_maior_gap_por_dia.png"), w = 18, h = 9)

  # Gráficos individuais por sensor
  dir_sens <- file.path(dir_pp, "sensores_individuais")
  dir.create(dir_sens, recursive = TRUE, showWarnings = FALSE)
  sensores_pp <- sort(unique(gaps_pp$Sensor))

  pb_sens <- progress::progress_bar$new(
    format = glue("Maior gap ({pp}) sensores [:bar] :current/:total (:percent) ETA: :eta"),
    total  = max(1, length(sensores_pp)), clear = FALSE, width = 60)

  for (sid in sensores_pp) {
    pb_sens$tick()
    g1 <- plot_gaps_sensor(gaps_pp, sid, titulo_extra = pp)
    if (!is.null(g1)) save_plot(g1, file.path(dir_sens, glue("sensor_{sid}_gaps_tempo.png")), w = 16, h = 6)
  }

  # Facets de 6 sensores por página
  dir_facets <- file.path(dir_pp, "facets_6_sensores")
  dir.create(dir_facets, recursive = TRUE, showWarnings = FALSE)
  chunks <- split(sensores_pp, ceiling(seq_along(sensores_pp) / 6))

  pb_facets <- progress::progress_bar$new(
    format = glue("Maior gap ({pp}) facets [:bar] :current/:total (:percent) ETA: :eta"),
    total  = max(1, length(chunks)), clear = FALSE, width = 60)

  for (i in seq_along(chunks)) {
    pb_facets$tick()
    g6 <- plot_gaps_facets_6(gaps_pp, chunks[[i]], titulo_extra = pp)
    if (!is.null(g6)) save_plot(g6, file.path(dir_facets, glue("gaps_6sensores_pagina_{i}.png")), w = 18, h = 10)
  }

  message("✅ Maior gap (", lote_id, " | ", pp, ") → ", dir_pp)
}
message("✅ MAIOR GAP finalizado para ", lote_id)

# ── Série temporal (T e/ou UR) ────────────────────────────────
rodar_entregaveis_serie <- function(df, var) {
  if (var == "UR") {
    if (!"UR" %in% names(df) || all(is.na(df$UR))) {
      message("UR não disponível neste lote: pulando umidade.")
      return(invisible(FALSE))
    }
  }

  spec           <- get_spec(var)
  dir_main       <- dir_result_lote(pasta_base, lote_id, spec$nome)
  dir_painel_var <- dir_result_lote(pasta_base, lote_id, spec$nome, "painel_periodos")
  dir_colunas_var <- dir_result_lote(pasta_base, lote_id, spec$nome, "colunas_individuais")
  dir_mapas_var  <- dir_result_lote(pasta_base, lote_id, spec$nome, "mapas")
  dir_linhas_var <- dir_result_lote(pasta_base, lote_id, spec$nome, "linhas_individuais")

  g_geral <- plot_geral(df, spec, lote_id, datas_eventos)
  if (!is.null(g_geral)) {
    if (mostrar_graficos) print(g_geral)
    save_plot(g_geral, file.path(dir_main, glue("00_{spec$nome}_geral.png")), w = 12, h = 7)
  }

  g_dia <- plot_por_dia(df, spec, lote_id)
  if (!is.null(g_dia)) {
    if (mostrar_graficos) print(g_dia)
    save_plot(g_dia, file.path(dir_main, glue("01_{spec$nome}_por_dia.png")), w = 16, h = 9)
  }

  lista_periodos <- as.character(periodos$Periodo)

  pb_pp <- progress::progress_bar$new(
    format = glue("{spec$nome} painéis [:bar] :current/:total (:percent) ETA: :eta"),
    total  = length(lista_periodos), clear = FALSE, width = 60)

  for (pp in lista_periodos) {
    pb_pp$tick()
    g <- plot_painel_periodo(df, spec, pp)
    if (!is.null(g)) {
      if (mostrar_graficos) print(g)
      save_plot(g, file.path(dir_painel_var, glue("painel_{pp}.png")), w = 16, h = 9)
    }
  }

  g_linha <- plot_por_linha(df, spec, lote_id, datas_eventos)
  if (!is.null(g_linha)) {
    if (mostrar_graficos) print(g_linha)
    save_plot(g_linha, file.path(dir_main, glue("02_{spec$nome}_por_linha.png")), w = 16, h = 7)
  }
  salvar_por_linha(df, spec, lote_id, datas_eventos, dir_linhas_var)

  g_col_5x2 <- plot_por_coluna_5x2(df, spec, lote_id, datas_eventos)
  if (!is.null(g_col_5x2)) {
    if (mostrar_graficos) print(g_col_5x2)
    save_plot(g_col_5x2, file.path(dir_main, glue("03_{spec$nome}_por_coluna_5x2.png")), w = 18, h = 12)
  }
  salvar_por_coluna(df, spec, lote_id, datas_eventos, dir_colunas_var)

  # Mapas temporais
  df_map <- df
  if (!is.null(periodo_mapas) && length(periodo_mapas) == 2)
    df_map <- df_map |> filter(data_hora >= periodo_mapas[1], data_hora <= periodo_mapas[2])

  pb_map <- progress::progress_bar$new(
    format = glue("{spec$nome} mapas [:bar] :current/:total (:percent) ETA: :eta"),
    total  = length(bins_map_min), clear = FALSE, width = 60)

  for (bm in bins_map_min) {
    pb_map$tick()
    if (bm < bin_minimo_mapas) next
    bin_str <- glue("{bm} mins")

    g_mapa <- plot_mapa_temporal_registros(df_map, spec, lote_id, bin = bin_str)
    if (!is.null(g_mapa)) {
      if (mostrar_graficos) print(g_mapa)
      save_plot(g_mapa, file.path(dir_mapas_var, glue("04_mapa_registros_{bm}min.png")), w = 18, h = 9)
    }

    g_mapa_pres <- plot_mapa_temporal_presenca(df_map, spec, lote_id, bin = bin_str)
    if (!is.null(g_mapa_pres)) {
      if (mostrar_graficos) print(g_mapa_pres)
      save_plot(g_mapa_pres, file.path(dir_mapas_var, glue("04_mapa_presenca_{bm}min.png")), w = 18, h = 9)
    }
  }

  invisible(TRUE)
}

if (gerar_temperatura) rodar_entregaveis_serie(banco, "T")
if (gerar_umidade)     rodar_entregaveis_serie(banco, "UR")

# ── Falhas — classificação por intervalo real de cada sensor ─────────
# Três classes: "ok" | "falha_gap" | "falha_comunicacao"
dir_tabs <- dir_result_lote(pasta_base, lote_id, "falhas", "tabelas")

falhas_df <- calcular_falhas(banco, sensor_config)

falhas_por_sensor <- falhas_df |>
  group_by(Sensor, Linha, Coluna, intervalo_min) |>
  summarise(
    n_ok              = sum(tem_falha == "ok",                na.rm = TRUE),
    n_falha_gap       = sum(tem_falha == "falha_gap",         na.rm = TRUE),
    n_falha_comunic   = sum(tem_falha == "falha_comunicacao", na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(Lote   = lote_id,
         Linha  = readr::parse_number(as.character(Linha)),
         Coluna = readr::parse_number(as.character(Coluna))) |>
  relocate(Lote, .before = Sensor) |>
  arrange(desc(n_falha_gap))

readr::write_csv(falhas_por_sensor, file.path(dir_tabs, "falhas_por_sensor_bio.csv"))
readr::write_rds(falhas_df,         file.path(dir_tabs, "falhas_brutas_bio.rds"))
message(glue::glue("✅ Falhas classificadas por intervalo real: ",
                   "sensor 5min → ok=5min | sensor 10min → ok=10min"))

# ── Gap dinâmico (5min vs 10min por sensor) ───────────────────
# Calcula e classifica gaps usando o intervalo real de cada sensor.
# Corrige o erro antigo que avaliava todos com regra de 5 min.
dir_gap_din <- dir_result_lote(pasta_base, lote_id, "falhas", "gap_dinamico")

gap_din_df <- calcular_gap_dinamico(banco, sensor_config)

tab_medidas_sensor <- gap_din_df |>
  group_by(Sensor) |>
  summarise(n_medidas_sensor = sum(!is.na(data_hora)),
            n_dif_validas    = sum(!is.na(dif_min)), .groups = "drop")

tab_gap_din <- gap_din_df |>
  filter(!is.na(classe_gap)) |>
  count(Sensor, Linha, Coluna, intervalo_min, tipo_sensor, cod_gap, classe_gap, name = "n") |>
  group_by(Sensor) |>
  mutate(n_dif_total = sum(n), freq_rel = n / n_dif_total) |>
  ungroup() |>
  left_join(tab_medidas_sensor, by = "Sensor") |>
  mutate(Linha  = readr::parse_number(as.character(Linha)),
         Coluna = readr::parse_number(as.character(Coluna))) |>
  arrange(Sensor, cod_gap)

readr::write_csv(tab_gap_din, file.path(dir_gap_din, "tabela_gap_dinamico_por_sensor.csv"))
readr::write_rds(gap_din_df,  file.path(dir_gap_din, "gap_dinamico_bruto.rds"))

# A — por sensor, cauda completa, Y log10 (plot original melhorado)
g_hist_5  <- plot_hist_dif_min_dinamico(gap_din_df, lote_id, tipo = "5min")
g_hist_10 <- plot_hist_dif_min_dinamico(gap_din_df, lote_id, tipo = "10min")
if (!is.null(g_hist_5))  save_plot(g_hist_5,  file.path(dir_gap_din, "hist_dif_min_sensores_5min.png"),  w = 18, h = 10)
if (!is.null(g_hist_10)) save_plot(g_hist_10, file.path(dir_gap_din, "hist_dif_min_sensores_10min.png"), w = 18, h = 10)

# B — agregado (todos sensores juntos), Y log10
g_agr_5  <- plot_hist_dif_min_agregado(gap_din_df, lote_id, tipo = "5min")
g_agr_10 <- plot_hist_dif_min_agregado(gap_din_df, lote_id, tipo = "10min")
if (!is.null(g_agr_5))  save_plot(g_agr_5,  file.path(dir_gap_din, "hist_dif_min_agregado_5min.png"),  w = 12, h = 7)
if (!is.null(g_agr_10)) save_plot(g_agr_10, file.path(dir_gap_din, "hist_dif_min_agregado_10min.png"), w = 12, h = 7)

# C — frequência relativa (%) por sensor
g_rel_5  <- plot_hist_dif_min_relativo(gap_din_df, lote_id, tipo = "5min")
g_rel_10 <- plot_hist_dif_min_relativo(gap_din_df, lote_id, tipo = "10min")
if (!is.null(g_rel_5))  save_plot(g_rel_5,  file.path(dir_gap_din, "hist_dif_min_relativo_5min.png"),  w = 18, h = 10)
if (!is.null(g_rel_10)) save_plot(g_rel_10, file.path(dir_gap_din, "hist_dif_min_relativo_10min.png"), w = 18, h = 10)

# D — colorido por classe (falha comunicação / ok / falha gap), Y log10
g_cor_5  <- plot_hist_dif_min_colorido(gap_din_df, lote_id, tipo = "5min")
g_cor_10 <- plot_hist_dif_min_colorido(gap_din_df, lote_id, tipo = "10min")
if (!is.null(g_cor_5))  save_plot(g_cor_5,  file.path(dir_gap_din, "hist_dif_min_colorido_5min.png"),  w = 18, h = 10)
if (!is.null(g_cor_10)) save_plot(g_cor_10, file.path(dir_gap_din, "hist_dif_min_colorido_10min.png"), w = 18, h = 10)

g_bar_5  <- plot_barras_gap_classes_dinamico(gap_din_df, lote_id, tipo = "5min")
g_bar_10 <- plot_barras_gap_classes_dinamico(gap_din_df, lote_id, tipo = "10min")
if (!is.null(g_bar_5))  save_plot(g_bar_5,  file.path(dir_gap_din, "freq_classes_gap_sensores_5min.png"),  w = 18, h = 10)
if (!is.null(g_bar_10)) save_plot(g_bar_10, file.path(dir_gap_din, "freq_classes_gap_sensores_10min.png"), w = 18, h = 10)

message("✅ Gap dinâmico finalizado em: ", dir_gap_din)

# ── Falhas de registro por sensor (série de T + pontos vermelhos) ──
dir_falhas_sensores <- dir_result_lote(pasta_base, lote_id, "falhas", "por_sensor")
sensores_todos <- sort(unique(banco$Sensor))

pb_falhas_plot <- progress::progress_bar$new(
  format = "Falhas por sensor [:bar] :current/:total (:percent) ETA: :eta",
  total  = length(limites_plot_falhas) * length(sensores_todos), clear = FALSE, width = 60)

for (lim in limites_plot_falhas) {
  for (sid in sensores_todos) {
    pb_falhas_plot$tick()
    g <- plot_falhas_sensor(banco, sensor_id = sid, limite_min = lim,
                             lote_id = lote_id, datas_eventos = datas_eventos)
    if (!is.null(g)) {
      if (mostrar_graficos) print(g)
      save_plot(g, file.path(dir_falhas_sensores, glue("falhas_sensor_{sid}_{lim}min.png")), w = 16, h = 6)
    }
  }
}

# ── Diagnóstico de frequência (tabela sensor × dia) ───────────
# Pedido da Profa. Simone: contar obs por sensor por dia para
# conferir onde estão as falhas e se algum sensor está acima do esperado.
dir_diag <- dir_result_lote(pasta_base, lote_id, "falhas", "diagnostico_frequencia")
diagnostico_frequencia_lote(banco, sensor_config, periodos, lote_id, dir_diag)

message("✅ ANÁLISE COMPLETA finalizada para: ", lote_id)
