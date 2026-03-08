# ============================================================
# scripts/2._main_analise.R
# ============================================================

# -------------------------------
# 0) CONFIGURAÇÕES (edite só aqui)
# -------------------------------
if (!exists("pasta_base")) pasta_base <- "C:/Users/LENOVO/Documents/BroindexAnalise"
if (!exists("lote_id"))    lote_id    <- "lote2"  # só usado se rodar o script sozinho

gerar_temperatura <- TRUE
gerar_umidade     <- TRUE

# >>> PADRÃO ÚNICO para TUDO (minutos)
padrao_min <- c(10, 15, 30, 60)

# limites em minutos (tudo sai daqui)
limites_metricas    <- padrao_min   # usados em métricas + paretos
bins_map_min        <- padrao_min   # usados nos mapas temporais
limites_plot_falhas <- padrao_min   # usados nos gráficos por sensor

# SAFE MODE
mostrar_graficos  <- FALSE  # não printar na tela (evita crash)
bin_minimo_mapas  <- 10     # evita bins muito pequenos
periodo_mapas     <- NULL   # ex: c(as.POSIXct("2025-10-01"), as.POSIXct("2025-10-10"))

# -------------------------------
# 1) PACOTES
# -------------------------------
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(readr)
  library(glue)
  library(viridis)
  library(progress)
})

# -------------------------------
# 2) PERÍODOS POR LOTE (fonte única)
# -------------------------------
source(file.path(pasta_base, "scripts", "0_periodos_lotes.R"))
periodos <- pegar_periodos_lote(lote_id, tz = tz_local)

# -------------------------------
# 2a) RÓTULOS PADRONIZADOS (VISUAL)
# -------------------------------
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

# -------------------------------
# 2b) Faixas ideais (oficial)
# -------------------------------
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
  "Alojamento",     40,     45,
  "Abertura1",      40,     45,
  "Abertura2",      40,     45,
  "Abertura3",      50,     70,
  "Abertura4",      50,     70,
  "Abertura5_8d",   50,     70,
  "Abertura5_15d",  50,     70
)

# -------------------------------
# 2c) Sensores por período
# OBS PROF: VS = TODOS sensores (1–30)
# -------------------------------
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

# -------------------------------
# 3) FUNÇÕES DE DIRETÓRIO / SALVAR
# -------------------------------
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

# -------------------------------
# 3a) CALENDÁRIO OPERACIONAL VISUAL
# -------------------------------
periodos_cal <- periodos |>
  mutate(
    rotulo = dplyr::recode(Periodo, !!!rotulos_eventos),
    rotulo = factor(rotulo, levels = rev(unique(rotulo)))
  ) |>
  mutate(meio = inicio + (fim - inicio) / 2)

g_cal <- ggplot(periodos_cal) +
  geom_segment(
    aes(x = inicio, xend = fim, y = rotulo, yend = rotulo, color = rotulo),
    linewidth = 10,
    lineend = "butt"
  ) +
  geom_text(
    aes(x = meio, y = rotulo, label = rotulo),
    color = "white",
    size = 4,
    fontface = "bold"
  ) +
  scale_x_datetime(
    date_breaks = "3 days",
    date_labels = "%d/%m",
    expand = expansion(mult = c(0.01, 0.01))
  ) +
  guides(color = "none") +
  labs(
    title = glue("Calendário operacional — {lote_id}"),
    x = "Data",
    y = NULL
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.x = element_text(angle = 0),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    plot.margin = margin(10, 20, 10, 20)
  )

save_plot(
  g_cal,
  file.path(dir_result_lote(pasta_base, lote_id), "calendario_operacional.png"),
  w = 16,
  h = 5
)

# -------------------------------
# 4) FUNÇÕES DE DADOS (carregar + linha/coluna)
# -------------------------------
carregar_banco <- function(pasta_base, lote_id) {
  arq <- file.path(dir_tratado_lote(pasta_base, lote_id), "banco_periodos.rds")
  if (!file.exists(arq)) {
    stop(
      "Não achei o banco tratado: ", arq,
      "\nRode antes o script 1_leitura_dados.R para esse lote."
    )
  }
  readr::read_rds(arq)
}

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

# -------------------------------
# 5) PADRÃO DE CORES (sensor)
# -------------------------------
sensor_levels <- as.character(1:30)
sensor_palette <- setNames(
  scales::hue_pal(l = 60, c = 100)(length(sensor_levels)),
  sensor_levels
)

scale_cor_sensor_padrao <- function(legend_title = "Sensor") {
  scale_color_manual(
    values = sensor_palette,
    breaks = sensor_levels,
    drop = TRUE,
    name = legend_title
  )
}

scale_cor_sensor_subset <- function(levels_subset, legend_title = "Sensor") {
  lev <- as.character(levels_subset)
  pal <- setNames(scales::hue_pal(l = 60, c = 120)(length(lev)), lev)
  scale_color_manual(values = pal, breaks = lev, drop = TRUE, name = legend_title)
}

# -------------------------------
# 6) CARREGA BANCO + EVENTOS
# -------------------------------
datas_eventos <- periodos |>
  filter(Periodo %in% names(rotulos_eventos)) |>
  transmute(
    evento = recode(Periodo, !!!rotulos_eventos),
    data_hora = case_when(
      Periodo == "VazioSanitario_Fim" ~ fim,
      TRUE                           ~ inicio
    )
  ) |>
  arrange(data_hora)

banco <- carregar_banco(pasta_base, lote_id) |>
  add_linha_coluna() |>
  mutate(
    data_hora = lubridate::with_tz(data_hora, tzone = tz_local),
    Data = as.Date(data_hora),
    hora = lubridate::hour(data_hora),
    Linha = factor(Linha, levels = c("Linha 1", "Linha 2", "Linha 3")),
    Coluna = factor(Coluna, levels = paste("Coluna", 1:10))
  ) |>
  filter(!is.na(data_hora)) |>
  mutate(Sensor = as.integer(Sensor))

# ============================================================
# 6b) MAIOR GAP (tempo sem registro) — POR PERÍODO
# ============================================================
dir_maior_gap <- dir_result_lote(pasta_base, lote_id, "falhas", "maior_gap")
dir.create(dir_maior_gap, recursive = TRUE, showWarnings = FALSE)

calc_gaps <- function(df) {
  df |>
    dplyr::filter(!is.na(data_hora), !is.na(Sensor)) |>
    dplyr::arrange(Sensor, data_hora) |>
    dplyr::group_by(Sensor, Linha, Coluna, Periodo) |>
    dplyr::mutate(
      dif_min = as.numeric(difftime(data_hora, dplyr::lag(data_hora), units = "mins")),
      inicio_gap = dplyr::lag(data_hora),
      fim_gap = data_hora,
      Dia = as.Date(fim_gap)
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.finite(dif_min), dif_min >= 0)
}

plot_ranking_por_sensor <- function(tab, titulo_extra = NULL) {
  ggplot(tab, aes(
    x = reorder(as.factor(Sensor), -max_gap_min),
    y = max_gap_min,
    fill = Linha
  )) +
    geom_col(alpha = 0.9) +
    geom_text(aes(label = round(max_gap_min, 1)), vjust = -0.25, size = 3) +
    scale_fill_viridis_d(option = "C") +
    geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey35") +
    labs(
      title = glue("Maior tempo sem registro por sensor — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = glue("Linhas tracejadas: {paste(padrao_min, collapse = ', ')} min"),
      x = "Sensor",
      y = "Maior gap (min)",
      fill = "Linha"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
}

plot_heatmap_maior_gap_por_dia <- function(df_day, titulo_extra = NULL) {
  ggplot(df_day, aes(x = Dia, y = factor(Sensor), fill = max_gap_dia)) +
    geom_tile(height = 0.9) +
    scale_fill_viridis_c(option = "C", trans = "sqrt", na.value = "grey95") +
    labs(
      title = glue("Heatmap — maior gap por dia (tempo × sensor) — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = "Cada célula = MAIOR intervalo (min) naquele dia para aquele sensor",
      x = "Dia",
      y = "Sensor",
      fill = "Maior gap\n(min)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.y = element_text(size = 7)
    )
}

plot_gaps_sensor <- function(gaps_df, sensor_id, titulo_extra = NULL) {
  d <- gaps_df |>
    dplyr::filter(Sensor == sensor_id) |>
    dplyr::filter(is.finite(dif_min), dif_min > 0) |>
    dplyr::arrange(fim_gap)
  
  if (nrow(d) == 0) return(NULL)
  
  max_gap <- max(d$dif_min, na.rm = TRUE)
  d_max <- d |> dplyr::slice(which.max(dif_min))
  
  ggplot(d, aes(x = fim_gap, y = dif_min)) +
    geom_point(alpha = 0.35, size = 1.1, color = "grey35") +
    geom_point(data = d_max, color = "red", size = 2.5) +
    geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey55") +
    geom_hline(yintercept = max_gap, color = "red", linewidth = 0.8) +
    annotate(
      "text",
      x = d_max$fim_gap,
      y = max_gap,
      label = glue("MAX {round(max_gap,1)} min"),
      vjust = -0.6,
      hjust = 0.5,
      color = "red",
      size = 3.2
    ) +
    labs(
      title = glue("Gaps ao longo do tempo — Sensor {sensor_id} — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = glue("Maior gap: {format(d_max$inicio_gap, '%d/%m %H:%M')} → {format(d_max$fim_gap, '%d/%m %H:%M')}"),
      x = "Tempo (fim do intervalo)",
      y = "Gap entre registros (min)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
}

plot_gaps_facets_6 <- function(gaps_df, sensors_6, titulo_extra = NULL) {
  d <- gaps_df |>
    dplyr::filter(Sensor %in% sensors_6) |>
    dplyr::filter(is.finite(dif_min), dif_min > 0) |>
    dplyr::mutate(Sensor_f = factor(Sensor, levels = sort(unique(Sensor))))
  
  if (nrow(d) == 0) return(NULL)
  
  ggplot(d, aes(x = fim_gap, y = dif_min)) +
    geom_point(alpha = 0.35, size = 1.0, color = "grey35") +
    geom_hline(yintercept = padrao_min, linetype = "dashed", color = "grey55") +
    facet_wrap(~Sensor_f, ncol = 3, scales = "free_x") +
    labs(
      title = glue("Gaps — 6 sensores por página — {lote_id}{if (!is.null(titulo_extra)) paste0(' — ', titulo_extra) else ''}"),
      subtitle = "Y = gap (min). Linhas tracejadas = limites padrão",
      x = "Tempo (fim do intervalo)",
      y = "Gap (min)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
}

periodos_existentes <- banco |>
  dplyr::filter(!is.na(Periodo)) |>
  dplyr::distinct(Periodo) |>
  dplyr::pull(Periodo) |>
  as.character()

pb_gap_periodos <- progress::progress_bar$new(
  format = "Maior gap (periodos) [:bar] :current/:total (:percent) ETA: :eta",
  total = max(1, length(periodos_existentes)),
  clear = FALSE,
  width = 60
)

for (pp in periodos_existentes) {
  pb_gap_periodos$tick()
  
  dir_pp <- file.path(dir_maior_gap, paste0("periodo_", pp))
  dir.create(dir_pp, recursive = TRUE, showWarnings = FALSE)
  
  banco_pp <- banco |> dplyr::filter(as.character(Periodo) == pp)
  
  gaps_pp <- calc_gaps(banco_pp)
  if (nrow(gaps_pp) == 0) next
  
  tab_pp <- gaps_pp |>
    dplyr::group_by(Sensor, Linha, Coluna) |>
    dplyr::summarise(
      max_gap_min = max(dif_min, na.rm = TRUE),
      inicio_max_gap = inicio_gap[which.max(dif_min)],
      fim_max_gap = fim_gap[which.max(dif_min)],
      .groups = "drop"
    ) |>
    dplyr::arrange(desc(max_gap_min))
  
  readr::write_csv(tab_pp, file.path(dir_pp, "maior_gap_por_sensor.csv"))
  readr::write_rds(tab_pp, file.path(dir_pp, "maior_gap_por_sensor.rds"))
  
  g_rank <- plot_ranking_por_sensor(tab_pp, titulo_extra = pp)
  save_plot(g_rank, file.path(dir_pp, "01_ranking_maior_gap_por_sensor.png"), w = 16, h = 7)
  
  df_day <- gaps_pp |>
    dplyr::filter(!is.na(Dia)) |>
    dplyr::group_by(Dia, Sensor) |>
    dplyr::summarise(max_gap_dia = max(dif_min, na.rm = TRUE), .groups = "drop")
  
  if (nrow(df_day) > 0) {
    dias <- seq(min(df_day$Dia), max(df_day$Dia), by = "day")
    sensores <- sort(unique(df_day$Sensor))
    df_day <- df_day |>
      tidyr::complete(Dia = dias, Sensor = sensores)
  }
  
  g_hm <- plot_heatmap_maior_gap_por_dia(df_day, titulo_extra = pp)
  save_plot(g_hm, file.path(dir_pp, "02_heatmap_maior_gap_por_dia.png"), w = 18, h = 9)
  
  dir_sens <- file.path(dir_pp, "sensores_individuais")
  dir.create(dir_sens, recursive = TRUE, showWarnings = FALSE)
  
  sensores_pp <- sort(unique(gaps_pp$Sensor))
  
  pb_gap_sens <- progress::progress_bar$new(
    format = glue("Maior gap ({pp}) sensores [:bar] :current/:total (:percent) ETA: :eta"),
    total = max(1, length(sensores_pp)),
    clear = FALSE,
    width = 60
  )
  
  for (sid in sensores_pp) {
    pb_gap_sens$tick()
    g1 <- plot_gaps_sensor(gaps_pp, sid, titulo_extra = pp)
    if (is.null(g1)) next
    save_plot(g1, file.path(dir_sens, glue("sensor_{sid}_gaps_tempo.png")), w = 16, h = 6)
  }
  
  dir_facets <- file.path(dir_pp, "facets_6_sensores")
  dir.create(dir_facets, recursive = TRUE, showWarnings = FALSE)
  
  chunks <- split(sensores_pp, ceiling(seq_along(sensores_pp) / 6))
  
  pb_gap_facets <- progress::progress_bar$new(
    format = glue("Maior gap ({pp}) facets [:bar] :current/:total (:percent) ETA: :eta"),
    total = max(1, length(chunks)),
    clear = FALSE,
    width = 60
  )
  
  for (i in seq_along(chunks)) {
    pb_gap_facets$tick()
    g6 <- plot_gaps_facets_6(gaps_pp, chunks[[i]], titulo_extra = pp)
    if (is.null(g6)) next
    save_plot(g6, file.path(dir_facets, glue("gaps_6sensores_pagina_{i}.png")), w = 18, h = 10)
  }
  
  message("✅ Maior gap (", lote_id, " | ", pp, ") salvo em: ", dir_pp)
}

message("✅ BLOCO MAIOR GAP FINALIZADO para ", lote_id, " em: ", dir_maior_gap)

# ============================================================
# 7) FUNÇÕES GENÉRICAS PARA SÉRIE (T ou UR)
# ============================================================
get_spec <- function(var) {
  if (var == "T") {
    list(var = "T", nome = "temperatura", titulo = "Temperatura", ylab = "Temperatura (°C)")
  } else if (var == "UR") {
    list(var = "UR", nome = "umidade", titulo = "Umidade Relativa", ylab = "Umidade Relativa (%)")
  } else {
    stop("Variável não suportada: ", var)
  }
}

layout_por_dias <- function(n_dias) {
  if (n_dias <= 1) return(list(ncol = 1))
  if (n_dias == 2) return(list(ncol = 2))
  if (n_dias == 3) return(list(ncol = 3))
  if (n_dias == 4) return(list(ncol = 2))
  if (n_dias <= 6) return(list(ncol = 3))
  list(ncol = 4)
}

plot_geral <- function(df, spec, lote_id, datas_eventos) {
  df2 <- df |> filter(!is.na(.data[[spec$var]]))
  if (nrow(df2) == 0) return(NULL)
  
  y_vals <- df2[[spec$var]]
  y_min  <- min(y_vals, na.rm = TRUE)
  y_max  <- max(y_vals, na.rm = TRUE)
  
  headroom <- (y_max - y_min) * 0.10
  if (!is.finite(headroom) || headroom <= 0) headroom <- 1
  y_label <- y_max + headroom
  
  ggplot(df2, aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor))) +
    geom_line(linewidth = 0.7, alpha = 0.8) +
    geom_vline(
      xintercept = datas_eventos$data_hora,
      color = "red",
      linetype = "dashed",
      linewidth = 0.5
    ) +
    geom_text(
      data = datas_eventos,
      aes(x = data_hora, y = y_label, label = evento),
      angle = 90,
      vjust = 0,
      hjust = 0,
      size = 3,
      color = "red",
      inherit.aes = FALSE
    ) +
    coord_cartesian(
      ylim = c(y_min, y_max + headroom * 1.6),
      clip = "off"
    ) +
    labs(
      title = glue("{spec$titulo} por sensor – visão geral ({lote_id})"),
      x = "Tempo", y = spec$ylab, color = "Sensor"
    ) +
    theme_minimal(base_size = 13) +
    theme(
      legend.position = "bottom",
      plot.margin = margin(35, 30, 10, 10),
      plot.title = element_text(face = "bold", hjust = 0.5)
    )
}

plot_por_dia <- function(df, spec, lote_id) {
  df2 <- df |>
    dplyr::filter(!is.na(data_hora), !is.na(.data[[spec$var]])) |>
    dplyr::mutate(
      Data = as.Date(data_hora),
      hora_dia = as.POSIXct(
        paste("1970-01-01", format(data_hora, "%H:%M:%S")),
        tz = ""
      )
    )
  
  if (nrow(df2) == 0) return(NULL)
  
  dias_seq <- seq(min(df2$Data), max(df2$Data), by = "day")
  
  df2 <- df2 |>
    dplyr::mutate(
      Data = factor(Data, levels = dias_seq),
      Sensor_f = factor(as.character(Sensor), levels = sensor_levels)
    )
  
  ggplot(df2, aes(x = hora_dia, y = .data[[spec$var]], color = Sensor_f, group = Sensor_f)) +
    geom_line(linewidth = 0.5, alpha = 0.8, na.rm = TRUE) +
    facet_wrap(~Data, ncol = 5, drop = FALSE) +
    scale_x_datetime(
      date_labels = "%H:%M",
      date_breaks = "4 hours",
      limits = as.POSIXct(
        c("1970-01-01 00:00:00", "1970-01-01 23:59:59"),
        tz = ""
      )
    ) +
    labs(
      title = glue("{spec$titulo} — todos os dias ({lote_id})"),
      x = "Hora (00:00–23:59)",
      y = spec$ylab
    ) +
    theme_bw(base_size = 11) +
    theme(
      axis.text.x = element_text(size = 6, angle = 45, hjust = 1),
      axis.text.y = element_text(size = 5),
      panel.spacing = unit(0.4, "lines"),
      strip.text = element_text(size = 6, face = "bold", margin = margin(1, 0, 1, 0)),
      strip.background = element_rect(fill = "grey97", color = NA, linewidth = 0.1),
      legend.position = "none",
      plot.title = element_text(face = "bold", hjust = 0.5)
    )
}

plot_painel_periodo <- function(df, spec, nome_periodo, quebra_horas = "4 hours") {
  p <- periodos |> dplyr::filter(Periodo == nome_periodo)
  if (nrow(p) == 0) stop("Período não encontrado: ", nome_periodo)
  
  sens_range <- sensores_periodo |> dplyr::filter(Periodo == nome_periodo)
  if (nrow(sens_range) == 0) stop("Regra de sensores não encontrada: ", nome_periodo)
  
  sensores_validos <- sens_range$sensor_min:sens_range$sensor_max
  
  dados_p <- df |>
    dplyr::filter(data_hora >= p$inicio, data_hora <= p$fim) |>
    dplyr::mutate(
      Data   = as.Date(data_hora),
      Sensor = as.integer(Sensor)
    ) |>
    dplyr::filter(Sensor %in% sensores_validos) |>
    dplyr::filter(!is.na(.data[[spec$var]]), !is.na(Data))
  
  if (nrow(dados_p) == 0) return(NULL)
  
  n_dias <- dplyr::n_distinct(dados_p$Data)
  if (n_dias == 0) return(NULL)
  
  layout <- layout_por_dias(n_dias)
  
  dados_p <- dados_p |>
    dplyr::mutate(
      hora_dia = as.POSIXct(
        paste("1970-01-01", format(data_hora, "%H:%M:%S")),
        tz = ""
      ),
      Sensor_f = factor(as.character(Sensor), levels = sensor_levels)
    )
  
  # ---- BLINDAGEM DA FAIXA IDEAL (se não existir, não desenha)
  usar_faixa <- FALSE
  faixa_dias <- NULL
  
  if (spec$var == "T") {
    faixa <- faixas_ideais_t |> dplyr::filter(Periodo == nome_periodo)
    
    usar_faixa <- (
      nrow(faixa) == 1 &&
        is.finite(faixa$Tmin) &&
        is.finite(faixa$Tmax)
    )
    
    if (usar_faixa) {
      faixa_dias <- dados_p |>
        dplyr::group_by(Data) |>
        dplyr::summarise(
          xmin = min(hora_dia),
          xmax = max(hora_dia),
          .groups = "drop"
        ) |>
        dplyr::mutate(
          ymin = faixa$Tmin,
          ymax = faixa$Tmax
        )
    }
    
  } else if (spec$var == "UR") {
    faixa <- faixas_ideais_ur |> dplyr::filter(Periodo == nome_periodo)
    
    usar_faixa <- (
      nrow(faixa) == 1 &&
        is.finite(faixa$URmin) &&
        is.finite(faixa$URmax)
    )
    
    if (usar_faixa) {
      faixa_dias <- dados_p |>
        dplyr::group_by(Data) |>
        dplyr::summarise(
          xmin = min(hora_dia),
          xmax = max(hora_dia),
          .groups = "drop"
        ) |>
        dplyr::mutate(
          ymin = faixa$URmin,
          ymax = faixa$URmax
        )
    }
  }
  
  faixa_txt <- if (usar_faixa) {
    if (spec$var == "T") {
      glue(" | Faixa ideal: {faixa$Tmin}–{faixa$Tmax}")
    } else {
      glue(" | Faixa ideal: {faixa$URmin}–{faixa$URmax}")
    }
  } else {
    " | Faixa ideal: (não definida para este período)"
  }
  
  subtitle_txt <- glue(
    "Sensores: {sens_range$sensor_min}–{sens_range$sensor_max} | ",
    "Período: {p$inicio} até {p$fim}",
    faixa_txt
  )
  
  dias_seq <- seq(as.Date(p$inicio), as.Date(p$fim), by = "day")
  
  dados_p <- dados_p |>
    dplyr::mutate(
      Data = factor(as.Date(data_hora), levels = dias_seq)
    )
  
  if (!is.null(faixa_dias)) {
    faixa_dias <- faixa_dias |>
      dplyr::mutate(
        Data = factor(Data, levels = dias_seq)
      )
  }
  
  g <- ggplot(
    dados_p,
    aes(
      x = hora_dia,
      y = .data[[spec$var]],
      color = Sensor_f,
      group = Sensor_f
    )
  )
  
  if (usar_faixa) {
    g <- g +
      geom_rect(
        data = faixa_dias,
        aes(
          xmin = xmin,
          xmax = xmax,
          ymin = ymin,
          ymax = ymax
        ),
        fill = "springgreen3",
        alpha = 0.15,
        inherit.aes = FALSE
      )
  }
  
  g +
    geom_line(alpha = 0.85, linewidth = 0.55) +
    facet_wrap(~Data, ncol = layout$ncol, drop = FALSE) +
    scale_x_datetime(
      date_labels = "%H:%M",
      date_breaks = quebra_horas,
      limits = as.POSIXct(
        c("1970-01-01 00:00:00", "1970-01-01 23:59:59"),
        tz = ""
      )
    ) +
    scale_cor_sensor_padrao("Sensor") +
    labs(
      title = glue("{spec$titulo} — {nome_periodo}"),
      subtitle = subtitle_txt,
      x = "Hora",
      y = spec$ylab
    ) +
    theme_bw(base_size = 11)
}

plot_por_linha <- function(df, spec, lote_id, datas_eventos = NULL) {
  df2 <- df |> dplyr::filter(!is.na(.data[[spec$var]]), !is.na(Linha))
  if (nrow(df2) == 0) return(NULL)
  
  y_min <- min(df2[[spec$var]], na.rm = TRUE)
  y_max <- max(df2[[spec$var]], na.rm = TRUE)
  headroom <- (y_max - y_min) * 0.12
  if (!is.finite(headroom) || headroom <= 0) headroom <- 1
  y_label <- y_max + headroom
  
  g <- ggplot(df2, aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
    geom_line(linewidth = 0.55, alpha = 0.85) +
    facet_wrap(~Linha, ncol = 1) +
    scale_cor_sensor_padrao("Sensor") +
    labs(title = glue("Sensores por linha — {spec$titulo} — {lote_id}"), x = "Tempo", y = spec$ylab) +
    coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", hjust = 0.5),
      plot.margin = margin(35, 20, 10, 10)
    )
  
  if (!is.null(datas_eventos) && nrow(datas_eventos) > 0) {
    g <- g +
      geom_vline(
        xintercept = datas_eventos$data_hora,
        color = "red",
        linetype = "dashed",
        linewidth = 0.4
      ) +
      geom_text(
        data = datas_eventos,
        aes(x = data_hora, y = y_label, label = evento),
        angle = 90,
        vjust = 0,
        hjust = 0,
        size = 3,
        color = "red",
        inherit.aes = FALSE
      )
  }
  
  g
}

plot_por_coluna_5x2 <- function(df, spec, lote_id, datas_eventos = NULL) {
  df2 <- df |> dplyr::filter(!is.na(.data[[spec$var]]), !is.na(Coluna))
  if (nrow(df2) == 0) return(NULL)
  
  y_min <- min(df2[[spec$var]], na.rm = TRUE)
  y_max <- max(df2[[spec$var]], na.rm = TRUE)
  headroom <- (y_max - y_min) * 0.12
  if (!is.finite(headroom) || headroom <= 0) headroom <- 1
  y_label <- y_max + headroom
  
  g <- ggplot(df2, aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
    geom_line(linewidth = 0.45, alpha = 0.8) +
    facet_wrap(~Coluna, ncol = 2) +  # <- FIX: não usar free_y (padroniza comparação)
    scale_cor_sensor_padrao("Sensor") +
    labs(
      title = glue("Sensores por coluna — {spec$titulo} — {lote_id} (10 colunas | 5×2)"),
      x = "Tempo",
      y = spec$ylab
    ) +
    coord_cartesian(clip = "off") +
    theme_bw(base_size = 11) +
    theme(
      legend.position = "bottom",
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text = element_text(face = "bold"),
      plot.margin = margin(35, 20, 10, 10)
    )
  
  if (!is.null(datas_eventos) && nrow(datas_eventos) > 0) {
    g <- g +
      geom_vline(
        xintercept = datas_eventos$data_hora,
        color = "red",
        linetype = "dashed",
        linewidth = 0.35
      ) +
      geom_text(
        data = datas_eventos,
        aes(x = data_hora, y = y_label, label = evento),
        angle = 90,
        vjust = 0,
        hjust = 0,
        size = 2.6,
        color = "red",
        inherit.aes = FALSE
      )
  }
  
  g
}

salvar_por_coluna <- function(df, spec, lote_id, datas_eventos, dir_out) {
  colunas <- paste("Coluna", 1:10)
  
  pb <- progress::progress_bar$new(
    format = glue("{spec$nome} colunas [:bar] :current/:total (:percent) ETA: :eta"),
    total = length(colunas),
    clear = FALSE,
    width = 60
  )
  
  for (cc in colunas) {
    pb$tick()
    
    d <- df |> filter(Coluna == cc) |> filter(!is.na(.data[[spec$var]]))
    if (nrow(d) == 0) next
    
    sensores_cc <- sort(unique(as.character(d$Sensor)))
    
    y_min <- min(d[[spec$var]], na.rm = TRUE)
    y_max <- max(d[[spec$var]], na.rm = TRUE)
    headroom <- (y_max - y_min) * 0.12
    if (!is.finite(headroom) || headroom <= 0) headroom <- 1
    y_label <- y_max + headroom
    
    g <- ggplot(d, aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
      geom_line(linewidth = 0.6, alpha = 0.85) +
      scale_cor_sensor_subset(sensores_cc, "Sensor") +
      labs(title = glue("{cc} — {spec$titulo} — {lote_id}"), x = "Tempo", y = spec$ylab) +
      coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
      theme_minimal(base_size = 12) +
      theme(
        legend.position = "bottom",
        plot.title = element_text(face = "bold", hjust = 0.5),
        plot.margin = margin(35, 20, 10, 10)
      )
    
    if (!is.null(datas_eventos) && nrow(datas_eventos) > 0) {
      g <- g +
        geom_vline(
          xintercept = datas_eventos$data_hora,
          color = "red",
          linetype = "dashed",
          linewidth = 0.4
        ) +
        geom_text(
          data = datas_eventos,
          aes(x = data_hora, y = y_label, label = evento),
          angle = 90,
          vjust = 0,
          hjust = 0,
          size = 3,
          color = "red",
          inherit.aes = FALSE
        )
    }
    
    save_plot(
      g,
      file.path(dir_out, glue("03_coluna_{str_replace(cc, ' ', '_')}.png")),
      w = 16,
      h = 6
    )
  }
}

salvar_por_linha <- function(df, spec, lote_id, datas_eventos, dir_out) {
  linhas <- c("Linha 1", "Linha 2", "Linha 3")
  
  pb <- progress::progress_bar$new(
    format = glue("{spec$nome} linhas [:bar] :current/:total (:percent) ETA: :eta"),
    total = length(linhas),
    clear = FALSE,
    width = 60
  )
  
  for (ll in linhas) {
    pb$tick()
    
    d <- df |> filter(Linha == ll) |> filter(!is.na(.data[[spec$var]]))
    if (nrow(d) == 0) next
    
    sensores_ll <- sort(unique(as.character(d$Sensor)))
    
    y_min <- min(d[[spec$var]], na.rm = TRUE)
    y_max <- max(d[[spec$var]], na.rm = TRUE)
    headroom <- (y_max - y_min) * 0.12
    if (!is.finite(headroom) || headroom <= 0) headroom <- 1
    y_label <- y_max + headroom
    
    g <- ggplot(d, aes(x = data_hora, y = .data[[spec$var]], color = factor(Sensor), group = Sensor)) +
      geom_line(linewidth = 0.6, alpha = 0.85) +
      scale_cor_sensor_subset(sensores_ll, "Sensor") +
      labs(title = glue("{ll} — {spec$titulo} — {lote_id}"), x = "Tempo", y = spec$ylab) +
      coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
      theme_minimal(base_size = 12) +
      theme(
        legend.position = "bottom",
        plot.title = element_text(face = "bold", hjust = 0.5),
        plot.margin = margin(35, 20, 10, 10)
      )
    
    if (!is.null(datas_eventos) && nrow(datas_eventos) > 0) {
      g <- g +
        geom_vline(
          xintercept = datas_eventos$data_hora,
          color = "red",
          linetype = "dashed",
          linewidth = 0.4
        ) +
        geom_text(
          data = datas_eventos,
          aes(x = data_hora, y = y_label, label = evento),
          angle = 90,
          vjust = 0,
          hjust = 0,
          size = 3,
          color = "red",
          inherit.aes = FALSE
        )
    }
    
    save_plot(
      g,
      file.path(dir_out, glue("02_linha_{str_replace(ll, ' ', '_')}.png")),
      w = 16,
      h = 6
    )
  }
}

# ============================================================
# 8) EXECUTA ENTREGÁVEIS DE SÉRIE (T e/ou UR)
# ============================================================
rodar_entregaveis_serie <- function(df, var) {
  if (var == "UR") {
    if (!"UR" %in% names(df) || all(is.na(df$UR))) {
      message("⚠️ UR não disponível neste lote: pulando umidade.")
      return(invisible(FALSE))
    }
  }
  
  spec <- get_spec(var)
  
  dir_main        <- dir_result_lote(pasta_base, lote_id, spec$nome)
  dir_painel_var  <- dir_result_lote(pasta_base, lote_id, spec$nome, "painel_periodos")
  dir_colunas_var <- dir_result_lote(pasta_base, lote_id, spec$nome, "colunas_individuais")
  dir_mapas_var   <- dir_result_lote(pasta_base, lote_id, spec$nome, "mapas")
  dir_linhas_var  <- dir_result_lote(pasta_base, lote_id, spec$nome, "linhas_individuais")
  
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
  
  lista_periodos <- c(
    "VazioSanitario_Ini",
    "Alojamento", "Abertura1", "Abertura2", "Abertura3", "Abertura4",
    "Abertura5_8d", "Abertura5_15d",
    "VazioSanitario_Fim"
  )
  
  pb_pp <- progress::progress_bar$new(
    format = glue("{spec$nome} painéis [:bar] :current/:total (:percent) ETA: :eta"),
    total = length(lista_periodos),
    clear = FALSE,
    width = 60
  )
  
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
  
  plot_mapa_temporal_registros <- function(df, lote_id, bin = "10 min") {
    df_ok <- df |> filter(!is.na(data_hora), !is.na(.data[[spec$var]]))
    if (nrow(df_ok) == 0) return(NULL)
    
    df_bin <- df_ok |>
      mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
      filter(!is.na(t_bin)) |>
      count(Sensor, t_bin, name = "n_reg") |>
      mutate(Sensor_f = factor(as.character(Sensor), levels = sensor_levels))
    
    if (nrow(df_bin) == 0) return(NULL)
    
    ggplot(df_bin, aes(x = t_bin, y = Sensor_f, fill = n_reg)) +
      geom_tile(height = 0.8) +
      scale_fill_viridis_c(option = "C", trans = "sqrt") +
      labs(
        title = glue("Mapa temporal de registros por sensor — {spec$titulo}"),
        subtitle = glue("{lote_id} | bin = {bin}"),
        x = "Tempo",
        y = "Sensor",
        fill = "Registros"
      ) +
      theme_minimal(base_size = 12) +
      theme(axis.text.y = element_text(size = 8), plot.title = element_text(face = "bold"))
  }
  
  plot_mapa_temporal_presenca <- function(df, lote_id, bin = "10 min") {
    df_ok <- df |> filter(!is.na(data_hora), !is.na(.data[[spec$var]]))
    if (nrow(df_ok) == 0) return(NULL)
    
    base <- df_ok |>
      mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
      filter(!is.na(t_bin)) |>
      distinct(Sensor, t_bin) |>
      mutate(presente = 1L)
    
    if (nrow(base) == 0) return(NULL)
    
    tmin <- min(base$t_bin, na.rm = TRUE)
    tmax <- max(base$t_bin, na.rm = TRUE)
    if (!is.finite(as.numeric(tmin)) || !is.finite(as.numeric(tmax)) || tmin >= tmax) return(NULL)
    
    grade_t <- seq(from = tmin, to = tmax, by = bin)
    
    df_bin <- base |>
      tidyr::complete(
        Sensor = 1:30,
        t_bin  = grade_t,
        fill   = list(presente = 0L)
      ) |>
      mutate(Sensor_f = factor(as.character(Sensor), levels = sensor_levels))
    
    ggplot(df_bin, aes(x = t_bin, y = Sensor_f, fill = presente)) +
      geom_tile(height = 0.85) +
      scale_fill_viridis_c(option = "C", breaks = c(0, 1), labels = c("Sem dado", "Com dado")) +
      labs(
        title = glue("Mapa temporal de presença — {spec$titulo}"),
        subtitle = glue("{lote_id} | bin = {bin}"),
        x = "Tempo",
        y = "Sensor",
        fill = "Presença"
      ) +
      theme_minimal(base_size = 12) +
      theme(axis.text.y = element_text(size = 8), plot.title = element_text(face = "bold"))
  }
  
  df_map <- df
  if (!is.null(periodo_mapas) && length(periodo_mapas) == 2) {
    df_map <- df_map |> filter(data_hora >= periodo_mapas[1], data_hora <= periodo_mapas[2])
  }
  
  pb_map <- progress::progress_bar$new(
    format = glue("{spec$nome} mapas [:bar] :current/:total (:percent) ETA: :eta"),
    total = length(bins_map_min),
    clear = FALSE,
    width = 60
  )
  
  for (bm in bins_map_min) {
    pb_map$tick()
    if (bm < bin_minimo_mapas) next
    bin_str <- glue("{bm} min")
    
    g_mapa <- plot_mapa_temporal_registros(df_map, lote_id, bin = bin_str)
    if (!is.null(g_mapa)) {
      if (mostrar_graficos) print(g_mapa)
      save_plot(g_mapa, file.path(dir_mapas_var, glue("04_mapa_registros_{bm}min.png")), w = 18, h = 9)
    }
    
    g_mapa_pres <- plot_mapa_temporal_presenca(df_map, lote_id, bin = bin_str)
    if (!is.null(g_mapa_pres)) {
      if (mostrar_graficos) print(g_mapa_pres)
      save_plot(g_mapa_pres, file.path(dir_mapas_var, glue("04_mapa_presenca_{bm}min.png")), w = 18, h = 9)
    }
  }
  
  invisible(TRUE)
}

if (gerar_temperatura) rodar_entregaveis_serie(banco, "T")
if (gerar_umidade)     rodar_entregaveis_serie(banco, "UR")

# ============================================================
# 10) FALHAS — métricas para vários limites
# ============================================================
dir_tabs <- dir_result_lote(pasta_base, lote_id, "falhas", "tabelas")

calcular_falhas <- function(df, limite_min) {
  df |>
    arrange(Sensor, data_hora) |>
    group_by(Sensor) |>
    mutate(
      dif_min     = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      tem_falha   = if_else(!is.na(dif_min) & dif_min > limite_min, 1L, 0L),
      excesso_min = if_else(!is.na(dif_min) & dif_min > limite_min, dif_min - limite_min, 0)
    ) |>
    ungroup()
}

resultados_falhas <- list()

pb_falhas <- progress::progress_bar$new(
  format = "Falhas (tabelas) [:bar] :current/:total (:percent) ETA: :eta",
  total = length(limites_metricas),
  clear = FALSE,
  width = 60
)

for (lim in limites_metricas) {
  pb_falhas$tick()
  
  falhas_df <- calcular_falhas(banco, lim)
  
  falhas_por_sensor <- falhas_df |>
    dplyr::group_by(Sensor, Linha, Coluna) |>
    dplyr::summarise(
      total_falhas = sum(tem_falha, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      Lote = lote_id,
      Linha = readr::parse_number(as.character(Linha)),
      Coluna = readr::parse_number(as.character(Coluna))
    ) |>
    dplyr::relocate(Lote, .before = Sensor) |>
    dplyr::arrange(dplyr::desc(total_falhas))
  
  write_csv(falhas_por_sensor, file.path(dir_tabs, glue("falhas_por_sensor_{lim}min.csv")))
  write_rds(falhas_df,         file.path(dir_tabs, glue("falhas_brutas_{lim}min.rds")))
  
  resultados_falhas[[paste0("limite_", lim)]] <- falhas_por_sensor
}

message(
  "✅ Métricas de falha calculadas e salvas para limites: ",
  paste(limites_metricas, collapse = ", "),
  " min."
)
# ============================================================
# 10b) DIF_MIN + regra oficial da prof (5 min) + histogramas (A) + barras (B)
# - A) Histograma do dif_min por sensor (facet)
# - B) Barras das classes (<5, =5, >5) por sensor (facet)
# Salva em: resultados/<lote>/falhas/gap5/
# ============================================================

dir_falhas_gap5 <- dir_result_lote(pasta_base, lote_id, "falhas", "gap5")

calcular_gap5 <- function(df) {
  df |>
    dplyr::filter(!is.na(Sensor), !is.na(data_hora)) |>
    dplyr::arrange(Sensor, data_hora) |>
    dplyr::group_by(Sensor, Linha, Coluna) |>
    dplyr::mutate(
      # diferença entre leituras consecutivas (min)
      dif_min = as.numeric(difftime(data_hora, dplyr::lag(data_hora), units = "mins")),
      
      # regra da prof (5 min)
      classe_gap_5 = dplyr::case_when(
        is.na(dif_min) ~ NA_character_,
        dif_min < 5    ~ "falha_comunicacao(<5)",
        dplyr::near(dif_min, 5) ~ "ok(=5)",  
        dif_min > 5    ~ "falha_maior_que_5(>5)"
      )
    ) |>
    dplyr::ungroup()
}

gap5_df <- calcular_gap5(banco)

# -------------------------------
# 1) Tabela resumo por sensor/linha/coluna (quantos em cada classe)
# -------------------------------
tab_gap5 <- gap5_df |>
  dplyr::filter(!is.na(classe_gap_5)) |>
  dplyr::count(Sensor, Linha, Coluna, classe_gap_5, name = "n") |>
  dplyr::mutate(
    Linha = readr::parse_number(as.character(Linha)),
    Coluna = readr::parse_number(as.character(Coluna))
  ) |>
  dplyr::arrange(Sensor, classe_gap_5)

readr::write_csv(tab_gap5, file.path(dir_falhas_gap5, "tabela_gap5_por_sensor.csv"))
readr::write_rds(gap5_df,  file.path(dir_falhas_gap5, "gap5_bruto.rds"))

# -------------------------------
# 2) (A) Histograma de dif_min por sensor (facet)
# -------------------------------
df_hist <- gap5_df |>
  dplyr::filter(
    !is.na(dif_min),
    is.finite(dif_min),
    dif_min >= 0,
    dif_min <= 60
  ) |>
  dplyr::mutate(
    Sensor = factor(Sensor, levels = sort(unique(Sensor)))
  )

y_max <- max(df_cat$n, na.rm = TRUE) * 1.05

g_hist <- ggplot(df_cat, aes(x = classe_gap_5, y = n, fill = classe_gap_5)) +
  geom_col(width = 0.72, color = "black", linewidth = 0.2) +
  facet_wrap(~Sensor, ncol = 6, scales = "fixed") +
  scale_fill_manual(values = cores_gap5) +
  scale_x_discrete(labels = labels_gap5, expand = c(0.08, 0.08)) +
  scale_y_continuous(
    limits = c(0, y_max),
    expand = c(0, 0)
  ) +
  labs(
    title = paste("Classes de gap (regra dos 5 min) por sensor —", lote_id),
    subtitle = "Classificação: <5 = falha de comunicação | =5 = leitura ok | >5 = falha maior que 5 min",
    x = NULL,
    y = "Contagem"
  ) +
  theme_bw(base_size = 11) +
  theme(
    panel.grid = element_blank(),
    panel.background = element_rect(fill = "white", color = "black", linewidth = 0.3),
    strip.background = element_blank(),
    strip.text = element_text(face = "bold", size = 8),
    axis.text.x = ggtext::element_markdown(size = 7.5, angle = 35, hjust = 1),
    axis.text.y = element_text(size = 7),
    axis.title = element_text(face = "bold"),
    legend.position = "none",
    plot.title = element_text(face = "bold", hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5),
    panel.spacing = unit(0.35, "lines"),
    plot.margin = margin(8, 8, 8, 8)
  )

save_plot(
  g_hist,
  file.path(dir_falhas_gap5, "hist_dif_min_por_sensor.png"),
  w = 18,
  h = 12
)
# -------------------------------
# 3) (B) Barras por classe (<5, =5, >5) por sensor (facet)
# -------------------------------

# se quiser rótulos coloridos no eixo x, precisa do ggtext
# install.packages("ggtext")
library(ggtext)

df_cat <- gap5_df |>
  dplyr::filter(!is.na(classe_gap_5)) |>
  dplyr::count(Sensor, classe_gap_5, name = "n") |>
  dplyr::mutate(
    Sensor = factor(Sensor, levels = sort(unique(Sensor))),
    classe_gap_5 = factor(
      classe_gap_5,
      levels = c(
        "falha_comunicacao(<5)",
        "ok(=5)",
        "falha_maior_que_5(>5)"
      )
    )
  )

cores_gap5 <- c(
  "falha_comunicacao(<5)" = "#F07C73",
  "ok(=5)"                = "#00BA38",
  "falha_maior_que_5(>5)" = "#619CFF"
)

labels_gap5 <- c(
  "falha_comunicacao(<5)" = "<span style='color:#B24A45;'><b>&lt; 5 min</b></span>",
  "ok(=5)"                = "<span style='color:#007A26;'><b>= 5 min</b></span>",
  "falha_maior_que_5(>5)" = "<span style='color:#2F5FB3;'><b>&gt; 5 min</b></span>"
)

g_bar <- ggplot(df_cat, aes(x = classe_gap_5, y = n, fill = classe_gap_5)) +
  geom_col(width = 0.72, color = "black", linewidth = 0.2) +
  facet_wrap(~Sensor, ncol = 6, scales = "free_y") +
  scale_fill_manual(values = cores_gap5) +
  scale_x_discrete(labels = labels_gap5, expand = c(0.08, 0.08)) +
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.04))
  ) +
  labs(
    title = glue("Classes de gap (regra dos 5 min) por sensor — {lote_id}"),
    subtitle = "Classificação: <5 = falha de comunicação | =5 = leitura ok | >5 = falha maior que 5 min",
    x = NULL,
    y = "Contagem"
  ) +
  theme_bw(base_size = 11) +
  theme(
    panel.grid = element_blank(),
    panel.background = element_rect(fill = "white", color = "black", linewidth = 0.3),
    strip.background = element_blank(),
    strip.text = element_text(face = "bold", size = 8),
    axis.text.x = ggtext::element_markdown(size = 7.5, angle = 35, hjust = 1),
    axis.text.y = element_text(size = 7),
    axis.title = element_text(face = "bold"),
    legend.position = "none",
    plot.title = element_text(face = "bold", hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5),
    panel.spacing = unit(0.35, "lines"),
    plot.margin = margin(8, 8, 8, 8)
  )

save_plot(
  g_bar,
  file.path(dir_falhas_gap5, "freq_classes_gap5_por_sensor.png"),
  w = 18,
  h = 12
)

# ============================================================
# 15) FALHAS DE REGISTRO POR SENSOR (linha + pontos vermelhos)
# ============================================================
dir_falhas_sensores <- dir_result_lote(pasta_base, lote_id, "falhas", "por_sensor")

plot_falhas_sensor <- function(df, sensor_id, limite_min, lote_id, datas_eventos = NULL) {
  d <- df |>
    filter(Sensor == sensor_id) |>
    arrange(data_hora) |>
    mutate(
      dif_min   = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      tem_falha = !is.na(dif_min) & dif_min > limite_min
    )
  
  if (nrow(d) == 0) return(NULL)
  
  if (!"T" %in% names(d) || all(is.na(d$T))) return(NULL)
  
  y_min <- min(d$T, na.rm = TRUE)
  y_max <- max(d$T, na.rm = TRUE)
  headroom <- (y_max - y_min) * 0.12
  if (!is.finite(headroom) || headroom <= 0) headroom <- 1
  y_label <- y_max + headroom
  
  g <- ggplot(d, aes(x = data_hora, y = T)) +
    geom_line(color = "grey40", linewidth = 0.4) +
    geom_point(data = d |> filter(tem_falha), color = "red", size = 2) +
    labs(
      title    = glue("Falhas de registro — Sensor {sensor_id}"),
      subtitle = glue("{lote_id} | Intervalos > {limite_min} min sem dados"),
      x = "Tempo",
      y = "Temperatura (°C)"
    ) +
    coord_cartesian(ylim = c(y_min, y_max + headroom * 1.6), clip = "off") +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold"),
      plot.margin = margin(35, 20, 10, 10)
    )
  
  if (!is.null(datas_eventos) && nrow(datas_eventos) > 0) {
    g <- g +
      geom_vline(
        xintercept = datas_eventos$data_hora,
        color = "red",
        linetype = "dashed",
        linewidth = 0.4
      ) +
      geom_text(
        data = datas_eventos,
        aes(x = data_hora, y = y_label, label = evento),
        angle = 90,
        vjust = 0,
        hjust = 0,
        size = 3,
        color = "red",
        inherit.aes = FALSE
      )
  }
  
  g
}

sensores_todos <- sort(unique(banco$Sensor))

pb_falhas_plot <- progress::progress_bar$new(
  format = "Falhas por sensor [:bar] :current/:total (:percent) ETA: :eta",
  total = length(limites_plot_falhas) * length(sensores_todos),
  clear = FALSE,
  width = 60
)

for (lim in limites_plot_falhas) {
  for (sid in sensores_todos) {
    pb_falhas_plot$tick()
    
    g <- plot_falhas_sensor(
      banco,
      sensor_id     = sid,
      limite_min    = lim,
      lote_id       = lote_id,
      datas_eventos = datas_eventos
    )
    if (is.null(g)) next
    
    if (mostrar_graficos) print(g)
    save_plot(
      g,
      file.path(dir_falhas_sensores, glue("falhas_sensor_{sid}_{lim}min.png")),
      w = 16,
      h = 6
    )
  }
}

message("✅ Script finalizado COMPLETO para ", lote_id)