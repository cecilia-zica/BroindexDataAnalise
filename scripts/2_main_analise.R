# ============================================================
# scripts/02_analises_e_graficos.R
# ÚNICO SCRIPT: métricas de falhas + gráficos de temperatura + painéis + paretos
# + gráficos por linha/coluna + mapa temporal + falhas por sensor
# (SEM LEITURA: espera que data/tratados/{lote_id}/banco_periodos.rds exista)
# ============================================================

# -------------------------------
# 0) CONFIG RÁPIDA (você só mexe aqui)
# -------------------------------
pasta_base <- "C:/Users/LENOVO/Documents/BroindexAnalise"
lote_id    <- "lote01"                 # <- troque para "lote02" etc.
limites    <- c(15, 30, 60)            # <- limites de falha em minutos (usado em 10 e 11)

# ✅ usados APENAS nos blocos 14 e 15 (como combinamos)
bins_map_min        <- c(5, 10, 60)    # bins do mapa (min)
limites_plot_falhas <- c(5, 10, 60)    # limites p/ gráficos de falhas por sensor

# -------------------------------
# 1) PACOTES
# -------------------------------
library(tidyverse)
library(lubridate)
library(readr)
library(glue)
library(viridis)

# -------------------------------
# 2) TABELAS GLOBAIS (períodos / faixas)
# -------------------------------
periodos <- tibble(
  Periodo = c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4","Abertura5"),
  inicio  = ymd_hm(c("2025-07-21 10:10","2025-07-23 07:00","2025-07-26 07:00",
                     "2025-07-28 07:00","2025-07-30 07:00","2025-08-04 07:00")),
  fim     = ymd_hm(c("2025-07-23 07:00","2025-07-26 07:00","2025-07-28 07:00",
                     "2025-07-30 07:00","2025-08-04 07:00","2025-09-03 00:00"))
)

faixas_ideais <- tibble(
  Periodo = c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4","Abertura5"),
  Tmin = c(32,32,32,29,26,23),
  Tmax = c(35,35,35,32,29,26)
)

# -------------------------------
# 3) FUNÇÕES DE PASTA / SALVAR
# -------------------------------
pasta_tratados <- file.path(pasta_base, "data", "tratados")
pasta_results  <- file.path(pasta_base, "resultados")

dir_tratado_lote <- function(lote_id) {
  file.path(pasta_tratados, lote_id)
}

dir_result_lote <- function(lote_id, ...) {
  d <- file.path(pasta_results, lote_id, ...)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

save_plot <- function(plot, path, w = 12, h = 7, dpi = 300) {
  ggsave(filename = path, plot = plot, width = w, height = h, dpi = dpi)
  message("✅ salvo: ", path)
}

# -------------------------------
# 4) FUNÇÕES DE DADOS (carregar + linha/coluna)
# -------------------------------
carregar_banco <- function(lote_id) {
  arq <- file.path(dir_tratado_lote(lote_id), "banco_periodos.rds")
  if (!file.exists(arq)) {
    stop("Não achei o banco tratado: ", arq,
         "\nRode antes o script de leitura/tratamento para esse lote.")
  }
  readr::read_rds(arq)
}

add_linha_coluna <- function(df) {
  df |>
    mutate(
      Sensor = as.integer(Sensor),
      Linha = case_when(
        Sensor %in% c(1,4,7,10,13,16,19,22,25,28) ~ "Linha 1",
        Sensor %in% c(2,5,8,11,14,17,20,23,26,29) ~ "Linha 2",
        Sensor %in% c(3,6,9,12,15,18,21,24,27,30) ~ "Linha 3",
        TRUE ~ NA_character_
      ),
      Coluna = case_when(
        Sensor %in% c(1,2,3) ~ "Coluna 1",
        Sensor %in% c(4,5,6) ~ "Coluna 2",
        Sensor %in% c(7,8,9) ~ "Coluna 3",
        Sensor %in% c(10,11,12) ~ "Coluna 4",
        Sensor %in% c(13,14,15) ~ "Coluna 5",
        Sensor %in% c(16,17,18) ~ "Coluna 6",
        Sensor %in% c(19,20,21) ~ "Coluna 7",
        Sensor %in% c(22,23,24) ~ "Coluna 8",
        Sensor %in% c(25,26,27) ~ "Coluna 9",
        Sensor %in% c(28,29,30) ~ "Coluna 10",
        TRUE ~ NA_character_
      )
    )
}

# -------------------------------
# 4.1) PADRÃO DE CORES (para sensores) — usado em 9, 12 e 13
#     (mais “colorido” e consistente)
# -------------------------------
sensor_levels <- as.character(1:30)
sensor_palette <- setNames(scales::hue_pal(l = 60, c = 100)(length(sensor_levels)), sensor_levels)

scale_cor_sensor_padrao <- function(legend_title = "Sensor") {
  scale_color_manual(values = sensor_palette, breaks = sensor_levels, drop = TRUE, name = legend_title)
}

# -------------------------------
# 5) CARREGA BANCO DO LOTE (⚠️ UMA ÚNICA VEZ!)
# -------------------------------
banco <- carregar_banco(lote_id) |>
  add_linha_coluna() |>
  mutate(
    data_hora = as.POSIXct(data_hora),
    Data      = as.Date(data_hora),
    hora      = lubridate::hour(data_hora),
    Linha     = factor(Linha, levels = c("Linha 1","Linha 2","Linha 3")),
    Coluna    = factor(Coluna, levels = paste("Coluna", 1:10))
  )

# -------------------------------
# 6) PASTAS DE SAÍDA (por lote)
# -------------------------------
dir_temp     <- dir_result_lote(lote_id, "temperatura")
dir_falhas   <- dir_result_lote(lote_id, "falhas")
dir_pareto   <- dir_result_lote(lote_id, "falhas", "pareto")
dir_tabs     <- dir_result_lote(lote_id, "tabelas")
dir_painel   <- dir_result_lote(lote_id, "painel_periodos")
dir_colunas  <- dir_result_lote(lote_id, "temperatura", "colunas_individuais")
dir_mapas    <- dir_result_lote(lote_id, "temperatura", "mapas")
dir_falhas_sensores <- dir_result_lote(lote_id, "falhas", "por_sensor")

# ============================================================
# 7) TEMPERATURA — (A) visão geral
# ============================================================
datas_aberturas <- periodos |> select(evento = Periodo, data_hora = inicio)
ylim_max <- max(banco$T, na.rm = TRUE) + 1

g_temp_geral <- ggplot(banco, aes(data_hora, T, color = factor(Sensor))) +
  geom_line(linewidth = 0.7, alpha = 0.8) +
  geom_vline(
    data = datas_aberturas,
    aes(xintercept = data_hora),
    color = "red", linetype = "dashed", linewidth = 0.5
  ) +
  geom_text(
    data = datas_aberturas,
    aes(x = data_hora, y = ylim_max + 1, label = evento),
    angle = 90, vjust = 0, hjust = 0, size = 3, color = "black"
  ) +
  coord_cartesian(
    ylim = c(min(banco$T, na.rm = TRUE), ylim_max + 1.5),
    clip = "off"
  ) +
  labs(
    title = glue("Temperatura por sensor – visão geral ({lote_id})"),
    x = "Tempo",
    y = "Temperatura (°C)",
    color = "Sensor"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position = "bottom",
    plot.margin = margin(10, 30, 10, 10),
    plot.title = element_text(face = "bold", hjust = 0.5)
  )

print(g_temp_geral)
save_plot(g_temp_geral, file.path(dir_temp, "00_temperatura_geral.png"), w = 12, h = 7)

# ============================================================
# 8) TEMPERATURA — (B) TODOS OS DIAS (facet por dia)
# ============================================================
g_temp_por_dia <- ggplot(
  banco,
  aes(x = data_hora, y = T, color = factor(Sensor), group = Sensor)
) +
  geom_line(linewidth = 0.5, alpha = 0.8) +
  facet_wrap(~ Data, ncol = 5, scales = "free_x") +
  scale_x_datetime(date_labels = "%H:%M", date_breaks = "4 hours") +
  labs(
    title = glue("Temperatura — todos os dias ({lote_id})"),
    x = "Hora",
    y = "Temperatura (°C)"
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

print(g_temp_por_dia)
save_plot(g_temp_por_dia, file.path(dir_temp, "01_temperatura_por_dia.png"), w = 16, h = 9)

# ============================================================
# 9) PAINÉIS POR PERÍODO — alinhado por hora (sensores filtrados)
#     AJUSTES APLICADOS:
#       - cores mais "coloridas" e consistentes (scale_cor_sensor_padrao)
#       - faixa cinza (strip) menor + texto menor
# ============================================================
layout_por_dias <- function(n_dias) {
  if (n_dias <= 1) return(list(ncol = 1))
  if (n_dias == 2) return(list(ncol = 2))
  if (n_dias == 3) return(list(ncol = 3))
  if (n_dias == 4) return(list(ncol = 2))
  if (n_dias <= 6) return(list(ncol = 3))
  list(ncol = 4)
}

sensores_periodo <- tibble(
  Periodo = c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4","Abertura5"),
  sensor_min = c(7, 7, 7, 7, 7, 1),
  sensor_max = c(15, 18, 21, 24, 30, 30)
)

plot_painel_periodo <- function(df, nome_periodo, quebra_horas = "4 hours") {
  p <- periodos |> filter(Periodo == nome_periodo)
  if (nrow(p) == 0) stop("Período não encontrado: ", nome_periodo)
  
  faixa <- faixas_ideais |> filter(Periodo == nome_periodo)
  if (nrow(faixa) == 0) stop("Faixa ideal não encontrada: ", nome_periodo)
  
  sens_range <- sensores_periodo |> filter(Periodo == nome_periodo)
  if (nrow(sens_range) == 0) stop("Regra de sensores não encontrada: ", nome_periodo)
  sensores_validos <- sens_range$sensor_min : sens_range$sensor_max
  
  dados_p <- df |>
    filter(data_hora >= p$inicio, data_hora <= p$fim) |>
    mutate(
      Data = as.Date(data_hora),
      Sensor = as.integer(Sensor)
    ) |>
    filter(Sensor %in% sensores_validos)
  
  if (nrow(dados_p) == 0) return(NULL)
  
  n_dias <- length(unique(dados_p$Data))
  layout <- layout_por_dias(n_dias)
  
  dados_p <- dados_p |>
    mutate(
      hora_dia = as.POSIXct(paste("1970-01-01", format(data_hora, "%H:%M:%S")), tz = ""),
      Sensor_f = factor(as.character(Sensor), levels = sensor_levels)
    )
  
  faixa_dias <- dados_p |>
    group_by(Data) |>
    summarise(
      xmin = min(hora_dia),
      xmax = max(hora_dia),
      .groups = "drop"
    ) |>
    mutate(
      ymin = faixa$Tmin,
      ymax = faixa$Tmax
    )
  
  ggplot(dados_p, aes(x = hora_dia, y = T, color = Sensor_f, group = Sensor_f)) +
    geom_rect(
      data = faixa_dias,
      aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
      fill = "springgreen3", alpha = 0.15,
      inherit.aes = FALSE
    ) +
    geom_line(alpha = 0.85, linewidth = 0.55) +
    facet_wrap(~ Data, ncol = layout$ncol) +
    scale_x_datetime(
      date_labels = "%H:%M",
      date_breaks = quebra_horas,
      limits = as.POSIXct(c("1970-01-01 00:00:00", "1970-01-01 23:59:59"), tz = "")
    ) +
    scale_cor_sensor_padrao("Sensor") +
    labs(
      title = glue("Temperatura — {nome_periodo}"),
      subtitle = glue(
        "Sensores: {sens_range$sensor_min}–{sens_range$sensor_max} | ",
        "Período: {p$inicio} até {p$fim} | Faixa ideal: {faixa$Tmin}–{faixa$Tmax} °C"
      ),
      x = "Hora",
      y = "Temperatura (°C)"
    ) +
    theme_bw(base_size = 11) +
    theme(
      axis.text.x = element_text(size = 7, angle = 45, hjust = 1),
      axis.text.y = element_text(size = 7),
      panel.spacing = unit(0.6, "lines"),
      
      # ✅ faixa cinza menor
      strip.text = element_text(size = 6, face = "bold", margin = margin(0.5, 0, 0.5, 0)),
      strip.background = element_rect(fill = "grey92", color = "grey75", linewidth = 0.25),
      
      legend.position = "bottom",
      legend.text = element_text(size = 8),
      legend.key.height = unit(0.25, "cm"),
      legend.key.width  = unit(0.55, "cm"),
      plot.title = element_text(size = 14, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 10, hjust = 0.5)
    )
}

lista_periodos <- c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4")
for (pp in lista_periodos) {
  g <- plot_painel_periodo(banco, pp)
  if (!is.null(g)) {
    print(g)
    save_plot(g, file.path(dir_painel, glue("painel_{pp}.png")), w = 16, h = 9)
  }
}

# ============================================================
# 10) FALHAS — métricas (tabelas + rds) para vários limites
# ============================================================
calcular_falhas <- function(df, limite_min) {
  df |>
    arrange(Sensor, data_hora) |>
    group_by(Sensor) |>
    mutate(
      dif_min   = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      tem_falha = if_else(!is.na(dif_min) & dif_min > limite_min, 1L, 0L)
    ) |>
    ungroup()
}

resultados_falhas <- list()

for (lim in limites) {
  falhas_df <- calcular_falhas(banco, lim)
  
  falhas_por_sensor <- falhas_df |>
    group_by(Sensor, Linha, Coluna) |>
    summarise(total_falhas = sum(tem_falha, na.rm = TRUE), .groups = "drop") |>
    arrange(desc(total_falhas))
  
  write_csv(falhas_por_sensor, file.path(dir_tabs, glue("falhas_por_sensor_{lim}min.csv")))
  write_rds(falhas_df,       file.path(dir_tabs, glue("falhas_brutas_{lim}min.rds")))
  
  resultados_falhas[[paste0("limite_", lim)]] <- falhas_por_sensor
}

message("✅ Métricas de falha calculadas e salvas para limites: ", paste(limites, collapse = ", "), " min.")

# ============================================================
# 11) PARETOS — simples / por linha / por coluna
# ============================================================
gerar_paretos_limite <- function(limite_falha) {
  falhas_heatmap <- calcular_falhas(banco, limite_falha)
  
  falhas_por_sensor <- falhas_heatmap |>
    group_by(Sensor) |>
    summarise(total_falhas = sum(tem_falha, na.rm = TRUE), .groups = "drop")
  
  pareto_df <- falhas_por_sensor |>
    arrange(desc(total_falhas)) |>
    mutate(Sensor_f = factor(Sensor, levels = Sensor)) |>
    left_join(banco |> distinct(Sensor, Linha, Coluna), by = "Sensor")
  
  g_pareto <- ggplot(pareto_df, aes(x = Sensor_f, y = total_falhas)) +
    geom_col(fill = "#4C72B0", alpha = 0.9) +
    geom_text(aes(label = total_falhas), vjust = -0.3, size = 3) +
    scale_y_continuous(
      name   = glue("Número de falhas (> {limite_falha} min)"),
      expand = expansion(mult = c(0, 0.05))
    ) +
    labs(
      title = glue("Pareto — falhas por sensor (limite > {limite_falha} min) — {lote_id}"),
      x = "Sensor",
      y = "Número de falhas"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title  = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  print(g_pareto)
  save_plot(g_pareto, file.path(dir_pareto, glue("pareto_{limite_falha}min_simples.png")), w = 14, h = 6)
  
  g_pareto_linha <- ggplot(pareto_df, aes(x = Sensor_f, y = total_falhas, fill = Linha)) +
    geom_col(alpha = 0.9) +
    geom_text(aes(label = total_falhas), vjust = -0.3, size = 3) +
    scale_y_continuous(
      name   = glue("Número de falhas (> {limite_falha} min)"),
      expand = expansion(mult = c(0, 0.05))
    ) +
    scale_fill_viridis_d(option = "C") +
    labs(
      title = glue("Pareto — por LINHA (>{limite_falha} min) — {lote_id}"),
      x = "Sensor",
      y = "Número de falhas",
      fill = "Linha"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title  = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  print(g_pareto_linha)
  save_plot(g_pareto_linha, file.path(dir_pareto, glue("pareto_{limite_falha}min_por_linha.png")), w = 14, h = 6)
  
  g_pareto_coluna <- ggplot(pareto_df, aes(x = Sensor_f, y = total_falhas, fill = Coluna)) +
    geom_col(alpha = 0.9) +
    geom_text(aes(label = total_falhas), vjust = -0.3, size = 3) +
    scale_y_continuous(
      name   = glue("Número de falhas (> {limite_falha} min)"),
      expand = expansion(mult = c(0, 0.05))
    ) +
    scale_fill_viridis_d(option = "D") +
    labs(
      title = glue("Pareto — por COLUNA (>{limite_falha} min) — {lote_id}"),
      x = "Sensor",
      y = "Número de falhas",
      fill = "Coluna"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title  = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  print(g_pareto_coluna)
  save_plot(g_pareto_coluna, file.path(dir_pareto, glue("pareto_{limite_falha}min_por_coluna.png")), w = 14, h = 6)
}

for (lf in limites) {
  gerar_paretos_limite(lf)
}

# ============================================================
# 12) GRÁFICO POR LINHA (RECUPERADO)
#     AJUSTES APLICADOS:
#       - cores padronizadas (mais coloridas)
#       - eventos descritos (geom_text)
# ============================================================
plot_temp_por_linha <- function(df, lote_id, datas_aberturas = NULL) {
  
  y_max <- max(df$T, na.rm = TRUE)
  
  g <- ggplot(df, aes(x = data_hora, y = T, color = factor(Sensor), group = Sensor)) +
    geom_line(linewidth = 0.55, alpha = 0.85) +
    facet_wrap(~ Linha, ncol = 1) +
    scale_cor_sensor_padrao("Sensor") +
    labs(
      title = glue("Sensores por linha — {lote_id}"),
      x = "Tempo",
      y = "Temperatura (°C)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold", hjust = 0.5)
    )
  
  if (!is.null(datas_aberturas)) {
    g <- g +
      geom_vline(
        data = datas_aberturas,
        aes(xintercept = data_hora),
        color = "red",
        linetype = "dashed",
        linewidth = 0.4
      ) +
      geom_text(
        data = datas_aberturas,
        aes(x = data_hora, y = y_max + 0.6, label = evento),
        angle = 90, vjust = -0.3, hjust = 0, size = 3, color = "red"
      )
  }
  
  g
}

g_linha <- plot_temp_por_linha(banco, lote_id, datas_aberturas)
print(g_linha)
save_plot(g_linha, file.path(dir_temp, "02_temperatura_por_linha.png"), w = 16, h = 7)

# ============================================================
# 13) GRÁFICO POR COLUNA (100% ARRUMADO)
#     AJUSTES APLICADOS:
#       - cores padronizadas (mais coloridas)
#       - layout 5x2 (ncol = 2) no gráfico conjunto
# ============================================================
plot_temp_por_coluna_5x2 <- function(df, lote_id, datas_aberturas = NULL) {
  
  y_max <- max(df$T, na.rm = TRUE)
  
  g <- ggplot(df, aes(x = data_hora, y = T, color = factor(Sensor), group = Sensor)) +
    geom_line(linewidth = 0.45, alpha = 0.8) +
    facet_wrap(~ Coluna, ncol = 2, scales = "free_y") +
    scale_cor_sensor_padrao("Sensor") +
    labs(
      title = glue("Sensores por coluna — {lote_id} (10 colunas | 5×2)"),
      x = "Tempo",
      y = "Temperatura (°C)"
    ) +
    theme_bw(base_size = 11) +
    theme(
      legend.position = "bottom",
      axis.text.x = element_text(angle = 45, hjust = 1),
      strip.text = element_text(face = "bold")
    )
  
  if (!is.null(datas_aberturas)) {
    g <- g +
      geom_vline(
        data = datas_aberturas,
        aes(xintercept = data_hora),
        color = "red",
        linetype = "dashed",
        linewidth = 0.35
      ) +
      geom_text(
        data = datas_aberturas,
        aes(x = data_hora, y = y_max + 0.6, label = evento),
        angle = 90, vjust = -0.3, hjust = 0, size = 2.6, color = "red"
      )
  }
  
  g
}

g_coluna_5x2 <- plot_temp_por_coluna_5x2(banco, lote_id, datas_aberturas)
print(g_coluna_5x2)
save_plot(g_coluna_5x2, file.path(dir_temp, "03_temperatura_por_coluna_5x2.png"), w = 18, h = 12)

salvar_por_coluna <- function(df, lote_id, datas_aberturas, dir_out) {
  colunas <- paste("Coluna", 1:10)
  
  for (cc in colunas) {
    d <- df |> filter(Coluna == cc)
    if (nrow(d) == 0) next
    
    y_max <- max(d$T, na.rm = TRUE)
    
    g <- ggplot(d, aes(x = data_hora, y = T, color = factor(Sensor), group = Sensor)) +
      geom_line(linewidth = 0.6, alpha = 0.85) +
      scale_cor_sensor_padrao("Sensor") +
      labs(
        title = glue("{cc} — {lote_id}"),
        x = "Tempo",
        y = "Temperatura (°C)"
      ) +
      theme_minimal(base_size = 12) +
      theme(
        legend.position = "bottom",
        plot.title = element_text(face = "bold", hjust = 0.5)
      )
    
    if (!is.null(datas_aberturas)) {
      g <- g +
        geom_vline(
          data = datas_aberturas,
          aes(xintercept = data_hora),
          color = "red",
          linetype = "dashed",
          linewidth = 0.4
        ) +
        geom_text(
          data = datas_aberturas,
          aes(x = data_hora, y = y_max + 0.6, label = evento),
          angle = 90, vjust = -0.3, hjust = 0, size = 3, color = "red"
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

salvar_por_coluna(banco, lote_id, datas_aberturas, dir_colunas)

# ============================================================
# 14) MAPA TEMPORAL DE REGISTROS POR SENSOR
#     AJUSTES APLICADOS:
#       - gera A) densidade e B) presença para 5, 10, 60 min (bins_map_min)
# ============================================================
plot_mapa_temporal_registros <- function(df, lote_id, bin = "10 min") {
  df_bin <- df |>
    mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
    count(Sensor, t_bin, name = "n_reg") |>
    mutate(Sensor_f = factor(Sensor, levels = sort(unique(Sensor))))
  
  ggplot(df_bin, aes(x = t_bin, y = Sensor_f, fill = n_reg)) +
    geom_tile(height = 0.8) +
    scale_fill_viridis_c(option = "C", trans = "sqrt") +
    labs(
      title = "Mapa temporal de registros por sensor",
      subtitle = glue("{lote_id} | bin = {bin}"),
      x = "Tempo",
      y = "Sensor",
      fill = "Registros"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.y = element_text(size = 8),
      plot.title = element_text(face = "bold")
    )
}

plot_mapa_temporal_presenca <- function(df, lote_id, bin = "10 min") {
  df_bin <- df |>
    mutate(t_bin = lubridate::floor_date(data_hora, unit = bin)) |>
    distinct(Sensor, t_bin) |>
    mutate(presente = 1L) |>
    complete(
      Sensor = 1:30,
      t_bin = seq(min(t_bin, na.rm = TRUE), max(t_bin, na.rm = TRUE), by = bin),
      fill = list(presente = 0L)
    ) |>
    mutate(Sensor_f = factor(as.character(Sensor), levels = sensor_levels))
  
  ggplot(df_bin, aes(x = t_bin, y = Sensor_f, fill = presente)) +
    geom_tile(height = 0.85) +
    scale_fill_viridis_c(option = "C", breaks = c(0, 1), labels = c("Sem dado", "Com dado")) +
    labs(
      title = "Mapa temporal de registros por sensor",
      subtitle = glue("{lote_id} | bin = {bin}"),
      x = "Tempo",
      y = "Sensor",
      fill = "Presença"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.y = element_text(size = 8),
      plot.title = element_text(face = "bold")
    )
}

for (bm in bins_map_min) {
  bin_str <- glue("{bm} min")
  
  g_mapa <- plot_mapa_temporal_registros(banco, lote_id, bin = bin_str)
  print(g_mapa)
  save_plot(g_mapa, file.path(dir_mapas, glue("04_mapa_registros_{bm}min.png")), w = 18, h = 9)
  
  g_mapa_pres <- plot_mapa_temporal_presenca(banco, lote_id, bin = bin_str)
  print(g_mapa_pres)
  save_plot(g_mapa_pres, file.path(dir_mapas, glue("04_mapa_presenca_{bm}min.png")), w = 18, h = 9)
}

# ============================================================
# 15) FALHAS DE REGISTRO POR SENSOR (linha + pontos vermelhos)
#     AJUSTES APLICADOS:
#       - gera para TODOS os sensores e para 5, 10, 60 min (limites_plot_falhas)
#       - eventos descritos (linhas + texto)
# ============================================================
plot_falhas_sensor <- function(df, sensor_id, limite_min, lote_id, datas_aberturas = NULL) {
  d <- df |>
    filter(Sensor == sensor_id) |>
    arrange(data_hora) |>
    mutate(
      dif_min = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      tem_falha = !is.na(dif_min) & dif_min > limite_min
    )
  
  if (nrow(d) == 0) return(NULL)
  
  y_max <- max(d$T, na.rm = TRUE)
  
  g <- ggplot(d, aes(x = data_hora, y = T)) +
    geom_line(color = "grey40", linewidth = 0.4) +
    geom_point(data = d |> filter(tem_falha), color = "red", size = 2) +
    labs(
      title = glue("Falhas de registro — Sensor {sensor_id}"),
      subtitle = glue("{lote_id} | Intervalos > {limite_min} min sem dados"),
      x = "Tempo",
      y = "Temperatura (°C)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
  
  if (!is.null(datas_aberturas)) {
    g <- g +
      geom_vline(
        data = datas_aberturas,
        aes(xintercept = data_hora),
        color = "red",
        linetype = "dashed",
        linewidth = 0.4
      ) +
      geom_text(
        data = datas_aberturas,
        aes(x = data_hora, y = y_max + 0.5, label = evento),
        angle = 90, vjust = -0.3, hjust = 0, size = 3, color = "red"
      )
  }
  
  g
}

sensores_todos <- sort(unique(banco$Sensor))

for (lim in limites_plot_falhas) {
  for (sid in sensores_todos) {
    g <- plot_falhas_sensor(banco, sensor_id = sid, limite_min = lim, lote_id = lote_id, datas_aberturas = datas_aberturas)
    if (is.null(g)) next
    
    print(g)
    save_plot(
      g,
      file.path(dir_falhas_sensores, glue("falhas_sensor_{sid}_{lim}min.png")),
      w = 16,
      h = 6
    )
  }
}

message("✅ Script finalizado COMPLETO para ", lote_id)
