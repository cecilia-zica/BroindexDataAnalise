# ============================================================
# scripts/02_analises_e_graficos.R
# ÚNICO SCRIPT: métricas de falhas + gráficos de temperatura + painéis + paretos
# (SEM LEITURA: espera que data/tratados/{lote_id}/banco_periodos.rds exista)
# ============================================================

# -------------------------------
# 0) CONFIG RÁPIDA (você só mexe aqui)
# -------------------------------
pasta_base <- "C:/Users/LENOVO/Documents/BroindexData"
lote_id    <- "lote01"                 # <- troque para "lote02" etc.
limites    <- c(15, 30, 60)            # <- limites de falha em minutos

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

save_plot <- function(plot, path, w=12, h=7, dpi=300) {
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
  df %>%
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
# 5) CARREGA BANCO DO LOTE
# -------------------------------
banco <- carregar_banco(lote_id) %>%
  add_linha_coluna() %>%
  mutate(
    data_hora = as.POSIXct(data_hora),
    Data      = as.Date(data_hora),
    hora      = hour(data_hora)
  )

# -------------------------------
# 6) PASTAS DE SAÍDA (por lote)
# -------------------------------
dir_temp   <- dir_result_lote(lote_id, "temperatura")
dir_falhas <- dir_result_lote(lote_id, "falhas")
dir_pareto <- dir_result_lote(lote_id, "falhas", "pareto")
dir_tabs   <- dir_result_lote(lote_id, "tabelas")
dir_painel <- dir_result_lote(lote_id, "painel_periodos")

# ============================================================
# 7) TEMPERATURA — (A) visão geral (igual seu 03 antigo)
# ============================================================
datas_aberturas <- periodos %>% select(evento = Periodo, data_hora = inicio)
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
save_plot(g_temp_geral, file.path(dir_temp, "00_temperatura_geral.png"), w=12, h=7)

# ============================================================
# 8) TEMPERATURA — (B) TODOS OS DIAS (facet por dia)
# ============================================================
g_temp_por_dia <- ggplot(banco, aes(x = data_hora, y = T, color = factor(Sensor), group = Sensor)) +
  geom_line(linewidth = 0.5, alpha = 0.8) +
  facet_wrap(~ Data, ncol = 5, scales = "free_x") +
  labs(
    title = glue("Temperatura — todos os dias ({lote_id})"),
    x = "Tempo",
    y = "Temperatura (°C)"
  ) +
  theme_bw(base_size = 11) +
  theme(
    axis.text.x = element_blank(),
    axis.text.y = element_text(size = 5),
    panel.spacing = unit(0.4, "lines"),
    strip.text = element_text(size = 6, face = "bold", margin = margin(1, 0, 1, 0)),
    strip.background = element_rect(fill = "grey97", color = NA, linewidth = 0.1),
    legend.position = "none"
  )

print(g_temp_por_dia)
save_plot(g_temp_por_dia, file.path(dir_temp, "01_temperatura_por_dia.png"), w=16, h=9)

# ============================================================
# 9) PAINÉIS POR PERÍODO — (igual seu 05 antigo)
# ============================================================
dados <- banco  # mantendo o nome que você usava

plot_painel_periodo <- function(nome_periodo) {
  
  p <- periodos %>% filter(Periodo == nome_periodo)
  if (nrow(p) == 0) stop("Período não encontrado em `periodos`: ", nome_periodo)
  
  dados_p <- dados %>%
    filter(data_hora >= p$inicio, data_hora <= p$fim)
  
  faixa <- faixas_ideais %>% filter(Periodo == nome_periodo)
  
  medias_diarias <- dados_p %>%
    group_by(Data, hora = hour(data_hora)) %>%
    summarise(T_media_dia = mean(T, na.rm = TRUE), .groups = "drop") %>%
    mutate(data_hora = ymd_hms(paste(Data, hora, "00:00")))
  
  media_periodo <- dados_p %>%
    mutate(hora = hour(data_hora)) %>%
    group_by(hora) %>%
    summarise(T_media_periodo = mean(T, na.rm = TRUE), .groups = "drop") %>%
    mutate(data_hora = ymd_hms(paste(min(dados_p$Data), hora, "00:00")))
  
  n_dias <- length(unique(dados_p$Data))
  ncol_panel <- ifelse(n_dias <= 3, n_dias, 3)
  
  ggplot(dados_p, aes(x = data_hora, y = T, color = Sensor, group = Sensor)) +
    annotate("rect",
             xmin = min(dados_p$data_hora),
             xmax = max(dados_p$data_hora),
             ymin = faixa$Tmin,
             ymax = faixa$Tmax,
             fill = "springgreen3", alpha = 0.15) +
    geom_line(alpha = 0.5, linewidth = 0.4) +
    geom_line(data = medias_diarias,
              aes(x = data_hora, y = T_media_dia),
              color = "blue4", linewidth = 0.8) +
    geom_line(data = media_periodo,
              aes(x = data_hora, y = T_media_periodo),
              color = "black", linewidth = 1.1) +
    facet_wrap(~ Data, ncol = ncol_panel, scales = "free_x") +
    scale_x_datetime(date_labels = "%H:%M", date_breaks = "6 hours") +
    labs(
      title = paste("Temperatura —", nome_periodo),
      subtitle = paste("Dias:", p$inicio, "até", p$fim,
                       "| Faixa ideal =", faixa$Tmin, "a", faixa$Tmax, "°C"),
      x = "Hora",
      y = "Temperatura (°C)"
    ) +
    theme_bw(base_size = 11) +
    theme(
      axis.text.x = element_text(size = 6, angle = 45, hjust = 1),
      axis.text.y = element_text(size = 5),
      panel.spacing = unit(0.6, "lines"),
      strip.text = element_text(size = 7, face = "bold"),
      legend.position = "none",
      plot.title = element_text(size = 14, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 10, hjust = 0.5)
    )
}

lista_periodos <- c("Alojamento","Abertura1","Abertura2","Abertura3","Abertura4","Abertura5")

for (p in lista_periodos) {
  g <- plot_painel_periodo(p)
  print(g)
  save_plot(g, file.path(dir_painel, glue("painel_{p}.png")), w=12, h=7)
}

# ============================================================
# 10) FALHAS — métricas (tabelas + rds) para vários limites (seu 02 antigo)
# ============================================================
calcular_falhas <- function(df, limite_min) {
  df %>%
    arrange(Sensor, data_hora) %>%
    group_by(Sensor) %>%
    mutate(
      dif_min   = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      tem_falha = if_else(!is.na(dif_min) & dif_min > limite_min, 1L, 0L)
    ) %>%
    ungroup()
}

resultados_falhas <- list()

for (lim in limites) {
  
  falhas_df <- calcular_falhas(banco, lim)
  
  falhas_por_sensor <- falhas_df %>%
    group_by(Sensor, Linha, Coluna) %>%
    summarise(total_falhas = sum(tem_falha, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(total_falhas))
  
  write_csv(falhas_por_sensor, file.path(dir_tabs, glue("falhas_por_sensor_{lim}min.csv")))
  write_rds(falhas_df,       file.path(dir_tabs, glue("falhas_brutas_{lim}min.rds")))
  
  resultados_falhas[[paste0("limite_", lim)]] <- falhas_por_sensor
}

message("✅ Métricas de falha calculadas e salvas para limites: ", paste(limites, collapse = ", "), " min.")

# ============================================================
# 11) PARETOS — simples / por linha / por coluna (seu 04 antigo)
# ============================================================
gerar_paretos_limite <- function(limite_falha) {
  
  falhas_heatmap <- calcular_falhas(banco, limite_falha)
  
  falhas_por_sensor <- falhas_heatmap %>%
    group_by(Sensor) %>%
    summarise(total_falhas = sum(tem_falha, na.rm = TRUE), .groups = "drop")
  
  pareto_df <- falhas_por_sensor %>%
    arrange(desc(total_falhas)) %>%
    mutate(Sensor_f = factor(Sensor, levels = Sensor)) %>%
    left_join(banco %>% distinct(Sensor, Linha, Coluna), by = "Sensor")
  
  # ---- Pareto simples
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
  save_plot(g_pareto, file.path(dir_pareto, glue("pareto_{limite_falha}min_simples.png")), w=14, h=6)
  
  # ---- Pareto por LINHA
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
  save_plot(g_pareto_linha, file.path(dir_pareto, glue("pareto_{limite_falha}min_por_linha.png")), w=14, h=6)
  
  # ---- Pareto por COLUNA
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
  save_plot(g_pareto_coluna, file.path(dir_pareto, glue("pareto_{limite_falha}min_por_coluna.png")), w=14, h=6)
}

for (lf in limites) {
  gerar_paretos_limite(lf)
}

message("✅ Script finalizado: temperatura + painéis + falhas + paretos para ", lote_id)
