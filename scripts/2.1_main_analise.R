# ============================================================
# scripts/02_analises_e_graficos.R
# Pipeline completo de análises e gráficos
# ============================================================

# -------------------------------
# 0) CONFIGURAÇÕES
# -------------------------------
pasta_base <- "C:/Users/LENOVO/Documents/BroindexAnalise"
lote_id    <- "lote01"

limites_metricas     <- c(15, 30, 60)   # métricas + paretos
bins_map_min         <- c(5, 10, 60)    # mapas temporais
limites_plot_falhas  <- c(5, 10, 60)    # gráficos individuais
tz_local             <- "America/Sao_Paulo"

# -------------------------------
# 1) PACOTES
# -------------------------------
library(tidyverse)
library(lubridate)
library(readr)
library(glue)
library(viridis)

# -------------------------------
# 2) PERÍODOS E FAIXAS (PADRÃO FINAL)
# -------------------------------
periodos <- tibble(
  Periodo = c("Alojamento","Abertura1","Abertura2",
              "Abertura3","Abertura4","Abertura5"),
  inicio  = ymd_hm(c("2025-07-23 07:00","2025-07-26 07:00",
                     "2025-07-28 07:00","2025-07-30 07:00",
                     "2025-08-04 07:00","2025-08-11 07:00"), tz = tz_local),
  fim     = ymd_hm(c("2025-07-26 07:00","2025-07-28 07:00",
                     "2025-07-30 07:00","2025-08-04 07:00",
                     "2025-08-11 07:00","2025-09-03 07:00"), tz = tz_local)
)

faixas_ideais <- tibble(
  Periodo = periodos$Periodo,
  Tmin = c(32,32,32,29,26,23),
  Tmax = c(35,35,35,32,29,26)
)

# -------------------------------
# 3) FUNÇÕES DE DIRETÓRIO / SALVAR
# -------------------------------
dir_result_lote <- function(lote_id, ...) {
  d <- file.path(pasta_base, "resultados", lote_id, ...)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

save_plot <- function(plot, path, w = 12, h = 7, dpi = 300) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  ggsave(path, plot, width = w, height = h, dpi = dpi)
  message("✅ salvo: ", path)
}

# -------------------------------
# 4) CARREGAR BANCO + LINHA/COLUNA
# -------------------------------
carregar_banco <- function(lote_id) {
  read_rds(file.path(pasta_base, "data", "tratados", lote_id, "banco_periodos.rds"))
}

add_linha_coluna <- function(df) {
  df |>
    mutate(
      Sensor = as.integer(Sensor),
      Linha = case_when(
        Sensor %in% c(1,4,7,10,13,16,19,22,25,28) ~ "Linha 1",
        Sensor %in% c(2,5,8,11,14,17,20,23,26,29) ~ "Linha 2",
        Sensor %in% c(3,6,9,12,15,18,21,24,27,30) ~ "Linha 3"
      ),
      Coluna = paste("Coluna", ((Sensor - 1) %/% 3) + 1)
    )
}

# -------------------------------
# 5) BANCO FINAL
# -------------------------------
banco <- carregar_banco(lote_id) |>
  add_linha_coluna() |>
  mutate(
    data_hora = as.POSIXct(data_hora, tz = tz_local),
    Data = as.Date(data_hora),
    Linha = factor(Linha, levels = c("Linha 1","Linha 2","Linha 3")),
    Coluna = factor(Coluna, levels = paste("Coluna", 1:10))
  )

# -------------------------------
# 6) PALETAS CONSISTENTES
# -------------------------------
sensor_levels <- as.character(1:30)

scale_cor_sensor <- function() {
  scale_color_viridis_d(option = "D", limits = sensor_levels, drop = TRUE)
}

# ============================================================
# 7) TEMPERATURA — VISÃO GERAL
# ============================================================
datas_aberturas <- periodos |> select(evento = Periodo, data_hora = inicio)

g_temp_geral <- ggplot(banco, aes(data_hora, T, color = factor(Sensor))) +
  geom_line(alpha = 0.8, linewidth = 0.6) +
  geom_vline(data = datas_aberturas,
             aes(xintercept = data_hora),
             linetype = "dashed", color = "red") +
  scale_cor_sensor() +
  labs(
    title = glue("Temperatura por sensor — {lote_id}"),
    x = "Tempo", y = "Temperatura (°C)", color = "Sensor"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "bottom")

save_plot(g_temp_geral,
          file.path(dir_result_lote(lote_id, "temperatura"),
                    "00_temperatura_geral.png"))

# ============================================================
# 8) PAINÉIS POR PERÍODO (ALINHADOS POR HORA)
# ============================================================
sensores_periodo <- tibble(
  Periodo = periodos$Periodo,
  sensor_min = c(7,7,7,7,7,1),
  sensor_max = c(15,18,21,24,30,30)
)

plot_painel_periodo <- function(df, nome_periodo) {
  p <- periodos |> filter(Periodo == nome_periodo)
  faixa <- faixas_ideais |> filter(Periodo == nome_periodo)
  s <- sensores_periodo |> filter(Periodo == nome_periodo)
  
  dados <- df |>
    filter(data_hora >= p$inicio,
           data_hora <= p$fim,
           Sensor >= s$sensor_min,
           Sensor <= s$sensor_max) |>
    mutate(
      hora_dia = as.POSIXct(
        paste("1970-01-01", format(data_hora, "%H:%M:%S")),
        tz = tz_local
      )
    )
  
  ggplot(dados, aes(hora_dia, T, color = factor(Sensor))) +
    geom_rect(aes(xmin = min(hora_dia),
                  xmax = max(hora_dia),
                  ymin = faixa$Tmin,
                  ymax = faixa$Tmax),
              fill = "springgreen3", alpha = 0.15,
              inherit.aes = FALSE) +
    geom_line(alpha = 0.8, linewidth = 0.5) +
    facet_wrap(~ Data, scales = "free_x") +
    scale_x_datetime(date_labels = "%H:%M", date_breaks = "4 hours") +
    scale_cor_sensor() +
    labs(
      title = glue("Temperatura — {nome_periodo}"),
      subtitle = glue("Sensores {s$sensor_min}–{s$sensor_max} | Faixa {faixa$Tmin}–{faixa$Tmax} °C"),
      x = "Hora", y = "Temperatura (°C)"
    ) +
    theme_bw(base_size = 11) +
    theme(legend.position = "bottom")
}

for (pp in periodos$Periodo[-6]) {
  g <- plot_painel_periodo(banco, pp)
  save_plot(g,
            file.path(dir_result_lote(lote_id, "painel_periodos"),
                      glue("painel_{pp}.png")),
            w = 16, h = 9)
}

# ============================================================
# 9) FALHAS — FUNÇÃO BASE
# ============================================================
calcular_falhas <- function(df, limite) {
  df |>
    arrange(Sensor, data_hora) |>
    group_by(Sensor) |>
    mutate(
      dif_min = as.numeric(difftime(data_hora, lag(data_hora), units = "mins")),
      tem_falha = !is.na(dif_min) & dif_min > limite
    ) |>
    ungroup()
}

# ============================================================
# 10) FALHAS POR SENSOR — TODOS / TODOS OS LIMITES
# ============================================================
sensores_todos <- sort(unique(banco$Sensor))

for (lim in limites_plot_falhas) {
  
  dir_lim <- dir_result_lote(lote_id, "falhas", "por_sensor", glue("{lim}min"))
  
  for (sid in sensores_todos) {
    
    d <- calcular_falhas(banco, lim) |> filter(Sensor == sid)
    if (nrow(d) == 0) next
    
    g <- ggplot(d, aes(data_hora, T)) +
      geom_line(color = "grey40", linewidth = 0.4) +
      geom_point(data = d |> filter(tem_falha),
                 color = "red", size = 2) +
      geom_vline(data = datas_aberturas,
                 aes(xintercept = data_hora),
                 linetype = "dashed", color = "red") +
      labs(
        title = glue("Falhas de registro — Sensor {sid}"),
        subtitle = glue("{lote_id} | > {lim} min sem dados"),
        x = "Tempo", y = "Temperatura (°C)"
      ) +
      theme_minimal(base_size = 12)
    
    save_plot(g,
              file.path(dir_lim,
                        glue("falhas_sensor_{str_pad(sid,2,'0')}_{lim}min.png")),
              w = 16, h = 6)
  }
}

message("✅ SCRIPT FINAL EXECUTADO COM SUCESSO — ", lote_id)
