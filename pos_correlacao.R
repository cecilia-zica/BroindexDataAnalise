# ============================================================
# scripts/post_correlacao_visuals_v5_scores_GREEDY_RECALC.R
# Pós-processamento (SEM rerodar pipeline):
#  - Dendrograma BONITO (labels + barra + legenda fora)
#  - Heatmap correlação ORDENADO pelo dendrograma
#  - Correlação vs distância física
#  - + Greedy recalculado do ZERO a partir do WIDE (controlado)
#      -> minimiza RMSE da reconstrução da média global a cada passo
#  - + Curva RMSE vs k (monótona) + elbow (curvatura) + k90
#  - + Score final por objetivo (CONTROLE vs CUSTO)
#  - + Rankings + plots + recomendações (k_elbow e k90)
# ============================================================

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
# 0) CONFIG
# ----------------------------
pasta_base <- "C:/Users/LENOVO/Documents/BroindexAnalise"
lote_id    <- "lote1"
periodo    <- "VazioSanitario_Ini"   # <-- TROQUE AQUI
var_ml     <- "T"                    # "T" ou "UR"

# greedy controlado: máximo de sensores selecionados
kmax_greedy <- 10

# bin_tag: se souber, defina (ex: "5min").
# se deixar NULL, o script auto-detecta o wide mais recente.
bin_tag    <- NULL

dir_pp <- file.path(
  pasta_base, "resultados", lote_id, "falhas", "maior_gap",
  paste0("periodo_", periodo)
)
if (!dir.exists(dir_pp)) stop("Não achei a pasta do período: ", dir_pp)

# ----------------------------
# 1) Pacotes para dendrograma
# ----------------------------
if (!requireNamespace("dendextend", quietly = TRUE)) install.packages("dendextend")
library(dendextend)

# ----------------------------
# 2) Utilidades: escrita robusta
# ----------------------------
safe_write_csv <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  tryCatch({
    readr::write_csv(df, path)
    message("✅ salvo: ", path)
  }, error = function(e) {
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.csv$", paste0("_ALT_", ts, ".csv"), path)
    readr::write_csv(df, path2)
    message("⚠️ não consegui escrever em: ", path)
    message("   motivo provável: arquivo aberto (Excel) / permissão / lock.")
    message("✅ salvei alternativa em: ", path2)
  })
}

# ----------------------------
# 3) Layout Linha/Coluna
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

# ----------------------------
# 4) Encontrar e ler o WIDE salvo pelo pipeline
# ----------------------------
pattern_wide <- glue("^ml_wide_{var_ml}_.+\\.rds$")
arquivos <- list.files(dir_pp, pattern = pattern_wide, full.names = TRUE)

if (length(arquivos) == 0) {
  stop("Não achei nenhum arquivo wide salvo com padrão: ", pattern_wide, "\nPasta: ", dir_pp)
}

arq_wide <- NULL
if (!is.null(bin_tag)) {
  cand <- file.path(dir_pp, glue("ml_wide_{var_ml}_{bin_tag}.rds"))
  if (file.exists(cand)) arq_wide <- cand
}
if (is.null(arq_wide)) {
  info <- file.info(arquivos)
  arq_wide <- rownames(info)[which.max(info$mtime)]
}

message("📌 usando wide: ", arq_wide)
wide <- readr::read_rds(arq_wide)
if (!("t_bin" %in% names(wide))) stop("O wide não tem coluna t_bin. Arquivo: ", arq_wide)

# ----------------------------
# 5) Layout (Sensor/Linha/Coluna) do período
# ----------------------------
arq_banco <- file.path(pasta_base, "data", "tratados", lote_id, "banco_periodos.rds")
if (!file.exists(arq_banco)) stop("Não achei banco_periodos.rds: ", arq_banco)

banco <- readr::read_rds(arq_banco) |>
  mutate(Periodo = as.character(Periodo), Sensor = as.integer(Sensor)) |>
  filter(Periodo == periodo) |>
  distinct(Sensor)

if (nrow(banco) == 0) stop("Não achei sensores para o período: ", periodo)

layout_pp <- banco |>
  add_linha_coluna() |>
  distinct(Sensor, Linha, Coluna)

# ============================================================
# 6) MATRIZ DE SENSORES "CONTROLADA" (somente colunas numéricas)
# ============================================================

# colunas de sensores no wide: nomes tipo "7","10","12"... (apenas dígitos)
is_sensor_col <- function(nm) stringr::str_detect(nm, "^\\d+$")
sensor_cols <- names(wide)[is_sensor_col(names(wide))]

if (length(sensor_cols) < 2) {
  stop("Não encontrei colunas de sensores numéricos no wide. Colunas disponíveis: ",
       paste(names(wide), collapse = ", "))
}

mat_s <- as.matrix(wide[, sensor_cols, drop = FALSE])
storage.mode(mat_s) <- "double"

# remove sensores com pouca informação
good <- colSums(is.finite(mat_s)) > 5
mat_s <- mat_s[, good, drop = FALSE]
sensor_cols <- colnames(mat_s)

if (length(sensor_cols) < 2) stop("Poucos sensores com dados válidos após filtro >5 pontos finitos.")

# ============================================================
# 7) Correlação + hclust (usando SOMENTE sensores)
# ============================================================

corr <- suppressWarnings(cor(mat_s, use = "pairwise.complete.obs"))
dist_mat <- as.dist(1 - corr)
hc <- hclust(dist_mat, method = "average")

# ----------------------------
# 8) Paletas fixas
# ----------------------------
pal_linha <- c(
  "Linha 1" = "#1f77b4",
  "Linha 2" = "#e377c2",
  "Linha 3" = "#bcbd22",
  "NA"      = "grey70"
)

col10 <- c(
  "#E41A1C", "#377EB8", "#4DAF4A", "#984EA3", "#FF7F00",
  "#FFFF33", "#A65628", "#F781BF", "#00CED1", "#999999"
)

pal_coluna <- setNames(col10, paste("Coluna", 1:10))
pal_coluna <- c(pal_coluna, "NA" = "grey70")

# ----------------------------
# 9) Função: dendrograma bonito
# ----------------------------
plot_dend_bonito <- function(hc,
                             layout_df,
                             cor_por = c("Linha", "Coluna"),
                             out_png,
                             titulo = "Dendrograma (1-corr)",
                             legend_ncol = 1) {
  
  cor_por <- match.arg(cor_por)
  
  dend <- as.dendrogram(hc)
  labs <- labels(dend)
  sens_ord <- as.integer(labs)
  
  lay <- layout_df |>
    mutate(Sensor = as.integer(Sensor))
  
  grupo <- lay[[cor_por]][match(sens_ord, lay$Sensor)]
  grupo <- as.character(grupo)
  grupo[is.na(grupo)] <- "NA"
  
  pal <- if (cor_por == "Linha") pal_linha else pal_coluna
  
  label_cols <- pal[grupo]
  label_cols[is.na(label_cols)] <- "grey70"
  
  dend2 <- dend |>
    dendextend::set("labels_col", label_cols) |>
    dendextend::set("labels_cex", 1.05) |>
    dendextend::set("branches_col", "grey35") |>
    dendextend::set("branches_lwd", 1.3)
  
  png(out_png, width = 1900, height = 1050, res = 160)
  
  op <- par(no.readonly = TRUE)
  on.exit({par(op); dev.off()}, add = TRUE)
  
  par(mar = c(8, 5, 6, 14), xpd = NA)
  
  plot(
    dend2,
    main = titulo,
    ylab = "Height (distância = 1 - corr)",
    xlab = "Sensor",
    cex.main = 1.2
  )
  
  dendextend::colored_bars(
    colors = label_cols,
    dend = dend2,
    rowLabels = cor_por,
    y_shift = -0.10,
    sort_by_labels_order = TRUE
  )
  
  levs <- unique(grupo)
  levs <- levs[order(levs)]
  
  legend(
    "topright",
    inset = c(-0.25, 0.0),
    legend = levs,
    fill = unname(pal[levs]),
    title = cor_por,
    border = NA,
    bty = "n",
    cex = 0.95,
    ncol = legend_ncol
  )
  
  message("✅ salvo: ", out_png)
}

# ----------------------------
# 10) Dendrogramas
# ----------------------------
plot_dend_bonito(
  hc = hc,
  layout_df = layout_pp,
  cor_por = "Linha",
  out_png = file.path(dir_pp, glue("ML_04_dendrograma_{var_ml}_BONITO_POR_LINHA.png")),
  titulo = glue("Dendrograma (1-corr) — {lote_id} — {periodo} — var={var_ml}\nLabels+barra por LINHA (ramos neutros)"),
  legend_ncol = 1
)

plot_dend_bonito(
  hc = hc,
  layout_df = layout_pp,
  cor_por = "Coluna",
  out_png = file.path(dir_pp, glue("ML_04_dendrograma_{var_ml}_BONITO_POR_COLUNA.png")),
  titulo = glue("Dendrograma (1-corr) — {lote_id} — {periodo} — var={var_ml}\nLabels+barra por COLUNA (ramos neutros)"),
  legend_ncol = 2
)

# ----------------------------
# 11) Heatmap ORDENADO pelo dendrograma
# ----------------------------
ord <- hc$labels[hc$order]
corr_ord <- corr[ord, ord]

dfm <- as.data.frame(as.table(corr_ord)) |>
  transmute(s1 = Var1, s2 = Var2, corr = Freq)

g_heat <- ggplot(dfm, aes(x = s1, y = s2, fill = corr)) +
  geom_tile() +
  scale_fill_viridis_c(option = "C", limits = c(-1, 1)) +
  labs(
    title = glue("Heatmap correlação ORDENADO — {lote_id} — {periodo} — var={var_ml}"),
    subtitle = "Sensores reordenados pelo dendrograma",
    x = "Sensor", y = "Sensor", fill = "corr"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
    axis.text.y = element_text(size = 9)
  )

out_heat <- file.path(dir_pp, glue("ML_03_heatmap_correlacao_{var_ml}_ORDENADO.png"))
ggsave(out_heat, g_heat, width = 12, height = 10, dpi = 300)
message("✅ salvo: ", out_heat)

# ----------------------------
# 12) Correlação vs distância física
# ----------------------------
meta_xy <- layout_pp |>
  mutate(
    col_n = as.integer(stringr::str_extract(Coluna, "\\d+")),
    lin_n = as.integer(stringr::str_extract(Linha,  "\\d+")),
    Sensor_chr = as.character(Sensor)
  ) |>
  select(Sensor_chr, lin_n, col_n)

pairs <- t(combn(ord, 2)) |> as_tibble(.name_repair = "minimal")
names(pairs) <- c("s1", "s2")

pairs <- pairs |>
  left_join(meta_xy, by = c("s1" = "Sensor_chr")) |>
  rename(lin1 = lin_n, col1 = col_n) |>
  left_join(meta_xy, by = c("s2" = "Sensor_chr")) |>
  rename(lin2 = lin_n, col2 = col_n) |>
  mutate(
    dist = sqrt((col1 - col2)^2 + (lin1 - lin2)^2),
    corr = purrr::map2_dbl(s1, s2, ~ corr[.x, .y])
  ) |>
  filter(is.finite(dist), is.finite(corr))

rho <- suppressWarnings(cor(pairs$dist, pairs$corr, use = "complete.obs"))

g_scatter <- ggplot(pairs, aes(x = dist, y = corr)) +
  geom_point(alpha = 0.65, color = "grey20") +
  geom_smooth(method = "lm", se = TRUE, color = "#ff7f0e") +
  labs(
    title = glue("Correlação vs distância física — {lote_id} — {periodo} — var={var_ml}"),
    subtitle = glue("Pearson(dist,corr) = {round(rho, 3)} | n_pares = {nrow(pairs)}"),
    x = "Distância no grid (Linha/Coluna)",
    y = "Correlação (Pearson)"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

out_scatter <- file.path(dir_pp, glue("ML_07_corr_vs_dist_{var_ml}.png"))
ggsave(out_scatter, g_scatter, width = 12, height = 6, dpi = 300)
message("✅ salvo: ", out_scatter)

out_resumo <- file.path(dir_pp, glue("ML_07_corr_vs_dist_resumo_{var_ml}.csv"))
safe_write_csv(
  tibble(cor_dist_vs_corr = rho, n_pares = nrow(pairs)),
  out_resumo
)

# ============================================================
# 13) GREEDY CONTROLADO do ZERO (minimiza RMSE a cada passo)
# ============================================================

# referência: média global (todos sensores disponíveis)
ref <- rowMeans(mat_s, na.rm = TRUE)

rmse_of_set <- function(cols_chr) {
  if (length(cols_chr) == 0) return(Inf)
  recon <- rowMeans(mat_s[, cols_chr, drop = FALSE], na.rm = TRUE)
  err <- recon - ref
  sqrt(mean(err^2, na.rm = TRUE))
}

kmax_use <- min(kmax_greedy, ncol(mat_s))
all_cols <- colnames(mat_s)

selected <- character(0)
remaining <- all_cols

rmse_vec <- numeric(kmax_use)
delta_vec <- rep(NA_real_, kmax_use)

for (k in seq_len(kmax_use)) {
  # avalia cada candidato adicionando ao conjunto atual
  rmse_cand <- map_dbl(remaining, ~ rmse_of_set(c(selected, .x)))
  
  best_idx <- which.min(rmse_cand)
  best_sensor <- remaining[best_idx]
  best_rmse <- rmse_cand[best_idx]
  
  selected <- c(selected, best_sensor)
  remaining <- setdiff(remaining, best_sensor)
  
  rmse_vec[k] <- best_rmse
  if (k >= 2) delta_vec[k] <- rmse_vec[k - 1] - rmse_vec[k]
  
  message(glue("greedy k={k}: +{best_sensor} | RMSE={round(best_rmse, 6)}"))
}

df_greedy <- tibble(
  k = seq_len(kmax_use),
  sensor = as.integer(selected)
)

out_greedy <- file.path(dir_pp, glue("ML_05_greedy_recalc_kmax{ kmax_use }_{var_ml}.csv"))
safe_write_csv(df_greedy, out_greedy)

# ============================================================
# 14) Curva RMSE vs k (greedy controlado) + elbow + k90
# ============================================================

df_recon <- tibble(
  k = seq_len(kmax_use),
  rmse = rmse_vec,
  delta_rmse = delta_vec,
  sensores = map_chr(seq_len(kmax_use), ~ paste(selected[1:.x], collapse = ","))
)

safe_write_csv(df_recon, file.path(dir_pp, glue("ML_06_reconstrucao_{var_ml}_recalc.csv")))

# elbow por curvatura discreta (em curva monótona)
d1 <- df_recon$rmse[-1] - df_recon$rmse[-nrow(df_recon)]
d1 <- -d1

if (length(d1) >= 2) {
  d2 <- d1[-length(d1)] - d1[-1]
  k_elbow <- which.max(abs(d2)) + 2
} else {
  k_elbow <- df_recon$k[which.min(df_recon$rmse)]
}

# k90 (90% do ganho total) — interpretável e robusto
rmse0 <- df_recon$rmse[1]
rmse_min <- min(df_recon$rmse)
ganho_total <- rmse0 - rmse_min

df_recon <- df_recon |>
  mutate(ganho_acum = rmse0 - rmse)

k90 <- df_recon |>
  filter(ganho_acum >= 0.90 * ganho_total) |>
  summarise(k = min(k)) |>
  pull(k)

df_elbow <- tibble(
  k_elbow = k_elbow,
  rmse_elbow = df_recon$rmse[df_recon$k == k_elbow],
  k90 = k90,
  rmse_k90 = df_recon$rmse[df_recon$k == k90]
)

safe_write_csv(df_elbow, file.path(dir_pp, glue("ML_06_elbow_{var_ml}_curvatura_e_k90.csv")))

g_recon <- ggplot(df_recon, aes(x = k, y = rmse)) +
  geom_line(color = "grey70", linewidth = 1.2) +
  geom_point(color = "#2c7fb8", size = 3) +
  geom_vline(xintercept = k_elbow, linetype = 2, color = "#ff7f0e", linewidth = 1) +
  geom_vline(xintercept = k90, linetype = 3, color = "#33a02c", linewidth = 1) +
  annotate("text", x = k_elbow, y = max(df_recon$rmse),
           label = glue("elbow k={k_elbow}"),
           vjust = -0.4, hjust = 0.5, color = "#ff7f0e", fontface = "bold") +
  annotate("text", x = k90, y = max(df_recon$rmse) * 0.98,
           label = glue("k90={k90}"),
           vjust = -0.4, hjust = 0.5, color = "#33a02c", fontface = "bold") +
  labs(
    title = glue("Curva RMSE reconstrução vs k (GREEDY recalculado) — {lote_id} — {periodo} — var={var_ml}"),
    subtitle = "Ref = média global (todos sensores). Greedy escolhe sensor que mais reduz RMSE a cada passo.",
    x = "k (nº sensores)",
    y = "RMSE( recon - referência )"
  ) +
  theme_minimal(base_size = 12)

out_recon_png <- file.path(dir_pp, glue("ML_06_curva_reconstrucao_{var_ml}_GREEDY_RECALC.png"))
ggsave(out_recon_png, g_recon, width = 11, height = 6, dpi = 300)
message("✅ salvo: ", out_recon_png)
message("📌 elbow (curvatura): k = ", k_elbow, " | k90 = ", k90)

# ============================================================
# 15) Ler MASTER (11...) e preparar scores (parse numérico robusto)
# ============================================================

pat_master <- glue("(?i)^11\\..*master.*{var_ml}.*\\.csv$")
cand_master <- list.files(dir_pp, pattern = pat_master, full.names = TRUE)

if (length(cand_master) == 0) {
  pat_master2 <- glue("(?i)master.*{var_ml}.*\\.csv$")
  cand_master <- list.files(dir_pp, pattern = pat_master2, full.names = TRUE)
}
if (length(cand_master) == 0) stop("Não encontrei arquivo MASTER em: ", dir_pp)

info_m <- file.info(cand_master)
arq_master <- rownames(info_m)[which.max(info_m$mtime)]
message("📌 usando master: ", arq_master)

master <- readr::read_csv(arq_master, show_col_types = FALSE) |>
  mutate(Sensor = as.integer(Sensor))

parse_num <- function(x) {
  if (is.numeric(x)) return(x)
  readr::parse_number(
    as.character(x),
    locale = readr::locale(grouping_mark = ".", decimal_mark = ",")
  )
}

num_candidates <- c(
  "max_gap_min","n_gaps","p95_gap_min",
  "n_gaps_acima_10","excesso_total_10",
  "n_gaps_acima_15","excesso_total_15",
  "n_gaps_acima_30","excesso_total_30",
  "n_gaps_acima_60","excesso_total_60",
  "bias","rmse","outlier_rate","n_pts",
  "max_corr_com_outros","unicidade"
)

for (nm in intersect(num_candidates, names(master))) {
  master[[nm]] <- parse_num(master[[nm]])
}

# ============================================================
# 16) Features do greedy recalculado
# ============================================================

pos_greedy <- df_greedy |>
  transmute(Sensor = sensor, posicao_greedy = k)

step_gain <- df_greedy |>
  left_join(df_recon |> select(k, delta_rmse), by = "k") |>
  transmute(Sensor = sensor, ganho_rmse = delta_rmse)

# ============================================================
# 17) Score por objetivo (CONTROLE vs CUSTO)
# ============================================================

robust_z <- function(x) {
  x <- as.numeric(x)
  med <- median(x, na.rm = TRUE)
  madv <- mad(x, center = med, constant = 1, na.rm = TRUE)
  if (!is.finite(madv) || madv == 0) {
    s <- sd(x, na.rm = TRUE)
    if (!is.finite(s) || s == 0) return(rep(0, length(x)))
    return((x - mean(x, na.rm = TRUE)) / s)
  }
  (x - med) / madv
}

df_score <- master |>
  left_join(pos_greedy, by = "Sensor") |>
  left_join(step_gain,  by = "Sensor") |>
  mutate(
    abs_bias = abs(bias),
    
    # logs (robustez)
    log_p95_gap = log1p(p95_gap_min),
    log_max_gap = log1p(max_gap_min),
    log_exc30   = log1p(excesso_total_30),
    log_n_gaps  = log1p(n_gaps),
    
    log_rmse    = log1p(rmse),
    log_absbias = log1p(abs_bias),
    log_outlier = log1p(outlier_rate),
    
    # Subscore FALHA (menor é melhor -> sinal negativo)
    z_p95_gap = -robust_z(log_p95_gap),
    z_max_gap = -robust_z(log_max_gap),
    z_exc30   = -robust_z(log_exc30),
    z_n_gaps  = -robust_z(log_n_gaps),
    score_falha_v2 = rowMeans(cbind(z_p95_gap, z_max_gap, z_exc30, z_n_gaps), na.rm = TRUE),
    
    # Subscore SINAL (menor é melhor -> sinal negativo)
    z_rmse    = -robust_z(log_rmse),
    z_absbias = -robust_z(log_absbias),
    z_outlier = -robust_z(log_outlier),
    score_sinal_v2 = rowMeans(cbind(z_rmse, z_absbias, z_outlier), na.rm = TRUE),
    
    # REPRESENTATIVIDADE (entrar cedo + ganho alto)
    z_pos   = -robust_z(posicao_greedy),
    z_ganho = robust_z(ganho_rmse),
    score_repr_v2 = rowMeans(cbind(z_pos, z_ganho), na.rm = TRUE),
    
    # UNICIDADE (maior = melhor)
    score_unico_v2 = robust_z(unicidade)
  ) |>
  mutate(
    score_controle =
      0.35 * score_sinal_v2 +
      0.30 * score_falha_v2 +
      0.25 * score_repr_v2 +
      0.10 * score_unico_v2,
    
    score_custo =
      0.40 * score_repr_v2 +
      0.30 * score_sinal_v2 +
      0.20 * score_falha_v2 +
      0.10 * score_unico_v2
  )

out_scores_csv <- file.path(dir_pp, glue("ML_08_scores_objetivos_{var_ml}.csv"))
safe_write_csv(df_score, out_scores_csv)

# ============================================================
# 18) Rankings + Plots (top/bottom)
# ============================================================

plot_rank <- function(df, col_score, titulo, out_png, n_show = 10) {
  col_score <- rlang::ensym(col_score)
  
  df2 <- df |>
    mutate(Sensor = as.factor(Sensor)) |>
    arrange(desc(!!col_score)) |>
    slice_head(n = n_show) |>
    mutate(Sensor = forcats::fct_reorder(Sensor, !!col_score))
  
  g <- ggplot(df2, aes(x = Sensor, y = !!col_score, fill = Linha)) +
    geom_col() +
    coord_flip() +
    labs(title = titulo, x = "Sensor", y = as.character(col_score)) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"))
  
  ggsave(out_png, g, width = 10, height = 6, dpi = 300)
  message("✅ salvo: ", out_png)
}

plot_rank(
  df_score,
  score_controle,
  titulo = glue("TOP sensores — Score CONTROLE — {lote_id} — {periodo} — var={var_ml}"),
  out_png = file.path(dir_pp, glue("ML_08_TOP_score_controle_{var_ml}.png")),
  n_show = min(10, nrow(df_score))
)

plot_rank(
  df_score,
  score_custo,
  titulo = glue("TOP sensores — Score CUSTO — {lote_id} — {periodo} — var={var_ml}"),
  out_png = file.path(dir_pp, glue("ML_08_TOP_score_custo_{var_ml}.png")),
  n_show = min(10, nrow(df_score))
)

plot_rank(
  df_score |> arrange(score_controle),
  score_controle,
  titulo = glue("PIOR sensores — Score CONTROLE (diagnóstico) — {lote_id} — {periodo} — var={var_ml}"),
  out_png = file.path(dir_pp, glue("ML_08_BOTTOM_score_controle_{var_ml}.png")),
  n_show = min(10, nrow(df_score))
)

plot_rank(
  df_score |> arrange(score_custo),
  score_custo,
  titulo = glue("PIOR sensores — Score CUSTO (diagnóstico) — {lote_id} — {periodo} — var={var_ml}"),
  out_png = file.path(dir_pp, glue("ML_08_BOTTOM_score_custo_{var_ml}.png")),
  n_show = min(10, nrow(df_score))
)

# ============================================================
# 19) Recomendações automáticas (k_elbow e k90) por objetivo
# ============================================================

sens_wide_int <- as.integer(colnames(mat_s))

recommend <- function(df, score_col, k) {
  score_col <- rlang::ensym(score_col)
  df |>
    filter(Sensor %in% sens_wide_int) |>
    arrange(desc(!!score_col)) |>
    slice_head(n = k) |>
    select(Sensor, Linha, Coluna, !!score_col)
}

recom_controle_elbow <- recommend(df_score, score_controle, k_elbow)
recom_custo_elbow    <- recommend(df_score, score_custo,    k_elbow)

recom_controle_k90 <- recommend(df_score, score_controle, k90)
recom_custo_k90    <- recommend(df_score, score_custo,    k90)

safe_write_csv(recom_controle_elbow, file.path(dir_pp, glue("ML_08_recomendacao_k{ k_elbow }_controle_{var_ml}.csv")))
safe_write_csv(recom_custo_elbow,    file.path(dir_pp, glue("ML_08_recomendacao_k{ k_elbow }_custo_{var_ml}.csv")))
safe_write_csv(recom_controle_k90,   file.path(dir_pp, glue("ML_08_recomendacao_k{ k90 }_controle_k90_{var_ml}.csv")))
safe_write_csv(recom_custo_k90,      file.path(dir_pp, glue("ML_08_recomendacao_k{ k90 }_custo_k90_{var_ml}.csv")))

message("\n✅ FINAL: pós-processamento + GREEDY recalculado + curva/elbow/k90 + scores concluídos em: ", dir_pp)