# ============================================================
# teste_otimizacao_autonomo_eta.R  (AUTÔNOMO) + BARRA + ETA
# - Gera dados sintéticos no formato "banco"
# - Valida estrutura e qualidade
# - Roda otimização error-based e entropy-based (k=1..k_max)
# - Mostra barra + percent + contagem + ETA (progress)
# - Imprime OK/FALHA por etapa no final
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(progress)
})

# ----------------------------
# 0) CONFIG
# ----------------------------
set.seed(123)

cfg <- list(
  n_sensores = 30,
  k_max = 3,                 # comece com 3; depois aumente para 5
  tol = 0.2,                 # tolerância de erro (°C)
  n_bins_entropy = 30,
  rodar_por_periodo = TRUE,
  periodos_alvo = c("Alojamento", "Abertura1"),
  n_pontos_tempo_por_periodo = 500,
  pb_width = 60
)

# ----------------------------
# 1) UTIL: status OK/FALHA
# ----------------------------
status <- tibble(
  etapa = character(),
  ok = logical(),
  msg = character()
)

add_status <- function(etapa, ok, msg = "") {
  status <<- bind_rows(status, tibble(etapa = etapa, ok = ok, msg = msg))
}

run_step <- function(etapa, expr) {
  tryCatch({
    force(expr)
    add_status(etapa, TRUE, "OK")
    TRUE
  }, error = function(e) {
    add_status(etapa, FALSE, conditionMessage(e))
    FALSE
  })
}

# ----------------------------
# 2) GERAR DADOS SINTÉTICOS "banco"
# ----------------------------
gerar_banco_sintetico <- function(n_sensores, periodos, n_t, tz = "America/Sao_Paulo") {
  base_time <- as.POSIXct("2026-01-01 00:00:00", tz = tz)
  
  df_periodos <- map2_dfr(periodos, seq_along(periodos), function(pp, i) {
    tibble(
      Periodo = pp,
      data_hora = base_time + (i - 1) * 86400 + seq(0, by = 600, length.out = n_t)
    )
  })
  
  df_periodos <- df_periodos %>%
    group_by(Periodo) %>%
    mutate(
      t_global = 26 + 2 * sin(2 * pi * row_number() / 144) + rnorm(n(), 0, 0.15),
      t_global = if_else(Periodo == periodos[2] & row_number() > n() * 0.65,
                         t_global - 1.2, t_global)
    ) %>%
    ungroup()
  
  sensores <- tibble(
    Sensor = 1:n_sensores,
    bias = rnorm(n_sensores, 0, 0.2),
    noise = runif(n_sensores, 0.05, 0.35),
    exposed = Sensor %in% sample(1:n_sensores, size = ceiling(n_sensores * 0.25))
  )
  
  crossing(df_periodos, sensores) %>%
    mutate(
      T = t_global + bias + rnorm(n(), 0, noise) +
        if_else(exposed, 0.8 * sin(2 * pi * as.numeric(data_hora) / 3600 / 6), 0),
      UR = 65 + rnorm(n(), 0, 2.5)
    ) %>%
    select(data_hora, Sensor, T, UR, Periodo)
}

# ----------------------------
# 3) CHECKS DO BANCO
# ----------------------------
check_banco <- function(banco, var = "T", rodar_por_periodo = TRUE) {
  req_cols <- c("data_hora", "Sensor", var)
  if (rodar_por_periodo) req_cols <- c(req_cols, "Periodo")
  
  miss <- setdiff(req_cols, names(banco))
  if (length(miss) > 0) stop("Faltando colunas: ", paste(miss, collapse = ", "))
  
  if (!inherits(banco$data_hora, "POSIXct")) stop("data_hora não é POSIXct.")
  if (!is.numeric(banco$Sensor) && !is.integer(banco$Sensor)) stop("Sensor não é numérico/integer.")
  if (!is.numeric(banco[[var]])) stop(var, " não é numérico.")
  
  dup_max <- banco %>%
    count(data_hora, Sensor) %>%
    summarise(mx = max(n), .groups = "drop") %>%
    pull(mx)
  
  if (is.na(dup_max)) dup_max <- 0
  
  list(
    n = nrow(banco),
    n_sensores = n_distinct(banco$Sensor),
    dup_max = dup_max
  )
}

# ----------------------------
# 4) MONTAR MATRIZ tempo × sensor
# ----------------------------
montar_matriz_ts <- function(df, var, periodo = NULL) {
  d <- df %>%
    filter(!is.na(data_hora), !is.na(Sensor), !is.na(.data[[var]]))
  
  if (!is.null(periodo)) d <- d %>% filter(Periodo == periodo)
  
  # agregação explícita (evita pivot_wider quebrar com duplicatas)
  d <- d %>%
    group_by(data_hora, Sensor) %>%
    summarise(value = mean(.data[[var]], na.rm = TRUE), .groups = "drop")
  
  wide <- d %>%
    mutate(Sensor = sprintf("S%02d", as.integer(Sensor))) %>%
    pivot_wider(names_from = Sensor, values_from = value)
  
  sensor_cols <- setdiff(names(wide), "data_hora")
  if (length(sensor_cols) < 2) return(NULL)
  
  list(
    time = wide$data_hora,
    X = as.matrix(wide[, sensor_cols, drop = FALSE]),
    sensors = sensor_cols
  )
}

# ----------------------------
# 5) ERROR-BASED (barra + ETA)
# ----------------------------
error_metrics <- function(e, tol = 0.2) {
  e <- e[is.finite(e)]
  tibble(
    mean_abs  = abs(mean(e)),
    sd_error  = sd(e),
    rmse      = sqrt(mean(e^2)),
    outlier_p = mean(abs(e) > tol)
  )
}

eval_combo_error <- function(X, sensors_all, sel, tol = 0.2) {
  ref <- rowMeans(X, na.rm = TRUE)
  idx <- match(sel, sensors_all)
  t_hat <- rowMeans(X[, idx, drop = FALSE], na.rm = TRUE)
  e <- ref - t_hat
  
  bind_cols(
    tibble(k = length(sel), sensors = paste(sel, collapse = ",")),
    error_metrics(e, tol = tol)
  )
}

run_error_bruteforce_eta <- function(mat, k_max = 3, tol = 0.2, label = "ERROR", pb_width = 60) {
  X <- mat$X
  sensors <- mat$sensors
  out <- vector("list", k_max)
  
  for (k in 1:k_max) {
    combs <- combn(sensors, k, simplify = FALSE)
    n_combs <- length(combs)
    
    message("\n▶ ", label, " | k=", k, " | combinações=", n_combs)
    
    pb <- progress::progress_bar$new(
      format = paste0("  [:bar] :percent | :current/:total | eta: :eta | ", label, " k=", k),
      total = n_combs,
      clear = FALSE,
      width = pb_width
    )
    
    res_k <- purrr::map_dfr(combs, function(sel) {
      pb$tick()
      eval_combo_error(X, sensors, sel, tol = tol)
    }) %>%
      arrange(rmse, sd_error, outlier_p)
    
    out[[k]] <- res_k
  }
  
  bind_rows(out)
}

# ----------------------------
# 6) ENTROPY-BASED (barra + ETA)
# ----------------------------
shannon_entropy <- function(x, bins = 30) {
  x <- x[is.finite(x)]
  if (length(x) < 30) return(NA_real_)
  brks <- pretty(range(x), n = bins)
  p <- table(cut(x, breaks = brks, include.lowest = TRUE))
  p <- as.numeric(p) / sum(p)
  p <- p[p > 0]
  -sum(p * log2(p))
}

run_entropy_sum_bruteforce_eta <- function(mat, k_max = 3, bins = 30, label = "ENTROPY", pb_width = 60) {
  X <- mat$X
  sensors <- mat$sensors
  
  # pré-cálculo das entropias 1D por sensor
  H <- apply(X, 2, shannon_entropy, bins = bins)
  H[!is.finite(H)] <- -Inf
  
  out <- vector("list", k_max)
  
  for (k in 1:k_max) {
    combs <- combn(seq_along(sensors), k, simplify = FALSE)
    n_combs <- length(combs)
    
    message("\n▶ ", label, " | k=", k, " | combinações=", n_combs)
    
    pb <- progress::progress_bar$new(
      format = paste0("  [:bar] :percent | :current/:total | eta: :eta | ", label, " k=", k),
      total = n_combs,
      clear = FALSE,
      width = pb_width
    )
    
    res_k <- purrr::map_dfr(combs, function(idx) {
      pb$tick()
      tibble(
        k = k,
        sensors = paste(sensors[idx], collapse = ","),
        entropy_sum = sum(H[idx])
      )
    }) %>%
      arrange(desc(entropy_sum))
    
    out[[k]] <- res_k
  }
  
  bind_rows(out)
}

# ----------------------------
# 7) RODAR PIPELINE + OK/FALHA
# ----------------------------
banco <- NULL
resultados <- list()

ok <- run_step("1) Gerar banco sintético", {
  banco <- gerar_banco_sintetico(
    n_sensores = cfg$n_sensores,
    periodos = cfg$periodos_alvo,
    n_t = cfg$n_pontos_tempo_por_periodo
  )
})

if (ok) {
  run_step("2) Check estrutura do banco", {
    info <- check_banco(banco, var = "T", rodar_por_periodo = cfg$rodar_por_periodo)
    if (info$dup_max > 1) message("⚠️ Aviso: há duplicatas (data_hora, Sensor). dup_max=", info$dup_max)
    message("Info banco: n=", info$n, " | sensores=", info$n_sensores, " | dup_max=", info$dup_max)
  })
}

run_step("3) Montar matriz (por período)", {
  if (!cfg$rodar_por_periodo) stop("Este script está configurado para rodar por período.")
  mats <- map(cfg$periodos_alvo, ~ montar_matriz_ts(banco, var = "T", periodo = .x))
  names(mats) <- cfg$periodos_alvo
  
  if (any(map_lgl(mats, is.null))) {
    bad <- names(mats)[map_lgl(mats, is.null)]
    stop("Períodos sem matriz (poucos sensores válidos?): ", paste(bad, collapse = ", "))
  }
  
  resultados$mats <- mats
})

run_step("4) Otimização ERROR-BASED (barra + ETA)", {
  mats <- resultados$mats
  resultados$error <- imap(mats, function(mat, pp) {
    run_error_bruteforce_eta(
      mat,
      k_max = cfg$k_max,
      tol = cfg$tol,
      label = paste0("ERROR | ", pp),
      pb_width = cfg$pb_width
    )
  })
})

run_step("5) Otimização ENTROPY-BASED (barra + ETA)", {
  mats <- resultados$mats
  resultados$entropy <- imap(mats, function(mat, pp) {
    run_entropy_sum_bruteforce_eta(
      mat,
      k_max = cfg$k_max,
      bins = cfg$n_bins_entropy,
      label = paste0("ENTROPY | ", pp),
      pb_width = cfg$pb_width
    )
  })
})

run_step("6) Resumo dos melhores (top1 por k)", {
  best_error <- imap_dfr(resultados$error, function(df, pp) {
    df %>%
      group_by(k) %>%
      slice(1) %>%
      ungroup() %>%
      mutate(Periodo = pp, metodo = "error")
  })
  
  best_entropy <- imap_dfr(resultados$entropy, function(df, pp) {
    df %>%
      group_by(k) %>%
      slice(1) %>%
      ungroup() %>%
      mutate(Periodo = pp, metodo = "entropy")
  })
  
  resultados$best <- bind_rows(best_error, best_entropy)
  print(resultados$best)
})

# ----------------------------
# 8) PRINT FINAL OK/FALHA
# ----------------------------
cat("\n====================\n")
cat("✅ CHECKLIST FINAL\n")
cat("====================\n\n")

print(
  status %>%
    mutate(
      resultado = if_else(ok, "OK", "FALHA"),
      detalhe = if_else(ok, "", msg)
    ) %>%
    select(etapa, resultado, detalhe)
)

cat("\nFim.\n")