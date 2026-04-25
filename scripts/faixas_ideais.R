# ============================================================
# scripts/faixas_ideais.R
# Faixas ideais de temperatura e umidade por dia do ciclo.
# Fonte: tabela de manejo avícola (referência do projeto).
#
# Funções exportadas:
#   faixa_ideal(dia)            → lista com temp/umid min e max
#   classificar_medida(...)     → 0 (dentro), +1 (acima), -1 (abaixo)
#   calcular_scores_diarios(df) → df agregado por dia/sensor/lote
# ============================================================

# - - - Periodos E Faixas - - -
#
#  Período       | Dias acum. | Temp (°C) | Umidade (%)
#  Alojamento    |   1 –  3   | 32 – 35   | 60 – 70
#  Abertura 1    |   4 –  5   | 32 – 35   | 60 – 70
#  Abertura 2    |   6 –  7   | 32 – 35   | 60 – 70
#  Abertura 3    |   8 – 12   | 29 – 32   | 60 – 70
#  Abertura 4    |  13 – 19   | 26 – 29   | 50 – 60
#  Abertura 5.1  |  20 – 27   | 23 – 26   | 50 – 60
#  Abertura 5.2  |  28 – 42   | 20 – 23   | 50 – 60

faixa_ideal = function(dia) {
  if      (dia <=  7) list(temp_min = 32, temp_max = 35, umid_min = 60, umid_max = 70)
  else if (dia <= 12) list(temp_min = 29, temp_max = 32, umid_min = 60, umid_max = 70)
  else if (dia <= 19) list(temp_min = 26, temp_max = 29, umid_min = 50, umid_max = 60)
  else if (dia <= 27) list(temp_min = 23, temp_max = 26, umid_min = 50, umid_max = 60)
  else if (dia <= 42) list(temp_min = 20, temp_max = 23, umid_min = 50, umid_max = 60)
  else                list(temp_min = NA_real_, temp_max = NA_real_,
                           umid_min = NA_real_, umid_max = NA_real_)
}

# - - - Classificar Medida - - -
#  0  → dentro da faixa
# +1  → acima
# -1  → abaixo

classificar_medida = function(valor, minimo, maximo) {
  dplyr::case_when(
    is.na(valor)   ~ NA_integer_,
    valor > maximo ~  1L,
    valor < minimo ~ -1L,
    TRUE           ~  0L
  )
}

# - - - Calcular Scores Diarios - - -
# Entrada: df com colunas dia, sensor, lote, temperatura, umidade
# Saída:   df agregado por dia/sensor/lote

calcular_scores_diarios = function(df) {
  
  # adiciona faixas por dia
  df$temp_min = NA_real_
  df$temp_max = NA_real_
  df$umid_min = NA_real_
  df$umid_max = NA_real_
  
  for (d in unique(df$dia)) {
    f = faixa_ideal(d)
    i = df$dia == d
    df$temp_min[i] = f$temp_min
    df$temp_max[i] = f$temp_max
    df$umid_min[i] = f$umid_min
    df$umid_max[i] = f$umid_max
  }
  
  # classifica cada medida
  df$ct = ifelse(is.na(df$temperatura), NA,
                 ifelse(df$temperatura > df$temp_max,  1L,
                        ifelse(df$temperatura < df$temp_min, -1L, 0L)))
  
  df$cu = ifelse(is.na(df$umidade), NA,
                 ifelse(df$umidade > df$umid_max,  1L,
                        ifelse(df$umidade < df$umid_min, -1L, 0L)))
  
  # agrega por dia/sensor/lote
  aggregate(
    cbind(
      temp_d    =  df$ct == 0L,
      temp_f    =  df$ct != 0L,
      umid_d    =  df$cu == 0L,
      umid_f    =  df$cu != 0L,
      n_medidas =  !is.na(df$ct)
    ) * 1L,
    by = list(dia = df$dia, sensor = df$sensor, lote = df$lote),
    FUN = sum,
    na.rm = TRUE
  )
}
