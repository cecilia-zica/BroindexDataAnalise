# ============================================================
# R/fun_faixas_ideais.R
# Tabela de faixas ideais de temperatura e umidade por dia de ciclo.
# Preencha os valores NA com os limites corretos antes de rodar testes.R.
# ============================================================

# - - - Tabela De Faixas - - -
# Cada linha cobre um intervalo de dias (dia_ini e dia_fim inclusivos).
# Edite temp_min, temp_max, umid_min e umid_max conforme referência do projeto.

tabela_faixas = data.frame(
  dia_ini  = c( 1,  8, 15, 22, 29, 36),
  dia_fim  = c( 7, 14, 21, 28, 35, 42),
  temp_min = c(NA_real_, NA_real_, NA_real_, NA_real_, NA_real_, NA_real_),
  temp_max = c(NA_real_, NA_real_, NA_real_, NA_real_, NA_real_, NA_real_),
  umid_min = c(NA_real_, NA_real_, NA_real_, NA_real_, NA_real_, NA_real_),
  umid_max = c(NA_real_, NA_real_, NA_real_, NA_real_, NA_real_, NA_real_)
)

# - - - Funcao Faixa Ideal - - -
# Retorna lista com temp_min, temp_max, umid_min e umid_max para o dia informado.
# Retorna NA em todos os campos se o dia estiver fora dos intervalos definidos.

faixa_ideal = function(dia) {
  linha = which(tabela_faixas$dia_ini <= dia & tabela_faixas$dia_fim >= dia)
  if (length(linha) == 0) {
    return(list(
      temp_min = NA_real_, temp_max = NA_real_,
      umid_min = NA_real_, umid_max = NA_real_
    ))
  }
  list(
    temp_min = tabela_faixas$temp_min[linha],
    temp_max = tabela_faixas$temp_max[linha],
    umid_min = tabela_faixas$umid_min[linha],
    umid_max = tabela_faixas$umid_max[linha]
  )
}
