-- !preview conn=DBI::dbConnect(RSQLite::SQLite())
--
-- Medições observadas vs esperadas por sensor e lote.
--
-- Esperado = span real do lote (excluindo Vazio Sanitário) ÷ intervalo do sensor.
--   Cálculo: (MAX(data_hora) − MIN(data_hora)) em minutos / intervalo_min
--
-- Regra de intervalo (config.R):
--   Coluna ímpar (C1, C3, C5, C7, C9)  → 5 min  → label '5_5'
--   Coluna par   (C2, C4, C6, C8, C10) → 10 min → label '10_10'
--   Coluna = ((Sensor − 1) / 3) + 1   (inteiro)
--
-- Pré-requisito: tabelas lote1, lote2, lote3 devem estar disponíveis
-- (o script 3_comparativo.R já as registra via duckdb_register).

WITH

-- ── 1. Config dos sensores ───────────────────────────────────────────────────
sensor_cfg AS (
  SELECT s                                                                 AS Sensor,
         CASE WHEN (((s - 1) / 3) + 1) % 2 = 1 THEN '5_5' ELSE '10_10' END AS intervalo_tempo,
         CASE WHEN (((s - 1) / 3) + 1) % 2 = 1 THEN  5    ELSE  10     END AS intervalo_min
  FROM (
         SELECT  1 AS s UNION ALL SELECT  2 UNION ALL SELECT  3 UNION ALL SELECT  4 UNION ALL SELECT  5
    UNION ALL SELECT  6 UNION ALL SELECT  7 UNION ALL SELECT  8 UNION ALL SELECT  9 UNION ALL SELECT 10
    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
    UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
    UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24 UNION ALL SELECT 25
    UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30
  )
),

-- ── 2. Une os três lotes excluindo os Vazios Sanitários ─────────────────────
todas AS (
  SELECT Sensor, data_hora, 'lote1' AS Lote FROM lote1
  WHERE Periodo NOT IN ('VazioSanitario_Ini', 'VazioSanitario_Fim')
  UNION ALL
  SELECT Sensor, data_hora, 'lote2' FROM lote2
  WHERE Periodo NOT IN ('VazioSanitario_Ini', 'VazioSanitario_Fim')
  UNION ALL
  SELECT Sensor, data_hora, 'lote3' FROM lote3
  WHERE Periodo NOT IN ('VazioSanitario_Ini', 'VazioSanitario_Fim')
),

-- ── 3. Span real (minutos) por lote e tipo de intervalo ─────────────────────
-- Usa todos os sensores do mesmo tipo para ter o span mais representativo.
span_lote AS (
  SELECT t.Lote,
         sc.intervalo_min,
         CAST(ROUND(
           (JULIANDAY(MAX(t.data_hora)) - JULIANDAY(MIN(t.data_hora))) * 1440
         ) AS INTEGER)                                                    AS span_min
  FROM todas t
  JOIN sensor_cfg sc ON sc.Sensor = t.Sensor
  GROUP BY t.Lote, sc.intervalo_min
),

-- ── 4. Esperado = span ÷ intervalo ──────────────────────────────────────────
esperado_lote AS (
  SELECT Lote, intervalo_min,
         span_min / intervalo_min AS Esperado
  FROM span_lote
),

-- ── 5. Contagem de observações por sensor e lote ─────────────────────────────
obs AS (
  SELECT Sensor, Lote, COUNT(*) AS n_obs
  FROM todas
  GROUP BY Sensor, Lote
)

-- ── Resultado final ──────────────────────────────────────────────────────────
SELECT
  sc.intervalo_tempo,
  sc.Sensor,
  MAX(CASE WHEN e.Lote = 'lote1' THEN e.Esperado END) AS Esperado,
  MAX(CASE WHEN o.Lote = 'lote1' THEN o.n_obs    END) AS lote1_obs,
  MAX(CASE WHEN o.Lote = 'lote2' THEN o.n_obs    END) AS lote2_obs,
  MAX(CASE WHEN o.Lote = 'lote3' THEN o.n_obs    END) AS lote3_obs
FROM sensor_cfg sc
LEFT JOIN obs o            ON o.Sensor      = sc.Sensor
LEFT JOIN esperado_lote e  ON e.Lote        = o.Lote
                          AND e.intervalo_min = sc.intervalo_min
GROUP BY sc.intervalo_tempo, sc.Sensor
ORDER BY sc.Sensor
