-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Agencia Libre de la NFL 2025
-- MAGIC
-- MAGIC Este Notebook se usa para obtener informes relacionados con la Agencia Libre.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datos agregados sobre Agentes Libres

-- COMMAND ----------

-- Selecciona los mejores Defensas que son UFA
SELECT Pos, COUNT(Pos) AS AgentesLibres
  FROM freeagents_2025 as fa 
  WHERE fa.Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
--  AND fa.Age < 30
  GROUP BY Pos
  ORDER BY AgentesLibres DESC
  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mejores Defensas

-- COMMAND ----------

-- Selecciona los mejores Defensas que son UFA
SELECT fa.Player, fa.Pos, fa.PrevTeam, fa.Age AS Edad,
-- Grades PFF
    pff.grades_defense AS DEF, pff.grades_run_defense AS RDEF, pff.grades_pass_rush_defense AS PRSH, pff.grades_coverage_defense AS COV, -- pff.grades_defense_penalty, pff.grades_tackle,
    fa.`Snaps%`, fa.SalaryPY AS SalarioAnual

  FROM freeagents_2025 as fa 
  JOIN defense_summary AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  AND fa.Pos in ("S") -- "EDGE", "IDL", "LB", "S", "CB"
  --AND fa.Age < 29
  -- AND fa.`Snaps%` > 20
  --AND pff.grades_defense > 45
  SORT BY pff.grades_defense DESC
  -- LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mejores Línea Ofensiva

-- COMMAND ----------

-- Selecciona los mejores Defensas que son UFA
SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age,
-- Grades PFF
    pff.grades_offense AS OFF, pff.grades_pass_block AS PBLK, pff.grades_run_block AS RBLK,
    fa.`Snaps%`, fa.SalaryPY 

  FROM freeagents_2025 as fa 
  JOIN offense_blocking AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  AND fa.Pos in ("LT", "RT") -- "QB", "WR", "LT", "RT". "TE", "RB", "FB", "LG", "RG"
  -- AND fa.Age < 30
  -- AND fa.`Snaps%` > 10
  --AND pff.grades_offense > 60
  SORT BY pff.grades_offense DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mejores RB

-- COMMAND ----------

SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age,
-- Grades PFF
    pff.grades_offense AS OFF, pff.grades_run AS RUN, pff.grades_pass_block AS PBLK, pff.grades_run_block AS RBLK,
    fa.`Snaps%`, fa.SalaryPY 

  FROM freeagents_2025 as fa 
  JOIN rushing_summary AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  AND fa.Pos in ("RB", "FB") -- "QB", "WR", "LT", "RT". "TE", "RB", "FB" "LG", , "RG"
  -- AND fa.Age < 30
  -- AND fa.`Snaps%` > 10
  -- AND pff.grades_offense > 60
  SORT BY pff.grades_offense DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mejores Receptores

-- COMMAND ----------

SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age,
-- Grades PFF
    pff.grades_offense AS OFF, pff.grades_pass_route AS RECV, pff.grades_pass_block AS PBLK,
    fa.`Snaps%`, fa.SalaryPY 

  FROM freeagents_2025 as fa 
  JOIN receiving_summary AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  AND fa.Pos in ("WR")
  -- AND fa.Age < 30
  -- AND fa.`Snaps%` > 10
  -- AND pff.grades_offense > 60
  SORT BY pff.grades_offense DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mejores Retornadores (KR/PR)

-- COMMAND ----------

SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age,
-- Grades PFF
pff.grades_return AS RETN, pff.grades_kick_return AS KRTN, pff.grades_punt_return AS PRTN,
    fa.`Snaps%`, fa.SalaryPY 

  FROM freeagents_2025 as fa 
  JOIN return_summary AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  -- AND fa.Pos in ("WR", "RB") -- "QB", "WR", "LT", "RT". "TE", "RB", "FB" "LG", , "RG"
  -- AND fa.Age < 30
  -- AND fa.`Snaps%` > 10
--  AND pff.grades_return > 60
SORT BY pff.grades_punt_return DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Mejores QBs

-- COMMAND ----------

SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age,
-- Grades PFF
    pff.grades_offense AS OFF, pff.grades_pass AS PASS, pff.grades_run AS RUN,
    fa.`Snaps%`, fa.SalaryPY 

  FROM freeagents_2025 as fa 
  JOIN passing_summary AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  AND fa.Pos in ("QB")
  -- AND fa.Age < 30
  -- AND fa.`Snaps%` > 10
--  AND pff.grades_return > 60
SORT BY pff.grades_offense DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Mejores TE

-- COMMAND ----------

SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age,
-- Grades PFF
    pff.grades_offense AS OFF, pff.grades_pass_route AS RUN, pff.grades_pass_block AS PBLK,
    fa.`Snaps%`, fa.SalaryPY 

  FROM freeagents_2025 as fa 
  JOIN receiving_summary AS pff 
  ON fa.Player = pff.player

  WHERE Type IN ("UFA", "Void") -- UFA, RFA, ERFA, SFA, Void, Option
  AND fa.Pos in ("TE")
  -- AND fa.Age < 30
  -- AND fa.`Snaps%` > 10
  -- AND pff.grades_offense > 60
  SORT BY pff.grades_offense DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Comprobar entradas que no estan en otras tablas
-- MAGIC

-- COMMAND ----------

SELECT fa.Player, fa.Pos, fa.PrevTeam , fa.Age
  FROM freeagents_2025 as fa 
  FULL OUTER JOIN receiving_summary AS pff 
  ON fa.Player = pff.player
  WHERE
    Type IN ("UFA", "Void")
    AND fa.Pos in ("TE") 
    AND pff.Player IS NULL