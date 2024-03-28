-- CTE — табличные выражения
-- https://habr.com/ru/articles/792630/
-- https://sky.pro/wiki/sql/rabota-s-peremennymi-i-vstavka-v-sq-lite-metody-i-primery/


WITH var(value) AS (
    SELECT 22
    )    
SELECT * FROM tblPeriods WHERE supplement_num = (SELECT value FROM var);   

WITH vars AS (
    SELECT 22 AS dd
    )    
SELECT * FROM tblPeriods 
JOIN vars
WHERE supplement_num = vars.dd;

 



WITH var(value) AS (
  SELECT val FROM vars WHERE name = 'Variable1'  -- Получаем значение первой "переменной"
  UNION ALL
  SELECT val FROM vars WHERE name = 'Variable2'  -- Добавляем значение второй "переменной"
)
SELECT * FROM your_table WHERE your_column IN (SELECT value FROM var);



BEGIN TRANSACTION;    -- Запускаем транзакцию
-- Здесь выполняются SQL команды
END TRANSACTION;    -- Закрываем транзакцию


WITH vars(value) AS (
  SELECT value FROM vars WHERE name = 'Variable1'  
  UNION ALL
  SELECT value FROM vars WHERE name = 'Variable2'  
)
SELECT value FROM vars;



WITH grouped AS
  (SELECT species,
          avg(body_mass_g) AS avg_mass_g
   FROM penguins
   GROUP BY species)
SELECT penguins.species,
       penguins.body_mass_g,
       round(grouped.avg_mass_g, 1) AS avg_mass_g
FROM penguins
JOIN grouped
WHERE penguins.body_mass_g > grouped.avg_mass_g
LIMIT 5;



