DROP TABLE IF EXISTS tblPropertiesMaterials;
DROP INDEX IF EXISTS idxPropertiesMaterials;

SELECT * FROM tblProducts;

CREATE TABLE IF NOT EXISTS tblPropertiesMaterials
        -- таблица для свойств Материалы Глава 1.
        (
            ID_tblPropertiesMaterial                 INTEGER PRIMARY KEY NOT NULL,
            FK_tblPropertiesMaterials_tblProducts     INTEGER NOT NULL,   -- id машины / родителя
            FK_tblPropertiesMaterials_tblStorageCosts INTEGER,            -- id записи где хранится %ЗСР
            -- id записи этой же таблицы откуда берется 'Код транспортировки' и 'Стоимость транспортировки'
            ID_transportation_cost  INTEGER REFERENCES tblPropertiesMaterials (ID_tblPropertiesMaterial),
            --
            RPC                     TEXT,		                -- ОКП (Russian Product Classifier)
            RPCA2                   TEXT,                       -- ОКПД2 (RPC by Types of Economic Activities)
            net_weight              REAL NOT NULL DEFAULT 0.0 CHECK(net_weight >= 0.0), -- вес нетто
            gross_weight            REAL NOT NULL DEFAULT 0.0 CHECK(net_weight >= 0.0), -- вес брутто
            basic_estimated_cost    REAL NOT NULL DEFAULT 0.0, -- базовая сметная стоимость
            selling_current_price   REAL NOT NULL DEFAULT 0.0, -- Отпуская текущая цена
            selling_previous_price  REAL NOT NULL DEFAULT 0.0, -- Отпускная предыдущая цена
            transport_cost_factor	REAL NOT NULL DEFAULT 0.0, -- Коэффициент пересчета транспортных затрат
            --
            last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
            UNIQUE (FK_tblPropertiesMaterials_tblProducts)
        );

select * from pragma_table_info('tblPropertiesMachines') as tblInfo;


CREATE UNIQUE INDEX IF NOT EXISTS idxPropertiesMaterials ON tblPropertiesMaterials (FK_tblPropertiesMaterials_tblProducts);
--
--
SELECT ID_tblProduct FROM tblProducts;
SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.0-7-11';
SELECT c.ID_tblStorageCosts FROM tblStorageCosts AS c WHERE c.name = 'Металлические конструкции';


SELECT pm.basic_estimated_cost FROM tblPropertiesMaterials AS pm WHERE pm.FK_tblPropertiesMaterials_tblProducts = (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.0-7-11');
SELECT pm.ID_tblPropertiesMaterial FROM tblPropertiesMaterials AS pm WHERE pm.FK_tblPropertiesMaterials_tblProducts = (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.0-7-11');
SELECT pm.basic_estimated_cost FROM tblPropertiesMaterials AS pm WHERE pm.ID_tblPropertiesMaterial = 1;

DELETE FROM tblPropertiesMaterials;

INSERT INTO tblPropertiesMaterials (
    FK_tblPropertiesMaterials_tblProducts, FK_tblPropertiesMaterials_tblStorageCosts, ID_transportation_cost,
    RPC, RPCA2, net_weight, gross_weight, basic_estimated_cost, selling_current_price, selling_previous_price, transport_cost_factor
    ) VALUES
	( (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.0-7-11'), NULL, NULL,
             NULL, NULL, 0.00, 1000.00, 60.01, 726.12, 661.05, 0.0);


INSERT INTO tblPropertiesMaterials (
    FK_tblPropertiesMaterials_tblProducts, FK_tblPropertiesMaterials_tblStorageCosts, ID_transportation_cost,
    RPC, RPCA2, net_weight, gross_weight, basic_estimated_cost, selling_current_price, selling_previous_price, transport_cost_factor
    ) VALUES
	(
             (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.1-1-45'),
             (SELECT c.ID_tblStorageCosts FROM tblStorageCosts AS c WHERE c.name = 'Строительные материалы'),
             (SELECT pm.ID_tblPropertiesMaterial FROM tblPropertiesMaterials AS pm WHERE pm.FK_tblPropertiesMaterials_tblProducts = (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.0-7-11')),
             '0256120002', '19.20.42.121.01.003', 1000.00, 1030.00, 3386.07, 21166.67, 19083.33, 12.1
          ),
          (
              (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.1-1-46'),
              (SELECT c.ID_tblStorageCosts FROM tblStorageCosts AS c WHERE c.name = 'Строительные материалы'),
              (SELECT pm.ID_tblPropertiesMaterial FROM tblPropertiesMaterials AS pm WHERE pm.FK_tblPropertiesMaterials_tblProducts = (SELECT p.ID_tblProduct FROM tblProducts AS p WHERE p.code = '1.0-7-11')),
              '0256110001', '19.20.42.121.01.004', 1000.00, 1030.00, 3501.78, 51666.67,	35833.33, 12.1
          );

DROP VIEW IF EXISTS viewPropertiesMaterials;
CREATE VIEW viewPropertiesMaterials AS
    SELECT *,
        ROUND(([тек.ОЦ] + [СтмТранс] * [КТЗ] * [брутто]/1000) * (1 + [%ЗСР]/100), 2) AS [СмТекЦена], --Сметная текущая цена
        ROUND((([тек.ОЦ] + [СтмТранс] * [КТЗ] * [брутто]/1000) * (1 + [%ЗСР]/100)) / [БазСС], 2) AS [КфСредСЦ] -- Коэффициент пересчета средних сметных цен
    FROM
        (SELECT
            pr.period AS [период],
            pr.code AS [шифр],
            pr.description AS [название],
            --
            pm.RPC AS 'ОКП',
            pm.RPCA2 AS 'ОКПД2',
            pm.net_weight AS [нетто],
            pm.gross_weight AS [брутто],
            pm.basic_estimated_cost AS [БазСС], --базовая сметная стоимость
            pm.selling_previous_price AS [пред.ОЦ],
            pm.selling_current_price AS [тек.ОЦ],
            pm.transport_cost_factor AS [КТЗ], -- Коэффицтент пересчета транспортных затарат
            --
            IIF(pm.FK_tblPropertiesMaterials_tblStorageCosts NOT NULL, sc.percent_storage_costs, 0.0) AS [%ЗСР],    -- Заготовительно-складские расходы. В %

            (SELECT code FROM tblProducts WHERE ID_tblProduct = tc.FK_tblPropertiesMaterials_tblProducts) AS [КодТр], -- код транспортировки

            IIF(pm.ID_transportation_cost NOT NULL, tc.basic_estimated_cost, 0.0) AS [СтмТранс]     --стоимость транспортировки,

         FROM tblPropertiesMaterials AS pm
         LEFT JOIN tblProducts AS pr ON pr.ID_tblProduct = pm.FK_tblPropertiesMaterials_tblProducts
         LEFT JOIN tblStorageCosts AS sc ON sc.ID_tblStorageCosts = pm.FK_tblPropertiesMaterials_tblStorageCosts
         LEFT JOIN tblPropertiesMaterials AS tc ON tc.ID_tblPropertiesMaterial = pm.ID_transportation_cost
        );


SELECT * FROM viewPropertiesMaterials;
