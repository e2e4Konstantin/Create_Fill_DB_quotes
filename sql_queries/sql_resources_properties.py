#
# SQL запросы для таблиц свойств ресурсов
#
sql_properties_resources_queries = {
    "delete_table_properties_resources": """DROP TABLE IF EXISTS tblPropertiesResources;""",
    "delete_index_properties_resources": """DROP INDEX IF EXISTS idxPropertiesResources;""",

    "create_table_properties_resources": """--sql
        CREATE TABLE IF NOT EXISTS tblPropertiesResources
        -- таблица для свойств Материалы Глава 1.
        (
            ID_tblPropertiesMaterial                  INTEGER PRIMARY KEY NOT NULL,
            FK_tblPropertiesResources_tblProducts     INTEGER NOT NULL,   -- id машины / родителя
            FK_tblPropertiesResources_tblStorageCosts INTEGER,            -- id записи где хранится %ЗСР
            -- id записи этой же таблицы откуда берется 'Код транспортировки' и 'Стоимость транспортировки'
            ID_transportation_cost  INTEGER REFERENCES tblPropertiesResources (ID_tblPropertiesMaterial),
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
            UNIQUE (FK_tblPropertiesResources_tblProducts)
        );

        """,

    "create_index_properties_resources": """-sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxPropertiesResources ON
            tblPropertiesResources (FK_tblPropertiesResources_tblProducts);
    """,

    "create_view_properties_resources": """--sql
        CREATE VIEW viewPropertiesResources AS
            SELECT
                pr.period AS 'период',
                pr.code AS 'шифр',
                pr.description AS 'название',
                --
                pm.RPC AS 'ОКП',
                pm.RPCA2 AS 'ОКПД2',
                pm.net_weight AS 'нетто',
                pm.gross_weight AS 'брутто',
                pm.basic_estimated_cost AS [базовая сметная стоимость],
                pm.transport_cost_factor AS [Коэффицтент пересчета транспортных затарат],

                st.percent_storage_costs AS [ЗСР],

                c.basic_estimated_cost  AS [стоимость транспортировки],
                cp.code AS [код транспортировки],


                -- сметная текущая цена
                pm.estimated_current_price,
                ((pm.selling_current_price + c.basic_estimated_cost * index_average_estimated_price * pm.gross_weight/1000) *
                    (1 + st.storage_costs/100)) AS [расчет estimated_current_price],

                pm.index_average_estimated_price,
                (c.basic_estimated_cost/pm.estimated_current_price)

            FROM tblPropertiesResources AS pm
            LEFT JOIN tblProducts AS pr ON pr.ID_tblProduct = pm.FK_tblPropertiesMaterial_tblProducts
            LEFT JOIN tblStorageCosts AS st ON st.ID_tblStorageCosts = pm.FK_tblPropertiesMaterial_tblStorageCosts


            LEFT JOIN tblPropertiesResources AS c ON c.ID_tblProduct = pm.ID_transportation_cost
            LEFT JOIN tblProducts AS cp ON pr.ID_tblProduct = c.FK_tblPropertiesMaterial_tblProducts

            ;
            --ORDER BY m.code
    """,

}
