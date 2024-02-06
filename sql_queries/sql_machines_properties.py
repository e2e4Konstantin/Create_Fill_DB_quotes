sql_properties_machines_queries = {

    "create_table_properties_machines": """
        CREATE TABLE IF NOT EXISTS tblPropertiesMachines
            -- таблица для свойств Машин Глава 1.
            -- поля tr_storage_costs и tr_transportation_cost вычисляются по триггеру
            (
                ID_tblPropertiesMachine                 INTEGER PRIMARY KEY NOT NULL,
                FK_tblPropertiesMachine_tblProducts     INTEGER NOT NULL, -- id машины / родителя
                FK_tblPropertiesMachine_tblStorageCosts INTEGER NOT NULL, -- id записи где хранится %ЗСР
                -- id записи этой же таблицы откуда берется код транспортировки и basic_estimated_cost
                ID_transportation_cost  
                    INTEGER REFERENCES tblPropertiesMachines (ID_tblPropertiesMachine), 
                --
                RPC                     TEXT,		                -- ОКП (Russian Product Classifier)
                RPCA2                   TEXT,                       -- ОКПД2 (RPC by Types of Economic Activities)
                net_weight              REAL NOT NULL DEFAULT 0.0,  -- вес нетто
                gross_weight            REAL NOT NULL DEFAULT 0.0,  -- вес брутто
                basic_estimated_cost    REAL NOT NULL DEFAULT 0.0,  -- базовая сметная стоимость
                    
                -- стоимость транспортировки берется из записи ID_transportation_code поля basic_estimated_cost      	
                transportation_cost     REAL NOT NULL DEFAULT 0.0,       
                -- id ЗСР%, Заготовительно-складские расходы (Purchasing and storage costs)
                -- берется из таблицы tblStorageCosts в соответствии с FK_tblPropertiesMachine_tblStorageCosts
                storage_costs           REAL NOT NULL DEFAULT 0.0,                 
                
                selling_current_price REAL NOT NULL DEFAULT 0.0,            -- Отпуская текущая цена 
                selling_previous_price REAL NOT NULL DEFAULT 0.0,           -- Отпускная предыдущая цена
                transport_cost_factor	REAL NOT NULL DEFAULT 0.0,          -- Коэффициент пересчета транспортных затрат
                
                -- Коэффициент пересчета средних сметных цен (Conversion coefficient of average estimated prices) 
                index_average_estimated_price REAL INT GENERATED ALWAYS AS
                    (basic_estimated_cost/estimated_current_price), 
                -- сметная текущая цена
                estimated_current_price INT GENERATED ALWAYS AS 
                    ((
                    selling_current_price + transportation_cost * index_average_estimated_price * gross_weight/1000) * 
                    (1 + storage_costs/100))
                    STORED,
                   	 
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                UNIQUE (FK_tblPropertiesMachine_tblProducts, RPC, RPCA2)
            );
        """,

    "create_index_properties_machines": """
        CREATE UNIQUE INDEX IF NOT EXISTS 
            idxPropertiesMachines ON tblPropertiesMachines (FK_tblPropertiesMachine_tblProducts);
    """,

    "create_view_properties_machines": """
        CREATE VIEW viewPropertiesMachines AS
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
                
                st.percent_storage_costs AS [ЗСР],

                c.basic_estimated_cost  AS [стоимость транспортировки],
                cp.code AS [код транспортировки],
                
                               
                -- сметная текущая цена
                pm.estimated_current_price,
                ((pm.selling_current_price + c.basic_estimated_cost * index_average_estimated_price * pm.gross_weight/1000) * 
                    (1 + st.storage_costs/100)) AS [рассчет estimated_current_price],
                    
                pm.index_average_estimated_price,
                (c.basic_estimated_cost/pm.estimated_current_price)    

            FROM tblPropertiesMachines AS pm
            LEFT JOIN tblProducts AS pr ON pr.ID_tblProduct = pm.FK_tblPropertiesMachine_tblProducts 
            LEFT JOIN tblStorageCosts AS st ON st.ID_tblStorageCosts = pm.FK_tblPropertiesMachine_tblStorageCosts
            
            
            LEFT JOIN tblPropertiesMachines AS c ON c.ID_tblProduct = pm.ID_transportation_cost
            LEFT JOIN tblProducts AS cp ON pr.ID_tblProduct = c.FK_tblPropertiesMachine_tblProducts
            
            ;
            --ORDER BY m.code
    """,

}