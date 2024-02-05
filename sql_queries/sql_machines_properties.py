sql_properties_machines_queries = {

    "create_table_properties_machines": """
        CREATE TABLE IF NOT EXISTS tblPropertiesMachines
            -- таблица для свойств Машин Глава 1.
            -- поля tr_storage_costs и tr_transportation_cost вычисляются по триггеру
            (
                ID_tblPropertiesMachine             INTEGER PRIMARY KEY NOT NULL,
                FK_tblPropertiesMachine_tblProducts INTEGER NOT NULL, -- id машины / родителя
                --
                RPC                     TEXT,		                -- ОКП (Russian Product Classifier)
                RPCA2                   TEXT,                       -- ОКПД2 (RPC by Types of Economic Activities)
                net_weight              REAL NOT NULL DEFAULT 0.0,  -- вес нетто
                gross_weight            REAL NOT NULL DEFAULT 0.0,  -- вес брутто
                basic_estimated_cost    REAL NOT NULL DEFAULT 0.0,  -- базовая сметная стоимость
                ID_transportation_code  
                    INTEGER REFERENCES tblPropertiesMachines (ID_tblPropertiesMachine), -- id Код транспортировки	
                tr_transportation_cost     REAL NOT NULL DEFAULT 0.0,       -- Стоимость транспортировки
                tr_storage_costs           REAL NOT NULL DEFAULT 0.0,       -- id ЗСР%, Заготовительно-складские расходы (Purchasing and storage costs)          
                
                selling_current_price REAL NOT NULL DEFAULT 0.0,            -- Отпуская текущая цена 
                selling_previous_price REAL NOT NULL DEFAULT 0.0,           -- Отпускная предыдущая цена
                transport_cost_factor	REAL NOT NULL DEFAULT 0.0,          -- Коэффициент пересчета транспортных затрат 
                index_average_estimated_price REAL NOT NULL DEFAULT 0.0,    -- Коэффициент (индекс) пересчета средних сметных цен (Conversion coefficient of average estimated prices)
                
                estimated_current_price INT GENERATED ALWAYS AS 
                    ((selling_current_price + tr_transportation_cost *  index_average_estimated_price * gross_weight/1000) * (1 + tr_storage_costs/100))
                    STORED,-- Сметная текущая цена	 
                --
                last_update INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),	
                UNIQUE (FK_tblPropertiesMachine_tblProducts)
            );
        """,

    "create_index_properties_machines": """
        CREATE UNIQUE INDEX IF NOT EXISTS 
            idxPropertiesMachines ON tblPropertiesMachines (FK_tblPropertiesMachine_tblProducts);
    """,
}