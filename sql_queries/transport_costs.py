sql_transport_costs = {
    "insert_transport_cost": """--sql
        INSERT INTO tblTransportCosts
            (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods, name, description, base_price, actual_price)
        VALUES
            (?, ?, ?, ?, ?, ?);
    """,
    "create_table_transport_costs": """--sql
        -- таблица Транспортных расходов
        CREATE TABLE IF NOT EXISTS tblTransportCosts
        (
            ID_tblTransportCost                 INTEGER PRIMARY KEY NOT NULL,
            FK_tblTransportCosts_tblProducts    INTEGER NOT NULL, -- id продукта
            FK_tblTransportCosts_tblPeriods     INTEGER NOT NULL, -- id периода
            name            TEXT NOT NULL,              -- наименование
            description     TEXT NOT NULL,              -- описание
            base_price      REAL DEFAULT 0 NOT NULL,    -- БЦ: базовая цена перевозки 1 т. груза
            actual_price    REAL DEFAULT 0 NOT NULL,    -- ТЦ: текущая цена перевозки 1 т. груза
            numeric_ratio   REAL DEFAULT 1 NOT NULL,    -- коэффициент, применяется после сложения
            inflation_ratio REAL GENERATED ALWAYS AS (round(actual_price / base_price, 4)) VIRTUAL,
            --
            FOREIGN KEY (FK_tblTransportCosts_tblProducts) REFERENCES tblProducts (ID_Product),
            FOREIGN KEY (FK_tblTransportCosts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
            --
            last_update     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
            UNIQUE (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods)
        );
    """,
    "create_index_transport_costs": """--sql
        CREATE UNIQUE INDEX IF NOT EXISTS idxTransportCosts ON tblTransportCosts (
            FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods, name
        );
    """,
    "": """""",
    "": """""",
}


# CREATE TABLE larix.transport_cost (
# 	id int8 NOT NULL,
# 	"period" int8 NOT NULL,
# 	title varchar(500) NOT NULL,
# 	pressmark varchar(256) NOT NULL,
# 	cmt varchar(200) NULL,
# 	price numeric(19, 8) DEFAULT 0 NOT NULL,
# 	cur_price numeric(19, 8) DEFAULT 0 NOT NULL,
# 	created_on timestamp DEFAULT LOCALTIMESTAMP NOT NULL,
# 	created_by varchar(50) NOT NULL,
# 	modified_on timestamp DEFAULT LOCALTIMESTAMP NULL,
# 	modified_by varchar(50) NULL,
# 	version_number int8 DEFAULT 1 NOT NULL,
# 	deleted int2 DEFAULT 0 NOT NULL,
# 	ratio numeric(19, 8) DEFAULT 1 NOT NULL,
# 	deleted_on timestamp NULL,
# 	infl_rate numeric(19, 8) GENERATED ALWAYS AS (
# CASE
#     WHEN price = 0::numeric THEN 0::numeric
#     ELSE round(cur_price / price, 2)
# END) STORED NULL,
# 	CONSTRAINT transport_cost_pkey PRIMARY KEY (id, period),
# 	CONSTRAINT transport_cost_pressmark_period_key UNIQUE (pressmark, period)
# );