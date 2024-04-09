 CREATE TABLE tblTransportCosts
    (
        ID_tblTransportCost                 INTEGER PRIMARY KEY NOT NULL,
        FK_tblTransportCosts_tblProducts    INTEGER NOT NULL, -- id продукта
        FK_tblTransportCosts_tblPeriods     INTEGER NOT NULL, -- id периода
        --
        base_price      REAL DEFAULT 0 NOT NULL,    -- БЦ: базовая цена перевозки 1 т. груза
        actual_price    REAL DEFAULT 0 NOT NULL,    -- ТЦ: текущая цена перевозки 1 т. груза
        numeric_ratio   REAL DEFAULT 1 NOT NULL,    -- коэффициент, применяется после сложения
        description     TEXT,                       -- описание
        inflation_ratio REAL GENERATED ALWAYS AS (
            CASE WHEN base_price = 0 THEN 0 ELSE ROUND(actual_price / base_price, 2) END
        ) VIRTUAL,
        last_update     INTEGER NOT NULL DEFAULT (UNIXEPOCH('now')),
        --
        FOREIGN KEY (FK_tblTransportCosts_tblProducts) REFERENCES tblProducts (ID_Product),
        FOREIGN KEY (FK_tblTransportCosts_tblPeriods) REFERENCES tblPeriods (ID_tblPeriod),
        --
        UNIQUE (FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods)
    );

CREATE UNIQUE INDEX idxTransportCosts ON tblTransportCosts (
            FK_tblTransportCosts_tblProducts, FK_tblTransportCosts_tblPeriods
        );
        
CREATE VIEW viewTransportCosts AS
            SELECT
                per.title AS 'период',
                p.code AS 'шифр',
                p.description AS 'название',
                tc.base_price AS 'базовая цена',
                tc.actual_price AS 'текущая цена',
                tc.inflation_ratio AS 'инфляц.коэф',
                tc.numeric_ratio AS 'коэф ?',
                tc.description AS 'описание'
            FROM tblTransportCosts tc
            LEFT JOIN tblProducts AS p ON p.ID_tblProduct = tc.FK_tblTransportCosts_tblProducts
            LEFT JOIN tblPeriods AS per ON per.ID_tblPeriod = tc.FK_tblTransportCosts_tblPeriods
            ORDER BY tc.FK_tblTransportCosts_tblPeriods, p.digit_code;