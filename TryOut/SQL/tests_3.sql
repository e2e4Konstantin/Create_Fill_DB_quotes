CREATE TABLE tblProducts (
    ID_product INTEGER PRIMARY KEY NOT NULL,
    code          TEXT    NOT NULL
);

CREATE TABLE tblTransportCosts (
    ID_tblTransportCost    INTEGER PRIMARY KEY NOT NULL,
    FK_id_Products         INTEGER,
    base_price             REAL DEFAULT 0.0 NOT NULL,
    
    FOREIGN KEY (FK_id_Products) REFERENCES tblProducts(ID_product)
    UNIQUE (FK_id_Products)
);


INSERT INTO tblTransportCosts (code)
VALUES ('abc'), ('mki'), ('xzx');

INSERT INTO tblTransportCosts (FK_id_Products, base_price)
VALUES (3, 5.5), (1, 3.3), (2, 99);

