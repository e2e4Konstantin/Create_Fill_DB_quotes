CREATE TABLE t1 (
    id_t1 INTEGER PRIMARY KEY NOT NULL,
    code TEXT, 
    amount INTEDER, 
    bin BLOB,
    UNIQUE (code, amount)
);

INSERT INTO t1 (code, amount)
VALUES
('abc', 55),
('BCD',33),
('xzx', 44);


CREATE TABLE t2 (
    id_t2 INTEGER PRIMARY KEY NOT NULL,
    code  TEXT,
    title TEXT,
    fk_t1 INTEGER NOT NULL,
    FOREIGN KEY (fk_t1) REFERENCES t1 (id_t1),
    UNIQUE (code, fk_t1)    
);

INSERT INTO t2 (code, title, fk_t1)
VALUES
('e1e', 'test', 3),
('f5f', '45test', 1),
('7e1', 'testgtg', 2);

DELETE FROM t2
WHERE id_t2 IN (48, 2, 77);
----------------------------------------------------------------------------

CREATE TABLE A
(
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT
 );

CREATE TABLE B
(
    id INTEGER NOT NULL PRIMARY KEY,
    id2 INTEGER,
    book TEXT,
    FOREIGN KEY(id2) REFERENCES A(id)
);

INSERT INTO A (name)
VALUES ('Jon'),('Bob'),('Anna');

INSERT INTO B (id2, book)
VALUES (1,'Lord of the Rings'),(1,'Catch 22'),(2,'Sum of All Fears'), (3,'Hunt for Red October');

delete from A where id=1;
