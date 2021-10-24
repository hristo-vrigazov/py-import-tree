CREATE TABLE NODES (
    id INTEGER PRIMARY KEY,
    code_str TEXT UNIQUE
);

CREATE TABLE IMPORT_DATA (
    id INTEGER PRIMARY KEY ,
    root TEXT,
    module TEXT,
    path TEXT,
    version TEXT,
    node_id INTEGER NOT NULL,
    FOREIGN KEY(node_id) REFERENCES NODES(id)
);

