CREATE TABLE IMPORTS (
    code_str TEXT PRIMARY KEY
);

CREATE TABLE IMPORT_DATA (
    id INTEGER PRIMARY KEY ,
    root TEXT,
    module TEXT,
    path TEXT,
    version TEXT,
    code_str TEXT NOT NULL,
    FOREIGN KEY(code_str) REFERENCES IMPORTS(code_str)
);

CREATE TABLE FILENAMES (
    path TEXT PRIMARY KEY
);

CREATE TABLE DEFINITIONS (
    id INTEGER PRIMARY KEY,
    type TEXT NOT NULL , --whether it's a class or a function
    name TEXT NOT NULL ,
    start_no INTEGER NOT NULL ,
    end_no INTEGER NOT NULL ,
    filename_path TEXT NOT NULL ,
    FOREIGN KEY (filename_path) REFERENCES FILENAMES(path)
);

CREATE TABLE DEFINITIONS_TO_IMPORTS (
    id INTEGER PRIMARY KEY,
    definition_id INTEGER NOT NULL ,
    import_code_str TEXT NOT NULL ,
    FOREIGN KEY (definition_id) REFERENCES DEFINITIONS(id),
    FOREIGN KEY (import_code_str) REFERENCES IMPORTS(code_str)
);

CREATE TABLE FILENAMES_TO_IMPORTS (
    id INTEGER PRIMARY KEY,
    filename_path TEXT NOT NULL ,
    import_code_str TEXT NOT NULL ,
    FOREIGN KEY (filename_path) REFERENCES FILENAMES(path),
    FOREIGN KEY (import_code_str) REFERENCES IMPORTS(code_str)
);
