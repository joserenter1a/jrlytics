CREATE TABLE
IF NOT EXISTS columns
(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            column_name TEXT NOT NULL,
            data_type TEXT NOT NULL,
            nullable BOOLEAN NOT NULL,
            FOREIGN KEY
(table_id) REFERENCES tables
(id)
        );
