
CREATE TABLE
IF NOT EXISTS segments
(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY
(table_id) REFERENCES tables
(id)
        );