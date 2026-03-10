CREATE TABLE IF NOT EXISTS segment_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    min_value TEXT,
    max_value TEXT,
    FOREIGN KEY(segment_id) REFERENCES segments(id)
);