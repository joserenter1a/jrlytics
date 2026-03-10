SELECT segments.file_path
FROM segments
JOIN tables on segments.table_id = tables.id
WHERE tables.name = ?