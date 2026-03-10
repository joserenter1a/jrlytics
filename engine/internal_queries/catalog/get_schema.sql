SELECT c.column_name, c.data_type, c.nullable
FROM columns c
    JOIN tables t ON c.table_id = t.id
WHERE t.name = ?
ORDER BY c.id