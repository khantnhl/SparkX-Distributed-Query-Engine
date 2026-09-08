-- name: left_join_unmatched_and_null
SELECT a.id, a.value, b.value FROM a LEFT JOIN b ON a.id = b.id;
-- name: compound_keys
SELECT a.id, a.value, b.value FROM a LEFT JOIN b ON a.id = b.id AND a.value = b.value;
-- name: aliases_and_filter
SELECT x.id AS key, y.value AS result FROM a AS x JOIN b AS y ON x.id = y.id WHERE x.value > 10;
-- name: empty_left
SELECT e.id, b.value FROM empty_input e LEFT JOIN b ON e.id = b.id;
-- name: empty_right
SELECT a.id, e.value FROM a LEFT JOIN empty_input e ON a.id = e.id;
-- name: no_matches
SELECT a.id, b.value FROM a LEFT JOIN b ON a.value = b.id;
-- name: duplicate_inner
SELECT a.id, b.value FROM a JOIN b ON a.id = b.id;
