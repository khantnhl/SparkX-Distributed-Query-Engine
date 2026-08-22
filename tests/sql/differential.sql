-- name: filter_and_projection
SELECT id, amount FROM sales WHERE amount > 10 ORDER BY id;

-- name: null_predicate
SELECT id, amount IS NULL AS missing FROM sales ORDER BY id;

-- name: boolean_three_valued_logic
SELECT id, amount > 10 OR amount IS NULL AS selected FROM sales ORDER BY id;

-- name: explicit_cast
SELECT id, CAST(amount AS BIGINT) AS amount_int
FROM sales
WHERE amount IS NOT NULL
ORDER BY id;

-- name: grouped_aggregate
SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
FROM sales
GROUP BY region;

-- name: inner_join
SELECT s.id, c.name
FROM sales AS s
JOIN customers AS c ON s.customer_id = c.id
ORDER BY s.id;

-- name: left_join
SELECT s.id, c.name
FROM sales AS s
LEFT JOIN customers AS c ON s.customer_id = c.id
ORDER BY s.id;
