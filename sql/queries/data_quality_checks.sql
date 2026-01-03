-- =========================================================
-- COMPLETENESS CHECKS
-- =========================================================

-- 1. NULL values in mandatory customer fields
SELECT 'customers.email' AS field, COUNT(*) AS null_count
FROM customers
WHERE email IS NULL;

-- 2. Empty email strings
SELECT 'customers.email_empty' AS field, COUNT(*) AS empty_count
FROM customers
WHERE TRIM(email) = '';

-- =========================================================
-- UNIQUENESS CHECKS
-- =========================================================

-- Duplicate customer emails
SELECT email, COUNT(*) AS cnt
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;

-- Duplicate transactions
SELECT customer_id, transaction_date, total_amount, COUNT(*) AS cnt
FROM transactions
GROUP BY customer_id, transaction_date, total_amount
HAVING COUNT(*) > 1;

-- =========================================================
-- REFERENTIAL INTEGRITY CHECKS
-- =========================================================

-- Orphan transactions (customer missing)
SELECT COUNT(*) AS orphan_transactions
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (transaction missing)
SELECT COUNT(*) AS orphan_items_transaction
FROM transaction_items ti
LEFT JOIN transactions t ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (product missing)
SELECT COUNT(*) AS orphan_items_product
FROM transaction_items ti
LEFT JOIN products p ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- =========================================================
-- RANGE & VALIDITY CHECKS
-- =========================================================

-- Invalid prices
SELECT COUNT(*) AS invalid_prices
FROM products
WHERE price <= 0;

-- Invalid discounts
SELECT COUNT(*) AS invalid_discounts
FROM transaction_items
WHERE discount < 0 OR discount > 100;

-- =========================================================
-- CONSISTENCY CHECKS
-- =========================================================

-- Line total mismatch
SELECT COUNT(*) AS line_total_mismatch
FROM transaction_items
WHERE line_total <> quantity * unit_price * (1 - discount / 100.0);

-- Transaction total mismatch
SELECT COUNT(*) AS transaction_total_mismatch
FROM transactions t
JOIN (
    SELECT transaction_id,
           SUM(line_total) AS calculated_total
    FROM transaction_items
    GROUP BY transaction_id
) x ON t.transaction_id = x.transaction_id
WHERE t.total_amount <> x.calculated_total;

-- =========================================================
-- BUSINESS RULES
-- =========================================================

-- Cost should be less than price
SELECT COUNT(*) AS cost_price_violations
FROM products
WHERE cost >= price;

-- Future transactions
SELECT COUNT(*) AS future_transactions
FROM transactions
WHERE transaction_date > CURRENT_DATE;
