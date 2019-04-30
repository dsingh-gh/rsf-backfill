-- Transactions for self delivery orders with service fee
SELECT oft.order_date, uof.order_id, trn.transaction_id, trn.net_amount, uof.line_item_type, uof.amount
FROM integrated_core.order_fact oft
JOIN source_mysql_core.user_order_financials uof ON oft.order_id = uof.order_id
JOIN source_mysql_core.transaction trn ON oft.order_id = trn.order_id
WHERE oft.order_date > date('2019-04-04')
  AND oft.delivery_ind = TRUE
  AND oft.managed_delivery_ind = FALSE
  AND trn.transaction_type_id IN (9,12)
  AND uof.line_item_type = 'SERVICE_FEE'
  AND uof.amount > 0
ORDER BY oft.order_date, uof.order_id ASC

-- Transactions for self delivery orders with service fee tax
SELECT oft.order_date, uof.order_id, trn.transaction_id, trn.net_amount, uof.line_item_type, uof.amount
FROM integrated_core.order_fact oft
JOIN source_mysql_core.user_order_financials uof ON oft.order_id = uof.order_id
JOIN source_mysql_core.transaction trn ON oft.order_id = trn.order_id
WHERE oft.order_date > date('2019-04-04')
  AND oft.delivery_ind = TRUE
  AND oft.managed_delivery_ind = FALSE
  AND trn.transaction_type_id IN (9,12)
  AND uof.line_item_type = 'SERVICE_FEE_TAX'
  AND uof.amount > 0
ORDER BY oft.order_date, uof.order_id ASC

-- Refund Transactions for self delivery orders
SELECT oft.order_date, oft.order_id, trn.transaction_id, trn.net_amount
FROM integrated_core.order_fact oft
JOIN source_mysql_core.transaction trn ON oft.order_id = trn.order_id
WHERE oft.order_date > date('2019-04-04')
  AND oft.delivery_ind = TRUE
  AND oft.managed_delivery_ind = FALSE
  AND trn.transaction_type_id in (14, 15)
ORDER BY oft.order_date, oft.order_id ASC