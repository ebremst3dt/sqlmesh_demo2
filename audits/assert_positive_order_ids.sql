AUDIT (
    name assert_positive_order_ids,
  );

  SELECT TOP 1000 *
  FROM @this_model
  WHERE
    item_id < 0
  