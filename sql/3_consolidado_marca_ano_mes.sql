CREATE OR REPLACE VIEW
  `polybox-394517.raw.consolidado_marca_ano_mes` AS
SELECT
  MARCA as marca,
  EXTRACT(YEAR FROM DATA_VENDA) AS ano_venda,
  EXTRACT(MONTH FROM DATA_VENDA) AS mes_venda,
  SUM(QTD_VENDA) AS qtd_venda
FROM
  `polybox-394517.raw.venda`
GROUP BY
  1,
  2,
  3