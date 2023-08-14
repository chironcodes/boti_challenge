CREATE OR REPLACE VIEW
  `polybox-394517.raw.consolidado_marca_linha` AS
SELECT
  MARCA AS marca,
  LINHA AS linha,
  SUM(QTD_VENDA) AS qtd_venda
FROM
  `polybox-394517.raw.venda`
GROUP BY
    1,
    2
