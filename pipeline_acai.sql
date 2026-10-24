-- =====================================================================
-- Pipeline "pipeline_acai"  |  Lakeflow Spark Declarative Pipelines
-- Catálogo padrão: workspace   Esquema padrão: aula_jobs
-- Cole este código no arquivo da pasta "transformations" do pipeline.
-- =====================================================================


-- ---------------------------------------------------------------------
-- BRONZE: ingestão incremental dos CSVs que chegam ao volume
-- STREAMING TABLE + read_files = Auto Loader: cada arquivo é lido UMA vez.
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_pedidos
COMMENT "Pedidos de açaí como chegaram dos pontos de venda (sem tratamento)."
AS SELECT
  *,
  _metadata.file_name AS arquivo_origem,
  current_timestamp()  AS ingerido_em
FROM STREAM read_files(
  '/Volumes/workspace/aula_jobs/arquivos/pedidos/',
  format => 'csv',
  header => 'true',
  schema => """
    id_pedido STRING, data_pedido DATE, municipio STRING, tipo_acai STRING,
    canal STRING, qtd_litros DOUBLE, preco_litro DOUBLE, id_cliente STRING
  """,
  schemaEvolutionMode => 'none'
);


-- ---------------------------------------------------------------------
-- SILVER: limpeza + regras de qualidade (expectations)
--   DROP ROW  -> a linha inválida é descartada
--   sem ação  -> a linha segue, mas a violação é contada (warn)
-- ---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_pedidos (
  CONSTRAINT pedido_com_id       EXPECT (id_pedido IS NOT NULL)                                   ON VIOLATION DROP ROW,
  CONSTRAINT quantidade_valida   EXPECT (qtd_litros IS NOT NULL AND qtd_litros > 0)               ON VIOLATION DROP ROW,
  CONSTRAINT preco_valido        EXPECT (preco_litro IS NOT NULL AND preco_litro BETWEEN 5 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT municipio_informado EXPECT (municipio IS NOT NULL)
)
COMMENT "Pedidos válidos, com município padronizado e valor total calculado."
AS SELECT
  id_pedido,
  data_pedido,
  NULLIF(TRIM(municipio), '')        AS municipio,
  tipo_acai,
  canal,
  qtd_litros,
  preco_litro,
  ROUND(qtd_litros * preco_litro, 2) AS valor_total,
  id_cliente,
  arquivo_origem
FROM STREAM(bronze_pedidos);


-- ---------------------------------------------------------------------
-- GOLD: visões de negócio (MATERIALIZED VIEW = recalculada a cada atualização)
-- ---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_vendas_municipio
COMMENT "Litros, receita e ticket médio por município."
AS SELECT
  COALESCE(municipio, 'Não informado') AS municipio,
  COUNT(*)                             AS pedidos,
  ROUND(SUM(qtd_litros), 1)            AS litros,
  ROUND(SUM(valor_total), 2)           AS receita,
  ROUND(AVG(valor_total), 2)           AS ticket_medio
FROM silver_pedidos
GROUP BY ALL;


CREATE OR REFRESH MATERIALIZED VIEW gold_vendas_dia_tipo
COMMENT "Litros e receita por dia e tipo de açaí."
AS SELECT
  data_pedido,
  tipo_acai,
  ROUND(SUM(qtd_litros), 1)  AS litros,
  ROUND(SUM(valor_total), 2) AS receita
FROM silver_pedidos
GROUP BY ALL;
