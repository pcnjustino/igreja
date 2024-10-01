# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, date_format

# Iniciar a sessão do Spark
spark = SparkSession.builder.appName("IgrejaFinanceiro").getOrCreate()

# Carregar os dados de doações e despesas
doacoes_df = spark.read.csv("data/doacoes.csv", header=True, inferSchema=True)
despesas_df = spark.read.csv("data/despesas.csv", header=True, inferSchema=True)

# Exibir as primeiras linhas para verificar o carregamento
print("Primeiras linhas de doações:")
doacoes_df.show(5)

print("Primeiras linhas de despesas:")
despesas_df.show(5)

# 1. Agregar as doações por tipo e mês
doacoes_mensal_df = doacoes_df.groupBy(
    date_format(col("data"), "yyyy-MM").alias("mes"),
    "tipo_doacao"
).agg(
    sum("valor").alias("total_doacoes")
).orderBy("mes")

print("Doações mensais agrupadas por tipo:")
doacoes_mensal_df.show()

# 2. Agregar as despesas por categoria e mês
despesas_mensal_df = despesas_df.groupBy(
    date_format(col("data"), "yyyy-MM").alias("mes"),
    "categoria"
).agg(
    sum("valor").alias("total_despesas")
).orderBy("mes")

print("Despesas mensais agrupadas por categoria:")
despesas_mensal_df.show()

# 3. Calcular o saldo mensal (Doações - Despesas)
saldo_mensal_df = doacoes_mensal_df.join(
    despesas_mensal_df, ["mes"], "outer"
).na.fill(0).withColumn(
    "saldo_mensal", col("total_doacoes") - col("total_despesas")
).select("mes", "total_doacoes", "total_despesas", "saldo_mensal")

print("Saldo mensal (Doações - Despesas):")
saldo_mensal_df.show()

# 4. Filtrar doações ou despesas por período específico 
filtro_periodo = "2024-07"
doacoes_periodo_df = doacoes_df.filter(date_format(col("data"), "yyyy-MM") == filtro_periodo)
despesas_periodo_df = despesas_df.filter(date_format(col("data"), "yyyy-MM") == filtro_periodo)

print(f"Doações no período {filtro_periodo}:")
doacoes_periodo_df.show()

print(f"Despesas no período {filtro_periodo}:")
despesas_periodo_df.show()

# Parar a sessão do Spark
spark.stop()
