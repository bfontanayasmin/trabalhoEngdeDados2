# Pipeline de dados

## Visão geral

O pipeline transporta os dados do SQL Server para o MinIO em duas etapas: primeiro como arquivos CSV e, depois, como tabelas Delta Lake.

| Etapa | Origem | Destino | Formato |
|---|---|---|---|
| Extração | Cinco tabelas do SQL Server | Bucket `landing-zone` | CSV |
| Conversão | CSVs do `landing-zone` | Bucket `bronze` | Delta Lake |

## Papel das ferramentas

| Ferramenta | Função no projeto |
|---|---|
| SQL Server | Armazenar as tabelas do banco de origem |
| MinIO | Armazenar os CSVs e os arquivos das tabelas Delta |
| Docker Compose | Iniciar os serviços SQL Server e MinIO |
| pymssql | Permitir a conexão do Python ao SQL Server |
| Boto3 | Acessar o MinIO pela API compatível com S3 |
| Apache Spark | Ler e processar os dados |
| PySpark | Permitir controlar o Spark usando Python |
| Delta Lake | Organizar os dados com controle de transações e versões |
| UV | Gerenciar o ambiente e as dependências Python |

O MinIO é executado localmente em um container. Sua interface pode ser acessada no navegador em `http://localhost:9021`.

## 1. Preparação do banco de origem

O notebook `00_setup_sqlserver.ipynb` cria o banco `LojaDB` e suas cinco tabelas.

Um exemplo do DDL é a criação de `categorias`:

```sql
CREATE TABLE dbo.categorias (
    id_categoria INT PRIMARY KEY,
    nome_categoria NVARCHAR(100) NOT NULL
);
```

Depois, os dados fictícios são inseridos:

```sql
INSERT INTO dbo.categorias VALUES
    (1, N'Informática'),
    (2, N'Papelaria');
```

A carga respeita as dependências entre as tabelas: categorias e clientes são inseridos antes dos registros que os referenciam.

## 2. Descoberta das tabelas

O notebook `01_sqlserver_to_minio_csv.ipynb` consulta o catálogo do SQL Server para identificar todas as tabelas do banco:

```sql
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

A consulta encontrou cinco tabelas, todas no schema `dbo`: `categorias`, `clientes`, `itens_pedido`, `pedidos` e `produtos`.

Essa descoberta permite percorrer as tabelas existentes durante a extração.

## 3. Extração para CSV

Para cada tabela, executamos um `SELECT`, recuperamos os nomes das colunas e os registros e montamos um CSV com cabeçalho.

O trecho abaixo mostra a geração do conteúdo após a consulta:

```python
colunas = [coluna[0] for coluna in cursor.description]
registros = cursor.fetchall()

buffer = io.StringIO()
escritor = csv.writer(buffer, lineterminator="\n")
escritor.writerow(colunas)
escritor.writerows(registros)

conteudo = buffer.getvalue().encode("utf-8")
```

O cabeçalho identifica as colunas. A codificação UTF-8 preserva caracteres como os acentos em “Informática”.

## 4. Armazenamento no landing-zone

O bucket `landing-zone` recebe um CSV por tabela:

| Arquivo | Registros |
|---|---:|
| `categorias.csv` | 2 |
| `clientes.csv` | 5 |
| `itens_pedido.csv` | 10 |
| `pedidos.csv` | 5 |
| `produtos.csv` | 10 |

O envio é realizado pelo Boto3:

```python
s3.put_object(
    Bucket="landing-zone",
    Key=arquivo,
    Body=conteudo,
    ContentType="text/csv; charset=utf-8"
)
```

`Bucket` identifica o espaço de armazenamento, e `Key` identifica o objeto dentro dele.

Após o envio, o notebook lê os CSVs do MinIO e confere seus cabeçalhos e quantidades. A extração foi validada com **cinco arquivos e 32 registros**.

## 5. Configuração do Spark para acessar o MinIO

O notebook `02_csv_to_delta.ipynb` inicia uma sessão Spark com suporte ao Delta Lake e ao armazenamento compatível com S3.

Entre as configurações utilizadas estão:

```python
.config(
    "spark.jars.packages",
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)
.config(
    "spark.sql.extensions",
    "io.delta.sql.DeltaSparkSessionExtension"
)
.config(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
)
.config(
    "spark.hadoop.fs.s3a.endpoint",
    os.environ["MINIO_ENDPOINT"]
)
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
```

As credenciais são carregadas do arquivo `.env`. O notebook também configura a chave de acesso, a chave secreta e a implementação `S3AFileSystem`.

Os caminhos com `s3a://` permitem ao Spark acessar os objetos do MinIO. Por exemplo:

```text
s3a://landing-zone/produtos.csv
```

Nesse caminho, `landing-zone` é o bucket e `produtos.csv` é o objeto.

## 6. Conversão para Delta Lake

Como CSV não registra os tipos das colunas, definimos os schemas conforme as tabelas de origem.

Para produtos, utilizamos:

```python
schema_produtos = (
    "id_produto INT, nome_produto STRING, "
    "preco DECIMAL(10,2), estoque INT, id_categoria INT"
)
```

A leitura e a gravação seguem este procedimento:

```python
dados = (
    spark.read
    .schema(schema_produtos)
    .option("header", "true")
    .option("mode", "FAILFAST")
    .csv("s3a://landing-zone/produtos.csv")
)

(
    dados.write
    .format("delta")
    .mode("overwrite")
    .save("s3a://bronze/produtos")
)
```

- `header` indica que o CSV possui cabeçalho.
- `schema` define os tipos das colunas.
- `FAILFAST` interrompe a leitura quando encontra um registro malformado.
- `format("delta")` seleciona o formato Delta Lake.
- `overwrite` substitui o conteúdo atual do destino, permitindo repetir a conversão.

A conversão preserva os dados e os tipos definidos. As restrições de chave primária e estrangeira do SQL Server não são transferidas.

## 7. Organização no bronze

Cada tabela possui seu próprio caminho:

```text
s3a://bronze/categorias
s3a://bronze/clientes
s3a://bronze/itens_pedido
s3a://bronze/pedidos
s3a://bronze/produtos
```

Os registros ficam em arquivos Parquet. O diretório `_delta_log` contém as informações que controlam as transações e as versões da tabela.

## 8. Validação da conversão

Para cada destino, verificamos:

- Se é uma tabela Delta válida.
- Se a quantidade de registros corresponde à origem.
- Se o schema corresponde aos tipos definidos.

Um exemplo de leitura é:

```python
dados_delta = (
    spark.read
    .format("delta")
    .load("s3a://bronze/produtos")
)
```

A conversão foi validada com **cinco tabelas Delta e 32 registros**, antes das operações DML.

## Referências

- [Projeto-modelo do professor](https://github.com/jlsilva01/spark-delta-minio-sqlserver)
- [Apache Spark 3.5.3](https://spark.apache.org/docs/3.5.3/)
- [Delta Lake — leitura e gravação de tabelas](https://docs.delta.io/delta-batch/)
- [Boto3 — cliente S3](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [pymssql](https://pymssql.readthedocs.io/)
