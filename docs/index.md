# Contextualização do trabalho

## Objetivo

Este trabalho demonstra um pipeline de dados utilizando SQL Server, MinIO, Apache Spark e Delta Lake.

O processo começa com a extração das tabelas de um banco relacional para arquivos CSV. Esses arquivos são armazenados no MinIO e, depois, convertidos para tabelas Delta Lake.

Nas tabelas Delta, demonstramos as operações `INSERT`, `UPDATE` e `DELETE`. Também comparamos o comportamento de tabelas gerenciadas (internas) e externas (não gerenciadas).

## Participante

Yasmin Bez Fontana.

## Cenário da loja

O banco representa uma loja com cinco tabelas:

| Tabela | Informações armazenadas |
|---|---|
| `categorias` | Identificador e nome da categoria |
| `produtos` | Identificador, nome, preço, estoque e categoria |
| `clientes` | Identificador, nome e e-mail do cliente |
| `pedidos` | Identificador, cliente, data e situação do pedido |
| `itens_pedido` | Identificador, pedido, produto, quantidade e preço unitário |

Uma categoria pode possuir vários produtos. Um cliente pode realizar vários pedidos, e cada pedido possui itens associados aos produtos.

No SQL Server, essas relações são representadas por chaves primárias e estrangeiras.

## Conjunto de dados

Os dados são fictícios e foram criados para o trabalho. A carga inicial possui:

| Tabela | Quantidade de registros |
|---|---:|
| `categorias` | 2 |
| `produtos` | 10 |
| `clientes` | 5 |
| `pedidos` | 5 |
| `itens_pedido` | 10 |
| **Total** | **32** |

Os registros são inseridos no SQL Server pelo notebook de preparação. Em seguida, todas as cinco tabelas são extraídas para CSV e convertidas para Delta Lake.

## Etapas do pipeline

1. **Banco de origem:** o SQL Server armazena as cinco tabelas preenchidas.
2. **Extração:** os registros são exportados para um CSV por tabela.
3. **Landing zone:** os cinco CSVs são gravados no bucket `landing-zone` do MinIO.
4. **Bronze:** o Spark lê os CSVs e grava as cinco tabelas Delta no bucket `bronze`.
5. **Manipulação:** os comandos `INSERT`, `UPDATE` e `DELETE` alteram a tabela Delta de produtos.

SQL Server e MinIO são executados em containers Docker no computador. O PySpark é executado pelo ambiente Python do projeto, gerenciado com UV.

## Relação com o trabalho anterior

O trabalho anterior demonstrou operações em Delta Lake e Apache Iceberg com arquivos armazenados no disco local.

Neste trabalho, ampliamos o cenário para cinco tabelas e acrescentamos a extração de um banco SQL Server e o armazenamento no MinIO. A conversão utiliza apenas Delta Lake, conforme o enunciado.

Os trabalhos possuem repositórios, ambientes, README e documentação separados.

## Organização dos notebooks

| Notebook | Finalidade |
|---|---|
| `00_setup_sqlserver.ipynb` | Criar e preencher o banco da loja |
| `01_sqlserver_to_minio_csv.ipynb` | Extrair todas as tabelas para CSV no MinIO |
| `02_csv_to_delta.ipynb` | Converter os CSVs para Delta Lake |
| `03_dml_delta.ipynb` | Demonstrar as operações e o histórico Delta |
| `04_tabelas_gerenciadas_externas.ipynb` | Comparar os efeitos de `DROP TABLE` |

Os notebooks apresentam explicações, códigos e resultados das execuções.
