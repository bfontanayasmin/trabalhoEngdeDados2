# Operações em Delta Lake

## Objetivo

O notebook `03_dml_delta.ipynb` demonstra `INSERT`, `UPDATE` e `DELETE` na tabela de produtos armazenada em Delta Lake no bucket `bronze`.

Cada operação apresenta consultas antes e depois da alteração.

## 1. Preparação da demonstração

Antes das operações, o notebook lê novamente `produtos.csv` do `landing-zone` e sobrescreve o conteúdo atual da tabela Delta de produtos:

```python
(
    spark.read
    .schema(
        "id_produto INT, nome_produto STRING, "
        "preco DECIMAL(10,2), estoque INT, id_categoria INT"
    )
    .option("header", "true")
    .option("mode", "FAILFAST")
    .csv("s3a://landing-zone/produtos.csv")
    .write
    .format("delta")
    .mode("overwrite")
    .save("s3a://bronze/produtos")
)
```

Essa etapa restaura os dez produtos originais, permitindo repetir a demonstração sem acumular registros das execuções anteriores.

## 2. Registro das tabelas para uso com SQL

As cinco tabelas Delta são registradas no namespace `loja_bronze`.

Para produtos, o registro utiliza:

```sql
CREATE TABLE IF NOT EXISTS loja_bronze.produtos
USING DELTA
LOCATION 's3a://bronze/produtos';
```

`USING DELTA` define o formato. `LOCATION` aponta para os arquivos existentes no MinIO.

A tabela é externa (não gerenciada), pois seu caminho foi informado explicitamente.

Os comandos SQL são executados pelo PySpark:

```python
spark.sql("""
    SELECT * FROM loja_bronze.produtos
    ORDER BY id_produto
""").show(truncate=False)
```

## 3. Inserção — INSERT

`INSERT INTO` adiciona registros à tabela.

Inserimos um Monitor com identificador 11, preço de R$ 449,90, estoque de sete unidades e categoria Informática:

```sql
INSERT INTO loja_bronze.produtos
    (id_produto, nome_produto, preco, estoque, id_categoria)
VALUES
    (11, 'Monitor', 449.90, 7, 1);
```

| Situação | Quantidade de produtos |
|---|---:|
| Antes do INSERT | 10 |
| Depois do INSERT | 11 |

A consulta após a inserção mostra o Monitor entre os produtos cadastrados.

## 4. Atualização — UPDATE

`UPDATE` altera registros existentes. `SET` define os novos valores, e `WHERE` seleciona o registro.

Repetimos a atualização do trabalho anterior:

```sql
UPDATE loja_bronze.produtos
SET preco = 59.90,
    estoque = 15
WHERE id_produto = 1;
```

| Campo do Mouse | Antes | Depois |
|---|---:|---:|
| Preço | R$ 49,90 | R$ 59,90 |
| Estoque | 20 | 15 |

O `WHERE` limita a atualização ao Mouse. Sem essa condição, todos os produtos receberiam os valores definidos no `SET`.

A quantidade de produtos permanece onze.

## 5. Exclusão — DELETE

`DELETE FROM` remove registros da versão atual da tabela.

Repetimos a exclusão da Caneta:

```sql
DELETE FROM loja_bronze.produtos
WHERE id_produto = 4;
```

| Situação | Quantidade de produtos |
|---|---:|
| Antes do DELETE | 11 |
| Depois do DELETE | 10 |

A tabela continua existindo. Apenas a Caneta é removida, e o Monitor inserido permanece.

Neste conjunto de dados, a Caneta não aparece em `itens_pedido`.

A exclusão do registro não significa a remoção imediata dos arquivos utilizados por versões anteriores da tabela.

## 6. Histórico das operações

Delta Lake registra as alterações no log de transações.

Utilizamos:

```python
spark.sql(
    "DESCRIBE HISTORY loja_bronze.produtos"
).select(
    "version", "timestamp", "operation"
).show(6, truncate=False)
```

O resultado apresenta a versão, a data e o tipo de operação, incluindo gravações, atualizações e exclusões.

Uma nova execução pode apresentar números de versão diferentes, pois o histórico continua sendo registrado.

## 7. Resultado final

| Verificação | Resultado |
|---|---|
| Monitor | Cadastrado com identificador 11 |
| Mouse | Preço de R$ 59,90 e estoque de 15 |
| Caneta | Removida |
| Quantidade de produtos | 10 |

As demais tabelas mantêm suas quantidades:

| Tabela | Registros |
|---|---:|
| `categorias` | 2 |
| `clientes` | 5 |
| `itens_pedido` | 10 |
| `pedidos` | 5 |

Os notebooks utilizam verificações com `assert` para conferir os resultados esperados.

## 8. Reiniciar a demonstração

### Repetir somente as operações

Execute o notebook `03_dml_delta.ipynb` desde o início.

Sua etapa de preparação restaura os dez produtos originais do CSV. Depois, as operações são executadas novamente:

1. Inserção do Monitor.
2. Atualização do Mouse.
3. Exclusão da Caneta.

Os CSVs do `landing-zone` e as tabelas do SQL Server permanecem como origem.

### Refazer o fluxo completo

Execute os notebooks na ordem:

| Notebook | Preparação realizada |
|---|---|
| 00 | Remove e recria as cinco tabelas do SQL Server, inserindo os dados iniciais |
| 01 | Substitui os CSVs do `landing-zone` pela nova extração |
| 02 | Sobrescreve o conteúdo atual das cinco tabelas Delta no `bronze` |
| 03 | Restaura os produtos e executa as operações DML |
| 04 | Limpa e recria sua área de comparação |

Essa sequência permite começar novamente com os dados iniciais.

### Dados e histórico

A restauração das tabelas Delta utiliza `overwrite`. Ela substitui o conteúdo atual, mas mantém o histórico sujeito às regras de retenção do Delta.


## Referências

- [Delta Lake — atualização e exclusão](https://docs.delta.io/delta-update/)
- [Delta Lake — leitura, gravação e histórico](https://docs.delta.io/delta-batch/)
- [Projeto-modelo do professor](https://github.com/jlsilva01/spark-delta-minio-sqlserver)
