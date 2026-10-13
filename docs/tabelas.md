# Tabelas gerenciadas e externas

## Objetivo

O notebook `04_tabelas_gerenciadas_externas.ipynb` compara o comportamento de `DROP TABLE` em uma tabela gerenciada (interna) e uma externa (não gerenciada).

As duas utilizam Delta Lake e armazenam seus arquivos no MinIO. Para o teste, criamos cópias das duas categorias da loja em caminhos próprios dentro de `bronze/laboratorio`.

## 1. Diferença entre os tipos

| Característica | Gerenciada (interna) | Externa (não gerenciada) |
|---|---|---|
| Caminho da tabela | Escolhido pelo Spark dentro da localização do namespace | Informado explicitamente por `LOCATION` |
| Registro no catálogo | Gerenciado pelo Spark | Gerenciado pelo Spark |
| Responsabilidade pelo ciclo de vida dos arquivos | Spark | Usuário ou processo responsável pelo armazenamento |
| Efeito de `DROP TABLE` neste laboratório | Remove o registro do catálogo e os arquivos | Remove o registro do catálogo e preserva os arquivos |

O local de armazenamento, por si só, não determina o tipo: uma tabela gerenciada também pode ter seus arquivos no MinIO.

## 2. Preparação do laboratório

Antes do teste, removemos o namespace e os arquivos de uma execução anterior da comparação. A limpeza é limitada à área `bronze/laboratorio`.

Depois, criamos um namespace com uma localização base:

```sql
CREATE DATABASE laboratorio_tabelas
LOCATION 's3a://bronze/laboratorio/gerenciadas';
```

Essa localização define onde o Spark pode armazenar as tabelas gerenciadas desse namespace.

Informar `LOCATION` no **namespace** não é o mesmo que informar `LOCATION` na criação de uma **tabela**.

## 3. Criação da tabela gerenciada

A tabela gerenciada é criada sem `LOCATION` na definição da tabela:

```sql
CREATE TABLE laboratorio_tabelas.gerenciada
USING DELTA
AS SELECT * FROM delta.`s3a://bronze/categorias`;
```

O Spark escolhe o caminho da nova tabela dentro da localização do namespace e grava uma cópia das categorias.

A expressão `delta.` seguida do caminho permite consultar diretamente uma tabela Delta armazenada naquele endereço.

## 4. Criação da tabela externa

Primeiro, gravamos uma cópia das categorias em um caminho específico:

```python
caminho_externa = "s3a://bronze/laboratorio/externas/categorias"

(
    spark.read.format("delta")
    .load("s3a://bronze/categorias")
    .write.format("delta")
    .mode("overwrite")
    .save(caminho_externa)
)
```

Depois, registramos a tabela apontando para esses arquivos:

```sql
CREATE TABLE laboratorio_tabelas.externa
USING DELTA
LOCATION 's3a://bronze/laboratorio/externas/categorias';
```

O catálogo passa a permitir o acesso pelo nome `laboratorio_tabelas.externa`.

## 5. Conferência dos tipos

Utilizamos o catálogo do Spark para conferir a classificação:

```python
spark.catalog.getTable(
    "laboratorio_tabelas.gerenciada"
).tableType
```

O resultado esperado é:

| Tabela | Tipo no catálogo | Registros |
|---|---|---:|
| `gerenciada` | `MANAGED` | 2 |
| `externa` | `EXTERNAL` | 2 |

O notebook também verifica se os arquivos das duas tabelas existem no MinIO antes da remoção.

## 6. Teste com DROP TABLE

Executamos:

```sql
DROP TABLE laboratorio_tabelas.gerenciada;
DROP TABLE laboratorio_tabelas.externa;
```

Depois, verificamos o catálogo e os caminhos no armazenamento.

| Verificação | Gerenciada | Externa |
|---|---|---|
| Registro no catálogo após DROP | Removido | Removido |
| Arquivos após DROP | Removidos | Preservados |

Na tabela externa, os arquivos de dados e o diretório `_delta_log` permanecem no MinIO.

Portanto, o `DROP TABLE` remove seu registro no catálogo, mas preserva os metadados Delta que ficam junto dos arquivos.

## 7. Registro novamente da tabela externa

Como seus arquivos foram preservados, podemos registrar a tabela novamente:

```sql
CREATE TABLE laboratorio_tabelas.externa
USING DELTA
LOCATION 's3a://bronze/laboratorio/externas/categorias';
```

A consulta recupera os dois registros originais. Também verificamos que a tabela principal de categorias da loja continua com dois registros.

## 8. Comparação entre os trabalhos

| Trabalho | Armazenamento das tabelas Delta | Registro utilizado | Tipo |
|---|---|---|---|
| Trabalho 1 | Disco local, em `data/delta` | `CREATE TABLE` com `LOCATION` | Externa |
| Trabalho 2 | MinIO, no bucket `bronze` | `CREATE TABLE` com `LOCATION` | Externa |
| Laboratório adicional | MinIO, em `bronze/laboratorio` | Criação sem `LOCATION` na tabela | Gerenciada |

Nos dois trabalhos, as tabelas principais Delta são externas (não gerenciadas), pois seus caminhos foram informados explicitamente.

No trabalho 1, a limpeza adicional removia os arquivos da pasta local. Essa exclusão era feita pelo código de limpeza, além da remoção das tabelas do catálogo.

Neste trabalho, o laboratório demonstra o efeito do `DROP TABLE` separadamente, permitindo observar a preservação dos arquivos da tabela externa.

## 9. Relação com DataFrames

Criar um DataFrame não determina, sozinho, se uma tabela será gerenciada ou externa.

Neste projeto:

- `write.format("delta").save(caminho)` grava arquivos em um caminho.
- `CREATE TABLE ... LOCATION` registra esses arquivos como uma tabela externa.
- A tabela gerenciada do laboratório é criada sem `LOCATION` na definição da tabela.

É necessário observar a forma de gravação e de registro no catálogo.

## Resultado validado

O teste confirmou:

- Remoção do registro e dos arquivos da tabela gerenciada.
- Remoção do registro e preservação dos arquivos da tabela externa.
- Recuperação dos dois registros ao registrar novamente a externa.
- Preservação da tabela principal de categorias da loja.

## Referências

- Material de aula: *Ciclo de vida e pipeline de dados — Apache Spark*
- [Delta Lake — controle da localização das tabelas](https://docs.delta.io/delta-batch/)
- [Apache Spark — CREATE TABLE](https://spark.apache.org/docs/3.5.3/sql-ref-syntax-ddl-create-table-datasource.html)
