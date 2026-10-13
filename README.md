# Pipeline de dados com SQL Server, MinIO e Delta Lake

Trabalho 2 de Engenharia de Dados. O projeto demonstra a extração de dados de um banco SQL Server, seu armazenamento em CSV no MinIO e a conversão para tabelas Delta Lake utilizando PySpark.

Também são demonstradas as operações `INSERT`, `UPDATE` e `DELETE`, além da diferença entre tabelas gerenciadas (internas) e externas (não gerenciadas).

## Participante

- Yasmin Bez Fontana

## Cenário e conjunto de dados

O cenário representa uma loja com cinco tabelas relacionadas:

| Tabela | Conteúdo | Registros iniciais |
|---|---|---:|
| `categorias` | Categorias dos produtos | 2 |
| `produtos` | Produtos, preços, estoque e categoria | 10 |
| `clientes` | Nomes e e-mails dos clientes | 5 |
| `pedidos` | Cliente, data e situação do pedido | 5 |
| `itens_pedido` | Produtos, quantidades e preços dos itens | 10 |

O conjunto de dados foi criado para o trabalho e contém **32 registros fictícios**. Os dados são inseridos no SQL Server pelo notebook de preparação.

Uma categoria pode possuir vários produtos. Um cliente pode realizar vários pedidos, e cada pedido possui itens associados aos produtos. Essas relações são representadas por chaves primárias e estrangeiras no banco de origem.

## Fluxo dos dados

O processamento ocorre em três etapas:

1. **SQL Server:** banco de origem com as cinco tabelas preenchidas.
2. **MinIO — `landing-zone`:** extração de todas as tabelas para um arquivo CSV por tabela.
3. **MinIO — `bronze`:** leitura dos CSVs pelo Spark e gravação das cinco tabelas em Delta Lake.

Os arquivos CSV permanecem no `landing-zone`. As operações de inserção, atualização e exclusão são realizadas nas tabelas Delta do `bronze`.

## Tecnologias e versões

| Tecnologia | Versão utilizada |
|---|---|
| Python | 3.11.9 |
| Java | OpenJDK 17 |
| PySpark | 3.5.3 |
| Delta Lake / `delta-spark` | 3.2.0 |
| Conector Hadoop AWS | 3.3.4 |
| SQL Server | 2025 Developer |
| MinIO | `RELEASE.2025-02-03T21-03-04Z` |
| JupyterLab | 4.6.4 |
| ipykernel | 7.4.0 |
| MkDocs | 1.6.1 |
| pymssql | 2.4.4 |
| Docker | 29.7.1 |
| Docker Compose | 5.4.0 |

O projeto utiliza **UV** para gerenciar o ambiente Python e suas dependências.

As bibliotecas estão declaradas em `pyproject.toml`, e as versões resolvidas, incluindo `boto3` e `python-dotenv`, estão registradas em `uv.lock`.

SQL Server e MinIO são executados em containers Docker com volumes para persistência dos dados.

## Organização do projeto

| Arquivo ou pasta | Finalidade |
|---|---|
| `docker-compose.yml` | Configuração dos serviços SQL Server e MinIO |
| `.env.example` | Modelo das configurações locais |
| `.env` | Configurações e senhas locais; ignorado pelo Git |
| `pyproject.toml` | Dependências e informações do projeto |
| `uv.lock` | Versões das dependências Python |
| `.python-version` | Versão do Python |
| `notebooks/` | Preparação, extração, conversão e demonstrações |
| `docs/` | Documentação em Markdown |
| `mkdocs.yml` | Configuração do site de documentação |

## Preparação do ambiente

As instruções abaixo foram utilizadas em Ubuntu no WSL.

### 1. Pré-requisitos

Tenha Docker com Docker Compose instalado e funcionando. Confira:

```bash
docker --version
docker compose version
docker ps
```

Instale Java 17, Git e curl:

```bash
sudo apt update
sudo apt install openjdk-17-jre-headless git curl
```

Confira o Java:

```bash
java -version
```

Instale o UV:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Reabra o terminal e confira:

```bash
uv --version
```

### 2. Baixar o projeto e instalar as dependências

```bash
git clone https://github.com/bfontanayasmin/trabalhoBancodeDados2.git
cd trabalhoBancodeDados2

uv python install 3.11.9
uv sync --locked
```

O comando `uv sync --locked` prepara o ambiente `.venv` com as dependências registradas no arquivo de versões.

### 3. Preparar as configurações locais

O arquivo `.env.example` contém valores de exemplo. O comando abaixo gera as senhas automaticamente e cria o arquivo `.env` com as configurações utilizadas pelo Docker e pelos notebooks.

Ao executar novamente o comando, um `.env` existente é preservado.

```bash
uv run python - <<'PY'
from pathlib import Path
import secrets

destino = Path(".env")

if not destino.exists():
    texto = Path(".env.example").read_text()
    texto = texto.replace(
        "SQLSERVER_PASSWORD=SUBSTITUA_POR_UMA_SENHA_FORTE",
        "SQLSERVER_PASSWORD=Loja@" + secrets.token_hex(16)
    )
    texto = texto.replace(
        "MINIO_ROOT_PASSWORD=SUBSTITUA_POR_UMA_SENHA_FORTE",
        "MINIO_ROOT_PASSWORD=" + secrets.token_hex(16)
    )
    destino.write_text(texto)
    destino.chmod(0o600)

print("Configuração local preparada.")
PY
```

As senhas desse arquivo devem ser mantidas entre as execuções, pois os serviços reutilizam os dados armazenados nos volumes Docker.

### 4. Iniciar SQL Server e MinIO

```bash
docker compose up -d
docker compose ps
```

O primeiro comando baixa as imagens quando necessário e inicia os serviços. Aguarde a inicialização do SQL Server antes de executar os notebooks.

| Serviço | Endereço |
|---|---|
| SQL Server | `127.0.0.1:1433` |
| API do MinIO | `http://127.0.0.1:9020` |
| Console do MinIO | `http://localhost:9021` |

As portas estão configuradas para acesso local.

## Execução dos notebooks

Na pasta principal, execute:

```bash
uv run jupyter lab
```

Abra o endereço informado no terminal e entre em `notebooks`.

Execute os notebooks na ordem abaixo:

| Ordem | Notebook | Finalidade |
|---|---|---|
| 00 | `00_setup_sqlserver.ipynb` | Criar e preencher as cinco tabelas do SQL Server |
| 01 | `01_sqlserver_to_minio_csv.ipynb` | Extrair todas as tabelas para CSV no `landing-zone` |
| 02 | `02_csv_to_delta.ipynb` | Converter os cinco CSVs para Delta no `bronze` |
| 03 | `03_dml_delta.ipynb` | Demonstrar INSERT, UPDATE, DELETE e histórico |
| 04 | `04_tabelas_gerenciadas_externas.ipynb` | Comparar o comportamento de DROP TABLE |

Execute cada notebook desde o início. Os notebooks que utilizam Spark encerram a sessão ao terminar.

Na primeira execução do Spark, é necessário acesso à internet para baixar as bibliotecas Java do Delta e do conector de armazenamento.

### Execução pelo terminal

Também é possível executar os notebooks e salvar seus resultados pelo terminal:

```bash
uv run jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 notebooks/00_setup_sqlserver.ipynb

uv run jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 notebooks/01_sqlserver_to_minio_csv.ipynb

uv run jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 notebooks/02_csv_to_delta.ipynb

uv run jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 notebooks/03_dml_delta.ipynb

uv run jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 notebooks/04_tabelas_gerenciadas_externas.ipynb
```

Execute os comandos em ordem e prossiga após a conclusão de cada notebook.

## Resultados esperados

### Extração e conversão

- Cinco arquivos CSV no bucket `landing-zone`.
- Cinco tabelas Delta no bucket `bronze`.
- Preservação dos 32 registros durante a conversão.
- Colunas com tipos definidos, incluindo datas e valores `DECIMAL(10,2)`.

As restrições de chave primária e estrangeira do SQL Server não são transferidas para as tabelas Delta.

### Operações DML

As operações são demonstradas na tabela `produtos`:

| Etapa | Resultado |
|---|---|
| Estado inicial | Dez produtos |
| INSERT | Monitor cadastrado; onze produtos |
| UPDATE | Mouse com preço de R$ 59,90 e estoque de 15; onze produtos |
| DELETE | Caneta removida; dez produtos |

As consultas antes e depois mostram as alterações. `DESCRIBE HISTORY` apresenta as operações registradas no histórico Delta.

### Tabelas gerenciadas e externas (não gerenciadas)

O laboratório adicional cria duas cópias das categorias no MinIO:

| Tipo | Resultado após DROP TABLE |
|---|---|
| Gerenciada (interna) | Registro do catálogo e arquivos da tabela removidos |
| Externa (não gerenciada) | Registro do catálogo removido; dados e `_delta_log` preservados |

A tabela externa é registrada novamente no catálogo, apontando para o mesmo caminho, para demonstrar que seus dois registros continuam disponíveis.

As cinco tabelas principais do `bronze` são externas (não gerenciadas), pois foram registradas com `LOCATION`. O laboratório de comparação utiliza caminhos próprios dentro de `bronze/laboratorio`.

## Repetição da demonstração

Os notebooks possuem etapas para permitir novas execuções:

- O notebook 00 recria e preenche as cinco tabelas do exemplo.
- O notebook 01 substitui os CSVs pela extração atual.
- O notebook 02 sobrescreve os dados atuais das tabelas Delta.
- O notebook 03 restaura os dez produtos originais antes das operações.
- O notebook 04 limpa e recria sua área de laboratório.

## Visualização no MinIO

Abra `http://localhost:9021` e entre com as credenciais do arquivo `.env`.

Para consultá-las localmente:

```bash
uv run python - <<'PY'
from dotenv import dotenv_values

config = dotenv_values(".env")
print("Usuário:", config["MINIO_ROOT_USER"])
print("Senha:", config["MINIO_ROOT_PASSWORD"])
PY
```

No **Object Browser**, consulte:

- `landing-zone`: arquivos CSV extraídos.
- `bronze`: arquivos das tabelas Delta e da comparação adicional.

## Documentação com MkDocs

Para visualizar a documentação localmente:

```bash
uv run mkdocs serve
```

Abra o endereço informado no terminal, normalmente `http://127.0.0.1:8000`.

Para validar a geração:

```bash
uv run mkdocs build --strict
```

Para publicar no GitHub Pages, após revisar e aprovar a documentação:

```bash
uv run mkdocs gh-deploy
```

Nas configurações do GitHub Pages, selecione a branch `gh-pages` e a pasta `/ (root)`.

## Encerrar os serviços

```bash
docker compose stop
```

Para iniciá-los novamente:

```bash
docker compose up -d
```

Os volumes Docker preservam os dados entre essas execuções.

## Documentação publicada

[Consultar a documentação do trabalho](https://bfontanayasmin.github.io/trabalhoBancodeDados2/)

## Referências

- [Projeto-modelo disponibilizado pelo professor](https://github.com/jlsilva01/spark-delta-minio-sqlserver)
- Material de aula: *Ciclo de vida e pipeline de dados — Apache Spark*
- [Apache Spark 3.5.3](https://spark.apache.org/docs/3.5.3/)
- [Delta Lake](https://docs.delta.io/)
- [UV](https://docs.astral.sh/uv/)
- [pymssql](https://pymssql.readthedocs.io/)
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [MkDocs](https://www.mkdocs.org/)
