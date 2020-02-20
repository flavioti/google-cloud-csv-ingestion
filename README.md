# google-cloud-csv-ingestion

## Sobre esse documento

- Para visualizar os diagramas de sequência, recomendo usar o plugin Markdown Preview Mermaid Support, disponível no VS Code.
- Outra opção é acessar [Mermaid Live Editor](https://mermaid-js.github.io/mermaid-live-editor), copiar e colar os diagramas para poder visualiza-los.
- Acima de cada diagrama existe um link para visualização alternativa em SVG

## Versionamento de código fonte

### Github

Vinculei minha chave pública na minha conta no Github e criei um repositório público, com o nome [google-cloud-csv-ingestion](https://github.com/flavioti/google-cloud-csv-ingestion) 


### Máquina local

```sh
git clone git@github.com:flavioti/google-cloud-csv-ingestion.git
```

Como boas práticas usei o padrão git flow. Instalei a ferramenta abaixo:

```sh
sudo apt-get install git-flow
```

## Configuração do CLI (gcloud)

```sh
gcloud config configurations list
gcloud config configurations create pessoal
gcloud config configurations activate pessoal
gcloud config set account flaviomarcioti@gmail.com
gcloud config set project treinamento-254613
```

## Criação do dataset e tabela do BigQuery

```sh
bq mk --location=us --dataset treinamento-254613:csv_ingestion

```

## Python (Preparação do ambiente)

```sh
sudo pip install virtualenv
type python3
virtualenv --python='/usr/bin/python3' python3
source python3/bin/activate
python --version
pip install google-cloud-bigquery
```

## Arquitetura


```mermaid
graph LR;
    A[Cloud Storage]
    B[Data fusion]
    C[BigQuery]
    A-->B-->C
```

## Volume de dados

Não vale a pena usar o Dataflow porque o Bruno falou que não compensa, então pronto :)



Alguns registros não tem referência nos arquivos
('TA-21197', {'price_quote': ['TA-21197,S-0026,2009-07-30,3,1,No,1,53.6186242161549'], 'bill_of_materials': ['TA-21197,C-1733,1,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA']})
('TA-21198', {'price_quote': [], 'bill_of_materials': ['TA-21198,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA']})
('TA-21199', {'price_quote': [], 'bill_of_materials': ['TA-21199,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA']})




As alternatvas abaixo não foram utilizadas pois não há volume de dados suficiente que justifique a aplicação

[Alternativa A](sequence-diagrams/alternativa_a.svg) 

```mermaid
graph LR;
    A[Python Publisher]
    B[Google Pubsub]
    C[BigQuery]
    D[Data Studio]
    A-->B-->C-->D
```
Por serem somente 3 arquivos CSV pequenos que provavelmente serão ingeridos em lote, não vejo a necessidade de usar mensageria.

[Alternativa B](sequence-diagrams/alternativa_b.svg) 

```mermaid
graph LR;
    A[Python Publisher]
    B[Google Pubsub]
    C[Dataflow]
    D[BigQuery]
    E[Data Studio]
    A-->B-->C-->D-->E
```

[Alternativa C](sequence-diagrams/alternativa_c.svg) 

```mermaid
graph LR;
    A[bq load]
    B[BigQuery]
    C[BigQuery Join]
    D[BigQuery]
    E[Data Studio]
    A-->B-->C-->D-->E
```