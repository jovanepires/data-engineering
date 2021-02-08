# Processo Seletivo "Data Engineer"

O Passei Direto é uma plataforma aberta com um modelo de negócios freemium, baseado em
assinaturas. É muito importante para a saúde do negócio entendermos bem as características
dos nossos usuários, principalmente os pagantes.
Nessa etapa será usada a BASE A, composta pelos seguintes Datasets:  


## ETAPA 1 

- **students.json** - Amostra de usuários que acessaram o Passei Direto no mês de
Novembro de 2017. Todos os outros datasets referenciam esses usuários
- **sessions.json** - Visitas que os usuários fizeram ao longo do mês
- **subscriptions.json** - Assinaturas dos usuários que aderiram ao Plano Premium do
Passei Direto
- **universities.json** - Lista de Universidades cadastradas no Passei Direto
- **courses.json** - Lista de Cursos cadastrados no Passei Direto
- **subjects.json** - Lista de Disciplinas cadastradas no Passei Direto
- **student_follow_subject.json** - Disciplinas que cada usuário está seguindo

### Modelagem utilizada

Seguindo a orientação de definir a modelagem de uma base analítica para que seja otimizada a análise do perfil dos usuários e suas segmentações relevantes, foi elaborada a seguinte matriz:

| fato \ dimensão                                  | tempo | localização | universidade | curso | plano |
|------------------------------------|-------|-------------|--------------|-------|-------|
| Quantidade de usuários registrados | x     | x           | x            | x     |       |
| Acessos realizados na plataforma   | x     | x           | x            | x     |       |
| Assinaturas                        | x     | x           | x            | x     | x     |
   
### Carga dos dados

Os dados foram disponibilizados em formato `json`, porém há a intenção de que seja utilizado em um ambiente transacional. Desta forma, escolhi utilizar a abordagem de criar uma área de *staging*, funcionando como um repositório intermediário entre o ambiente de produção e o ambiente destinado às análises (data warehouse). 

A premissa é que ao popular o ambiente `staging`, sejam filtrados apenas os dados desde a ultima carga.

Quando executado o pipeline `dw_pipeline.py`, este irá popular todas as tabelas `dimensões` e `fatos`, conforme definidas na matiz acima.

> Observação: todas as `fatos` foram pensadas com o objetivo de funções de agregação, ex.: total de assinaturas(fato) por região(dimensão), seria utilizada a função `count` da linguagem SQL.

### Confirguração do Ambiente

Para configurar o ambiente, executar o script na raiz do projeto:
```sh
$ chmod +x ./setup.sh
$ ./setup.sh
```   

### Execução dos Pipelines

Para configurar o ambiente, executar o script na raiz do projeto:
```sh
$ chmod +x ./start.sh
$ ./start.sh
```


### Melhorias   
- Pré-processamento para valores nulos
- Modelar as `fatos` de forma que minimizem o uso de funções de agregação
- Otimização dos tipos de dados das colunas
- Otimização dos indices
- Testes unitários
- Ferramenta para gerenciamento dos pipelines (Ex.: Airflow)

## ETAPA 2

Nessa etapa será usada a BASE B, composta de apenas um Dataset:
- **part-[0000x].json** - Eventos de Page View que nossos usuários realizaram no dia 16 de Novembro de 2017. Esse Dataset foi dividido em alguns arquivos.


## Análise Exploratória

Na análise exploratória, foi observada a seguinte estrutura dos eventos: 

```
root
 |-- Last Accessed Url: string (nullable = true)
 |-- Page Category: string (nullable = true)
 |-- Page Category 1: string (nullable = true)
 |-- Page Category 2: string (nullable = true)
 |-- Page Category 3: string (nullable = true)
 |-- Page Name: string (nullable = true)
 |-- at: string (nullable = true)
 |-- browser: string (nullable = true)
 |-- carrier: string (nullable = true)
 |-- city_name: string (nullable = true)
 |-- clv_total: long (nullable = true)
 |-- country: string (nullable = true)
 |-- custom_1: string (nullable = true)
 |-- custom_2: string (nullable = true)
 |-- custom_3: string (nullable = true)
 |-- custom_4: string (nullable = true)
 |-- device_new: boolean (nullable = true)
 |-- first-accessed-page: string (nullable = true)
 |-- install_uuid: string (nullable = true)
 |-- language: string (nullable = true)
 |-- library_ver: string (nullable = true)
 |-- marketing_campaign: string (nullable = true)
 |-- marketing_medium: string (nullable = true)
 |-- marketing_source: string (nullable = true)
 |-- model: string (nullable = true)
 |-- name: string (nullable = true)
 |-- nth: long (nullable = true)
 |-- os_ver: string (nullable = true)
 |-- platform: string (nullable = true)
 |-- region: string (nullable = true)
 |-- session_uuid: string (nullable = true)
 |-- studentId_clientType: string (nullable = true)
 |-- type: string (nullable = true)
 |-- user_type: string (nullable = true)
 |-- uuid: string (nullable = true)
 ```

 Podem haver variações na estrutura, como o campo `first-accessed-page` que aparece em alguns eventos, mas é possivél notar que `custom_1` corresponde a universidade do usuário, e `custom_2` o seu curso. Já o campo `custom_4` funciona como uma classificação do perfil de usuário.

```
+-----------------------------------------------------------+
|collect_set(custom_4)                                      |
+-----------------------------------------------------------+
|[Core User, unknown, Casual User, Cold User, New User, yes]|
+-----------------------------------------------------------+
```

Utilizando as funções de agregação do `pyspark`, podemos ver que o curso com mais usuários na plataforma (para o período de amostragem) é `Direito`:

```
+-------------------------------------+------+
|custom_2                             |count |
+-------------------------------------+------+
|null                                 |100653|
|Direito                              |96020 |
|Administração                        |53755 |
|unknown                              |43128 |
|Engenharia Civil                     |33256 |
|Pedagogia                            |32081 |
|Contabilidade / Ciências Contábeis   |30788 |
|Fisioterapia                         |19321 |
|Psicologia                           |17719 |
|Nutrição                             |16534 |
|Enfermagem e Obstetrícia             |14978 |
|Engenharia de Produção               |14371 |
|Educação Física                      |10913 |
|Gestão de Recursos Humanos           |10249 |
|Análise e Desenvolvimento de Sistemas|10028 |
|Engenharia Elétrica                  |9603  |
|Engenharia Mecânica                  |9246  |
|Farmácia / Ciências Farmacêuticas    |8282  |
|Sistemas de Informação / Informática |7169  |
|Serviço Social                       |6229  |
+-------------------------------------+------+
```

## Executando 

Utilizando docker para executar o `eda.ipynb`: 

```
docker run --name sparkbook -p 8881:8888 -v "$PWD":/home/jovyan/work jupyter/pyspark-notebook start.sh jupyter lab --LabApp.token=''
```

O comando acima irá baixar a imagem (na primeira execução), e executar o *container*. Para acessar o `Jupyter Lab`, basta digitar o endereço `http://localhost:8881` no navegador.

## Melhorias

- Pré-processamento para valores nulos
- Buscar mais insights relacionando as fontes de dados
- Criar pipeline pra automatizar a ingestão de dados
- Utilizar `spark submit` para `deploy` do pipeline
- Testes unitários
- Ferramenta para gerenciamento dos pipelines (Ex.: Airflow)