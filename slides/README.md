[spark-logo]: spark.png
[spark-cluster]: cluster-overview.png
[meh]: meh.jpg
[shiny-eyes]: shiny-eyes.png
[yay-sql]: yaySql.png
[import-export]: import-export.jpg
[show-me-the-code]: showmethecode.jpg
[pipeline]: pipeline.png

![Apache Spark][spark-logo]

##“A Bola da Vez”

---

[Apache Spark](http://spark.apache.org/)
========================================

Uma ferramenta genérica e veloz para processamento de dados em larga escala.

- Veloz
  - Operações são distribuídas eficientemente
  - Dados ficam em memória sempre que possível, reduzindo I/O

- Fácil uso
  - APIs simples
  - com versões em Scala, Java, Python e R
  - Pode ser usado em uma shell iterativa (Exceto Java)

- Extensões facilitam as operações mais comuns:
  - **[SparkSQL](https://spark.apache.org/sql/)**: Para trabalhar com dados tabulares
  - **[Spark Streaming](https://spark.apache.org/streaming/)**: Para trabalhar com um fluxo continuo de dados em tempo real.
  - **[GraphX](https://spark.apache.org/graphx/)**: Para trabalhar com Grafos
  - **[MLib](https://spark.apache.org/mllib/)**: Para realizar aprendizado de máquina

---

[Usando Spark em uma aplicação](http://spark.apache.org/docs/latest/programming-guide.html#initializing-spark)
==============================================================================================================

```java
public class SparkTest {
    public static void main(String[] args) {
        SparkConf sparkConfig = new SparkConf()
                .setAppName("Testando o Spark")
                .setMaster("local[*]")              //Executa localmente
                //.setMaster("spark://master:7077")   //Executa em um cluster
                .setJars(new String[]{"/path/to/my.jar"});

        // sparkContext é a sua conexão com o cluster ;)
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {

            // Usa Spark para alguma coisa....

        }
    }
}
```

---

[Cluster Spark](http://spark.apache.org/docs/latest/cluster-overview.html)
==========================================================================

Um cluster Spark consiste em 3 partes:

- **Driver Program**: O programa que você escreve, onde são definidas as operações a ser realizadas.

  - Pode ser executado externamente, ou enviado para dentro do cluster Spark via `spark-submit`

- **Worker Node (*Executor*)**: Onde o processamento é executado.

- **Cluster Manager (*Spark Master*)**: O processo que intermedia tudo, alocando tarefas para os executores, priorizando, etc.

![Spark Cluster][spark-cluster]

---

# [RDDs](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds)

A estrutura básica do Spark é o RDD (*Resilient Distributed Dataset*):

- **Dataset**: Representa um conjunto de dados (*Finito!*)

- **Distributed**: Os dados são particionados, e cada partição é processada em paralelo

- **Resilient**: Se ocorre um erro, qualquer partição pode ser re-calculada

RDDs suportam operações funcionais, tais como `map()`, `filter()`, `reduce()`, etc...

---

Criando um RDD
==============

Um RDD pode ser criado de duas formas:
- [A partir de uma fonte de dados externa](http://spark.apache.org/docs/latest/programming-guide.html#external-datasets)

  - Listas (`List<T>`)

  - Arquivos

  - Bancos de dados

  - Existem plugins para tudo...


- [Aplicando transformações intermediárias em RDDs existente](http://spark.apache.org/docs/latest/programming-guide.html#transformations)

  - `map`

  - `filter`

  - `intersection`

  - `zip`

  - `sortBy`

  - `groupBy`

---

[Operações](http://spark.apache.org/docs/latest/programming-guide.html#rdd-operations)
=====================================================================================

Podemos aplicar 2 tipos diferentes de operações com um RDD:

- **Transformações**: modifica um RDD existente, retornando um novo RDD.

- **Ações**: Executa uma computação e retorna o resultado

---

[Transformações](http://spark.apache.org/docs/latest/programming-guide.html#transformations)
===========================================================================================

O resultado de uma transformação é um novo RDD.

Uma transformação deve ser:
- **Funcional**: Não alteram o RDD original, o estado da aplicação e nem causam nenhum efeito colateral.

- **Determinística**: Se a transformação for computada várias vezes, o resultado devem ser sempre o mesmo.

- **Lazy**: O resultado da transformação não é armazenado, apenas a "receita". 

  Os dados são computados sob demanda, e (geralmente) descartados em seguida.

---

Principais transformações
=========================

- `map(func)` - Aplica `func` a cada elemento do RDD original

- `filter(func)` - Mantém os elementos do RDD original onde `func(obj)` retorna true

- `mapPartitions` - Recebe a partição inteira e retorna todos os resultados da transformação em uma única chamada (Ideal para realizar operações em lote)

- `sample(fraction)` - Retorna um novo RDD com uma amostra do RDD original

- `union(other)` - Retorna um novo RDD com a união (concatenação) dos dados dos RDDs original e `other`.

- `distinct()` - Remove elementos duplicados

- `sortBy(func)` - Cria um novo RDD com os elementos do RDD original ordenados

- `repartition(numPartitions)` - novo RDD com os elementos do RDD original distribuidos em uma quantidade diferente de partições.

---

[Ações](http://spark.apache.org/docs/latest/programming-guide.html#actions)
===========================================================================

Uma ação processa todos os dados do RDD, retornando um resultado (Agregação) ou causando efeitos colaterais (Salvando no banco, etc).

Os RDDs e suas transformações não são computados até que uma ação seja executada.

O processamento de uma ação é um **Job**

---

Principais ações
================

- `reduce(func)`: Usa a função para agregar todos os elementos no RDD e retorna o resultado

- `foreach(func)`: Executa a função com cada elemento do RDD

- `count()`: tamanho do RDD

- `collect()`: Transforma o RDD em uma lista (Assumindo que o resultado cabe na memória...)

- `take(n)`: Retorna os primeiros N elementos em uma lista

- `takeSampled(n)`: Retorna N elementos aleatórios do RDD

- `takeOrdered(n)`: Ordena e retorna N primeiros elementos

- `saveAsObjectFile(path)`: Grava os dados do RDD em um arquivo

---

background-image: url(showmethecode.jpg)

---

Tipos de transformações
=======================

Pipelines
---------

Muitas operações podem ser aplicadas em cada elemento (ou partição) isoladamente - É o caso de `map()`, `filter()`, etc.

Nestes casos, é possível aplicar todas as operações em série, eficientemente (*Pipeline*). Não há custo de I/O, e valores intermediários não precisam sequer ser armazenados!

Uma série de operações deste tipo consiste em um **Estágio** (*Stage*). O cálculo de um estágio em uma determinada partição é uma **Tarefa** (*Task*).

![Pipeline][pipeline]

---

Tipos de transformações
=======================

[Shuffle](http://spark.apache.org/docs/latest/programming-guide.html#shuffle-operations)
----------------------------------------------------------------------------------------

Outras operações necessitam mover dados entre partições/servidores - É o caso de `sortBy`, `groupBy`, `distinct`, `repartition`, etc.

Para efetuar esses operações, é preciso computador o estágio anterior por inteiro (Todas as partições), serializar os dados, transmitir via rede e remontar o resultado no destino.

Apesar do Spark oferece implementações eficientes, estas são operações **muito custosas**.

---

[Persistência](http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence)
==========================================================================================

Em geral, RDDs são lazy, isto é, calculados sob demanda, **A CADA ACESSO**.

Por exemplo:

```java
JavaRDD<Foo> slowOperationResultRdd = rdd.map(slowOperation);

int resultCount = slowOperationResultRdd.count();

int resultSum = slowOperationResultRdd.reduce((a,b) -> a+b);
```

Neste caso, `slowOperation` será executada 2 vezes, uma durante `count()` e outra durante `reduce()`.

É possível evitar esse problema utilizando um cache:

```java
JavaRDD<Foo> slowOperationResultRdd = rdd.map(slowOperation).cache();
```

É claro, a economia de CPU será compensada por um uso maior de memória.

Em alguns casos, pode valer a pena recomputar ;)

---

Persistência
============

Usando `rdd.cache()` ou `rdd.persist(level)`, o Spark irá armazenar e reutilizar os resultados deste RDD.

Os principais niveis de cache são:

- **MEMORY_ONLY (Padrão)**: Objetos são armazenados na memória da JVM. Muito rápido, porém utiliza muita RAM. Se não houver espaço suficiente em RAM, o cache é descartado e o RDD é re-calculado.

- **MEMORY_ONLY_SER**: Similar a `MEMORY_ONLY`, porém os objetos são armazenados em RAM na forma de um buffer serializado. Usa menos RAM, porém exige uma quantide considerável de CPU a cada acesso.

- **MEMORY_AND_DISK**: Similar a `MEMORY_ONLY`, porém se não houver espaço suficiente em RAM, os dados são armazenados em disco.

- **MEMORY_AND_DISK_SER**: `MEMORY_AND_DISK` + `MEMORY_ONLY_SER`

Quando os dados em cache não são mais necessários, é possível remove-los explicitamente com `rdd.unpersist()`.


---

Caso especial: DoubleRDD
========================

Equivalente a `RDD<Double>`, adiciona algumas operações comuns:

- `mean()`: Média

- `sum()`: Soma

- `stdev()`: Desvio padrão

- `variance()`: Variância

- `stats()`: Todos os anteriores ;)

- `histogram()`: Histograma

Para obter um `DoubleRDD` a partir de um `RDD`, basta usar `rdd.mapToDouble(...)`.

![Meh][meh]

---

[Caso especial: PairRDD<K,V>](http://spark.apache.org/docs/latest/programming-guide.html#working-with-key-value-pairs)
=======================================================================================================

Muitas operações só podem ser feitas com pares chave-valor, principalmente aquelas relacionadas a agrupamento.

O tipo `PairRDD<K,V>` (Equivalente a `RDD<Pair<K, V>>`) adiciona operações como:

- `countByKey`: Conta número de elementos com cada chave.

- `reduceByKey(func)`: Agrupa elementos por chave e usa `func()` para agregar cada grupo.

- `sortByKey()`: Retorna um RDD ordenado pela chave.

- `join(other)`: Realiza um inner-join entre RDDs.

A maioria das operações `XxxByKey()` são do tipo *shuffle*, portanto bastante custosas.

Para obter um `PairRDD` a partir de um `RDD`, basta usar `rdd.mapToPair(...)` ou `rdd.groupBy`.

![Yay][shiny-eyes]

---

Extensões
=========

- O Spark trata o conteúdo dos RDDs como caixas-pretas

  - Dado pode ter qualquer tipo

  - Você pode aplicar qualquer operação nele

  - O Spark não precisa saber nada.

- Abordagem é bastante flexível, mas evita que os casos mais comuns sejam tratados de forma mais amigável e otimizada.

- Para estes casos, existem extensões:

  - **[SparkSQL](https://spark.apache.org/sql/)**: Para trabalhar com dados tabulares
 
  - **[Spark Streaming](https://spark.apache.org/streaming/)**: Para trabalhar com um fluxo continuo de dados em tempo real.
 
  - **[GraphX](https://spark.apache.org/graphx/)**: Para trabalhar com Grafos

  - **[MLib](https://spark.apache.org/mllib/)**: Para realizar aprendizado de máquina


- SparkSQL é provavelmente a extensão mais interessante...
---

[SparkSQL](https://spark.apache.org/sql/)
=========================================

Um dos casos mais comuns é que os dados sejam tabulares:

- Todos os dados possuem os mesmos atributos (colunas)

- Cada coluna tem um nome e um tipo pré-determinado

O SparkSQL provê a classe `DataFrame`, uma abstração construída para dados tabulares:

- Internamento construido sobre RDDs

- Permite realizar transformações, filtros, joins, etc, de forma simples

- Estruturado de forma similar a SQL

---

[SparkSQL ⟺ RDD](http://spark.apache.org/docs/latest/sql-programming-guide.html#interoperating-with-rdds)
===========================================================================================================

É fácil converter dataframes em RDDs:

```java
RDD<Row> rdd = dataframe.rdd()
```

e RDDs em DataFrames:

```java
//Por reflexão, no caso de beans simples:
RDD<Record> records = ...
DataFrame dataframe = sparkSqlContext
    .createDataFrame(records, Record.class);
```

```java
//Ou definindo o mapeamento e o schema manualmente
StructType schema = DataTypes.createStructType(Arrays.asList(
    DataTypes.createStructField("field1", DataTypes.StringType),
    DataTypes.createStructField("field2", DataTypes.IntegerType)
));


RDD<Row> recordAsRows = records
    .map((record) -> RowFactory.create(record.field1, record.field2));

DataFrame dataframe = sparkSqlContext.createDataFrame(recordAsRows, schema);
```

---

[Transformações em um Dataframe](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations)
=====================================================================================================================

É possível aplicar filtros (`where`), transformações (`select`) e ordenação (`orderBy`) em um dataframe, de forma análoga à uma consulta SQL:

```java
DataFrame olderPeopleNamesAndYear = peopleDataframe
    .where(
        peopleDataframe.col("age").geq(30) //age >=30
    )
    .orderBy(
        peopleDataframe.col("age")
    )
    .select( 
        peopleDataframe.col("name"), 
        peopleDataframe.col("age").multiply(-1).plus(2015).as("birth")
    );
```

*(É **bem** mais bonito em Python e Scala, com sobrecarga de operadores...)*

---

[Transformações em um Dataframe](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations)
=====================================================================================================================

Também é possível fazer agregações (`Group By`):

```java
DataFrame meanAgeByLanguage = peopleDataframe
    .groupBy("favoriteLanguage")
    .agg(functions.mean(peopleDataframe.col("age")).as("meanAge"))
    .orderBy("meanAge");
```

E Joins:

```java
DataFrame peopleAndLanguagePopularity = peopleDataframe
    .join(
        languagesDataframe, 
        peopleDataframe.col("favoriteLanguage").equalTo(languagesDataframe.col("name")));
```

---

[Usando um Dataframe com SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#running-sql-queries-programmatically)
==================================================================================================================================

Outra maneira mais compacta é definir as expressões do `select`, `where`, etc, com Strings:

```java
DataFrame olderPeopleNamesAndYear2 = peopleDataframe
    .where("age >= 30")
    .orderBy("age")
    .selectExpr("name", "age + 1 as birth");         
```

Chutando o balde, podemos usar SQL de uma vez!

```java
peopleDataframe.registerTempTable("people"); //Associa um nome ao DataFrame
DataFrame olderPeopleNamesAndYear3 = sqlSparkContext
    .sql("select name, 2015-age as birth from people where age >= 30");
```

![SQL!][yay-sql]

---

[Importação e Exportação de Dados](http://spark.apache.org/docs/latest/sql-programming-guide.html#generic-loadsave-functions)
=============================================================================================================================

- É comum ler dados de um DB para o Spark e gravar os resultados de volta para o DB (Ou arquivo, etc)

- Porém, é dificil fazê-lo de forma genérica, pois o tipo do RDD é opaco.

- Dataframes, por outro lado, podem ser facilmente/automaticamente mapeados para banco de dados, arquivos CSV, documentos JSON, etc...

- A leitura e escrita é feita via plugins, e suporta praticamente tudo...

---

[Importação de Dados](http://spark.apache.org/docs/latest/sql-programming-guide.html#generic-loadsave-functions)
================================================================================================================

A partir do Oracle:

```java
DataFrame jdbcDataframe = sqlSparkContext.read()
    .format("jdbc")
    .option("url", "jdbc:oracle:thin:user/password@phoenix:1521/gfn")
    .option("dbtable", "ADMIN.EMPRESA")
    .load();
```

Ou do mongo:

```java
DataFrame mongoDataframe = sqlSparkContext.read()
    .format("com.stratio.datasource.mongodb")
    .option("host", "localhost")
    .option("database", "company_0")
    .option("collection", "smartReport")
    .load();
```

Ou de um arquivo CSV:

```java
DataFrame csvDataframe = sqlSparkContext.read()
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .load("/home/pcosta/Downloads/Parking_Citations.csv");
```

---

[Exportação de Dados](http://spark.apache.org/docs/latest/sql-programming-guide.html#generic-loadsave-functions)
================================================================================================================

É similar à importação:

```java
csvDataframe.write()
                .format("com.stratio.datasource.mongodb")
                .option("host", "localhost")
                .option("database", "spark")
                .option("collection", "csv2mongo")
                .mode(SaveMode.Overwrite)
                .save();
```

![Import and Export][import-export]

---

Importador de arquivos CSV
==========================

Durante os últimos sprints, começamos a migrar o importador para usar Storm para aumentar a capacidade e velocidade de processamento.

Também testamos o Spark como base de um importador simples, com inferência de tipos.

O resultado é bastante simples, compacto e roda rápido.

---

background-image: url(thatsall.jpg)

