1- Qual o objetivo do comando cache em Spark? 

O Spark suporta a inserção de conjuntos de dados em um cache de memória em todo o cluster. Isso é muito útil quando os dados são acessados ​​repetidamente, ou ao executar um algoritmo iterativo. Um conjunto de dados pode ser armazenado em cache, apenas quando  uma ação for executada, visto que essa operação é lazy (Preguiçosa), não ocorre imediatamente.
Isso permite que ações futuras sejam muito mais rápidas.

2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê? 

A capacidade de armazenamento dos discos cresceu massivamente. No entanto, a velocidade de acesso aos dados não acompanhou esse crescimento.  
A principal diferença entre eles está na abordagem do processamento: o Spark pode fazer isso na memória, sendo Tolerante a falha , com baixa latência.Enquanto o Hadoop MapReduce precisa ler e gravar em disco. 
Como resultado, a velocidade de processamento difere significativamente. 
O Apache Spark com armazenamento in-memory apresenta até 100 vezes mais melhoria na performance. Com o Spark Streaming o processamento chega em real time ,  onde posso utilizar os mesmos códigos tanto no Apache Spark quanto no Componente Spark Streaming , dependendo do meu caso de uso…. 

3 - Qual é a função do SparkContext? 

Toda aplicação Spark requer um Spark Context  é o ponto de entrada da Spark API. 
O Spark Shell instancia um contexto e o habilita com o nome sc. 
Como um driver.

Um exemplo seria :
ligar o Python puro ao Kernel do Spark
Chamo o contexto pyspark , no terminal ,  executo sc no jupyter notebook por exemplo. Inicializo o contexto do Spark

ou de uma maneira mais eficiente , criou uma estrutura de projetos dentro do próprio diretório Spark . com a inicialização imediata do contexto. digitando pyspark no terminal.

4 - Explique com suas palavras o que é RDD?

RDDs são a unidade fundamental do Spark , é uma coleção de objetos distribuídos e imutáveis. Caso os dados em memória são perdidos, eles podem ser recriados, o armazenado em memória é feito através do cluster.  Os dados podem vir de um arquivo ou serem criados por meio de um programa, ou através de outro RDD.
O Spark trabalha em modo Lazy (Preguiçoso) , com ações e transformações.
Ele não irá executar ate receber uma ação, como: count(), collect(), take() reduce(), por exemplo, que nesse caso seria um comando de ação.
Transformação são operações que retornam um novo RDD. ação são operações que retorna um valor (resultado) .

5 - GroupByKey é menos eficiente que ReduceByKey em grandes dataset. Por quê? 

GroupByKey - é mais custoso , quando se trabalha em grandes volumes de dados , podendo ocorrer derramamento de dados para o disco, gerando exceção de memória insuficiente. Transfere todo dataset pela rede , usa shuffle no inicio 

ReduceByKey  - não usa shuffle no inicio.  aplica redução e retorna o resultado da função, Calcula somas locais para cada chave e partição. Isso causa uma redução significativa no tráfego na rede. O único problema é que os valores para cada chave precisam ser do mesmo tipo de dados. Se houver tipos de dados diferentes, ele deverá ser convertido explicitamente. Essa desvantagem pode ser resolvida usando combineByKey.

6 - Explicação do código Scala.

Cria um RDD com todo conteúdo de um arquivo de texto alocado no HDFS , através do SparkContext sc.
A operação Map () se aplica a cada elemento do RDD e retorna o resultado como novo RDD.  Enquanto o FlatMap () semelhante ao Map, permite retornar 0, 1 ou mais elementos da função map. Fazendo split() de palavra por palavra , através do espaço. mapeando chave-valor com a palavra sendo a chave e o valor 1.
O comando reduceByKey então irá reduzir todas as ocorrências de palavras repetidas,  o código finaliza salvando o resultado no HDFS.
 
