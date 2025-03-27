package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        //gera configurações básicas para que o hadoop funcione
        //também controla os logs, aparece várias mensagens no console
        BasicConfigurator.configure();

        //cria configuração c
        Configuration c = new Configuration();

        //arquivos são carregados a partir dos argumentos
        //argumentos são carregados na função Main de cima
        //objetivo é que usuário chame a classe indicando o arquivo de
        //entrada e de saída
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada na posição 0
        Path input = new Path(files[0]);

        // arquivo de saida na posição 1
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcountBible");

        // o que falta fazer? 4 coisas

        //registro das classes
        //definição dos tipos de saída
        //cadastro dos arquivos de entrada e saída
        // criar o exit do programa


        // 1. registro das classes
        //preciso passar para o job 3 classes

        j.setJarByClass(WordCount.class);//primeira classe: main
        j.setMapperClass(MapForWordCount.class);//classe do map
        j.setReducerClass(ReduceForWordCount.class);//classe do reduce

        //2. Definição dos tipos de saída (saída do reduce)
        //olhar na função reduce e ver quais são os tipos de saída
        //da chave e valor
        //neste exemplo a chave é text
        // neste exemplo o valor é intwritable
        j.setOutputKeyClass(Text.class); //classe da chave de saida
        j.setOutputValueClass(IntWritable.class);//classe do valor de saída

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        //4. criar exit
        System.exit(j.waitForCompletion(true)?0:1);


        // vai dar erro, precisamos passar arquivo de entrada e saida



    }


    //Função map

    /**
     * 4 tipos:
     *  - tipo de chave de entrada
     *  - tipo do valor de entrada
     *  - tipo da chave de saída
     *  - tipo da chave de saída
     */

    /**
     * Por que não uso tipos específicos do java e sim tipos esquisitos?
     * Internamente são wrapper, eles tem float, string,...
     * Precisamos utilizar porque o hadoop sabe não apenas transmitir isso pela rede, mas também salvar em disco
     * se usar tipos normais, ele não vai saber trabalhar muito bem com isso.
     *
     *
     * Lembrete: Longwritable, intwritable, floatwritable, text são wrappers.
     * Wrapper permitem que o haddop transfira objetos pela rede e também salva-los em disco.
     *
     * Nós podemos criar nossos próprios writables ou usar os padrões do hadoop.
     *
     */
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // lembrete: o map é executado por bloco de arquivo
            // porém, na realidade o map é executado por linha do arquivo

            //map recebe chave, valor e contexto
            // a linha chega no valor(value)
            //por isso vou converter a linha em string
            String linha = value.toString();

            // como faço para chegar nas palavras?
            //vamos ver o arquivo.
            //vamos separar por espaço, usar split
            //resultado será um vetor de strings

            String[] palavras = linha.split(" ");

            //próximo passo é gerar um loop para gerar as tuplas
            // lembrando que as tuplas precisam estar no formato
            // <chave=palavra, valor=frquência=1>

            for(String p: palavras){
                //criando chave de saída
                Text chaveSaida = new Text(p);
                //criando valor de saída
                IntWritable valorSaida = new IntWritable(1);


                //o que falta?
                //criar tupla e gerar a saída

                // emitir a tupla.(chave,valor)
                //Usamos o contexto
                con.write(chaveSaida, valorSaida);

                //dessa forma geramos a tupla de saída
            }
            //map é realizado para cada linha de forma automática
            // não é necessário criar um laço para linha, ele faz sozinho
            // para cada linha estou quebrando em palavras e para
            //cada palavra estamos enviando a frequencia


            // quando o write é chamado no map, o mapreduce sabe que a saída
            //vai para o sort/shufle
        }
    }

    /**
     * 4 tipos
     * -tipo da chave de entrada
     * -tipo do valor de entrada
     * -tipo da chave de saída
     * -tipo do valor de saída
     */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         *entrada key, lista
         *
         * a entrada do reduce será (chave=palavra, lista de valores=lista de ocorrencias 1s)
         *
         *
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //objetivo do reduce: fazer um loop para somar todas as ocorrencias
            // de uma palavra e no final, emitir a palavra com a ocorrência já
            //somada.

            //criar um int para guardar a minha soma e iniciar com zero
            int soma = 0;
            //vamos fazer um for
            //para somar todos os elementos da lista de uma palavra
            for (IntWritable valor: values){
                soma += valor.get(); //retorna uma int
            }

            //qial o próximo passo:
            // criar a tupla de saída com chave e valor
            //(chave =palavra, valor=soma)
            //vou salvar em disco
            // vamos salvar apenas no disco usando o con

            //não posso passar direto a soma, porque é int
            //preciso converter o int para intwritable
            con.write(key, new IntWritable(soma));

            //no reduce, o con.write escreve em arquivo.

            //quando o write é chamado no map, ele escreve no sort/shufle
            // quando o write é chamado no reduce, ele escreve em disco.


        }
    }

}


// Por que arquivo de saída é uma pasta?
// Porque o resultado também vai blocos e varias partes, por isso virou uma pasta.
// estamos simulando um hdfs, porém com um computador...
