package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //1. registrar classes
        //2. definir tipos de saídas
        //3. cadastrar os arquivos de entrada e saída
        //4. lancar o job

        // 1. registro de classes
        j.setJarByClass(AverageTemperature.class);// classe main
        j.setMapperClass(MapForAverage.class);//classe map
        j.setReducerClass(ReduceForAverage.class);//classe reduce

        //2. tipos de saídas
        //em alguns casos é importante mapear as saídas do map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        // descendo do reduce
        // text e floatwritable
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);


        //3. cadastrar arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j,output);

        System.exit(j.waitForCompletion(true)?0:1);

    }

    //funcao de map é sempre realizada por bloco
    //na verdade é sempre realizada por linha

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //obter o conteudo da linha
            //obter o conteudo da coluna 8
            //no fim emitir a chave e valor
            // chave deve ser um elemento comum
            // valor deve ser um elemento composto (soma = valor, n=1)

            //vamos ver o arquivo

            //pegar a linha
            String linha = value.toString();

            // agora vamos pegar o conteudo da coluna 8
            //fazer split por virgula
            String[] colunas = linha.split(",");

            //vou pegar conteudo da coluna 8 e guardar em variavel
            float temp = Float.parseFloat(colunas[8]);

            //enviar para o sort/shufle
            //con.write(new Text("auxiliar"), new FireAvgTempWritable(1,temp));
            // usar sempre a mesma chave para garantir que todos os resultados
            //vao para o mesmo reduce

            // vamos precisar criar uma classe chamada writable
            // a classe é usada para passar dois valores com um valor.

            //por mês
            con.write(new Text(colunas[2]), new FireAvgTempWritable(1,temp));


        }
    }
// vamos comentar esta classe por enquanto
//    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
//        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
//                throws IOException, InterruptedException {
//        }
//    }

    /**
     * Recebe como entrada <chave, lista de valores>
     *     chave: "auxiliar"
     *     lista de valores: lista que contempla diferentes FireAvgTempWritables(n,soma)
     *
     *  objetivo: somar todas as somas
     * somar todos os ns
     * dividir as somas das somas pela soma dos ns
     *
     */
    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //criar atributos
            int somaN = 0;
            float somaSoma =0;

            //fazer um for para somar todas as somas
            for(FireAvgTempWritable obj: values){
                somaN += obj.getN();
                somaSoma += obj.getSoma();
            }

            // posso calcular a média
            float media = somaSoma/somaN;

            //falta o que?
            // escrever no contexto
            //con.write(new Text("media"), new FloatWritable(media));

            //por mês
            con.write(new Text(key), new FloatWritable(media));


        }
    }

}
