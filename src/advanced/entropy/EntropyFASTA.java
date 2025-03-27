package advanced.entropy;

import basic.Teste;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("in/JY157487.1.fasta");

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path("output/entropia.txt");


        Job job1 = new Job(c, "etapa A");
        Job job2 = new Job(c, "etapa B");

        job1.setJarByClass(EntropyFASTA.class);
        job1.setMapperClass(MapEtapaA.class);
        job1.setReducerClass(ReduceEtapaA.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediate);

        job1.waitForCompletion(true);

        job2.setJarByClass(EntropyFASTA.class);
        job2.setMapperClass(MapEtapaB.class);
        job2.setReducerClass(ReduceEtapaB.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(BaseQtdWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, intermediate);
        FileOutputFormat.setOutputPath(job2, output);

        job2.waitForCompletion(true);

    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith(">gi")) return;
            String[] c = linha.split("");

            for(String l: c) {
                con.write(new Text(l), new LongWritable(1));
            }

            con.write(new Text("count"), new LongWritable(c.length));
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            long soma = 0;
            for (LongWritable l: values) {
                soma += l.get();
            }

            con.write(key, new LongWritable(soma));

        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String[] c = linha.split("\t");

            con.write(new Text("global"), new BaseQtdWritable(c[0], Long.parseLong(c[1])));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            long count = 0;

            for(BaseQtdWritable base: values) {
                if (base.getBase().equals("count")) {
                    count = base.getQtd();
                    break;
                }
            }

            for(BaseQtdWritable base: values) {
                if (base.getBase().equals("count")) continue;
                double valorC = base.getQtd();
                double prop = valorC / (double) count;
                double entropia = -prop * ( Math.log10(prop)/Math.log10(2) );

                con.write(new Text(base.getBase()), new DoubleWritable(entropia));
            }

        }
    }
}
