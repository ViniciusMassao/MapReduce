package advanced.customwritable;


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

import java.io.IOException;

public class Ex01 {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "ex01");

        //registrao das classes
        j.setJarByClass(Ex01.class);
        j.setMapperClass(Ex01.MapForEx01.class);
        j.setReducerClass(Ex01.ReduceForEx01.class);
        j.setCombinerClass(Ex01.CombinerForEx01.class);

        //definicao dos tipos de entrada e saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);




        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForEx01 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //obtendo a linha
            String linha = value.toString();

            //quabrando em colunas
            String [] colunas = linha.split(";");

            //obtendo a mercadoria
            String mercadoria = colunas[3];


            //para cada coluna de brasil, emitir (mercadoria, 1)
                if(colunas[0].equals("Brazil")){
                    con.write(new Text(mercadoria), new IntWritable(1 ));
                }


        }
    }

    public static class CombinerForEx01 extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Reducer.Context con)
                throws IOException, InterruptedException {
            //somando os valores que chegam na lista

            int sum = 0;

            //percorrendo a lista e somando Ns
            for(IntWritable v : values){
                sum += v.get();
            }

            //escrevendo o resultado final
            con.write(word, new IntWritable(sum));
        }

    }

    public static class ReduceForEx01 extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            //percorrendo a lista e somando Ns
            for(IntWritable v : values){
                sum += v.get();
            }

            //escrevendo o resultado final
            con.write(word, new IntWritable(sum));



        }
    }

}
