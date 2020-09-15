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
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-professor");

        //cadastro das classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        //definicao de tipos
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     classe de map

     1o parametro: tipo da chave de entrada
     2o parametro: tipo do valor da entrada
     3o parametro: tipo da chave de saida
     4o parametro: tipo do valor de saida
     */


    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //value eh uma linha do arquivo de entrada
            String linha = value.toString();

            //quebrando em palavras
            String[] palavras = linha.split(" ");


            //emitir (palavra 1) para cada palavra do arrau declarado acima
            for(String p : palavras){

                //criando a chave
                Text outputKey = new Text(p);

                //criando o valor
                IntWritable outputValue = new IntWritable(1);

                //enviando isso para sort/shuffle e depois para o reduce
                con.write(outputKey, outputValue);
            }



        }
    }
    /**
     classe de reduce

     1o parametro: tipo da chave de entrada
     2o parametro: tipo do valor da entrada
     3o parametro: tipo da chave de saida
     4o parametro: tipo do valor de saida
     */

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

                //somando os valore da lista
                int sum = 0;
                for(IntWritable w : values){
                    sum += w.get();
                }

                //escrevendo o resultado final
                con.write(word, new IntWritable(sum));
        }
    }

}
