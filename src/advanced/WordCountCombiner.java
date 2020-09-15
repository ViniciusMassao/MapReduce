package advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Iterator;

public class WordCountCombiner {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcountcombiner-estudante");

        //registrar as classes
        j.setJarByClass(WordCountCombiner.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setCombinerClass(CombinerForWordCount.class);

        //registrando tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);


        //atributos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //lendo a linha
            String linha = value.toString();

            //quebrando em palavras
            String[] palavras = linha.split(" ");

            //para cada palavra, emitir (palavra, 1)
            for (String p : palavras)
                con.write(new Text(p), new IntWritable(1 ));
        }
    }

    public static class CombinerForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Reducer.Context con)
                throws IOException, InterruptedException {
            //somando os valores que chegam na lista
            int sum = 0;
            for(IntWritable v : values){
                sum += v.get();
            }

            //emitir (chave, sum)
            con.write(word, new IntWritable(sum));
        }

    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //somando os valores que chegam na lista
            int sum = 0;
            for(IntWritable v : values){
                sum += v.get();
            }

            //emitir (chave, sum)
            con.write(word, new IntWritable(sum));
        }
    }

}
