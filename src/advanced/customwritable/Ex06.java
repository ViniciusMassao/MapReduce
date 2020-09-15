package advanced.customwritable;


import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class Ex06 {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "ex06");

        //registrao das classes
        j.setJarByClass(Ex06.class);
        j.setMapperClass(Ex06.MapForEx06.class);
        j.setReducerClass(Ex06.ReduceForEx06.class);
        j.setCombinerClass(Ex06.CombinerForEx06.class);

        //definicao dos tipos de entrada e saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Ex06Writable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);




        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForEx06 extends Mapper<LongWritable, Text, Text, Ex06Writable> {

        // Funcao de map
        /*
        chamada para cada linha e como resultado vai gerar (chave, valor)
        a chave eh global, entao deve existir uma chave unica (MEDIA)
        o valor eh um objeto composto por N e VALUE

         */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //obtendo a linha
            String linha = value.toString();

            //quabrando em colunas
            String [] colunas = linha.split(";");

            //obtendo a unidade de medida
            String unidade = colunas[7];

            //obtendo a mercadoria
            String mercadoria = colunas[3];

            //obtendo o preco da mercadoria
            float preco = Float.parseFloat(colunas[5]);

            //criando objeto composto
            Ex06Writable val = new Ex06Writable(mercadoria, preco);

            //para cada coluna emitir(unidade de medida e val(mercadoria e preco))
            con.write(new Text(unidade), val);
        }
    }

    public static class CombinerForEx06 extends Reducer<Text, Ex06Writable, Text, Ex06Writable> {
        // Funcao de reduce
        public void reduce(Text word, Iterable<Ex06Writable> values, Reducer.Context con)
                throws IOException, InterruptedException {
            float maxpreco = Float.MIN_VALUE;
            String merc = " ";
            for(Ex06Writable w : values){
                if(w.getPreco() > maxpreco){
                    merc = w.getMercadoria();
                    maxpreco = w.getPreco();
                }
            }

            //escrevendo o resultado final
            con.write(word , new Ex06Writable(merc, maxpreco));
        }

    }

    public static class ReduceForEx06 extends Reducer<Text, Ex06Writable, Text, Ex06Writable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<Ex06Writable> values, Context con)
                throws IOException, InterruptedException {

            float maxpreco = Float.MIN_VALUE;
            String merc = " ";
            for(Ex06Writable w : values){
                if(w.getPreco() > maxpreco){
                    merc = w.getMercadoria();
                    maxpreco = w.getPreco();
                }
            }

            //escrevendo o resultado final
            con.write(word , new Ex06Writable(merc, maxpreco));



        }
    }

}
