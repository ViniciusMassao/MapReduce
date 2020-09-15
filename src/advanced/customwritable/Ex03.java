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

public class Ex03 {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "ex03");

        //registrao das classes
        j.setJarByClass(Ex03.class);
        j.setMapperClass(Ex03.MapForEx03.class);
        j.setReducerClass(Ex03.ReduceForEx03.class);
        j.setCombinerClass(Ex03.CombinerForEx03.class);

        //definicao dos tipos de entrada e saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Ex03Writable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForEx03 extends Mapper<LongWritable, Text, Text, Ex03Writable> {

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
            String[] colunas = linha.split(";");


            //obtendo o valor da mercadoria
            String mercadoria = colunas[3];

            //obtendo o valor do pais
            String pais = colunas[0];

            //obtendo o valor do ano
            String ano = colunas[1];

            //obtendo o valor do fluxo
            String fluxo = colunas[4];

            //obtendo o valor da qtde
            String qntde = colunas[8];


            //obtendo apenas a mercadoria que for de importação, do brasil, no ano de 2016
            if (pais.equals("Brazil") && ano.equals("2016") && fluxo.equals("Import")) {
                if(qntde.equals(""))
                    qntde = "0";
                    con.write(new Text("mercadoria"), new Ex03Writable(mercadoria, Long.parseLong(qntde)));

            }
        }
    }

    public static class CombinerForEx03 extends Reducer<Text, Ex03Writable, Text, Ex03Writable> {
        // Funcao de reduce
        public void reduce(Text word, Iterable<Ex03Writable> values, Reducer.Context con)
                throws IOException, InterruptedException {
            //obtendo o valor da mercadoria com mais transações
            String merc = "";
            long maxmerc = Long.MIN_VALUE;
            for (Ex03Writable w : values) {
                if (w.getQtde() > maxmerc) {
                    merc = w.getMercadoria();
                    maxmerc = w.getQtde();
                }
            }
            //escrevendo o resultado final
            con.write(new Text("mercadoria"), new Ex03Writable(merc, maxmerc));
        }
    }

    public static class ReduceForEx03 extends Reducer<Text, Ex03Writable, Text, Ex03Writable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<Ex03Writable> values, Context con)
                throws IOException, InterruptedException {

            //obtendo o valor da mercadoria com mais transações
            String merc = "";
            long maxmerc = Long.MIN_VALUE;
            for (Ex03Writable w : values) {
                if (w.getQtde() > maxmerc) {
                    merc = w.getMercadoria();
                    maxmerc = w.getQtde();
                }
            }
            //escrevendo o resultado final
            con.write(new Text("mercadoria"), new Ex03Writable(merc, maxmerc));

        }
    }
}


