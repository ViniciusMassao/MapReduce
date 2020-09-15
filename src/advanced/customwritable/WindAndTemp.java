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

public class WindAndTemp {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-estudante");


        //registrando classes
        j.setJarByClass(WindAndTemp.class);
        j.setMapperClass(MapForWindTemp.class);
        j.setReducerClass(ReduceForWindTemp.class);

        //registrando tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(WindAndTempWritable.class);


        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWindTemp extends Mapper<LongWritable, Text, Text, WindAndTempWritable> {

        // Funcao de map
        /*
        chamada para cada linha e como resultado vai gerar (chave, valor)
        a chave eh global, entao deve existir uma chave unica (MEDIA)
        o valor eh um objeto composto por N e VALUE

         */
        public void map(Text key, Text value, Context con)
                throws IOException, InterruptedException {
            //obtendo a linha
            String linha = value.toString();

            //quabrando em colunas
            String [] colunas = linha.split(",");

            //obtendo o valor do mes
            String mes = colunas[2];

            //obtendo o valor do vento
            float wind = Float.parseFloat(colunas[10]);

            //obtendo o valor da maior temperatura
            float hightemp = Float.parseFloat(colunas[8]);

            //criando objeto composto
            WindAndTempWritable val = new WindAndTempWritable(wind, hightemp);

            //passando para o reduce no formato <mes, objeto composto>
            con.write(new Text(mes), val);
        }
    }


    public static class ReduceForWindTemp extends Reducer<Text, WindAndTempWritable, Text, WindAndTempWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<WindAndTempWritable> values, Context con)
                throws IOException, InterruptedException {
            float maxwind = Float.MIN_VALUE;
            float maxtemp = Float.MIN_VALUE;
            for(WindAndTempWritable w : values){
                if(w.getValue() > maxtemp){
                    maxtemp = w.getValue();
                }

                if(w.getWind() > maxwind){
                    maxwind = w.getWind();
                }

            }


            //emitir (chave, sum)
            con.write(word , new WindAndTempWritable(maxwind, maxtemp));

        }
    }

}
