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

public class Ex07 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "ex07");

        //registrao das classes
        j.setJarByClass(Ex07.class);
        j.setMapperClass(Ex07.MapForEx07.class);
        j.setReducerClass(Ex07.ReduceForEx07.class);
        j.setCombinerClass(Ex07.CombinerForEx07.class);

        //definicao dos tipos de entrada e saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForEx07 extends Mapper<LongWritable, Text, Text, IntWritable> {

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
            String fluxo = colunas[4];

            //obtendo a mercadoria
            String ano = colunas[1];

            //criando objeto composto
            //Ex07Writable chave = new Ex07Writable(fluxo, ano);
            Text chave = new Text(fluxo+"\t"+ano);

            //para cada coluna de brasil, emitir (mercadoria, 1)
            con.write(chave, new IntWritable(1));
        }
    }

    public static class CombinerForEx07 extends Reducer<Text, IntWritable, Text, IntWritable> {
        //funcao de combine para deixar mais rapido o trabalho do reduce

        public void reduce(Text chave, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            //variavel de contador
            int sum = 0;
            //loop para somar a quantidade de transacoes de acordo com a chave
            for(IntWritable i : values){
                //somando na variavel
                sum += i.get();
            }
            //escrevendo e passando para o reduce
            con.write(chave,new IntWritable(sum));
        }
    }

    public static class ReduceForEx07 extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text chave, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            //variavel de contador
            int sum = 0;
            //loop para somar a quantidade de transacoes de acordo com a chave
            for(IntWritable i : values){
                //somando na variavel
                sum += i.get();
            }
            //escrevendo no arquivo
            con.write(chave,new IntWritable(sum));
        }
    }

}
