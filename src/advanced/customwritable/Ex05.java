package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class Ex05 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "ex05");

        // Cadastro das classes
        j.setJarByClass(Ex05.class);
        j.setMapperClass(Ex05.MapForEx05.class);
        //j.setCombinerClass(Ex05.CombinerForEx05.class);
        j.setReducerClass(Ex05.ReduceForEx05.class);

        //definicao dos tipos de dado de saida do map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Ex05Writable.class);

        //definicao dos tipos de entrada e saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForEx05 extends Mapper<LongWritable, Text, Text, Ex05Writable> {
        // Funcao de map

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            //obtem linha
            String linha = value.toString();

            //quebrando linhas em colunas em colunas
            String[] colunas = linha.split(";");

            if(colunas[0].equals("Brazil")){
                //obtem mercadoria
                String mercadoria = colunas[2];

                //obtem ano
                String ano = colunas[1];

                //obtem peso
                String peso = colunas[6];
                //criando a chave composta
                Text chave = new Text(mercadoria+"\t"+ano);
                //Se o valo de paso nao tiver nada, eh trocado por 0
                if(peso.equals("")){
                    peso = "0";
                }
                //cria a relacao (chave, valor)
                con.write(chave, new Ex05Writable(Float.parseFloat(peso),1.0f));
            }
        }
    }

    public static class CombinerForEx05 extends Reducer<Text, Ex05Writable, Text, FloatWritable>{
        //funcao de combine para deixar mais rapido o trabalho do reduce

        public void reduce(Text chave, Iterable<Ex05Writable> valor, Context con) throws IOException, InterruptedException {
            //variavel para guardar os pesos somados
            float sumPeso = 0.0f;
            //variavel para guardar a quantidade de pesos para o calculo da media
            float sumMedia = 0.0f;

            //loop para percorrer a lista de pesos
            for(Ex05Writable i : valor){
                //somando os pesos
                sumPeso += i.getPeso();
                //somando a quantidade
                sumMedia += i.getQnt();
            }
            //escrevendo e passando para o reduce
            con.write(chave, new FloatWritable(sumPeso/sumMedia));
        }
    }

    public static class ReduceForEx05 extends Reducer<Text, Ex05Writable, Text, FloatWritable>{
        //funcao de reduce

        public void reduce(Text chave, Iterable<Ex05Writable> valor, Context con) throws IOException, InterruptedException {
            //variavel para guardar os pesos somados
            float sumPeso = 0.0f;
            //variavel para guardar a quantidade de pesos para o calculo da media
            float sumMedia = 0.0f;

            //loop para percorrer a lista de pesos
            for(Ex05Writable i : valor){
                //somando os pesos
                sumPeso += i.getPeso();
                //somando a quantidade
                sumMedia += i.getQnt();
            }
            //escrevendo no arquivo
            con.write(chave, new FloatWritable(sumPeso/sumMedia));
        }
    }
}
