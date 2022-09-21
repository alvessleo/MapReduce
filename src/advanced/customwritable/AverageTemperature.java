package advanced.customwritable;

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

public class AverageTemperature
{

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        /*
        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
         */

        // -=-=-=-=-=-=-=-=-=-=-=-= PARTE FINAL -=-=-=-=-=-=-=-=-=-=-=-=
        // Registro das classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setCombinerClass(CombineForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // Definição das tipos de saída
        j.setOutputKeyClass(Text.class); //chave do map
        j.setMapOutputValueClass(FireAvgTempWritable.class); //Valor do Map
        j.setOutputKeyClass(Text.class); // chave do reduce
        j.setOutputValueClass(FloatWritable.class); // Valor do reduce

        // Cadastrar arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);

    }

    // FireAvgTempWritable será criado uma classe com esse nome para depois somar os valores das temperaturas
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable>
    {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Converte a variável value que representa a linha do arquivo de Text para String
            String linha = value.toString();
            // Divide a linha em várias colunas para que seja possivel pegar a temperatura
            String[] colunas = linha.split (",");
            // Transforma a temperatura que anteriormente era lida como String para Float
            float temperatura = Float.parseFloat(colunas[8]);
            // Armazenar somente o mês da Ocorrencia
            Text mes = new Text(colunas[2]);
            // Ocorrencia
            int n = 1;
            // Passando chave (valor1, valor 2) para o cont sort/shuffle
            con.write(new Text("média global"), new FireAvgTempWritable(temperatura, n));
            con.write(mes, new FireAvgTempWritable(temperatura, n));

        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable>
    {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            float somaTemp = 0;
            int somaNs = 0;

            // Somando temperatura e ocorrencia
            for (FireAvgTempWritable o:values)
            {
                somaTemp += o.getSomaTemperatura();
                somaNs += o.getOcorrencia();
            }

            // calculando a média com base nas somas das temperaturas e ocorrencias
            FloatWritable media = new FloatWritable(somaTemp / somaNs);
            // Escrever os resultados na HDFS
            con.write(key, media);

        }
    }

    // Criação da Classe Combiner
    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>
    {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con) throws IOException, InterruptedException
        {
            // No combiner, vamos somar as temperaturas do bloco e as ocorrencias
            float somaTemp = 0;
            int somaNs = 0;

            // Somando temperatura e ocorrencia
            for (FireAvgTempWritable o:values)
            {
                somaTemp += o.getSomaTemperatura();
                somaNs += o.getOcorrencia();
            }

            con.write(key, new FireAvgTempWritable(somaTemp, somaNs));
        }
    }

}
