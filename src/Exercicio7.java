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

import java.io.IOException;

// 2. The number of transactions per flow type and year.

public class Exercicio7
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/exercicio7");

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // -=-=-=-=-=-=-=-=-=-=-=-= PARTE FINAL -=-=-=-=-=-=-=-=-=-=-=-=
        // Registro das classes
        j.setJarByClass(Exercicio7.class);
        j.setMapperClass(Exercicio7.MapExercicio7.class);
        j.setReducerClass(Exercicio7.ReduceExercicio7.class);

        // Definição das tipos de saída

        j.setMapOutputValueClass(IntWritable.class); //Valor do Map
        j.setMapOutputKeyClass(Exercicio7Writable.class); // chave do reduce
        j.setOutputKeyClass(Exercicio7Writable.class); //chave do map
        j.setOutputValueClass(IntWritable.class); // Valor do reduce

        // Cadastrar arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);

    }

    // Exercicio4Writable será criado uma classe com esse nome para depois somar os valores das temperaturas
    public static class MapExercicio7 extends Mapper<LongWritable, Text, Exercicio7Writable, IntWritable>
    {
        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Converte a variável value que representa a linha do arquivo de Text para String
            String linha = value.toString();
            // ignora o conteudo do cabeçalho
            if (linha.startsWith("country_or_area")) return;
            // Divide a linha em várias colunas para que seja possivel pegar a temperatura
            String[] colunas = linha.split (";");
            // Armazenar o ano
            String ano  = colunas[1];
            // Armazenar o flow type
            String flow = colunas[4];
            // Armazenar as ocorrencias
            int ocorrencia = 1;
            // Passando chave (valor1, valor 2) para o cont sort/shuffle
            con.write(new Exercicio7Writable(ano, flow), new IntWritable(ocorrencia));

        }
    }

    public static class ReduceExercicio7 extends Reducer<Exercicio7Writable, IntWritable, Exercicio7Writable,
            IntWritable>
    {
        public void reduce(Exercicio7Writable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException
        {

            int somaOcorrencia = 0;

            // Somando a ocorrencia
            for (IntWritable v:values)
            {
                somaOcorrencia += v.get();
            }

            // Escrever os resultados na HDFS
            con.write(key, new IntWritable(somaOcorrencia));

        }
    }
}
