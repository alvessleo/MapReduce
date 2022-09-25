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

// The most commercialized commodity (summing the Amount column) in 2016, per flow type.


public class Exercicio3Old
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/exercicio3");

        // criacao do job e seu nome
        Job j = new Job(c, "Exercicio3");

        // -=-=-=-=-=-=-=-=-=-=-=-= PARTE FINAL -=-=-=-=-=-=-=-=-=-=-=-=
        // Registro das classes
        j.setJarByClass(Exercicio3Old.class);
        j.setMapperClass(Exercicio3Old.MapExercicio3.class);
        j.setReducerClass(Exercicio3Old.ReduceExercicio3.class);

        // Definição das tipos de saída
        j.setOutputKeyClass(Exercicio3WritableOld.class); //chave do map
        j.setMapOutputValueClass(IntWritable.class); //Valor do Map
        j.setOutputKeyClass(Exercicio3WritableOld.class); // chave do reduce
        j.setOutputValueClass(IntWritable.class); // Valor do reduce

        // Cadastrar arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);

    }

    // Exercicio4Writable será criado uma classe com esse nome para depois somar os valores das temperaturas
    public static class MapExercicio3 extends Mapper<LongWritable, Text, Exercicio3WritableOld, IntWritable >
    {
        // Funcao de map    ANO UNIDADE CATEGORIA PRECO FLOW COUNTRY
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Converte a variável value que representa a linha do arquivo de Text para String
            String linha = value.toString();

            // ignora o conteudo do cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // Divide a linha em várias colunas para que seja possivel pegar a temperatura
            String[] colunas = linha.split (";");

            // commodity
            String commodity = colunas[2];

            // Armazenar o flow
            String flow = colunas[4];

            // Armazenar o ano da ocorrência
            String ano = colunas[1];

            // Armazenar unidade
            int quantidade = Integer.parseInt(colunas[8]);

            if (!String.valueOf(ano).equals("2016"))
                return;


            // Ocorrencia
            int ocorrencia = 1;
            // Passando chave (valor1, valor 2) para o cont sort/shuffle
            con.write(new Exercicio3WritableOld(commodity, flow), new IntWritable(quantidade));

        }
    }


    public static class ReduceExercicio3 extends Reducer<Exercicio3WritableOld, IntWritable, Exercicio3WritableOld, IntWritable>
    {
        public void reduce(Exercicio3WritableOld key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;
            for (IntWritable v: values)
            {
                soma += v.get();
            }

            IntWritable valorSaida = new IntWritable(soma); // Cast  de Int > IntWritable

            // Salva os resultados no HDFS
            con.write(key, valorSaida); // A key é a mesma recebida do Map, porem o valor saída é o valor somado.

        }
    }
}
