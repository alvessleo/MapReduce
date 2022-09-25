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

// The average price of commodities per unit type, year, and category in the export flow in Brazil;

public class Exercicio5
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");
        // arquivo de saida
        Path output = new Path("./output/exercicio5");

        // criacao do job e seu nome
        Job j = new Job(c, "Exercicio5");

        // Registro das classes
        j.setJarByClass(Exercicio5.class);
        j.setMapperClass(Exercicio5.MapExercicio5.class);
        j.setReducerClass(Exercicio5.ReduceExercicio5.class);

        // Definição das tipos de saída
        j.setOutputKeyClass(Exercicio5Writable.class); //chave do map
        j.setMapOutputValueClass(DoubleWritable.class); //Valor do Map
        j.setOutputKeyClass(Exercicio5Writable.class); // chave do reduce
        j.setOutputValueClass(DoubleWritable.class); // Valor do reduce

        // Cadastrar arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);

    }

    public static class MapExercicio5 extends Mapper<LongWritable, Text, Exercicio5Writable, DoubleWritable >
    {
        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Converte a variável value que representa a linha do arquivo de Text para String
            String linha = value.toString();

            // ignora o conteudo do cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // Divide a linha em várias colunas
            String[] colunas = linha.split (";");

            // Transforma a preço das commodities que anteriormente era lida como String para Double
            double preco = Double.parseDouble(colunas[5]);

            // Armazenar o flow
            Text flow = new Text(colunas[4]);

            // Armazenar o ano
            String ano = colunas[1];

            // Armazenar a categoria
            String categoria = colunas[9];

            // Armazenar unidade
            String quantidade = colunas[7];

            // Armazenar pais
            String pais = colunas[0];


            if (!String.valueOf(pais).equals("Brazil"))
                return;

            if (!String.valueOf(flow).equals("Export"))
                return;

            // Passando chave (valor1, valor 2) para o cont sort/shuffle
            con.write(new Exercicio5Writable(ano, quantidade, categoria), new DoubleWritable(preco));

        }
    }


    public static class ReduceExercicio5 extends Reducer<Exercicio5Writable, DoubleWritable, Exercicio5Writable, DoubleWritable>
    {
        public void reduce(Exercicio5Writable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaPreco = 0;
            int somaOcorrencia = 0;

            // Somando preco e ocorrencias para fazer a media
            for (DoubleWritable o:values)
            {
                somaPreco += o.get();
                somaOcorrencia++;
            }

            // calculando a média com base nas somas dos precos e ocorrencias
            DoubleWritable media = new DoubleWritable(somaPreco / somaOcorrencia);
            // Escrever os resultados na HDFS
            con.write(key, media);

        }
    }
}
