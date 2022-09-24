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

// O preço médio das commodities por ano;

public class Exercicio6
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/exercicio6");

        // criacao do job e seu nome
        Job j = new Job(c, "exercicio6");

        // -=-=-=-=-=-=-=-=-=-=-=-= PARTE FINAL -=-=-=-=-=-=-=-=-=-=-=-=
        // Registro das classes
        j.setJarByClass(Exercicio6.class);
        j.setMapperClass(Exercicio6.MapExercicio6.class);
        j.setReducerClass(Exercicio6.ReduceExercicio6.class);

        // Definição das tipos de saída

        j.setMapOutputValueClass(Exercicio6Writable.class); //Valor do Map
        j.setMapOutputKeyClass(Text.class); // chave do reduce
        j.setOutputKeyClass(Text.class); //chave do map
        j.setOutputValueClass(Exercicio6Writable.class); // Valor do reduce

        // Cadastrar arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);

    }

    // Exercicio4Writable será criado uma classe com esse nome para depois somar os valores das temperaturas
    public static class MapExercicio6 extends Mapper<LongWritable, Text, Text, Exercicio6Writable>
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

            String ano = colunas[1];
            String commodity = colunas[3];
            String unittype = colunas[7];
            double preco = Double.parseDouble(colunas[5]);
            long quantidade = Long.parseLong(colunas[8]);

            // Passando chave (valor1, valor 2) para o cont sort/shuffle
            con.write(new Text(ano + " " + unittype), new Exercicio6Writable(commodity, preco, quantidade));

        }
    }

    public static class ReduceExercicio6 extends Reducer<Text, Exercicio6Writable, Text, Exercicio6Writable>
    {
        public void reduce(Text key, Iterable<Exercicio6Writable> values, Context con)
                throws IOException, InterruptedException
        {

            Exercicio6Writable aux = new Exercicio6Writable();

            for (Exercicio6Writable i: values)
            {
                if (i.getPreco() > aux.getPreco())
                {
                    aux = i;
                }
            }

            // Escrever os resultados na HDFS
            con.write(key, aux);

        }
    }
}

