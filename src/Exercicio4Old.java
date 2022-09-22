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

public class Exercicio4Old
{
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("./output/exercicio4");

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // -=-=-=-=-=-=-=-=-=-=-=-= PARTE FINAL -=-=-=-=-=-=-=-=-=-=-=-=
        // Registro das classes
        j.setJarByClass(Exercicio4Old.class);
        j.setMapperClass(Exercicio4Old.MapExercicio4.class);
        j.setReducerClass(Exercicio4Old.ReduceExercicio4.class);

        // Definição das tipos de saída
        j.setOutputKeyClass(Text.class); //chave do map
        j.setMapOutputValueClass(Exercicio4WritableOld.class); //Valor do Map
        j.setOutputKeyClass(Text.class); // chave do reduce
        j.setOutputValueClass(DoubleWritable.class); // Valor do reduce

        // Cadastrar arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Execução do job
        j.waitForCompletion(true);

    }

    // Exercicio4Writable será criado uma classe com esse nome para depois somar os valores das temperaturas
    public static class MapExercicio4 extends Mapper<LongWritable, Text, Text, Exercicio4WritableOld>
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
            // Transforma a preço das commodities que anteriormente era lida como String para Double
            double preco = Double.parseDouble(colunas[5]);
            // Armazenar o ano da ocorrência
            Text ano = new Text(colunas[1]);
            // Ocorrencia
            int ocorrencia = 1;
            // Passando chave (valor1, valor 2) para o cont sort/shuffle
            con.write(ano, new Exercicio4WritableOld(preco, ocorrencia));

        }
    }

    public static class ReduceExercicio4 extends Reducer<Text, Exercicio4WritableOld, Text, DoubleWritable>
    {
        public void reduce(Text key, Iterable<Exercicio4WritableOld> values, Context con)
                throws IOException, InterruptedException {

            double somaPreco = 0;
            int somaOcorrencia = 0;

            // Somando temperatura e ocorrencia
            for (Exercicio4WritableOld o:values)
            {
                somaPreco += o.getSomaPreco();
                somaOcorrencia += o.getOcorrencia();
            }

            // calculando a média com base nas somas das temperaturas e ocorrencias
            DoubleWritable media = new DoubleWritable(somaPreco / somaOcorrencia);
            // Escrever os resultados na HDFS
            con.write(key, media);

        }
    }
}
