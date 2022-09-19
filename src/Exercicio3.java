import java.io.IOException;

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

// The most commercialized commodity (summing the Amount column) in 2016, per flow type.

// 2016 fejifjfnw export

public class Exercicio3 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "exercicio3");

        // registro das classes
        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // pega o conteudo da linha
            String linha = value.toString();
            // ignora o conteudo do cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // quebra a linha de caracteres
            String[] info = linha.split(";");
            // pegar o ano
            String commodity = info[3];

            if (!info[1].equals("2016")){
                return;
            }
            String ano = info[1];

            String quantidade = info[8];

            String tipo = info[4];
            String chave = ano + " " + commodity + " " + tipo;


            int numQtd = 0;
            try
            {
                numQtd = Integer.parseInt(quantidade);
            }
            catch (Exception e)
            {

            }

            con.write(new Text(chave), new IntWritable(numQtd));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // soma das ocorrencia de cada caracter (C, G, T....) e total

            long soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            // escreve o resultado no HDFS
            con.write(key, new IntWritable((int)soma));

        }
    }

    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // soma das ocorrencia de cada caracter (C, G, T....) e total
            long soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            // escreve o resultado no HDFS
            con.write(key, new IntWritable((int)soma));

        }
    }
}
