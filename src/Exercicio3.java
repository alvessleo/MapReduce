import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Exercicio3
{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");

        // arquivo intermediario
        Path intermediate = new Path("./output/intermediate");

        // arquivo de saida
        Path output = new Path("./output/exercicio3");

        // criacao da primeira rotina MapReduce
        Job j1 = new Job(c, "contagem");

        // definicao das classes
        j1.setJarByClass(Exercicio3.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        // definicao ds tipos de saida das classes
        j1.setMapOutputKeyClass(Exercicio3Writable.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Exercicio3Writable.class);
        j1.setOutputValueClass(LongWritable.class);

        // definicao dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // execucao do job 1
        j1.waitForCompletion(true);

        // execucao do job 2
        Job j2 = new Job(c, "entropia");

        // definicao das classes
        j2.setJarByClass(Exercicio3.class);
        j2.setMapperClass(Exercicio3.MapEtapaB.class);
        j2.setReducerClass(Exercicio3.ReduceEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(Exercicio3WritableB.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Exercicio3WritableB.class);


        //definição dos arquivos de entrada/saída
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        //execução do job 2
        if (!j2.waitForCompletion(true))
        {
            System.err.println("Erro no Job 2");
            System.exit(1);
        }
    }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Exercicio3Writable, LongWritable>
    {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Converte a variável value que representa a linha do arquivo de Text para String
            String linha = value.toString();
            // ignora o conteudo do cabeçalho
            if (linha.startsWith("country_or_area")) return;
            // Divide a linha
            String[] colunas = linha.split (";");

            String commodity = colunas[2];
            String flow = colunas[4];
            String ano = colunas[1];
            long quantidade = Long.parseLong(colunas[8]);

            if (ano != "2016")
            {
                return;
            }

            con.write(new Exercicio3Writable(commodity, flow), new LongWritable(quantidade));

        }
    }

    public static class ReduceEtapaA extends Reducer<Exercicio3Writable, LongWritable, Exercicio3Writable, LongWritable>
    {
        public void reduce(Exercicio3Writable key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException
        {

            long soma = 0;

            for (LongWritable value : values)
            {
                soma += value.get();
            }

            // escreve o resultado no HDFS
            con.write(key, new LongWritable(soma));
        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, Exercicio3WritableB>
    {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtem a linha do arquivo intermediario
            String linha = value.toString();
            // quebra linha em campos
            String[] campos = linha.split("\t");
            // armazena cada um dos campos
            String commodity = campos[0];
            String flow = campos[1];
            Long soma = Long.parseLong(campos[2]);
            // passa para o reduce:
            con.write(new Text(flow), new Exercicio3WritableB(commodity, soma));
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, Exercicio3WritableB, Text, Exercicio3WritableB>
    {
        public void reduce(Text key, Iterable<Exercicio3WritableB> values, Context con)
                throws IOException, InterruptedException {

            Exercicio3WritableB max = null;

            for (Exercicio3WritableB value : values)
            {
                if (max == null || value.getSoma() > max.getSoma())
                {
                    max = value;
                }
            }

            con.write(key, max);

        }
    }

}