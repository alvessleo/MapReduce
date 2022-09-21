package advanced.entropy;

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

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/JY157487.1.fasta");
        // Arquivo Intermediário
        Path intermediate = new Path("./output/intermediate.tmp");
        // arquivo de saida
        Path output = new Path("./output/entropia");

        // 9 - Criação da primeira rotina MapReduce
        Job j1 = new Job(c, "Contagem");

        // 10 - Definição das classses
        j1.setJarByClass(EntropyFASTA.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        // 11 - Definição dos tipos de saída das Classes
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);

        // 12 - Definição dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // 13 - execução do Job 1
        if (!j1.waitForCompletion(true))
        {
            System.err.println("Erro no Job 1");
            System.exit(1);
        }

        // 31 - Criação do Job 2
        Job j2 = new Job(c, "Entropia");

        // 32 - Definição das Classes
        j2.setJarByClass(EntropyFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        // 33 - Definição dos arquivos de entrada/saída
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // 34 - Execução do Job 2
        if (!j2.waitCompletion(true))
        {
            System.err.println("Erro no Job 2");
            System.exit(1);
        }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // 1 -pega o conteudo da linha
            String linha = value.toString();
            // 2 - Ignorar o conteudo do cabeçalho
            if (linha.stratsWith(">")) return;

            // 3 - Quebrar a linha em caracteres
            String[] caracteres = linha.split("");

            // 4 - Percorrer o array de Caracteres
            for (String c: caracteres)
            {
                // 5 - Emiti caractere e ocorrencia
                con.write(new Text(c), new LongWritable(1));

                // 6 - Emitir total e ocorrencia
                con.write(new Text("Total"), new LongWritable(1));
            }
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            // 7 - Soma as ocorrencias de cada caractere (C, G, T...) e Total
            long soma = 0;
            for (LongWritable i : values)
            {
                soma += i.get();
            }

            // 8 - Escreve o resultado no HDFS
            con.write(key, new LongWritable(soma));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // 14 - Obtém a linha do arquivo intermediário
            String linha = value.toString();

            // 15 - Quebra a linha em campos (Caracter e quantidade)
            String[] campos = linha.split("\t");

            // 16 - Armezanar cada um dos campos
            String caracter = campos[0];
            Long qtde = Long.parseLong(campos[1]);

            // 17 - Passar para o Reduce


            // 18 - Chave compartilhada: "entropia" e valor composto (caracter, qtde)
            con.write (new Text("entropia"), new BaseQtdWritable(caracter, qtde));


            // 19 - Abrir BaseQtdWritable
        }

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException
        {
            /*
                O Reduce recebe como entrada um Iterable com o seguinte formato:
                ("entrada", (A, 125), (C, 246), (G, 271), (T, 358), (Total, 1000))

                Logo, precisamos encontrar o valor total e então calcular a entropia de cada caracter
            */

            // 23 - Encontrar o valor Total
            long qtdeTotal = 0;
            for (BaseQtdWritable o: values)
            {
                if (o.getChave().equals("Total"))
                {
                    qtdeTotal = o.getQtde();
                    break;
                }
            }

            // 24 - Calcular a entropia de cada caracter e escrever o resultado
            for (BaseQtdWritable o: values)
            {
                // 25 - Se a chave for diferente de Total
                if (!o.getChave().equals("Total"))
                {
                    // 26 - Pega o texto da chave
                    String chave = o.getChave();
                    // 27 - Pega a Quantidade
                    Long qtdeCaracter = o.getQtde();
                    // 28 - Calcular a probalidade
                    double prob = qtdeCaracter / qtdeTotal;
                    // 29 - Calcular a entropia - Log2(x) = log10(x) / log10(2) (Transformar log de base 10 para base 2)
                    double entropia = -prob * (Math.log10(prob) / Math.log10(2));
                    // 30 - Escrever o resultado no HDFS
                    con.write(new Text(chave), new DoubleWritable(entropia));
                }
            }
        }
    }

}
