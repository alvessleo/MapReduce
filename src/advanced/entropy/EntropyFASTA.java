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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/JY157487.1.fasta");

        // arquivo intermediario
        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path("./output/entropia");

        // criacao da primeira rotina MapReduce
        Job j1 = new Job(c, "contagem");

        // definicao das classes
        j1.setJarByClass(EntropyFASTA.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        // definicao ds tipos de saida das classes
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);

        // definicao dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // execucao do job 1
        j1.waitForCompletion(true);

        // execucao do job 2
        Job j2 = new Job(c, "entropia");

        // definicao das classes
        j2.setJarByClass(EntropyFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);


        //definição dos arquivos de entrada/saída
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        //execução do job 2
        if(!j2.waitForCompletion(true)) {
            System.err.println("Erro no Job 2");
            System.exit(1);
        }
    }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // pega o conteudo da linha
            String linha = value.toString();
            // ignora o conteudo do cabeçalho
            if (linha.startsWith(">")) return;

            // quebra a linha de caracteres
            String[] caracteres = linha.split("");

            // percorre o array de caracteres
            for (String c: caracteres){
                // emite caracter e ocorrencia
                con.write(new Text(c), new LongWritable(1));

                // emite total e ocorrencia
                con.write(new Text("Total"), new LongWritable(1));
            }

        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            // soma das ocorrencia de cada caracter (C, G, T....) e total
            long soma = 0;
            for (LongWritable i : values){
                soma += i.get();
            }
            // escreve o resultado no HDFS
            con.write(key, new LongWritable(soma));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // obtem a linha do arquivo intermediario
            String linha = value.toString();

            // quebra linha em campos (caracter e quantidade)
            String[] campos = linha.split("\t");

            // armazena cada um dos campos
            String caracter = campos[0];
            long qtde = Long.parseLong(campos[1]);

            // passa para o reduce:
            // chave compartilhada: "entropia" e valor compostos(caracter, qtde)
            con.write(new Text("entropia"), new BaseQtdWritable(caracter, qtde));
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            /*
            O reduce recebe como entrada um Iterable com o seguinte formato:
            ("entropia", (A, 125), (C, 246), (G, 271), (T, 358), (Total, 1000))

            logo, precisamos encontrar o valor total e entao calcular a entropia de cada caracter
            */

            // encontrar o valor total
            long qtdeTotal = 0;
            for (BaseQtdWritable o : values){
                if (o.getChave().equals("Total")){
                    qtdeTotal = o.getQtde();
                    break;
                }

            }
            // calcular a entropia de cada caracter e escrever o resultado
            for (BaseQtdWritable o : values){
                // se a chave for diferente de Total
                if (!o.getChave().equals("Total")){
                    // pega texto de chave
                    String chave = o.getChave();
                    // pega a quantidade
                    long qtdeCaracter = o.getQtde();

                    // calcular a probabilidade
                    double prob = qtdeCaracter/(double)qtdeTotal;

                    // log2(x) = log10(x)/log10(2)
                    double entropia = -prob * (Math.log10(prob)/Math.log10(2));

                    // escreve o resultado no HDFS
                    con.write(new Text(chave), new DoubleWritable(entropia));
                }
            }
        }
    }

}
