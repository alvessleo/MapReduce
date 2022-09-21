package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.jute.compiler.JString;
import org.apache.log4j.BasicConfigurator;

/*
    Classe Principal  (World Count) do contador de palavras
    Dentro dessa classe temos:
    1) Método principal (main): configuração hadoop, input, output...
    2) Classe MapX estende a classe Mapper do Hadoop
    3) Classe ReduceX estende a classe Reducer do Hadoop
*/

public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        //Registro das classes
        j.setJarByClass(WordCount.class); // Classe principal
        j.setMapperClass(MapForWordCount.class); // Classe Mapper
        j.setReducerClass(ReduceForWordCount.class); // Classe Reducer

        // Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(Text.class); // Chave de saída do Map
        j.setMapOutputValueClass(IntWritable.class); //Valor de saída do Map
        j.setOutputKeyClass(Text.class); // Chave saída do Reduce
        j.setOutputValueClass(IntWritable.class); // Valor saída do Reduce

        // Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); // Arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); // Arquivo de saída

        // Execução do Job
        j.waitForCompletion(true);

    }

    /*
        Paramêtro da classa Mapper <P1, P2, P3>
        Tipo 1 (P1): Tipo de chave de entrada
        Tipo 2 (P2): Tipo de valor de entrada
        Tipo 3 (P3): Tipo de chave de saída
        Tipo 4 (P4): Tipo do valor de saída
     */

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Obtendo o conteúdo da linha do arquivo de entrada (value)
            // Deixar em caixa baixa (toLowerCase) e tirar caracteres especiais
            String linha = value.toString().toLowerCase().replaceAll("[^a-z]", " ");

            // Quebrando o conteúdo da linha em palavras (vetor de String)
            String[] palavras = linha.split(" ");

            // Gerar (chave, valor) com base no vetor de string palavras
            for (String p: palavras)
            {
                // Registrar (p, 1)

                if (p.length() > 1) // Caso a palavra for mais de 1 caractere ele vai coloca-lá no arquivo
                {
                    Text chaveSaida = new Text(p); // cast de String para Text
                    IntWritable valorSaida = new IntWritable(1); // Cast de Int (1) para IntWritable

                    // Enviando pares (chave, valor) para o Sort/Shuffle
                    con.write(chaveSaida, valorSaida);
                }
            }
        }
    }

    /*
        Parâmetros da classe Reducer
        Tipo 1: Tipo da chave de entrada (Mesmo tipo da chave de saída do map)
        Tipo 2: Tipo do valor da entrada (mesmo tipo do valor de saida do map)
        Tipo 3: Tipo da chave de saída
        Tipo 4: Tipo do valor de saída
     */

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // Dada uma chave, somar todas as suas concorrências
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
