import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

// The commodity with the highest price per unit type and year;

public class Exercicio6 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("./in/operacoes_comerciais_inteira.csv");
        // arquivo de saida
        Path output = new Path("./output/exercicio6");

        Job j = new Job(c, "Exercicio6");

        // registro de classes
        j.setJarByClass(Exercicio6.class);
        j.setMapperClass(MapExercicio6.class);
        j.setReducerClass(ReducerExercicio6.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Exercicio6Writable.class); // chave map
        j.setMapOutputValueClass(DoubleWritable.class); // valor map
        j.setOutputKeyClass(Exercicio6Writable.class); // chave reduce
        j.setOutputValueClass(DoubleWritable.class); // valor reduce

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // espera para executar o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapExercicio6 extends Mapper<Object, Text, Exercicio6Writable, DoubleWritable> {
        public void map(Object key, Text value, Context con) throws IOException,
                InterruptedException {

            // Converte a variável value que representa a linha do arquivo de Text para String
            String linha = value.toString();

            // ignora o conteudo do cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // Divide a linha em colunas
            String[] coluna = linha.split(";");

            // armazenar o ano
            String ano = coluna[1];

            // armazenar o tipo de quantidade
            String quantityName = coluna[7];

            // armazenar e transformar o preco para double
            double preco = Double.parseDouble(coluna[5]);

            con.write(new Exercicio6Writable(quantityName, ano), new DoubleWritable(preco));
        }
    }

    public static class ReducerExercicio6 extends Reducer<Exercicio6Writable, DoubleWritable, Exercicio6Writable, DoubleWritable> {

        public void reduce(Exercicio6Writable key,
                           Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;

            // pegando o maior valor
            for(DoubleWritable o : values){
                if (o.get() > max) max = o.get();

            }

            // escrevendo os maiores valores em arquivo
            context.write(key, new DoubleWritable(max));

        }
    }



}