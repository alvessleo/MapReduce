import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    Precisamos que essa nova classe seja serializavel (Writable) para transmissão dos dados entre os DataNodes.
    No hadoop, o tipo Writable e sempre um Java Bean.
    Java Bean e caracterizado por um construtor padrão (e outro vazio), atributos privados e getters e setters
    para cada atributo.
*/

public class Exercicio4Writable implements WritableComparable<Exercicio4Writable>
{
    double somaPreco;
    int ocorrencia;

    // Construtor vazio
    public Exercicio4Writable()
    {

    }

    // Construtor
    public Exercicio4Writable(double somaPreco, int ocorrencia)
    {
        this.somaPreco = somaPreco;
        this.ocorrencia = ocorrencia;
    }

    public double getSomaPreco()
    {
        return somaPreco;
    }

    public void setSomaPreco(double somaPreco)
    {
        this.somaPreco = somaPreco;
    }

    public int getOcorrencia()
    {
        return ocorrencia;
    }

    public void setOcorrencia(int ocorrencia)
    {
        this.ocorrencia = ocorrencia;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }

    @Override
    public String toString()
    {
        return super.toString();
    }

    @Override
    public int compareTo(Exercicio4Writable o)
    {
        // Sempre é assim
        if(this.hashCode() < o.hashCode())
        {
            return -1;
        }
        else if (this.hashCode() > o.hashCode())
        {
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(somaPreco);
        dataOutput.writeInt(ocorrencia);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        somaPreco = dataInput.readDouble();
        ocorrencia = dataInput.readInt();
    }
}
