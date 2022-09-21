package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
    Precisamos que essa nova classe seja serializavel (Writable) para transmissão dos dados entre os DataNodes.
    No hadoop, o tipo Writable e sempre um Java Bean.
    Java Bean e caracterizado por um construtor padrão (e outro vazio), atributos privados e getters e setters
    para cada atributo.
*/

public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable>
{
    private float somaTemperatura;
    private  int ocorrencia;

    public FireAvgTempWritable()
    {

    }

    public FireAvgTempWritable(float somaTemperatura, int ocorrencia)
    {
        this.somaTemperatura = somaTemperatura;
        this.ocorrencia = ocorrencia;
    }

    public float getSomaTemperatura()
    {
        return somaTemperatura;
    }

    public void setSomaTemperatura(float somaTemperatura)
    {
        this.somaTemperatura = somaTemperatura;
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
    public String toString()
    {
        return "FireAvgTempWritable{" +
                "somaTemperatura=" + somaTemperatura +
                ", ocorrencia=" + ocorrencia +
                '}';
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

    // Metodo que realiza o comparativo entre diferentes Objetos na etapa de Sort/Shuffle
    // para a ordenação de acordo com as chaves
    // dado 2 objetos. Comparar se um é maior que o outro, maior, ou igual
    @Override
    public int compareTo(FireAvgTempWritable o)
    {
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
        dataOutput.writeFloat(somaTemperatura);
        dataOutput.writeInt(ocorrencia);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        somaTemperatura = dataInput.readFloat();
        ocorrencia = dataInput.readInt();
    }
}
