import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
Classe custom writale com atributos privados, construtor vazio.
getters e setters para todos os atributos
A classe tambem deve ser comparavel com ela mesma
*/

public class Exercicio3WritableB implements WritableComparable<Exercicio3WritableB>
{
    private String commodity;
    private long soma;

    public Exercicio3WritableB()
    {

    }

    public Exercicio3WritableB(String commodity, long soma)
    {
        this.commodity = commodity;
        this.soma = soma;
    }

    public String getCommodity()
    {
        return commodity;
    }

    public void setCommodity(String commodity)
    {
        this.commodity = commodity;
    }

    public long getSoma()
    {
        return soma;
    }

    public void setSoma(long soma)
    {
        this.soma = soma;
    }

    @Override
    public int hashCode()
    {
        int hashcode = commodity.hashCode() + Long.hashCode(soma);
        return hashcode;
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }

    @Override
    public String toString()
    {
        return commodity + " " + soma;
    }

    @Override
    public int compareTo(Exercicio3WritableB o)
    {
        if (this.hashCode() < o.hashCode())
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
        dataOutput.writeUTF(commodity);
        dataOutput.writeLong(soma);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        commodity = dataInput.readUTF();
        soma = dataInput.readLong();
    }
}