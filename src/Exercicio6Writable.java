import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio6Writable implements WritableComparable<Exercicio6Writable>
{
    private String commodity;
    private double preco;
    private long quantidade;

    public Exercicio6Writable()
    {

    }

    public Exercicio6Writable(String commodity, double preco, long quantidade)
    {
        this.commodity = commodity;
        this.preco = preco;
        this.quantidade = quantidade;
    }

    public String getCommodity()
    {
        return commodity;
    }

    public void setCommodity(String commodity)
    {
        this.commodity = commodity;
    }

    public double getPreco()
    {
        return preco;
    }

    public void setPreco(double preco)
    {
        this.preco = preco;
    }

    public long getQuantidade()
    {
        return quantidade;
    }

    public void setQuantidade(long quantidade)
    {
        this.quantidade = quantidade;
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
        return commodity + "  " + preco + "  " + quantidade;
    }

    @Override
    public int compareTo(Exercicio6Writable o)
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
        Text.writeString(dataOutput, commodity);
        dataOutput.writeDouble(preco);
        dataOutput.writeLong(quantidade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        commodity = Text.readString(dataInput);
        preco = dataInput.readDouble();
        quantidade = dataInput.readLong();
    }
}
