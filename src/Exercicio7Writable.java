import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio7Writable implements WritableComparable<Exercicio7Writable>
{
    private String ano;
    private String flow;

    public Exercicio7Writable()
    {

    }

    public Exercicio7Writable(String ano, String flow)
    {
        this.ano = ano;
        this.flow = flow;
    }

    public String getAno()
    {
        return ano;
    }

    public void setAno(String ano)
    {
        this.ano = ano;
    }

    public String getFlow()
    {
        return flow;
    }

    public void setFlow(String flow)
    {
        this.flow = flow;
    }

    @Override
    public int hashCode()
    {
        int hashcode = ano.hashCode() + flow.hashCode();
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
        return ano + "  " + flow;
    }

    @Override
    public int compareTo(Exercicio7Writable o)
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
        Text.writeString(dataOutput, ano);
        Text.writeString(dataOutput, flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        ano = Text.readString(dataInput);
        flow = Text.readString(dataInput);
    }
}
