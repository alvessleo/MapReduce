import org.apache.hadoop.io.Text;
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
    private String ano;
    private String commoditie;

    public Exercicio4Writable()
    {

    }

    public Exercicio4Writable(String ano, String commoditie)
    {
        this.ano = ano;
        this.commoditie = commoditie;
    }

    public String getAno()
    {
        return ano;
    }

    public void setAno(String ano)
    {
        this.ano = ano;
    }

    public String getCommoditie()
    {
        return commoditie;
    }

    public void setCommoditie(String commoditie)
    {
        this.commoditie = commoditie;
    }

    @Override
    public int hashCode()
    {

        int hashcode = ano.hashCode() + commoditie.hashCode();
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
        return ano + "  " + commoditie;
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
        Text.writeString(dataOutput,ano);
        Text.writeString(dataOutput,commoditie);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        ano = Text.readString(dataInput);
        commoditie = Text.readString(dataInput);
    }
}
