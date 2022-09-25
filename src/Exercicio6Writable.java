import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Exercicio6Writable implements WritableComparable<Exercicio6Writable>
{
    String quantityName;
    String ano;

    public Exercicio6Writable() {
    }

    public Exercicio6Writable(String quantityName, String ano) {
        this.quantityName = quantityName;
        this.ano = ano;
    }

    public String getQuantityName() {
        return quantityName;
    }

    public void setQuantityName(String quantityName) {
        this.quantityName = quantityName;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    @Override
    public int hashCode() {
        int hashcode = quantityName.hashCode() + ano.hashCode();
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return quantityName + " " + ano;
    }

    @Override
    public int compareTo(Exercicio6Writable o) {
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
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(quantityName);
        dataOutput.writeUTF(ano);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        quantityName = dataInput.readUTF();
        ano = dataInput.readUTF();
    }
}