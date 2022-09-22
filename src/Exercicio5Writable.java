
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

public class Exercicio5Writable implements WritableComparable<Exercicio5Writable>
{
    String categoria;
    String ano;
    String quantidade;


    public Exercicio5Writable() {
    }

    public Exercicio5Writable(String categoria, String ano, String quantidade) {
        this.categoria = categoria;
        this.ano = ano;
        this.quantidade = quantidade;
    }

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    public String getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(String quantidade) {
        this.quantidade = quantidade;
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return categoria + " " + ano + " " + quantidade;
    }

    @Override
    public int compareTo(Exercicio5Writable o) {
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
        dataOutput.writeUTF(categoria);
        dataOutput.writeUTF(ano);
        dataOutput.writeUTF(quantidade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        categoria = dataInput.readUTF();
        ano = dataInput.readUTF();
        quantidade = dataInput.readUTF();
    }
}

