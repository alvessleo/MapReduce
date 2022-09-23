import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Exercicio3Writable implements WritableComparable<Exercicio3Writable> {
    String commodity;
    String flow;

    public Exercicio3Writable() {
    }

    public Exercicio3Writable(String commodity, String flow) {
        this.commodity = commodity;
        this.flow = flow;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public int hashCode() {
        int hashcode = commodity.hashCode() + flow.hashCode();
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return commodity + " " + flow;
    }

    @Override
    public int compareTo(Exercicio3Writable o) {
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
        dataOutput.writeUTF(commodity);
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        flow = dataInput.readUTF();
    }
}
