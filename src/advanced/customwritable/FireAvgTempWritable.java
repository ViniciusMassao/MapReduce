package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FireAvgTempWritable implements Writable {

    //quantidade
    private int n;
    //valor da temperatura
    private float value;

    //construtor vazio
    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(int n, float value) {
        this.n = n;
        this.value = value;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = Integer.parseInt(in.readUTF());
        value = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(value));
    }

}
