package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WindAndTempWritable implements Writable {

    //vento
    private float wind;
    //valor da temperatura
    private float value;

    //construtor vazio

    public WindAndTempWritable() {
    }

    public WindAndTempWritable(float wind, float value) {
        this.wind = wind;
        this.value = value;
    }

    public float getWind() {
        return wind;
    }

    public void setWind(float wind) {
        this.wind = wind;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        wind = Float.parseFloat(in.readUTF());
        value = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(wind));
        out.writeUTF(String.valueOf(value));
    }

    @Override
    public String toString() {
        return "WindAndTempWritable{" +
                "wind=" + wind +
                ", temperature=" + value +
                '}';
    }
}
