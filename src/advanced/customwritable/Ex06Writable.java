package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Ex06Writable implements Writable {

    //mercadoria
    private String mercadoria;
    //valor do preco
    private float preco;

    //construtor vazio


    public Ex06Writable() {
    }

    public Ex06Writable(String mercadoria, float preco) {
        this.mercadoria = mercadoria;
        this.preco = preco;
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }

    public float getPreco() {
        return preco;
    }

    public void setPreco(float preco) {
        this.preco = preco;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mercadoria = in.readUTF();
        preco = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(mercadoria));
        out.writeUTF(String.valueOf(preco));
    }

    @Override
    public String toString() {
        return "Ex06Writable{" +
                "Mercadoria=" + mercadoria +
                ", Preco" + preco +
                '}';
    }
}
