package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Ex05Writable implements Writable {
    //variavel de peso
    private float peso;
    //variavel de quantidade de ocorrencias
    private float qnt;

    //construtor vazio
    public Ex05Writable(){}

    //construtor
    public Ex05Writable(float peso, float qnt) {
        this.peso = peso;
        this.qnt = qnt;
    }

    //getter de peso
    public float getPeso() {
        return peso;
    }

    //getter de quantidade
    public float getQnt() {
        return qnt;
    }

    //setter de peso
    public void setPeso(float peso) {
        this.peso = peso;
    }

    //setter de quantidade
    public void setQnt(float qnt) {
        this.qnt = qnt;
    }

    @Override
    public String toString() {
        return "Ex04Writable{" +
                "peso=" + peso +
                ", qnt=" + qnt +
                '}';
    }

    //funcao que escreve serializada
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(peso));
        out.writeUTF(String.valueOf(qnt));
    }

    //funcao que le serializada
    @Override
    public void readFields(DataInput in) throws IOException {
        peso = Float.parseFloat(in.readUTF());
        qnt = Float.parseFloat(in.readUTF());
    }
}
