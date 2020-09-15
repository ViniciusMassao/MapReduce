package advanced.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Ex03Writable implements Writable {

    //mercadoria
    private String mercadoria;
    //qtde
    private long qtde;

    //construtor vazio
    public Ex03Writable() {
    }

    public Ex03Writable(String mercadoria, Long qtde) {
        this.mercadoria = mercadoria;
        this.qtde = qtde;
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }

    public Long getQtde() {
        return qtde;
    }

    public void setQtde(Long qtde) {
        this.qtde = qtde;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mercadoria = in.readUTF();
        qtde = Long.parseLong(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(mercadoria));
        out.writeUTF(String.valueOf(qtde));
    }

    @Override
    public String toString() {
        return
                "mercadoria='" + mercadoria + '\'' +
                ", qtde=" + qtde +
                '}';
    }
}
