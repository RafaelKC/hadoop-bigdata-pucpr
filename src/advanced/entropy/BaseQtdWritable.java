package advanced.entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

public class BaseQtdWritable implements Writable {
    private String base;
    private long qtd;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(String base, long qtd) {
        this.base = base;
        this.qtd = qtd;
    }

    public long getQtd() {
        return qtd;
    }

    public void setQtd(long qtd) {
        this.qtd = qtd;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(base);
        dataOutput.writeUTF(String.valueOf(qtd));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        base = dataInput.readUTF();
        qtd = Long.parseLong(dataInput.readUTF());
    }
}