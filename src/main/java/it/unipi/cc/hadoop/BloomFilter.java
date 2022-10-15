package it.unipi.cc.hadoop;

 import org.apache.hadoop.io.ByteWritable;
 import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilter implements Writable {
    IntWritable k;
    IntWritable m;
    public BitSet bf;

    static Hash murmurHashInstance = MurmurHash.getInstance(MURMUR_HASH);


    public BloomFilter(){
        k = new IntWritable();
        m = new IntWritable();
        bf = new BitSet();
    }

    public BloomFilter(BloomFilter bf){
        this.bf = (BitSet) bf.bf.clone();
        this.m = bf.m;
        this.k = bf.k;
    }

    public void setM(IntWritable m) {
        this.m = m;
    }

    public void setK(IntWritable k) {
        this.k = k;
    }

    public IntWritable getM() {
        return m;
    }

    public IntWritable getK() {
        return k;
    }

    public BytesWritable serializeBf(BitSet bs) {
        byte[] arr = bs.toByteArray();
        BytesWritable writable = new BytesWritable();
        writable.setSize(arr.length);
        writable.set(arr, 0, arr.length);
        return writable;
    }

    public boolean insert(String id) {
        int index;
        for(int i = 0; i < k.get(); i++)
        {
            index = Math.abs(murmurHashInstance.hash(id.getBytes(), i) % m.get());
            System.out.println(index);
            bf.set(index, true);
        }
        return true;
    }

    public boolean find(String id) {
        int index;
        for(int i=0; i<k.get(); i++){
            index = Math.abs(murmurHashInstance.hash(id.getBytes(), i));
            index = index % m.get();
            if (!bf.get(index))
                return false;
        }
        return true;
    }

    public void or(BitSet next_bf) {
        for (int i=0; i < m.get(); i++)
            bf.set(i, bf.get(i) || next_bf.get(i));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        serializeBf(bf).write(dataOutput);
        k.write(dataOutput);
        m.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        serializeBf(bf).readFields(dataInput);
        k.readFields(dataInput);
        m.readFields(dataInput);
    }

}
