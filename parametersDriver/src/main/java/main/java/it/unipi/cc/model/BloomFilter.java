package main.java.it.unipi.cc.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilter implements Writable {
    private int k;
    private int m;
    private BitSet bs;

    public BloomFilter(){}

    public BloomFilter(int k, int m, BitSet b){
        this.k = k;
        this.m = m;
        this.bs = b;
    }

    public void setM(int m) {
        this.m = m;
    }

    public void setK(int k) {
        this.k = k;
    }

    public void setBitSet(BitSet b) { this.bs = b; }

    public BitSet getBitSet(){
        return bs;
    }

    public int getM() {
        return m;
    }

    public int getK() {
        return k;
    }

    public boolean insert(String id) {
        int index;
        for(int i=0; i<k; i++) {
            index = Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(id.getBytes(StandardCharsets.UTF_8), i));
            index = index % m;
            bs.set(index);
        }
        return true;
    }

    public boolean find(String id) {
        int index;
        for(int i=0; i<k; i++){
            index = Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(id.getBytes(StandardCharsets.UTF_8), i));
            index = index % m;
            if (!bs.get(index))
                return false;
        }
        return true;
    }

    public String toString() {
        return bs.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.k);
        out.writeInt(this.m);

        long[] arr = bs.toLongArray();
        out.writeInt(arr.length);
        for (int i = 0; i < arr.length; i++) {
            out.writeLong(arr[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        k = in.readInt();
        m = in.readInt();

        long[] longs = new long[in.readInt()];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = in.readLong();
        }
        bs = BitSet.valueOf(longs);
    }

}
