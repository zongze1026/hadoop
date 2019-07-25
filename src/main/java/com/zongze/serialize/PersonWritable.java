package com.zongze.serialize;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create By xzz on 2019/7/24
 */
public class PersonWritable implements Writable {
    private Person person;

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(person.getId());
        out.writeUTF(person.getName());
        out.writeBoolean(person.isMarry());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        person = new Person();
        person.setId(in.readInt());
        person.setMarry(in.readBoolean());
        person.setName(in.readUTF());
    }
}
