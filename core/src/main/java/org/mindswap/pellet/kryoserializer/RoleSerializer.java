package org.mindswap.pellet.kryoserializer;

import org.mindswap.pellet.Role;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RoleSerializer extends Serializer<Role> {
    @Override
    public void write(Kryo kryo, Output output, Role object) {
        //kryo.writeClassAndObject(output, object);
    	kryo.writeObject(output, object);
    }

    @Override
    public Role read(Kryo kryo, Input input, Class<Role> type) {
        //return (Role) kryo.readClassAndObject(input);
    	return (Role) kryo.readObject(input, Role.class);
    }
}