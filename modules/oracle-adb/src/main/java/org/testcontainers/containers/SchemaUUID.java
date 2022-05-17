package org.testcontainers.containers;

import java.util.UUID;

public class SchemaUUID {
    final static String BASE = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_";
    final static int MODULO = BASE.length();

    private static String encode(long value) {
        final StringBuilder sb = new StringBuilder();
        while (value != 0) {
            int index = (int) (value % MODULO);

            if(index<0) {
                index += MODULO;
            }

            sb.append(BASE.charAt(index) );
            value /= MODULO;
        }
        while (sb.length() < 10) {
            sb.append(0);
        }
        return sb.reverse().substring(0,10);
    }

    public static String get(UUID uuid) {
        return encode(uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits());
    }
}
