package bzzz.java.store;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.Arrays;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Accountable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisOutputStream extends IndexOutput implements Accountable {
    public final RedisDirectory dir;
    private byte[] global_name;
    private String name;
    private int bufferPosition;
    private final Checksum crc;

    public RedisOutputStream(String name, RedisDirectory dir) {
        this.dir = dir;
        this.name = name;
        this.global_name = dir.get_global_filename_key(name);
        crc = new BufferedChecksum(new CRC32());
    }
    public void reset() {
        this.bufferPosition = 0;
        crc.reset();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        writeBytes(ByteBuffer.allocate(1).put(b).array(), 0, 1);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0)
            return;
        assert b != null;
        crc.update(b, offset, len);

        byte[] copy = Arrays.copyOfRange(b, offset, (int) (offset + len));

        ShardedJedis jd = dir.redisPool.getResource();
        try {
            long l = jd.setrange(global_name,bufferPosition,copy);
            dir.setFileLength(name,l,jd);
            bufferPosition += len;
        } finally {
            dir.redisPool.returnResource(jd);
        }
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public long getFilePointer() {
        return bufferPosition;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    public String toString() {
        try {
            return new String(global_name, "UTF-8") + "@" + getFilePointer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
