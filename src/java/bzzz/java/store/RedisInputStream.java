package bzzz.java.store;
import java.io.IOException;
import java.io.EOFException;
import org.apache.lucene.store.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisInputStream extends IndexInput implements Cloneable {
    public int bufferPosition;
    public int currentBufferIndex;
    public int realBufferLen = 0;
    public int BUFFER_SIZE = 10000;
    private byte[] global_name;
    private byte[] BUFFER = null;
    public final RedisDirectory dir;
    String name;

    public RedisInputStream(String name, RedisDirectory dir) throws IOException {
        this(name,dir,0);
    }

    public RedisInputStream(String name, RedisDirectory dir, long offset) throws IOException {
        super(name);
        this.dir = dir;
        this.name = name;
        this.global_name = dir.get_global_filename_key(name);
        currentBufferIndex = -1;
        setPosition(offset);
    }

    public void setPosition(long pos) throws IOException {
        int updatedBufferIndex = (int) (pos / BUFFER_SIZE);
        bufferPosition = (int) (pos % BUFFER_SIZE);
        if (BUFFER == null || updatedBufferIndex != currentBufferIndex) {
            currentBufferIndex = updatedBufferIndex;
            ShardedJedis jd = dir.redisPool.getResource();
            try {
                refreshBuffer(jd);
            } finally {
                dir.redisPool.returnResource(jd);
            }
        }
    }

    @Override
    public byte readByte() throws IOException {
        byte[] b = new byte[1];
        readBytes(b, 0, 1);
        return b[0];
    }

    public void refreshBuffer(ShardedJedis jd) throws IOException {
        int from = absolutePosition(0);
        BUFFER = jd.getrange(global_name, from, from + BUFFER_SIZE - 1);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        ShardedJedis jd = dir.redisPool.getResource();
        try {
            while (len > 0) {
                if (bufferPosition >= BUFFER.length) {
                    currentBufferIndex++;
                    bufferPosition = 0;
                    refreshBuffer(jd);
                }

                int remainInBuffer = BUFFER.length - bufferPosition;
                if (remainInBuffer <= 0)
                    throw new IOException("remainInBuffer <= 0, current BUFFER.length: " + BUFFER.length + " bufferPosition: " + bufferPosition);
                int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
                System.arraycopy(BUFFER, bufferPosition, b, offset, bytesToCopy);
                offset += bytesToCopy;
                len -= bytesToCopy;
                bufferPosition += bytesToCopy;
            }
        } finally {
            dir.redisPool.returnResource(jd);
        }
    }

    public int absolutePosition(long n) {
        return (currentBufferIndex * BUFFER_SIZE) + (int) n;
    }

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : absolutePosition(bufferPosition);
    }

    @Override
    public void seek(long pos) throws IOException {
        setPosition(pos);
    }

    @Override
    public void close() {}

    @Override
    public long length() {
        try {
            return dir.fileLength(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, final long offset, final long length) throws IOException {
        return new RedisInputStream(name, dir, offset) {
            @Override
            public void seek(long pos) throws IOException {
                if (pos < 0L) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this);
                }
                super.seek(pos + offset);
            }

            @Override
            public long getFilePointer() {
                return super.getFilePointer() - offset;
            }

            @Override
            public long length() {
                return length;
            }

            @Override
            public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
                return super.slice(sliceDescription, offset + ofs, len);
            }
        };
    }
}
