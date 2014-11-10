package bzzz.java.store;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Accountable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;


public class RedisDirectory extends BaseDirectory implements Accountable {
    int BLOCK_SIZE = 10240;
    public JedisPool redisPool;
    public String dir_name;
    public byte[] dir_name_bytes;
    public LockFactory lf;

    public RedisDirectory(String name, JedisPool pool) {
        redisPool = pool;
        dir_name = name;
        dir_name_bytes = name.getBytes();
        try {
            setLockFactory(new RedisLockFactory(name,pool));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getLockID() {
        return "lucene-" + Integer.toHexString(hashCode());
    }

    @Override
    public final String[] listAll() {
        ensureOpen();
        Jedis rds = redisPool.getResource();
        try {
            Set<String> ls = rds.hkeys(dir_name);
            if( ls == null ){
                return new String[0];
            }
            String[] ret = new String[ls.size()];
            ls.toArray(ret);
            return ret;
        } finally {
            redisPool.returnResourceObject(rds);
        }
    }

    /** Returns true iff the named file exists in this directory. */
    @Override
    public final boolean fileExists(String name) {
        ensureOpen();
        boolean ret = false;
        Jedis rds = redisPool.getResource();
        try {
            return rds.hexists(dir_name_bytes, name.getBytes());
        } finally {
            redisPool.returnResourceObject(rds);
        }
    }

    @Override
    public final long fileLength(String name) throws IOException {
        ensureOpen();

        Jedis jd = redisPool.getResource();
        try {
            long current = 0;
            byte[] b = jd.hget(dir_name_bytes, name.getBytes());
            if (b != null)
                current = ByteBuffer.wrap(b).asLongBuffer().get();
            else
                throw new FileNotFoundException(name);
            return current;
        } finally {
            redisPool.returnResource(jd);
        }
    }

    @Override
    public final long ramBytesUsed() {
        ensureOpen();
        return 0;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        Jedis jd = redisPool.getResource();
        try {
            jd.del(get_global_filename_key(name));
            jd.hdel(dir_name_bytes,name.getBytes());
        } finally {
            redisPool.returnResource(jd);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        deleteFile(name);
        return new RedisOutputStream(name, this);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        if(!fileExists(name))
            throw new FileNotFoundException(name);

        return new RedisInputStream(name, this);
    }

    @Override
    public void close() {
        Jedis jds = redisPool.getResource();
        try {
            jds.bgsave();
            isOpen = false;
        }finally {
            redisPool.returnResourceObject(jds);
        }
    }

    public byte[] get_global_filename_key(String name) {
        return String.format("@%s:%s", dir_name, name).getBytes();
    }

    public void setFileLength(String name, long l, Jedis jd) {
        jd.hset(dir_name_bytes, name.getBytes(), ByteBuffer.allocate(Long.SIZE).putLong(l).array());
    }
}
