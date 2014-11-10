package bzzz.java.store;
import java.io.IOException;
import org.apache.lucene.store.Lock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisLock extends Lock {
    String name;
    JedisPool pool;
	
    public RedisLock(String nm, JedisPool pl) {
        name = nm;
        pool = pl;
    }

    @Override
    public boolean isLocked() throws IOException {
        Jedis jds = pool.getResource();
        try {
            return jds.exists(name);
        } finally {
            pool.returnResource(jds);
        }
    }

    @Override
    public boolean obtain() throws IOException {
        if( isLocked() )
            return false;
        Jedis jds = pool.getResource();
        try {
            return jds.set(name, "1") != null;
        } finally {
            pool.returnResource(jds);
        }
    }

    @Override
    public void close() throws IOException {
        Jedis jds = pool.getResource();
        try {
            jds.del(name);
        } finally {
            pool.returnResource(jds);
        }
    }
}
