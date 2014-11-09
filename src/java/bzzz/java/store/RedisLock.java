package bzzz.java.store;
import java.io.IOException;
import org.apache.lucene.store.Lock;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisLock extends Lock {
    String name;
    ShardedJedisPool pool;
	
    public RedisLock(String nm, ShardedJedisPool pl) {
        name = nm;
        pool = pl;
    }

    @Override
    public boolean isLocked() throws IOException {
        ShardedJedis jds = pool.getResource();
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
        ShardedJedis jds = pool.getResource();
        try {
            return jds.set(name, "1") != null;
        } finally {
            pool.returnResource(jds);
        }
    }

    @Override
    public void close() throws IOException {
        ShardedJedis jds = pool.getResource();
        try {
            jds.del(name);
        } finally {
            pool.returnResource(jds);
        }
    }
}
