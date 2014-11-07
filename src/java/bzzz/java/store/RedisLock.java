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
        boolean ret = jds.exists(name);
        pool.returnResource(jds);
        return ret;
    }

    @Override
    public boolean obtain() throws IOException {
        if( isLocked() )
            return false;
        ShardedJedis jds = pool.getResource();
        String ret = jds.set(name, "1");
        pool.returnResource(jds);
        return ret != null;
    }

    @Override
    public void close() throws IOException {
        ShardedJedis jds = pool.getResource();
        jds.del(name);
        pool.returnResource(jds);
    }
}
