package bzzz.java.store;
import java.io.IOException;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisLockFactory extends LockFactory {
    protected JedisPool pool;
    String root;
    public RedisLockFactory(String root, JedisPool pl) {
        pool = pl;
        this.root = root;
    }

    @Override
    public void clearLock(String name) throws IOException {
        Jedis jds = pool.getResource();
        try {
            jds.del(root + name);
        } finally {
            pool.returnResource(jds);
        }
    }

    @Override
    public Lock makeLock(String name) {
        return new RedisLock(root + name, pool);
    }

}
