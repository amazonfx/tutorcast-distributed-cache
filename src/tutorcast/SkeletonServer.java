package tutorcast;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.util.Future;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import thrift.Skeletons;

public class SkeletonServer implements Skeletons.FutureIface {
  private static String awsAccessKey = "AKIAICZIDKXKJDZ4GIZQ";
  private static String awsSecretKey = "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";
  private static String S3_RECORD_BUCKET = "recordedboard";

  private static String REDIS_SERVER = "localhost";
  private static int REDIS_PORT = 6379;

  private static String SERVER = "localhost";
  private static int PORT = 8080;

  private static JedisPool pool;

  @Override
  public Future<Boolean> append(String key, String value) {
    Jedis jedis = pool.getResource();
    Pipeline pipeline = jedis.pipelined();
    StringReader in = null;
    try {
      pipeline.multi();
      boolean endOfBuffer = false;
      int c = 0;
      in = new StringReader(value);
      StringBuffer commandBuffer = new StringBuffer();
      while (!endOfBuffer) {
        c = in.read();
        if (c < 0) {
          endOfBuffer = true;
        }
        if (endOfBuffer || ((char) c) == ',') {
          if (commandBuffer.length() > 0){
            String command = commandBuffer.toString();
            
            String[] commandList = command.split("\\|");
            Long ts = Long.parseLong(commandList[0]);
            //System.err.println("inserting command:"+command);
            pipeline.zadd(key, ts, command);
            commandBuffer.delete(0, commandBuffer.length());
          }
        } else {
          commandBuffer.append((char) c);
        }
      }
      pipeline.exec();
      pipeline.execute();
      pool.returnResource(jedis);
      return Future.value(true);
    } catch (Exception e) {
      e.printStackTrace();
      pipeline.discard();
      return Future.value(false);
    } finally {
      in.close();
    }
  }

  @Override
  public Future<String> get(String key) {
    InputStream in = null;
    try {
      AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
      S3Service s3Service = new RestS3Service(awsCredentials);
      S3Object s3Result = s3Service.getObject(S3_RECORD_BUCKET, key);
      StringBuffer resultBuffer = new StringBuffer();
      in = s3Result.getDataInputStream();
      int c = 0;
      do {
        c = in.read();
        if (c > 0) {
          resultBuffer.append((char) c);
        }
      } while (c >= 0);
      return Future.value(resultBuffer.toString());
    } catch (Exception e) {
      e.printStackTrace();
      return Future.value("");
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Future<Boolean> clear(String key) {
    Jedis jedis = pool.getResource();
    try {
      jedis.zremrangeByScore(key, Double.MIN_VALUE, Double.MAX_VALUE);
      pool.returnResource(jedis);
      return Future.value(true);
    } catch (Exception e) {
      e.printStackTrace();
      pool.returnResource(jedis);
      return Future.value(false);
    }
  }

  @Override
  public Future<Boolean> finalize(String key) {
    try {
      AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
      S3Service s3Service = new RestS3Service(awsCredentials);
      Jedis jedis = pool.getResource();
      Set<String> values = jedis.zrange(key, 0, -1);
      StringBuffer resultBuffer = new StringBuffer();
      for (String v : values) {
        //System.err.println("finalizing value:"+v);
        if (resultBuffer.length() <= 0) {
          resultBuffer.append(v);
        } else {
          resultBuffer.append("," + v);
        }
      }
      
      S3Object stringObject = new S3Object(key, resultBuffer.toString());
      s3Service.putObject(S3_RECORD_BUCKET, stringObject);
      pool.returnResource(jedis);
      return Future.value(true);
    } catch (Exception e) {
      e.printStackTrace();
      return Future.value(false);
    }
  }

  public static void main(String[] args) {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxActive(250);
    config.setTestWhileIdle(true);
    pool = new JedisPool(config, REDIS_SERVER, REDIS_PORT);
    Skeletons.FutureIface server = new SkeletonServer();
    ServerBuilder.safeBuild(
        new Skeletons.FinagledService(server, new TBinaryProtocol.Factory()),
        ServerBuilder.get().name("SkeletonServer").codec(ThriftServerFramedCodec.get())
            .bindTo(new InetSocketAddress(SERVER, PORT)));

    System.out.println("running skeleton server");
  }

}
