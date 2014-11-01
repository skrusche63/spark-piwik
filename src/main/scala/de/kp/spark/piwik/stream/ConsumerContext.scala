package de.kp.spark.piwik.stream

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer.DefaultDecoder

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{PooledObject,BasePooledObjectFactory}

class ConsumerContext(
    
  /*
   * The high-level API hides the details of brokers from the consumer and allows 
   * consuming off the cluster of machines without concern for the underlying topology. 
   * 
   * It also maintains the state of what has been consumed. The high-level API also provides 
   * the ability to subscribe to topics that match a filter expression (i.e. either a whitelist 
   * or a blacklist regular expression).  
   * 
   * This topic is a whitelist only but can change with re-factoring below on the filterSpec
   */
  topic:String, 
  /* 
   * A string that uniquely identifies the group of consumer processes to which this consumer 
   * belongs. By setting the same group id multiple processes indicate that they are all part 
   * of the same consumer group.
   */
  group:String, 
  /*
   * Specifies the zookeeper connection string in the form hostname:port where host and port 
   * are the host and port of a zookeeper server. To allow connecting through other zookeeper 
   * nodes when that zookeeper machine is down you can also specify multiple hosts in the form 
   * 
   * hostname1:port1,hostname2:port2,hostname3:port3. 
   * 
   * The server may also have a zookeeper chroot path as part of it's zookeeper connection string 
   * which puts its data under some path in the global zookeeper namespace. 
   * 
   * If so the consumer should use the same chroot path in its connection string. For example to 
   * give a chroot path of /chroot/path you would give the connection string as hostname1:port1,
   * hostname2:port2,hostname3:port3/chroot/path.
   */
  zklist: String, 
  readFromStartOfStream: Boolean = true
  /* 
  * What to do when there is no initial offset in Zookeeper or if an offset is out of range: 
  * 
  * 1) smallest : automatically reset the offset to the smallest offset 
  * 
  * 2) largest : automatically reset the offset to the largest offset 
  * 
  * 3) anything else: throw exception to the consumer. If this is set to largest, the consumer 
  *    may lose some messages when the number of partitions, for the topics it subscribes to, 
  *    changes on the broker. 

  ****************************************************************************************
  To prevent data loss during partition addition, set auto.offset.reset to smallest

  This make sense to change to true if you know you are listening for new data only as of
  after you connect to the stream new things are coming out.  you can audit/reconcile in
  another consumer which this flag allows you to toggle if it is catch-up and new stuff or
  just new stuff coming out of the stream.  This will also block waiting for new stuff so
  it makes a good listener.

  //readFromStartOfStream: Boolean = true
  readFromStartOfStream: Boolean = false
  ****************************************************************************************

  */
  ) {
  
  val props = new Properties()

  props.put("group.id", group)
  props.put("zookeeper.connect", zklist)
  
  props.put("auto.offset.reset", if (readFromStartOfStream) "smallest" else "largest")

  val config:ConsumerConfig = new ConsumerConfig(props)
  val connector = Consumer.create(config)

  val filterSpec = new Whitelist(topic)
  val stream = connector.createMessageStreamsByFilter(filterSpec,1,new DefaultDecoder(),new DefaultDecoder())(0)

  def read(write:(Array[Byte]) => Unit) = {

    for (data <- stream) {
      
      try {                
        write(data.message)
      
      } catch {
        
        case e: Throwable => throw e
        
      }
      
    } 
    
  }

  def shutdown() {
    connector.shutdown()
  }

}


abstract class ConsumerContextFactory(topic:String,group:String,zklist:String) extends Serializable {
  def newInstance(): ConsumerContext  
}

class BaseConsumerContextFactory(topic:String,group:String,zklist:String) extends ConsumerContextFactory(topic,group,zklist) {
  override def newInstance() = new ConsumerContext(topic,group,zklist)
}

/**
 * An object factory for ConsumerContexts, which is used to create a pool of such contexts 
 * (think: DB connection pool).
 *
 * We use this class in WebSocket processing when consuming data from Kafka. A pool is typically 
 * the preferred pattern to minimize TCP connection overhead
 */

// TODO: Time out / shutdown consumers if they haven't been used in a while.

class PooledConsumerContextFactory(val factory:ConsumerContextFactory) extends BasePooledObjectFactory[ConsumerContext] with Serializable {

  override def create(): ConsumerContext = factory.newInstance()

  override def wrap(obj: ConsumerContext): PooledObject[ConsumerContext] = new DefaultPooledObject(obj)

  /** 
   * From the Commons Pool docs: "Invoked on every instance when it is being "dropped" from the pool.  
   * There is no guarantee that the instance being destroyed will be considered active, passive or 
   * in a generally consistent state."
   * 
   */
  override def destroyObject(pobj:PooledObject[ConsumerContext]): Unit = {
    
    pobj.getObject.shutdown()
    super.destroyObject(pobj)
    
  }
}