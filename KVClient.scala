package rings

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import akka._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}

sealed trait KVClientAPI
case class Begin(key: BigInt, value: Any, tid: BigInt) extends KVClientAPI
case class Commit(key: BigInt, value: Any, tid: BigInt) extends KVClientAPI
case class Abort(key: BigInt, value: Any, tid: BigInt) extends KVClientAPI
case class ChangeCache(key: BigInt, value: Any, tid: BigInt) extends KVClientAPI
case class Prepared(key: BigInt, value: Any, tid: BigInt) extends KVClientAPI
case class Read(key: BigInt, tid: BigInt) extends KVClientAPI
case class Write(key: BigInt, value: Any, tid: BigInt) extends KVClientAPI
case class EndTransaction(tid: BigInt) extends KVClientAPI


class AnyMap extends scala.collection.mutable.HashMap[BigInt, Any]
class TidMap extends scala.collection.mutable.HashMap[BigInt, AnyMap]
class PollMap extends scala.collection.mutable.HashMap[BigInt, Int]

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (stores: Seq[ActorRef], lock_server: ActorRef) extends Actor {
  private val cache = new AnyMap
  // private val dirtyset = new AnyMap
  private val tid_dirtyset = new TidMap
  private val count_vote = new PollMap
  private val count_no = new PollMap
  implicit val timeout = Timeout(60 seconds)
  var endpoints: Option[Seq[ActorRef]] = None

  def receive() = {
    case View(e) => {
      endpoints = Some(e)
    }
    case ChangeCache(key, value, tid) => {
    	cache.put(key,value)
    }
    case Begin(key, value, tid) => {
      var temp = cache.put(key, value)
      // println("111", temp)
      println("111", temp.toString, tid)
      // println("222", cache.get(key), tid)
      // sender ! true
    }
    case LockGranted(result, key, value) => {
      print("LockGranted", key, value)
      if (result) {
        println("got lock")
      }
      else {
        println("lock not free") 
      }
    }
    case Abort(key, value, tid) => {
    	println("abort called", sender.path.name)
    	count_no.put(tid,1)
    	var temp = count_vote.get(tid)
    	count_vote.put(tid,temp.get + 1)
    	println(temp.get + 1)
    	if (temp.get+1 == stores.size) {
    		println(s"Transaction $tid aborted. send abort to all participants.")
    		for (i <- stores) {
				i ! Abort(key, value, tid)
			}
    	}
    }
    case Commit(key, value, tid) => {
    	println("commit called", sender.path.name)
    	var temp = count_vote.get(tid)
    	count_vote.put(tid,temp.get + 1)
    	println(temp.get + 1)
    	if (temp.get+1 == stores.size) {
    		temp = count_no.get(tid)
    		if (temp.get > 0) {
    			println(s"Transaction $tid aborted. send abort to all participants.")
    			for (i <- stores) {
					i ! Abort(key, value, tid)
				}
    		}
    		else {
    			println(s"Transaction $tid successful. send commit to all participants.")	
    			for (i <- stores) {
					i ! Commit(key, value, tid)
					purge()
				}
				// for (i <- endpoints.get) {
				// 	i ! ChangeCache(key, value, tid)
				// }
    		}
    	}
    }
    case EndTransaction(tid) => {
    	var temp_map = tid_dirtyset.get(tid)
	    // temp_map = temp_map.get
    	push(temp_map.get, tid)
    	lock_server ! ReleaseAll(tid)
	    // val future = ask(lock_server, Release(key, tid)).mapTo[Boolean]
    	// val done = Await.result(future, 30 seconds)
    }
    case Read(key, tid) => {
      println("Read called", tid)
      val future = ask(lock_server, Acquire(key, tid)).mapTo[Boolean]
	  val done = Await.result(future, 5 seconds)
	  if (done) {
	  	println("acquired lock on ", key, " by ", tid)
	  	// val future = ask(lock_server, Release(key, tid)).mapTo[Boolean]
    // 	val done = Await.result(future, 30 seconds)
	  	// lock_server ! Release(key, tid)
	    sender ! read(key, tid)
	  }
	  else {
	  	println("lock ", key, " not free for ", tid)
	  	abort(tid, sender)
	  }
    }
    case Write(key, value, tid) => {
      println("Write called", tid)
      val future = ask(lock_server, Acquire(key, tid)).mapTo[Boolean]
	  val done = Await.result(future, 30 seconds)
	  if (done) {
	  	println("acquired lock on ", key, " by ", tid)
	  	count_vote.put(tid,0)
	  	count_no.put(tid,0)
	  	// for (i <- stores) {
	  	//   i ! Begin(key, value, tid) // store replies either commit or abort
	  	// }
	    var write_dirtyset = allocate_dirtyset(tid)
	    var t_map = tid_dirtyset.get(tid)
	    var temp_map = t_map.get
	    temp_map.put(key, value)
	    tid_dirtyset.put(tid, temp_map)
	    cache.put(key, value)
	    // push(temp_map.get, tid)
	    // val future = ask(lock_server, Release(key, tid)).mapTo[Boolean]
    	// val done = Await.result(future, 30 seconds)
	    // lock_server ! Release(key, tid)
	    // sender ! write(key, value, write_dirtyset, tid)
	    sender ! value
	  }
	  else {
	  	println("lock ", key, " not free for ", tid)
	  	abort(tid, sender)
	  }
    }
  }

  def allocate_dirtyset(tid: BigInt): AnyMap = {
  	var temp = tid_dirtyset.get(tid)
  	if (temp.isEmpty) {
  		var temp_map = new AnyMap
  		tid_dirtyset.put(tid, temp_map)
  		temp_map
  	}
  	else {
  		temp.get
  	}
  }
  def abort(tid: BigInt, sender: ActorRef) = {
  	var temp_map = new AnyMap
  	tid_dirtyset.put(tid, temp_map)
  	lock_server ! ReleaseAll(tid)
  	sender ! false
  }
 //  def two_pc(tid: BigInt): Unit = {
 //  	var temp_map = tid_dirtyset.get(tid)
 //  	for (i <- stores) {
 //  		i ! 
 //  	}
	// for (i <- endpoints.get) {
 //   	  println("in 2pc")
 //   	  i ! Begin(key, value, tid)
	//   val future = ask(i, Begin(key, value, tid)).mapTo[Boolean]
	//   val done = Await.result(future, 5 seconds)
	//   println("response:", done.toString)
	//   if (!done) {
	//     abort and end
	//     false
	//   } 
	//   else {
	//     carry on
	//   }
 //    }
 //    val future = ask(lock_server, Acquire(key, tid)).mapTo[Boolean]
 //    val done = Await.result(future, timeout.duration)
 //    if (done) {
 //      println("Lock granted:", key, tid)
 //      write(key, value, dirtyset, tid)
 //      push(dirtyset, tid)
 //    }
 //    else {
 //      println("Lock grant failed:", key, tid)	
 //    }
 //    true
 //  }


  import scala.concurrent.ExecutionContext.Implicits.global

  /** Cached read */
  def read(key: BigInt, tid: BigInt): Option[Any] = {
    var value = cache.get(key)
    if (value.isEmpty) {
      println("not from cache", tid)
      value = directRead(key, tid)
      if (value.isDefined)
        cache.put(key, value.get)
    }
    value
  }

  /** Cached write: place new value in the local cache, record the update in dirtyset. */
  def write(key: BigInt, value: Any, dirtyset: AnyMap, tid: BigInt) = {
    cache.put(key, value)
    dirtyset.put(key, value)
    tid_dirtyset.put(tid, dirtyset)
  }

  /** Push a dirtyset of cached writes through to the server. */
  def push(dirtyset: AnyMap, tid: BigInt) = {
    val futures = for ((key, v) <- dirtyset)
      // directWrite(key, v, tid)
      for (i <- stores) {
	    i ! Begin(key, v, tid) // store replies either commit or abort
	  }
    dirtyset.clear()
    var temp_map = new AnyMap
    tid_dirtyset.put(tid, temp_map)
  }

  /** Purge every value from the local cache.  Note that dirty data may be lost: the caller
    * should push them.
    */
  def purge() = {
    cache.clear()
  }

  /** Direct read, bypass the cache: always a synchronous read from the store, leaving the cache unchanged. */
  def directRead(key: BigInt, tid: BigInt): Option[Any] = {
    val future = ask(route(key), Get(key, tid)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Any, tid: BigInt) = {
    val future = ask(route(key), Put(key,value,tid)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  import java.security.MessageDigest

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    * @param nodeID Which client
    * @param seqID Unique sequence number within client
    */
  def hashForKey(nodeID: Int, seqID: Int): BigInt = {
    val label = "Node" ++ nodeID.toString ++ "+Cell" ++ seqID.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }

  /**
    * @param key A key
    * @return An ActorRef for a store server that stores the key's value.
    */
  private def route(key: BigInt): ActorRef = {
    stores((key % stores.length).toInt)
  }
}

object KVClient {
  def props(stores: Seq[ActorRef], lock_server: ActorRef): Props = {
    Props(classOf[KVClient], stores, lock_server)
  }
}