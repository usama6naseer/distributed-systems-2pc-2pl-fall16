package rings

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorRef
import akka.util.Timeout
import akka._

sealed trait KVStoreAPI
case class Put(key: BigInt, value: Any, tid: BigInt) extends KVStoreAPI
case class Get(key: BigInt, tid: BigInt) extends KVStoreAPI
case class DirectPut(key: BigInt, value: Any, tid: BigInt) extends KVStoreAPI
case class DirectGet(key: BigInt, tid: BigInt) extends KVStoreAPI


/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */
class LogCell(var tid: BigInt, var key: BigInt)

class KVStore extends Actor {
  private val store = new scala.collection.mutable.HashMap[BigInt, Any]
  private val log = new scala.collection.mutable.HashMap[BigInt, BigInt]
  private val logMap = new scala.collection.mutable.HashMap[LogCell, Any]
  var endpoints: Option[Seq[ActorRef]] = None
  implicit val timeout = Timeout(60 seconds) // For ask
  var previous_tid = BigInt(-1)
  var current_tid = BigInt(-1)

  override def receive = {
    case View(e) =>
      endpoints = Some(e)
    case Begin(key, value, tid) => {
      // if succesful send commit
      println("Begin for: ", key, value, tid)
      sender ! Commit(key, value, tid)
      // if unsuccessful send abort
      // sender ! Abort(key, value, tid)
    }
    case Commit(key, value, tid) => {
    	sender ! true
    	println("commit &&&&&& ", key, value)
    	var temp = new LogCell(tid,key)
    	logMap.put(temp,value)

    	var prevT = log.get(key)
    	if (prevT.isEmpty) {
    		log.put(key,tid)	
    		previous_tid = BigInt(-1)
    		current_tid = tid
    	}
    	else {
    		if (previous_tid != prevT.get && current_tid != prevT.get) {
    			previous_tid = prevT.get
    			current_tid = tid
	    		log.put(key,tid)
    		}
    	}
    	println("keystore COMMIT called")
    	store.put(key,value)
    }
    case Abort(key, value, tid) => {
    	sender ! false
        println("keystore ABORT called")
    	// store.put(key,null)
    	// var prev = log.get(key)
    	// if (!prev.isEmpty) {
    	// 	store.put(key, prev.get)	
    	// }
    	if (previous_tid == BigInt(-1)) {
    		store.put(key,null)
    	}
    	else {
    		var temp = new LogCell(previous_tid,key)
	    	var prev = logMap.get(temp)
	    	if (!prev.isEmpty) {
	    		store.put(key,prev.get)
	    	}
	    	else {
	    		store.put(key,null)	
	    	}
    	}
    }  
    case Put(key, value, tid) => {
      sender ! store.put(key,value)
    }
    case Get(key, tid) =>
      sender ! store.get(key)
  }
}

object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}
