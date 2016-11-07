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

class KVStore extends Actor {
  private val store = new scala.collection.mutable.HashMap[BigInt, Any]
  var endpoints: Option[Seq[ActorRef]] = None
  implicit val timeout = Timeout(60 seconds) // For ask

  override def receive = {
    case View(e) =>
      endpoints = Some(e)
    case Begin(key, value, tid) => {
      sender ! Commit(key, value, tid)
    }
    case Commit(key, value, tid) => {
    	println("keystore COMMIT called")
    	store.put(key,value)
    }
    case Abort(key, value, tid) => {
        println("keystore ABORT called")
    	store.put(key,null)
    }  
    case DirectPut(key, value, tid) => {
      // var temp = store.put(key,value)
      // println("temp",temp)
      // sender ! true
      sender ! store.put(key,value)
    }
    case DirectGet(key, tid) => {
      // var temp = store.get(key)
      // sender ! true
      sender ! store.get(key)
    }
    case Put(key, value, tid) => {
      // 2PC
      // sender ! store.put(key,value)
      // for (i <- endpoints.get) {
      //   val future = ask(i, DirectPut(key, value, tid)).mapTo[Boolean]
      //   val done = Await.result(future, 5 seconds)
      //   if (done == true) {
      //     println(i," says commit")
      //   }
      //   else {
      //     println(i," says abort")  
      //   }
      // }
      store.put(key,value)
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
