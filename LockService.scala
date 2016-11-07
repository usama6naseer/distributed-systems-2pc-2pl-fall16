package rings

import akka._
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

// LockClient sends these messages to LockServer
sealed trait LockServiceAPI
case class Acquire(lockId: BigInt, senderId: BigInt) extends LockServiceAPI
case class Release(lockId: BigInt, senderId: BigInt) extends LockServiceAPI
case class ReleaseAll(tid: BigInt) extends LockServiceAPI
case class KeepAlive(lockId: BigInt, senderId: BigInt) extends LockServiceAPI

// Responses to the LockClient
sealed trait LockResponseAPI
case class LockGranted(result: Boolean, lockID: BigInt, clientId: BigInt) extends LockResponseAPI

class LockServer (system: ActorSystem, t: Int) extends Actor {
  import system.dispatcher

  val generator = new scala.util.Random
  var lockMap = new mutable.HashMap[BigInt, BigInt]()
  var clientServers: Seq[ActorRef] = null
  implicit val timeout = Timeout(t seconds)
  private val master_lock = new Object()
  var flag: Int = 0

  def receive() = {
    case View(clients: Seq[ActorRef]) =>
      clientServers = clients
    case Acquire(lock: BigInt, id: BigInt) => {
      sender ! acquire(sender, lock, id) 
    }
    case Release(lock: BigInt, id: BigInt) => {
      release(lock, id)
      sender ! true
    }
    case ReleaseAll(tid: BigInt) => {
      release_all(tid)
    }
  }

  def acquire(client: ActorRef, lock: BigInt, clientId: BigInt) : Boolean = master_lock.synchronized {
    println(s"Acquire request from: $clientId for lock: $lock")
    do {
      // master_lock.synchronized sychronizes calls. While for double protections using semaphores.
    } 
    while(flag == 1);
    flag = 1
    val cell = directRead(lock)
    // println(cell, " acquire ", clientId, " ", lock)
    if (cell.isEmpty || cell.get == BigInt(-1)) {
      var temp = directWrite(lock, clientId)
      println("***************")
      // client ! LockGranted(true, lock, clientId)
      flag = 0
      return true
    }
    else {
      if (cell.get == clientId) {
      	flag = 0
      	return true
      }
      // client ! LockGranted(false, lock, clientId)
      println(directRead(lock), "****************")
      flag = 0
      return false
    }
  }

  def release(lock: BigInt, clientId: BigInt) = {
    var temp = directWrite(lock, BigInt(-1))
    println(s"Release lock request from: $clientId for lock: $lock")
    // val cell = directRead(lock)
    // if (cell.isEmpty) {
    // }
    // else {
    //   var temp = directWrite(lock, null)
    // }
  }

  def release_all(tid: BigInt) = {
    println(s"Release all lock from: $tid")
    for ((k,v) <- lockMap) {
    	if (v == tid) {
    		var temp = directWrite(k, -1)
    	}
    }
  }

  def directRead(key: BigInt): Option[BigInt] = {
    val result = lockMap.get(key)
    if (result.isEmpty) None
    else
      Some(result.get.asInstanceOf[BigInt])
  }

  def directWrite(key: BigInt, value: BigInt): Option[BigInt] = {
    val result = lockMap.put(key, value)
    if (result.isEmpty) None
    else
      Some(result.get.asInstanceOf[BigInt])
  }
}

object LockServer {
  def props(system: ActorSystem, t: Int): Props = {
    Props(classOf[LockServer], system, t)
  }
}