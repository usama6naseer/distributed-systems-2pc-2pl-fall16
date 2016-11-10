package rings

import akka._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet


sealed trait GroupServiceAPI

class GroupCell(var groupId: BigInt, var groupMemberIds: HashSet[BigInt])

class GroupServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, cellstoreActor: ActorRef) extends Actor {
  val generator = new scala.util.Random
  val cellstore = cellstoreActor
  implicit val timeout = Timeout(60 seconds) // For ask
  var stats = new Stats
  var endpoints: Option[Seq[ActorRef]] = None
  var messageNumber: Int = 0

  def receive() = {
    case Prime() =>
      // allocCell
    case Command() =>
      // incoming(sender)
      command
    case View(e) =>
      // endpoints = Some(e)
  }

  private def command() = {
    var temp = generator.nextInt(1000000)
    var s1 = (myNodeID+1).toString
    var s2 = temp.toString
    var temp1 = s1.concat(s2)
    var tid = BigInt(temp1) 
    println(s1,s2,temp1,tid)
    t1(tid)
    
    Thread.sleep(5000);
    
    temp = generator.nextInt(1000000)
    s1 = (myNodeID+1).toString
    s2 = temp.toString
    temp1 = s1.concat(s2)
    tid = BigInt(temp1) 
    println(s1,s2,temp1,tid)
    t2(tid)
    // directWrite(BigInt(101), BigInt(777), tid)
    // println("***",directRead(BigInt(101), tid))
  }
  def t1(tid: BigInt) = {
  	directWrite(BigInt(101), BigInt(7770), tid)
  	directWrite(BigInt(101), BigInt(7777), tid)
  	directWrite(BigInt(101), BigInt(7771), tid)
  	directWrite(BigInt(101), BigInt(7772), tid)
  	directWrite(BigInt(101), BigInt(7773), tid)
  	directWrite(BigInt(111), BigInt(2222), tid)
  	directWrite(BigInt(1117), BigInt(2223), tid)
  	directWrite(BigInt(1118), BigInt(2223), tid)
  	directWrite(BigInt(1116), BigInt(2223), tid)
  	directWrite(BigInt(1115), BigInt(2223), tid)
  	directWrite(BigInt(1114), BigInt(2223), tid)
  	cellstoreActor ! EndTransaction(tid)
    // Thread.sleep(5000);
  	// t2(tid)
  	// directRead(BigInt(101), tid)
    // directWrite(BigInt(101), BigInt(888), tid)
  }
  def t2(tid: BigInt) = {
  	directRead(BigInt(101), tid)
  	directRead(BigInt(111), tid)
  	directRead(BigInt(1117), tid)
  	directRead(BigInt(1118), tid)
  	directRead(BigInt(1116), tid)
  	directRead(BigInt(1115), tid)
  	directRead(BigInt(1114), tid)
  	directRead(BigInt(111), tid)
  	cellstoreActor ! EndTransaction(tid)
   //  directRead(BigInt(101), tid)
  }

  // Code to access KVClient
  private def directRead(key: BigInt, tid: BigInt): Unit = {
    val future = ask(cellstore, Read(key, tid)).mapTo[Any]
    val done = Await.result(future, 30 seconds)
    println("directRead in GroupServer ",done)
    if (done == false) {
    	println(s"transaction $tid aborted.")
    }
  }
  private def directWrite(key: BigInt, value: Any, tid: BigInt): Unit = {
    val future = ask(cellstore, Write(key, value, tid)).mapTo[Any]
    val done = Await.result(future, 30 seconds)
    println("directWrite in GroupServer ",done)
    if (done == false) {
    	println(s"transaction $tid aborted.")
    }
  }
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, cellstoreActor: ActorRef): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, storeServers, burstSize, cellstoreActor)
  }
}