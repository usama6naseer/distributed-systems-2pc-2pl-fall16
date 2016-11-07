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
// Actors send the following commands to GroupServers
case class JoinGroup(senderNodeID: BigInt, groupId: BigInt) extends GroupServiceAPI
case class LeaveGroup(senderNodeID: BigInt, groupId: BigInt) extends GroupServiceAPI
case class Multicast(senderNodeID: BigInt, groupId: BigInt, msg: String) extends GroupServiceAPI
// case class EndTransaction(tid: BigInt) extends GroupServiceAPI

// GroupServers send the following command to Actors
case class Message(senderNodeID: BigInt, message: String) extends GroupServiceAPI

class GroupCell(var groupId: BigInt, var groupMemberIds: HashSet[BigInt])

class GroupServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, cellstoreActor: ActorRef) extends Actor {
  val generator = new scala.util.Random
  val cellstore = cellstoreActor
  implicit val timeout = Timeout(60 seconds) // For ask
  var stats = new Stats
  var endpoints: Option[Seq[ActorRef]] = None
  var messageNumber: Int = 0

  def receive() = {
  	// case EndTransaction(tid) => {
  	//   println(s"KVClient says: transaction $tid is aborted. Retry later.***********888")
  	//   t2(tid)
  	// } 
    case Prime() =>
      // allocCell
    case Command() =>
      // incoming(sender)
      command
    case View(e) =>
      // endpoints = Some(e)
      // println(s"endpoints: $endpoints")
    case JoinGroup(id, groupId) =>
      // addActorToGroup(id, groupId)
    case LeaveGroup(id, groupId) =>
      // removeActorFromGroup(id, groupId)
    case Multicast(id, groupId, msg) =>
      // multicastGroup(id, groupId, msg)
    case Message(id, msg) =>
      // handleMessage(id, msg)
  }

  private def command() = {
    var temp = generator.nextInt(1000000)
    var s1 = (myNodeID+1).toString
    var s2 = temp.toString
    var temp1 = s1.concat(s2)
    var tid = BigInt(temp1) 
    println(s1,s2,temp1,tid)
    val sample = generator.nextInt(100)
    if (myNodeID == BigInt(4)) {
      t2(tid)
    } else {
      t1(tid)
    }
    // directWrite(BigInt(101), BigInt(777), tid)
    // println("***",directRead(BigInt(101), tid))
  }
  def t1(tid: BigInt) = {
  	directWrite(BigInt(101), BigInt(7770), tid)
  	directWrite(BigInt(101), BigInt(7777), tid)
  	directWrite(BigInt(101), BigInt(7771), tid)
  	directWrite(BigInt(101), BigInt(7772), tid)
  	directWrite(BigInt(101), BigInt(7773), tid)
  	cellstoreActor ! EndTransaction(tid)
    Thread.sleep(5000);
  	t2(tid)
  	// directRead(BigInt(101), tid)
    // directWrite(BigInt(101), BigInt(888), tid)
  }
  def t2(tid: BigInt) = {
  	directRead(BigInt(101), tid)
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


  // private def ed(e) = {
  //   endpoints = Some(e: Option[Seq[ActorRef]])
  //   println(s"endpoints: $endpoints")
  // }
  // private def allocCell() = {
    // No need to initialize anything.
  // }

  // private def incoming(master: ActorRef) = {
  //   stats.messages += 1
  //   if (stats.messages >= burstSize) {
  //     master ! BurstAck(myNodeID, stats)
  //     stats = new Stats
  //   }
  // }

  

  // Actor code
  /**
    * Actor picks a random group to join. Sends request to server in charge of that group asking to join.
    */
  // private def joinRandomGroup(): Unit = {
    // Check size of memberships before executing
  //   val groupId = generator.nextInt(numGroups)
  //   route(groupId) ! JoinGroup(myNodeID, groupId)
  // }

  /**
    * Actor picks random group to leave. Sends request to server in charge of that group asking to leave.
    */
  // private def leaveRandomGroup(): Unit = {
  //   // Check size of memberships before executing
  //   val groupId = generator.nextInt(numGroups)
  //   route(groupId) ! LeaveGroup(myNodeID, groupId)
  // }

  /**
    * Actor picks random group to message. Sends message and group number to server for processing.
    */
  // private def messageRandomGroup(): Unit = {
  //   val groupId = generator.nextInt(numGroups)
  //   val msg = s"Message: $messageNumber, Hey it's Actor: $myNodeID, just wanted to say what's up to group $groupId"
  //   route(groupId) ! Multicast(myNodeID, groupId, msg)
  //   messageNumber += 1
  // }

  /**
    * Display the message received from the server.
    * @param senderId the Actor (GroupServer) that sent the message
    * @param msg the message from the Actor (GroupServer)
    */
  // private def handleMessage(senderId: BigInt, msg: String): Unit = {
  //   println(s"Actor: $myNodeID received message: $msg")
  // }

  // Server code
  /**
    * Adds the actor to the specified group. Will create the group if it does not exist.
    * Will not add the actor to the group if it is already a member.
    * @param senderId The id of the actor to add to the group.
    * @param groupId The id of the group to add the actor to.
    */
  // private def addActorToGroup(senderId: BigInt, groupId: BigInt): Unit = {
  //   val cell = directRead(groupId)
  //   if (cell.isEmpty) {
  //     var gc = new GroupCell(groupId, new HashSet[BigInt]())
  //     gc.groupMemberIds += senderId
  //     directWrite(groupId, gc)
  //     stats.allocated += 1
  //   } else {
  //     var gc = cell.get
  //     if(! gc.groupMemberIds.contains(senderId)) {
  //       gc.groupMemberIds += senderId
  //       directWrite(groupId, gc)
  //     }
  //   }
  // }

  /**
    * Removes the actor from the group specified by groupId. Will only make a call to KVClient if the
    * actor is in the specified group.
    * @param senderId The id of the actor to remove from the group.
    * @param groupId The id of the group to remove the actor from.
    */
  // private def removeActorFromGroup(senderId: BigInt, groupId: BigInt): Unit = {
  //   val cell = directRead(groupId)
  //   if (! cell.isEmpty) {
  //     var gc = cell.get
  //     if(gc.groupMemberIds.contains(senderId)) {
  //       gc.groupMemberIds -= senderId
  //       directWrite(groupId, gc)
  //     }
  //   }
  // }

  /**
    * Multicasts message received by senderId to group specified by groupId. Will only process the message
    * if the actor is a member of that group.
    * @param senderId The id of the actor that sent the multicast request.
    * @param groupId The id of the group to send the multicast message.
    * @param msg The actual message to send to the group.
    */
  // private def multicastGroup(senderId: BigInt, groupId: BigInt, msg: String): Unit = {
  //   val cell = directRead(groupId)
  //   if (! cell.isEmpty) {
  //     var gc = cell.get
  //     if (gc.groupMemberIds.contains(senderId)) {
  //       val membersToMessage = gc.groupMemberIds
  //       val servers = endpoints.get
  //       val newMessage = s"$msg ($membersToMessage)"
  //       for (member <- membersToMessage) {
  //         val server = servers(member.toInt)
  //         server ! Message(senderId, newMessage)
  //       }
  //     }
  //   }
  // }

  
  // private def directRead(key: BigInt): Option[GroupCell] = {

    // val result = cellstore.directRead(key)
    // if (result.isEmpty) None else
    //   Some(result.get.asInstanceOf[GroupCell])
  // }

  // private def directWrite(key: BigInt, value: GroupCell): Option[GroupCell] = {
    // val result = cellstore.directWrite(key, value)
    // if (result.isEmpty) None else
    //   Some(result.get.asInstanceOf[GroupCell])
  // }

  /**
    * Returns the server that manages group identified by 'groupId'
    * @param groupId id of group to access
    * @return ActorRef to the server that manages group with id 'groupId'
    */
  // private def route(groupId: BigInt): ActorRef = {
  //   val servers = endpoints.get
  //   val po = (groupId % servers.length).toInt 
  //   val pop = servers.length
  //   // println(s"**************** $po $groupId $pop")
  //   servers((groupId % servers.length).toInt)
  // }
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, cellstoreActor: ActorRef): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, storeServers, burstSize, cellstoreActor)
  }
}