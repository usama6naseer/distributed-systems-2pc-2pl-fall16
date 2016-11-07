package rings

import akka.actor.{ActorSystem, ActorRef, Props}

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Command() extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI

/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */

object KVAppService {

  def apply(system: ActorSystem, numNodes: Int, ackEach: Int): ActorRef = {

    val lock_server = system.actorOf(LockServer.props(system, 60), "LockServer")
    /** Storage tier: create K/V store servers */
    val stores = for (i <- 0 until numNodes)
      yield system.actorOf(KVStore.props(), "RingStore" + i)

    val store_clients = for (i <- 0 until numNodes)
      yield system.actorOf(KVClient.props(stores, lock_server), "KVClient" + i)

    /** Service tier: create app servers */
    val servers = for (i <- 0 to numNodes-1)
      yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach, store_clients(i)), "GroupServer" + i)

    /** If you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach), "GroupServer" + i)
      * For that you need to implement the GroupServer object and the companion actor class.
      * Following the "rings" example.
      */


    /** Tells each server the list of servers and their ActorRefs wrapped in a message. */
    for (server <- servers)
      server ! View(servers)

    for (store_client <- store_clients)
      store_client ! View(store_clients)

    for (store <- stores)
      store ! View(stores)

    lock_server ! View(store_clients)

    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numNodes, servers, ackEach), "LoadMaster")
    master
  }
}

