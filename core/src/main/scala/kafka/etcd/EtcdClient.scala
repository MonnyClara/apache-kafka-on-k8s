/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package kafka.etcd

import com.coreos.jetcd.Client
import com.coreos.jetcd.kv.{DeleteResponse, GetResponse}
import com.coreos.jetcd.options.{DeleteOption, GetOption, PutOption}
import com.coreos.jetcd.watch.WatchEvent
import kafka.metastore.KafkaMetastore
import kafka.utils.Logging
import kafka.zookeeper._
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

class EtcdClient(connectionString: String = "127.0.0.1:2379/kafka") extends KafkaMetastore with Logging {

  import Implicits._

  private val connectionStringWithoutChRoot = connectionString.substring(0, connectionString.indexOf("/"))
  private val root = connectionString.substring(connectionString.indexOf("/"))

  private val client: Client = Client.builder.endpoints(connectionStringWithoutChRoot).build()

  // Event handlers to handler events received from ETCD
  private val createHandlers = new ChangeHandlers
  private val updateHandlers = new ChangeHandlers
  private val deleteHandlers = new ChangeHandlers
  private val childHandlers = new ChangeHandlers


  // Subscribe to etcd events
  private val etcdListener = EtcdListener(client) {
    event: WatchEvent =>
      val eventType = event.getEventType
      val eventData = Option(event.getKeyValue)
      val key: Option[String] = eventData.map(_.getKey)
      val value: Option[String] = eventData.map(_.getValue)

      info(s"Received change notification: '$eventType' : '$key' -> '$value'")

      eventData.foreach {
        kv =>
          eventType match {
            case WatchEvent.EventType.PUT if kv.getVersion == 1L =>
              createHandlers.triggerOn(key.get)
              parentOf(key.get).foreach(childHandlers.triggerOn)

            case WatchEvent.EventType.PUT =>
              updateHandlers.triggerOn(key.get)

            case WatchEvent.EventType.DELETE =>
              deleteHandlers.triggerOn(key.get)
              parentOf(key.get).foreach(childHandlers.triggerOn)

            case _ =>
              error(s"Received unrecognized ETCD event type received for '$key'!")
              throw new Exception(s"Received unrecognized ETCD event type for '$key'!")
          }

      }

  }


  override def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    registerChangeHandler(zNodeChangeHandler.path,
      zNodeChangeHandler.handleCreation,
      zNodeChangeHandler.handleDataChange,
      zNodeChangeHandler.handleDeletion
    )

  }

  override def unregisterZNodeChangeHandler(path: String): Unit = {
    createHandlers.unregisterChangeHandler(path)
    updateHandlers.unregisterChangeHandler(path)
    deleteHandlers.unregisterChangeHandler(path)
  }

  override def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    val key = zNodeChildChangeHandler.path

    info(s"Register change handler for children of '$key'")

    childHandlers.registerChangeHandler(key, zNodeChildChangeHandler.handleChildChange)
  }

  override def unregisterZNodeChildChangeHandler(path: String): Unit = {
    info(s"Register change handler for children of '$path'")

    childHandlers.unregisterChangeHandler(path)
  }

  override def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = {
    info("Register state change handler")

    // TODO: implement me
  }

  override def unregisterStateChangeHandler(name: String): Unit = {
    info("Unregister state change handler")

    // TODO: implement me
  }


  override def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    info(s"Hanlde request: $request")

    request match {
      case ExistsRequest(path, ctx) =>
        val response: Try[Boolean] = tryExists(EtcdClient.absolutePath(root, path))

        val zkResult = new ZkExistsResponse(response)
        ExistsResponse(zkResult.resultCode, path, ctx, null).asInstanceOf[Req#Response]

      case GetDataRequest(path, ctx) =>
        val response: Try[Option[Array[Byte]]] = tryGetData(EtcdClient.absolutePath(root, path)) {
          response =>
            if (exists(response)) {
              Some(response.getKvs.get(0).getValue)
            }
            else {
              None
            }
        }

        val zkResult = new ZkGetDataResponse(response)
        GetDataResponse(zkResult.resultCode, path, ctx, zkResult.data.orNull, null).asInstanceOf[Req#Response]

      case GetChildrenRequest(path, ctx) =>
        val parent = if (path.endsWith("/")) path else s"$path/"

        val response: Try[Set[String]] =
          tryGetData(
            EtcdClient.absolutePath(root, parent),
            GetOption.newBuilder()
              .withPrefix(EtcdClient.absolutePath(root, parent))
              .withKeysOnly(true)
              .build()
          ) {
            response =>
              val keys: Set[String] = response.getKvs.asScala.map(_.getKey.toStringUtf8).map {
                k =>
                  val startIdx = EtcdClient.absolutePath(root, parent).length

                  k.indexOf('/', startIdx) match {
                    case endIdx if endIdx >= startIdx => k.substring(startIdx, endIdx)
                    case _ => k.substring(startIdx)
                  }
              }.toSet

              keys
          }

        val zkResult = new ZkGetChildrenResponse(response)

        GetChildrenResponse(zkResult.resultCode, path, ctx, zkResult.childrenKeys.toSeq, null).asInstanceOf[Req#Response]

      case CreateRequest(path, data, _, createMode, ctx) =>
        createMode match {
          case CreateMode.EPHEMERAL => createWithLease(path, data, ctx)
          case CreateMode.PERSISTENT => create(path, data, ctx)
          case CreateMode.PERSISTENT_SEQUENTIAL => ???
          case _ => ???
        }

      case SetDataRequest(path, data, _, ctx) =>
        val response = tryCreate(EtcdClient.absolutePath(root, path), data, ctx)
        val zkResult = new ZkSetDataResponse(response)

        SetDataResponse(zkResult.resultCode, path, ctx, null).asInstanceOf[Req#Response]

      case DeleteRequest(path, _, ctx) =>
        val response = tryDelete(EtcdClient.absolutePath(root, path), ctx)(deleted)
        val zkResult = new ZkDeleteResponse(response)

        kafka.zookeeper.DeleteResponse(zkResult.resultCode, path, ctx).asInstanceOf[Req#Response]

      case GetAclRequest(path, ctx) => ???
      case SetAclRequest(path, acl, version, ctx) => ???
    }
  }


  override def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    info(s"Handle requests: $requests")

    val asynResponses: Seq[Future[Req#Response]] = requests.map(req => Future(handleRequest(req)))
    val responses: Future[Seq[Req#Response]] = Future.fold(asynResponses)(Seq.empty[Req#Response])(_ :+ _)

    Await.result(responses, Duration.Inf)
  }

  override def waitUntilConnected(): Unit = {
    info("Waiting until connected.")

    // TODO: implement me

    info("Connected.")
  }

  override def close(): Unit = {
    // Close all watchers
    etcdListener.close()

    // Close etcd client
    client.close()
  }

  private def registerChangeHandler(key: String,
                                    onCreate: ChangeHandlers#Handler,
                                    onValueChange: ChangeHandlers#Handler,
                                    onDelete: ChangeHandlers#Handler): Unit = {
    createHandlers.registerChangeHandler(key, onCreate)
    updateHandlers.registerChangeHandler(key, onValueChange)
    deleteHandlers.registerChangeHandler(key, onDelete)
  }

  private def parentOf(key: String): Option[String] = {
    Option(key) match {
      case Some(k) if !k.isEmpty =>
        Some(k.substring(0, k.lastIndexOf('/')))
      case _ => None
    }
  }

  private def tryExists(key: String): Try[Boolean] = {
    tryGetData(key,
      GetOption
        .newBuilder()
        .withCountOnly(true)
        .build()
    )(exists)
  }


  private def exists(response: GetResponse): Boolean = {
    response.getCount match {
      case 0 => false
      case 1 => true
      case _ => throw new Error
    }
  }

  private def deleted(response: com.coreos.jetcd.kv.DeleteResponse): Boolean = {
    response.getDeleted match {
      case 0 => false
      case x if 1 >= x => true
      case _ => throw new Error
    }
  }

  private def tryGetData[U](
                             key: String,
                             option: GetOption = GetOption.DEFAULT)
                           (extractResult: GetResponse => U): Try[U] = Try {
    val asyncResponse = client.getKVClient.get(key, option)

    asyncResponse.get
  }.map(extractResult(_))


  private def tryGrantLease(ttl: Long): Try[Long] = Try {
    val leaseAsyncResp = client.getLeaseClient.grant(ttl)
    leaseAsyncResp.get.getID
  }

  private def tryKeepLeaseAlive(leaseId: Long): Try[Any] = Try {
    client.getLeaseClient.keepAlive(leaseId)
  }

  private def tryCreateWithLease[Req <: AsyncRequest](
                                                    key: String,
                                                    data: Array[Byte],
                                                    ctx: Option[Any]): Try[Unit] = Try {
    // Create lease
    val leaseAsyncResp = client.getLeaseClient.grant(8L)
    val leaseId = leaseAsyncResp.get.getID

    // Create
    tryCreate(EtcdClient.absolutePath(key, root), data, ctx, PutOption.newBuilder.withLeaseId(leaseId).build).get

    // Lease keep alive
    client.getLeaseClient.keepAlive(leaseId)
    ()
  }

  private def createWithLease[Req <: AsyncRequest](
                                                    key: String,
                                                    data: Array[Byte],
                                                    ctx: Option[Any]): Req#Response = {
    val response = tryCreateWithLease(key, data, ctx)
    val zkResult = new ZkCreateResponse(response)
    CreateResponse(zkResult.resultCode, key, ctx, "").asInstanceOf[Req#Response]
  }


  private def create[Req <: AsyncRequest](key: String,
                     data: Array[Byte],
                     ctx: Option[Any],
                     option: PutOption = PutOption.DEFAULT):Req#Response = {

    val response = tryCreate(EtcdClient.absolutePath(root, key), data, ctx, option)
    val zkResult = new ZkCreateResponse(response)

    CreateResponse(zkResult.resultCode, key, ctx, "").asInstanceOf[Req#Response]
  }


  private def tryCreate[Req <: AsyncRequest](
                                              key: String,
                                              data: Array[Byte],
                                              ctx: Option[Any],
                                              option: PutOption = PutOption.DEFAULT): Try[Unit] = Try {
    val createAsyncResp = client.getKVClient.put(key, data, option)
    createAsyncResp.get

    ()
  }

  private def tryDelete[U](
                            key: String,
                            ctx: Option[Any],
                            option: DeleteOption = DeleteOption.DEFAULT)
                          (extractResult: DeleteResponse => U): Try[U]= Try {
    val asyncResp = client.getKVClient.delete(key)
    asyncResp.get()
  }.map(extractResult(_))
}

private[etcd] object EtcdClient {
  def absolutePath(root: String, path: String) = s"$root$path"
}
