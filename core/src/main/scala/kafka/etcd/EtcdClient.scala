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
import com.coreos.jetcd.kv.TxnResponse
import com.coreos.jetcd.op.{Cmp, CmpTarget, Op}
import com.coreos.jetcd.options.{DeleteOption, GetOption, PutOption}
import com.coreos.jetcd.watch.WatchEvent
import kafka.metastore.KafkaMetastore
import kafka.utils.Logging
import kafka.zookeeper._
import org.apache.zookeeper.CreateMode

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

class EtcdClient(connectionString: String = "127.0.0.1:2379/kafka") extends KafkaMetastore with Logging {

  import Implicits._


  private val connStringParts = connectionString.split('/')

  private val connectionStringWithoutPrefix = connStringParts.head
  private val prefix = connStringParts.tail.mkString("/", "/", "")

  private val client: Client = Client.builder.endpoints(connectionStringWithoutPrefix).build()

  // Event handlers to handler events received from ETCD
  private val createHandlers = new ChangeHandlers
  private val updateHandlers = new ChangeHandlers
  private val deleteHandlers = new ChangeHandlers
  private val childHandlers = new ChangeHandlers


  // Subscribe to etcd events
  private val etcdListener = EtcdListener(prefix, client) {
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
      case ExistsRequest(path, ctx) => exists(path, ctx)

      case GetDataRequest(path, ctx) => getData(path, ctx)

      case GetChildrenRequest(path, ctx) => getChildrenData(path, ctx)

      case CreateRequest(path, data, _, createMode, ctx) =>
        createMode match {
          case CreateMode.EPHEMERAL => createWithLease(path, data, ctx)
          case CreateMode.PERSISTENT => create(path, data, ctx)
          case CreateMode.PERSISTENT_SEQUENTIAL => ???
          case _ => ???
        }

      case SetDataRequest(path, data, _, ctx) => setData(path, data, ctx)

      case DeleteRequest(path, _, ctx) => delete(path, ctx)

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

  private def exists [Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val response = tryExists(EtcdClient.absolutePath(prefix, key))
    val zkResult = new ZkExistsResponse(response)
    ExistsResponse(zkResult.resultCode, key, ctx, null).asInstanceOf[Req#Response]
  }

  private def getData[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val response = tryGetData(EtcdClient.absolutePath(prefix, key))
    val zkResult = new ZkGetDataResponse(response)
    GetDataResponse(zkResult.resultCode, key, ctx, zkResult.data.orNull, null).asInstanceOf[Req#Response]
  }

  private def getChildrenData[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val parent = if (key.endsWith("/")) key else s"$key/"
    val response = tryGetData(
      EtcdClient.absolutePath(prefix, parent),
      GetOption.newBuilder().withKeysOnly(true).withPrefix(EtcdClient.absolutePath(prefix, parent)).build())
    val zkResult = new ZkGetChildrenResponse(response, parent)
    GetChildrenResponse(zkResult.resultCode, key, ctx, zkResult.childrenKeys.toSeq, null).asInstanceOf[Req#Response]
  }

  private def delete[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val response = tryDelete(EtcdClient.absolutePath(prefix, key))
    val zkResult = new ZkDeleteResponse(response)
    DeleteResponse(zkResult.resultCode, key, ctx).asInstanceOf[Req#Response]
  }

  //This method only sets data does not create it. This can be done differently
  // in ETCD but ZK cannot do it in one request
  private def setData[Req <: AsyncRequest](
                                            key: String,
                                            data: Array[Byte],
                                            ctx: Option[Any]): Req#Response = {
    val response = trySetData(EtcdClient.absolutePath(prefix, key), data, ctx)
    val zkResult = new ZkSetDataResponse(response)
    SetDataResponse(zkResult.resultCode, key, ctx, null).asInstanceOf[Req#Response]
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
                                          option: PutOption = PutOption.DEFAULT): Req#Response = {

    val response = tryCreate(EtcdClient.absolutePath(prefix, key), data, ctx, option)
    val zkResult = new ZkCreateResponse(response)

    CreateResponse(zkResult.resultCode, key, ctx, "").asInstanceOf[Req#Response]
  }

  private def tryCreateWithLease[Req <: AsyncRequest](
                                                       key: String,
                                                       data: Array[Byte],
                                                       ctx: Option[Any]): Try[TxnResponse] = Try {
    // Create lease
    val leaseAsyncResp = client.getLeaseClient.grant(8L)
    val leaseId = leaseAsyncResp.get.getID

    // Create
    val txnResponse = tryCreate(
      EtcdClient.absolutePath(key, prefix),
      data,
      ctx,
      PutOption.newBuilder.withLeaseId(leaseId).build).get

    // Lease keep alive
    if (txnResponse.isSucceeded)
      client.getLeaseClient.keepAlive(leaseId)

    txnResponse
  }


  // In ETCD if version for the given key is zero it means that key does not exist
  private def tryCreate[Req <: AsyncRequest](
                                              key: String,
                                              data: Array[Byte],
                                              ctx: Option[Any],
                                              option: PutOption = PutOption.DEFAULT): Try[TxnResponse] = Try {
    client.getKVClient.txn().If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))).
      Then(Op.put(key, data, option)).commit().get()
  }

  // In ETCD if version for the given key is bigger than zero it means that key exists
  private def trySetData[Req <: AsyncRequest](
                                               key: String,
                                               data: Array[Byte],
                                               ctx: Option[Any],
                                               option: PutOption = PutOption.DEFAULT): Try[TxnResponse] = Try {
    client.getKVClient.txn().If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.version(0))).
      Then(Op.put(key, data, option)).commit().get()
  }

  private def tryDelete[Req <: AsyncRequest](
                                              key: String,
                                              option: DeleteOption = DeleteOption.DEFAULT): Try[TxnResponse] = Try {
    client.getKVClient.txn().If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.version(0))).
      Then(Op.delete(key, option)).commit().get()
  }

  private def tryExists[Req <: AsyncRequest](key: String): Try[Boolean] = Try {
    val response = client.getKVClient.txn().If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.version(0))).commit().get()
    response.isSucceeded
  }

  private def tryGetData[Req <: AsyncRequest](
                                               key: String,
                                               option: GetOption = GetOption.DEFAULT): Try[TxnResponse] = Try {
    client.getKVClient.txn().If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.version(0))).
      Then(Op.get(key, option)).commit().get()
  }
}

private[etcd] object EtcdClient {
  val DEFAULT_PREFIX = "/"

  def absolutePath(prefix: String, path: String) = if (prefix != DEFAULT_PREFIX) s"$prefix$path" else path
}
