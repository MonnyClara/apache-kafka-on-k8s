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

import java.util.concurrent.CompletableFuture

import com.coreos.jetcd.Client
import com.coreos.jetcd.exception.{ErrorCode, EtcdException}
import com.coreos.jetcd.options.{GetOption, PutOption}
import com.coreos.jetcd.watch.WatchEvent
import kafka.metastore.KafkaMetastore
import kafka.utils.Logging
import kafka.zookeeper._
import org.apache.zookeeper.{CreateMode, KeeperException}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters.asScalaBufferConverter

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
        val asyncResponse = client.getKVClient.get(EtcdClient.absolutePath(root, path), GetOption.newBuilder().withCountOnly(true).build())

        val (respStatus, _) = handleAsyncRequestResponse(asyncResponse) { response =>
          response.getCount match {
            case 0 => (KeeperException.Code.NONODE, None)
            case 1 => (KeeperException.Code.OK, None)
            case _ => (KeeperException.Code.SYSTEMERROR, None)
          }
        }
        ExistsResponse(respStatus, path, ctx, null).asInstanceOf[Req#Response]

      case GetDataRequest(path, ctx) =>
        val asyncResponse = client.getKVClient.get(EtcdClient.absolutePath(root, path))

        val (respStatus, data) = handleAsyncRequestResponse(asyncResponse) { response =>
          response.getCount match {
            case 0 => (KeeperException.Code.NONODE, None)
            case 1 =>
              val v: Option[Array[Byte]] = Some(response.getKvs.get(0).getValue)
              (KeeperException.Code.OK, v)
            case _ => (KeeperException.Code.SYSTEMERROR, None)
          }
        }
        GetDataResponse(respStatus, path, ctx, data.orNull, null).asInstanceOf[Req#Response]

      case GetChildrenRequest(path, ctx) =>
        val parent = if (path.endsWith("/")) path else s"$path/"

        val asyncResponse = client.getKVClient.get(
          EtcdClient.absolutePath(root, parent),
          GetOption.newBuilder()
              .withPrefix(EtcdClient.absolutePath(root, parent))
              .withKeysOnly(true)
            .build()
        )
        val (respStatus, children) = handleAsyncRequestResponse(asyncResponse) { response =>
          val keys: Set[String] = response.getKvs.asScala.map(_.getKey.toStringUtf8).map {
            k =>
              val startIdx = EtcdClient.absolutePath(root, parent).length

              k.indexOf('/', startIdx) match {
                case endIdx if endIdx >= startIdx => k.substring(startIdx, endIdx)
                case _ => k.substring(startIdx)
              }
          }.toSet

          (KeeperException.Code.OK, Some(keys))
        }

        GetChildrenResponse(respStatus, path, ctx, children.get.toSeq, null).asInstanceOf[Req#Response]

      case CreateRequest(path, data, acl, createMode, ctx) =>
        createMode match {
          case CreateMode.EPHEMERAL => createWithLease(EtcdClient.absolutePath(root, path), data, ctx)
          case CreateMode.PERSISTENT => create(EtcdClient.absolutePath(root, path), data, ctx)
          case CreateMode.PERSISTENT_SEQUENTIAL => ???
          case _ => ???
        }

      case SetDataRequest(path, data, _, ctx) =>
        val asyncResponse = client.getKVClient.put(EtcdClient.absolutePath(root, path), data)

        val (respStatus, _) = handleAsyncRequestResponse(asyncResponse){ _ => (KeeperException.Code.OK, None) }
        SetDataResponse(respStatus, path, ctx, null).asInstanceOf[Req#Response]

      case DeleteRequest(path, version, ctx) => ???
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


  private def handleAsyncRequestResponse[R, U](
                    asyncResponse: CompletableFuture[R])
                    (handleResponse: R =>(KeeperException.Code, Option[U])): (KeeperException.Code, Option[U])  = {

    Try(asyncResponse.get) match {
      case Success(response) => handleResponse(response)
      case Failure(ex: EtcdException) if ex.getErrorCode == ErrorCode.UNAVAILABLE  =>
        (KeeperException.Code.CONNECTIONLOSS, None)
      case Failure(ex: EtcdException) if ex.getErrorCode == ErrorCode.INVALID_ARGUMENT  =>
        (KeeperException.Code.BADARGUMENTS, None)
      case _ =>
        (KeeperException.Code.SYSTEMERROR, None)
    }

  }

  private def parentOf(key: String): Option[String] = {
    Option(key) match {
      case Some(k) if !k.isEmpty =>
        Some(k.substring(0, k.lastIndexOf('/')))
      case _ => None
    }
  }

  private def createWithLease[Req <: AsyncRequest](path: String, data: Array[Byte], ctx: Option[Any]):Req#Response = {
    // Grant lease
    val leaseAsyncResp = client.getLeaseClient.grant(8L)
    val (respStatus, Some(leaseId)) = handleAsyncRequestResponse(leaseAsyncResp) {
      r => (KeeperException.Code.OK, Some(r.getID))
    }

    if (respStatus != KeeperException.Code.OK)
      CreateResponse(respStatus, path, ctx, "").asInstanceOf[Req#Response]

    // Create
    val createResponse = create(path, data, ctx,
      PutOption.newBuilder
        .withLeaseId(leaseId)
      .build
    )

    if (createResponse.resultCode != KeeperException.Code.OK)
      createResponse

    // Lease keep alive
    val (status, _) = Try(client.getLeaseClient.keepAlive(leaseId)) match {
      case Success(_) => (KeeperException.Code.OK, None)
      case Failure(ex: EtcdException) if ex.getErrorCode == ErrorCode.INVALID_ARGUMENT =>
        (KeeperException.Code.BADARGUMENTS, None)
      case _ =>
        (KeeperException.Code.SYSTEMERROR, None)
    }


    CreateResponse(status, path, ctx, "").asInstanceOf[Req#Response]
  }

  private def create[Req <: AsyncRequest](
                                           path: String,
                                           data: Array[Byte],
                                           ctx: Option[Any],
                                           option: PutOption = PutOption.DEFAULT):Req#Response = {
    val createAsyncResp = client.getKVClient.put(
      path,
      data,
      option
    )

    val (createStatus, _) = handleAsyncRequestResponse(createAsyncResp) { _ => (KeeperException.Code.OK, None) }

    CreateResponse(createStatus, path, ctx, "").asInstanceOf[Req#Response]
  }

}

private[etcd] object EtcdClient {
  def absolutePath(root: String, path: String) = s"$root$path"
}
