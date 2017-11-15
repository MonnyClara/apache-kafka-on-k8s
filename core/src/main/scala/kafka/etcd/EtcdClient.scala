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
import com.coreos.jetcd.data.ByteSequence
import com.coreos.jetcd.exception.{ErrorCode, EtcdException}
import com.coreos.jetcd.options.GetOption
import com.coreos.jetcd.watch.WatchEvent
import kafka.metastore.KafkaMetastore
import kafka.utils.Logging
import kafka.zookeeper._
import org.apache.zookeeper.KeeperException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class EtcdClient
  extends KafkaMetastore
  with Watching with Logging {

  implicit private val client: Client = Client.builder.endpoints("http://127.0.0.1:2379").build()

  private val createHandlers = new ChangeHandlers
  private val updateHandlers = new ChangeHandlers
  private val deleteHandlers = new ChangeHandlers



  implicit def bytes2ByteSequence(b: Array[Byte]): ByteSequence = ByteSequence.fromBytes(b)
  implicit def byteSequence2StringUtf8(bs: ByteSequence): String = bs.toStringUtf8
  implicit def byteSequence2Bytes(bs: ByteSequence): Array[Byte] = bs.getBytes



  override def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {

    registerChangeHandler(zNodeChangeHandler.path,
      zNodeChangeHandler.handleCreation,
      zNodeChangeHandler.handleDataChange,
      zNodeChangeHandler.handleDeletion
    )

  }

  override def unregisterZNodeChangeHandler(path: String): Unit = {
    unwatch(path)

    createHandlers.unregisterChangeHandler(path)
    updateHandlers.unregisterChangeHandler(path)
    deleteHandlers.unregisterChangeHandler(path)
  }

  override def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    watchChildren(zNodeChildChangeHandler.path)
  }

  override def unregisterZNodeChildChangeHandler(path: String): Unit = {
    unwatchChildren(path)
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
      case ExistsRequest(key, ctx) =>
        val asyncResponse = client.getKVClient.get(key, GetOption.newBuilder().withCountOnly(true).build())

        val (respStatus, _) = handleAsyncRequestResponse(asyncResponse) { response =>
          response.getCount match {
            case 0 => (KeeperException.Code.NONODE, None)
            case 1 => (KeeperException.Code.OK, None)
            case _ => (KeeperException.Code.SYSTEMERROR, None)
          }
        }
        ExistsResponse(respStatus, key, ctx, null).asInstanceOf[Req#Response]

      case GetDataRequest(key, ctx) =>
        val asyncResponse = client.getKVClient.get(key)

        val (respStatus, data) = handleAsyncRequestResponse(asyncResponse) { response =>
          response.getCount match {
            case 0 => (KeeperException.Code.NONODE, None)
            case 1 =>
              val v: Option[Array[Byte]] = Some(response.getKvs.get(0).getValue)
              (KeeperException.Code.OK, v)
            case _ => (KeeperException.Code.SYSTEMERROR, None)
          }
        }
        GetDataResponse(respStatus, key, ctx, data.orNull, null).asInstanceOf[Req#Response]

      case SetDataRequest(key, data, _, ctx) =>
        val asyncResponse = client.getKVClient.put(key, data)

        val (respStatus, _) = handleAsyncRequestResponse(asyncResponse){ _ => (KeeperException.Code.OK, None) }
        SetDataResponse(respStatus, key, ctx, null).asInstanceOf[Req#Response]

      case _ =>
        null.asInstanceOf[Req#Response]
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
    unwatchAll()

    // Close etcd client
    client.close()
  }




  private def watchChildren(key: ByteSequence) = {
    info(s"Register change handler for children of '$key'")

    // TODO: implement me
  }

  private def unwatchChildren(key: ByteSequence) = {
    info(s"Stop watcher for children of '$key'")

    // TODO: implement me
  }


  private def registerChangeHandler(key: String,
                                    onCreate: ChangeHandlers#Handler,
                                    onValueChange: ChangeHandlers#Handler,
                                    onDelete: ChangeHandlers#Handler): Unit = {


    createHandlers.registerChangeHandler(key, onCreate)
    updateHandlers.registerChangeHandler(key, onValueChange)
    deleteHandlers.registerChangeHandler(key, onDelete)

    watch(key) {
      event: WatchEvent =>
          val eventType = event.getEventType
          val eventData = Option(event.getKeyValue)
          val key: Option[String] = eventData.map(_.getKey)
          val value: Option[String] = eventData.map(_.getValue)

          info(s"Received change notification: '$eventType' : '$key' -> '$value'")

          eventData.foreach {
            kv => eventType match {
              case WatchEvent.EventType.PUT if kv.getVersion == 1L => createHandlers.triggerOn(key.get)
              case WatchEvent.EventType.PUT => updateHandlers.triggerOn(key.get)
              case WatchEvent.EventType.DELETE => deleteHandlers.triggerOn(key.get)
              case _ =>
                error(s"Received unrecognized ETCD event type received for '$key'!")
                throw new Exception(s"Received unrecognized ETCD event type for '$key'!")
            }

          }
    }
  }


  private def handleAsyncRequestResponse[R, U](
                    asyncResponse: CompletableFuture[R])
                    (handleResponse: R =>(KeeperException.Code, Option[U])): (KeeperException.Code, Option[U])  = {

    Try(asyncResponse.get) match {
      case Success(response) => handleResponse(response)
      case Failure(ex: EtcdException) if ex.getErrorCode == ErrorCode.UNAVAILABLE  =>
        (KeeperException.Code.CONNECTIONLOSS, None)
      case _ =>
        (KeeperException.Code.SYSTEMERROR, None)
    }

  }
}
