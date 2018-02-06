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
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.BadVersionException

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.collection.JavaConverters._

class EtcdClient(connectionString: String, time: Time) extends KafkaMetastore with Logging {

  import Implicits._
  import EtcdClient._

  implicit private val _time: Time = time
  private val connStringParts = connectionString.split('/')

  private val connectionStringWithoutPrefix = connStringParts.head
  private val prefix = connStringParts.tail.mkString("/", "/", "")

  private val client: Client = Client.builder.endpoints(s"http://$connectionStringWithoutPrefix").build()

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
    info(s"Register change handler for path:${zNodeChangeHandler.path}")
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
    info(s"Unregister change handler for children of '$path'")

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
          case CreateMode.PERSISTENT_SEQUENTIAL => createSequential(path, data, ctx)
          case _ => ???
        }

      case SetDataRequest(path, data, version, ctx) => setData(path, data, version, ctx)

      case DeleteRequest(path, _, ctx) => delete(path, ctx)

      case GetAclRequest(path, ctx) => ???
      case SetAclRequest(path, acl, version, ctx) => ???
    }
  }


  override def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    info(s"Handle requests: $requests")

    val asyncResponses: Seq[Future[Req#Response]] = requests.map(req => Future(handleRequest(req)))
    val responses: Future[Seq[Req#Response]] = Future.fold(asyncResponses)(Seq.empty[Req#Response])(_ :+ _)

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

  private def exists[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val (result, sendTime, receiveTime) = duration { tryExists(absolutePath(prefix, key)) }
    val zkResult = new ZkExistsResponse(result)

    ExistsResponse(zkResult.resultCode, key, ctx, null, ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  private def getData[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val keyAbsolutePath = absolutePath(prefix, key)
    val (result, sendTime, receiveTime) = duration { tryGetData(keyAbsolutePath, keyAbsolutePath) }
    val zkResult = new ZkGetDataResponse(result)

    GetDataResponse(zkResult.resultCode, key, ctx, zkResult.data.orNull, zkResult.stat.orNull, ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  private def getChildrenData[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val parent = if (key.endsWith("/")) key.substring(0, key.length-2) else key
    val parentAbsolutePath = absolutePath(prefix, parent)

    val (result, sendTime, receiveTime) = duration {
      tryGetData(
        s"$parentAbsolutePath/",
        parentAbsolutePath,
        GetOption.newBuilder()
          .withKeysOnly(true)
          .withPrefix(s"$parentAbsolutePath/")
          .build())
    }
    val zkResult = new ZkGetChildrenResponse(result, s"$parentAbsolutePath/")

    GetChildrenResponse(zkResult.resultCode, key, ctx, zkResult.childrenKeys.toSeq, null, ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  private def delete[Req <: AsyncRequest](key: String, ctx: Option[Any]): Req#Response = {
    val (result, sendTime, receiveTime) = duration { tryDelete(absolutePath(prefix, key)) }
    val zkResult = new ZkDeleteResponse(result)
    DeleteResponse(zkResult.resultCode, key, ctx, ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  //This method only sets data does not create it. This can be done differently
  // in ETCD but ZK cannot do it in one request
  private def setData[Req <: AsyncRequest](
                                            key: String,
                                            data: Array[Byte],
                                            version: Int,
                                            ctx: Option[Any]): Req#Response = {

    val (result, sendTime, receiveTime) = duration { trySetData(absolutePath(prefix, key), data, version, ctx) }
    val zkResult = new ZkSetDataResponse(result)

    SetDataResponse(zkResult.resultCode, key, ctx, zkResult.stat.orNull, ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  private def createWithLease[Req <: AsyncRequest](
                                                    key: String,
                                                    data: Array[Byte],
                                                    ctx: Option[Any]): Req#Response = {

    val (result, sendTime, receiveTime) = duration { tryCreateWithLease(absolutePath(prefix, key), data, ctx) }
    val zkResult = new ZkCreateResponse(result)

    CreateResponse(zkResult.resultCode, key, ctx, "", ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  private def create[Req <: AsyncRequest](key: String,
                                          data: Array[Byte],
                                          ctx: Option[Any],
                                          option: PutOption = PutOption.DEFAULT): Req#Response = {

    val (result, sendTime, receiveTime) = duration { tryCreate(absolutePath(prefix, key), data, ctx, option) }
    val zkResult = new ZkCreateResponse(result)

    CreateResponse(zkResult.resultCode, key, ctx, "", ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
  }

  private def createSequential[Req <: AsyncRequest](key: String,
                                                    data: Array[Byte],
                                                    ctx: Option[Any]): Req#Response = {
    val (result, sendTime, receiveTime) = duration { tryCreateSequential(absolutePath(prefix, key), data) }
    val zkResult = new ZkCreateResponse(result)

    CreateResponse(zkResult.resultCode, key, ctx, "", ResponseMetadata(sendTime, receiveTime)).asInstanceOf[Req#Response]
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
      key,
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

    client.getKVClient.txn()
      .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
      .Then(Op.put(key, data, option))
      .commit().get()
  }

  // ETCD does not support SEQUENTIAL keys out of the box.
  // This implementation is based on https://github.com/coreos/etcd/blob/master/contrib/recipes/key.go
  // TODO: we need to handle various error cases when <key>nnnn and __<key> get out of sync
  private def tryCreateSequential[Req <: AsyncRequest](key: String,
                                                       data: Array[Byte]): Try[TxnResponse] = Try {
    // sequential keys are stored as <key>nnnnn
    // e.g. key00001
    //      key00002
    //      key00003

    @tailrec
    def createSeqKV(): TxnResponse = {
      // get the last sequential key
      val seqKeys = client.getKVClient.get(
        key,
        GetOption.newBuilder()
          .withKeysOnly(true)
          .withPrefix(key)
          .withSortField(GetOption.SortTarget.KEY)
          .withSortOrder(GetOption.SortOrder.DESCEND)
          .withLimit(1L)
          .build()
      ).get.getKvs.asScala


      // determine next sequence number
      val nextSeqNumber: Int = seqKeys.headOption.map {
        kv =>
          val key: String = kv.getKey
          key match {
            case SEQUENCEKEY_REGEX(_, seqNumber) => seqNumber.toInt + 1 // increment current seq number by 1
            case _ =>
              error(s"'$key' is not a sequence key !")
              throw new Exception(s"'$key' is not a sequence key !")
          }
      }.getOrElse(0) // if no <key> found than default sequence number to 0

      val nextKey = f"$key$nextSeqNumber%010d"

      // the key where we track the revision number of when the sequence key was created
      // if the modification revision number of __<key> is the same as the creation revision number of
      // last sequential <key> than no new sequential key was created meanwhile so we can go ahead and create a new one
      // if meanwhile there was a new sequential <key> created since we grabbed the last <key> than the modification
      // revision number moved which mean we need to start from the beginning in order to generate
      // the appropriate next sequence key
      val revisionTrackerKey = s"__$key"

      val revision = seqKeys.headOption.map(_.getCreateRevision).getOrElse(0L)

      // if __<key> hasn't been created yet OR
      // modification revision of __<key> hasn't been changed by someone else
      val txnResponse = client.getKVClient.txn
        .If(
          new Cmp(revisionTrackerKey, Cmp.Op.EQUAL, CmpTarget.modRevision(revision))
        ).If(
        new Cmp(revisionTrackerKey, Cmp.Op.EQUAL, CmpTarget.version(0))
      ).Then(
        Op.put(revisionTrackerKey, "", PutOption.DEFAULT),
        Op.put(nextKey, data, PutOption.DEFAULT)
      ).commit().get()

      if (txnResponse.isSucceeded)
        txnResponse
      else createSeqKV
    }

    createSeqKV
  }

  // In ETCD if version for the given key is bigger than zero it means that key exists
  private def trySetData[Req <: AsyncRequest](
                                               key: String,
                                               data: Array[Byte],
                                               version: Int,
                                               ctx: Option[Any],
                                               option: PutOption = PutOption.DEFAULT): Try[TxnResponse] = Try {
    val ifStatement = version match {
      case -1 => client.getKVClient.txn().
        If(new Cmp(key, Cmp.Op.GREATER, CmpTarget.version(0)))
      case _ => client.getKVClient.txn().
        If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(version)))
    }
    val response = ifStatement.
      Then(Op.put(key, data, option), Op.get(key, GetOption.DEFAULT)).
      Else(Op.get(key, GetOption.DEFAULT)).commit().get()
    if (version != -1 && !response.isSucceeded) {
      if (response.getGetResponses.get(0).getKvs.get(0).getVersion > version)
        throw new BadVersionException(key)
    }
    response
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
                                               applyConditionOnKey: String,
                                               option: GetOption = GetOption.DEFAULT): Try[TxnResponse] = Try {
    client.getKVClient.txn().If(new Cmp(applyConditionOnKey, Cmp.Op.GREATER, CmpTarget.version(0))).
      Then(Op.get(key, option)).commit().get()
  }
}

private[etcd] object EtcdClient {
  val DEFAULT_PREFIX = "/"
  val SEQUENCEKEY_REGEX = """(.+)(\d+)$""".r

  def absolutePath(prefix: String, path: String): String = if (prefix != DEFAULT_PREFIX) s"$prefix$path" else path

  def duration[R](body: => R)(implicit time: Time): (R, Long, Long) = {
    val sendTimeMs: Long = time.hiResClockMs
    val result: R = body
    val receivedTimeMs:Long = time.hiResClockMs

    (result, sendTimeMs, receivedTimeMs)
  }
}
