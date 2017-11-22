package kafka.etcd

import org.apache.zookeeper.KeeperException

import scala.util.Try

private[etcd] class ZkDeleteResponse(response: Try[Boolean]) extends ZkResult(response) {
  private val zkResult: Try[KeeperException.Code] = response.map {
    deleted => if (deleted) KeeperException.Code.OK else KeeperException.Code.NONODE
  } recover(onError)

  override def resultCode = zkResult.get
}
