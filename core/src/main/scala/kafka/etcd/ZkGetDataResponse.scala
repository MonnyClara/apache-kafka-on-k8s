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

import com.coreos.jetcd.kv.TxnResponse
import org.apache.zookeeper.KeeperException

import scala.util.Try

private[etcd] class ZkGetDataResponse(tryResponse: Try[TxnResponse]) extends ZkResult(tryResponse) {

  override def resultCode: KeeperException.Code = tryResponse.map { resp =>
    if (resp.isSucceeded) {
      Some(resp.getGetResponses.get(0).getKvs.get(0).getValue)
      KeeperException.Code.OK
    } else KeeperException.Code.NONODE
  }.recover(onError).get

  // We need to think this through because I think this one does not works
  def data: Option[Array[Byte]] =
    tryResponse.map { resp =>
      if (resp.isSucceeded) {
        Some(resp.getGetResponses.get(0).getKvs.get(0).getValue.getBytes)
      }
      else None
    }.getOrElse(None)
}
