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
import scala.collection.JavaConverters._

import scala.util.Try

private[etcd] class ZkGetChildrenResponse(tryResponse: Try[TxnResponse], parent: String) extends ZkResult(tryResponse){

  override def resultCode: KeeperException.Code = tryResponse.map{ resp =>
    if(resp.isSucceeded) KeeperException.Code.OK else  KeeperException.Code.NONODE
  }.recover(onError).get

  def childrenKeys: Set[String] = {
    val startIdx = parent.length
    tryResponse.map { resp =>
      if(resp.isSucceeded){
        resp.getGetResponses.get(0).getKvs.asScala.map(_.getKey.toStringUtf8).map {
          k =>
            k.indexOf('/', startIdx) match {
              case endIdx if endIdx >= startIdx => k.substring(startIdx, endIdx)
              case _ => k.substring(startIdx)
            }
        }.toSet
      } else
        Set.empty[String]
    }.getOrElse(Set.empty[String])
  }
}