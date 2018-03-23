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

import com.coreos.jetcd.common.exception.{ErrorCode, EtcdException}
import kafka.utils.Logging
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.BadVersionException

private[etcd] abstract class ZkResult[R](result: R) extends Logging {
  protected val onError = PartialFunction[Throwable, KeeperException.Code] {
    case ex: EtcdException if ex.getErrorCode == ErrorCode.UNAVAILABLE  => KeeperException.Code.CONNECTIONLOSS
    case ex: EtcdException if ex.getErrorCode == ErrorCode.INVALID_ARGUMENT  => KeeperException.Code.BADARGUMENTS
    case ex: BadVersionException => ex.code()
    case ex: Error =>
      error(ex.getMessage)
      KeeperException.Code.SYSTEMERROR
  }

  def resultCode: KeeperException.Code
}
