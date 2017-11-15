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

package kafka.metastore

import kafka.zookeeper.{AsyncRequest, StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}

trait KafkaMetastore {

  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit
  def unregisterZNodeChangeHandler(path: String): Unit
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit
  def unregisterZNodeChildChangeHandler(path: String): Unit
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit
  def unregisterStateChangeHandler(name: String): Unit
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response]
  def waitUntilConnected(): Unit
  def sessionId: Long = ???
  def close(): Unit

}
