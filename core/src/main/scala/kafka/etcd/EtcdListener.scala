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
import com.coreos.jetcd.Watch.Watcher
import com.coreos.jetcd.options.WatchOption
import com.coreos.jetcd.watch.WatchEvent

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

private[etcd] class EtcdListener(keyPrefix: String, client: Client)(eventHandler: (WatchEvent) =>Unit) {

  import Implicits._

  // Watcher for watching all keys prefixed with `keyPrefix`
  private val watcher: Watcher = client.getWatchClient.watch(
    keyPrefix,
    WatchOption.newBuilder()
        .withPrefix(keyPrefix)
      .build()
  )

  Future {
    while(true) {
      val watchResponse = blocking {
        watcher.listen()
      }

      watchResponse.getEvents.asScala.foreach(eventHandler)
    }
  }

  def close(): Unit = {
    watcher.close()
  }

}

private[etcd] object EtcdListener {
  def apply(keyPrefix: String, client: Client)(eventHandler: (WatchEvent) =>Unit): EtcdListener = new EtcdListener(keyPrefix, client)(eventHandler)
  
}
