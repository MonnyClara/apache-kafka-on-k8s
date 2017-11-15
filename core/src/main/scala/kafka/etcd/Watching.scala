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
import com.coreos.jetcd.data.ByteSequence
import com.coreos.jetcd.options.WatchOption
import com.coreos.jetcd.watch.WatchEvent
import kafka.utils.Logging

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.concurrent.Map
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success}

trait Watching extends Logging {
  implicit def string2ByteSequence(s: String): ByteSequence = ByteSequence.fromString(s)

  private val watchers: Map[String, Watcher] = new java.util.concurrent.ConcurrentHashMap[String, Watcher].asScala

  def watch(key: String)(eventHandler: (WatchEvent) =>Unit)(implicit client: Client): Unit = {
    val watcher = watchers.getOrElseUpdate(key, watcherFor(key))

    val listener = listen(watcher)(eventHandler)

    listener onComplete {
      case Success(_) => info(s"Processing events on '$key' finished.")
      case Failure(ex) => info(s"Processing events on '$key' exited due to $ex")
    }
  }

  def unwatch(key: String): Unit = {
    info(s"Stop watcher for '$key'")

    val watcher = watchers.remove(key)
    watcher.foreach(_.close)
  }

  def unwatchAll(): Unit = {
    watchers.foreach {
      case (_, watcher)  =>
        watcher.close()
    }
  }


  private def watcherFor(key: ByteSequence)(implicit client: Client) = {
    client.getWatchClient.watch(key, WatchOption.DEFAULT)
  }


  private def listen(watcher: Watcher)(eventHandler: (WatchEvent) =>Unit) = Future {
    while(true) {
      val watchResponse = blocking {
        watcher.listen()
      }

      watchResponse.getEvents.asScala.foreach(eventHandler)
    }
  }


}
