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

import java.nio.charset.StandardCharsets

import com.coreos.jetcd.data.ByteSequence

private[etcd] object Implicits {
  //This "[null]" value needs to be set for etcd because, zk can handle empty value for a node but etcd can't
  implicit def bytes2ByteSequence(b: Array[Byte]): ByteSequence = {
    val etcdData = Option(b).getOrElse("[null]".getBytes(StandardCharsets.UTF_8))
    ByteSequence.fromBytes(etcdData)
  }
  implicit def byteSequence2StringUtf8(bs: ByteSequence): String = bs.toStringUtf8
  implicit def string2ByteSequence(s: String): ByteSequence = ByteSequence.fromString(s)
  implicit def byteSequence2Bytes(bs: ByteSequence): Array[Byte] = bs.getBytes

}
