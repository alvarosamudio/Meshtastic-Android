/*
 * Copyright (c) 2025 Meshtastic LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  自 <https://www.gnu.org/licenses/>.
 */

package com.geeksville.mesh.service

import android.util.Log
import com.geeksville.mesh.BuildConfig
import com.geeksville.mesh.concurrent.handledLaunch
import dagger.Lazy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.meshtastic.core.data.repository.MeshLogRepository
import org.meshtastic.core.database.entity.MeshLog
import org.meshtastic.core.service.ServiceRepository
import org.meshtastic.proto.MeshProtos
import org.meshtastic.proto.MeshProtos.FromRadio.PayloadVariantCase
import org.meshtastic.proto.MeshProtos.MeshPacket
import org.meshtastic.proto.Portnums
import org.meshtastic.proto.fromRadio
import timber.log.Timber
import java.util.ArrayDeque
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.time.Duration.Companion.milliseconds

@Singleton
class MeshMessageProcessor
@Inject
constructor(
    private val nodeManager: MeshNodeManager,
    private val packetHandler: PacketHandler,
    private val serviceRepository: ServiceRepository,
    private val meshLogRepository: Lazy<MeshLogRepository>,
    private val router: MeshRouter,
    private val mqttManager: MeshMqttManager,
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val logUuidByPacketId = ConcurrentHashMap<Int, String>()
    private val logInsertJobByPacketId = ConcurrentHashMap<Int, Job>()

    private val earlyReceivedPackets = ArrayDeque<MeshPacket>()
    private val MAX_EARLY_PACKET_BUFFER = 128

    fun start() {
        nodeManager.isNodeDbReady
            .onEach { ready ->
                if (ready) {
                    flushEarlyReceivedPackets("dbReady")
                }
            }
            .launchIn(scope)
    }

    fun handleFromRadio(bytes: ByteArray, myNodeNum: Int?) {
        runCatching { MeshProtos.FromRadio.parseFrom(bytes) }
            .onSuccess { proto ->
                if (proto.payloadVariantCase == PayloadVariantCase.PAYLOADVARIANT_NOT_SET) {
                    Timber.w("Received FromRadio with PAYLOADVARIANT_NOT_SET. rawBytes=${bytes.toHexString()}")
                }
                handleFromRadio(proto, myNodeNum)
            }
            .onFailure { primaryException ->
                runCatching {
                    val logRecord = MeshProtos.LogRecord.parseFrom(bytes)
                    handleLogRecord(logRecord)
                }
                    .onFailure { _ ->
                        Timber.e(
                            primaryException,
                            "Failed to parse radio packet (len=${bytes.size} contents=${bytes.toHexString()}). " +
                                "Not a valid FromRadio or LogRecord.",
                        )
                    }
            }
    }

    @Suppress("CyclomaticComplexMethod")
    private fun handleFromRadio(proto: MeshProtos.FromRadio, myNodeNum: Int?) {
        when (proto.payloadVariantCase) {
            PayloadVariantCase.PACKET -> handleReceivedMeshPacket(proto.packet, myNodeNum)
            PayloadVariantCase.MY_INFO -> router.configFlowManager.handleMyInfo(proto.myInfo)
            PayloadVariantCase.METADATA -> router.configFlowManager.handleLocalMetadata(proto.metadata)
            PayloadVariantCase.NODE_INFO -> {
                router.configFlowManager.handleNodeInfo(proto.nodeInfo)
                serviceRepository.setStatusMessage("Nodes (${router.configFlowManager.newNodeCount})")
            }
            PayloadVariantCase.CONFIG_COMPLETE_ID ->
                router.configFlowManager.handleConfigComplete(proto.configCompleteId)
            PayloadVariantCase.MQTTCLIENTPROXYMESSAGE ->
                mqttManager.handleMqttProxyMessage(proto.mqttClientProxyMessage)
            PayloadVariantCase.QUEUESTATUS -> packetHandler.handleQueueStatus(proto.queueStatus)
            PayloadVariantCase.CONFIG -> router.configHandler.handleDeviceConfig(proto.config)
            PayloadVariantCase.MODULECONFIG -> router.configHandler.handleModuleConfig(proto.moduleConfig)
            PayloadVariantCase.CHANNEL -> router.configHandler.handleChannel(proto.channel)
            PayloadVariantCase.CLIENTNOTIFICATION -> {
                serviceRepository.setClientNotification(proto.clientNotification)
                packetHandler.removeResponse(proto.clientNotification.replyId, complete = false)
            }
            PayloadVariantCase.LOG_RECORD -> handleLogRecord(proto.logRecord)
            PayloadVariantCase.REBOOTED -> handleRebooted(proto.rebooted)
            PayloadVariantCase.XMODEMPACKET -> handleXmodemPacket(proto.xmodemPacket)
            PayloadVariantCase.DEVICEUICONFIG -> handleDeviceUiConfig(proto.deviceuiConfig)
            PayloadVariantCase.FILEINFO -> handleFileInfo(proto.fileInfo)
            else -> Timber.d("Processor handling ${proto.payloadVariantCase}")
        }
    }

    private fun handleLogRecord(logRecord: MeshProtos.LogRecord) {
        insertMeshLog(
            MeshLog(
                uuid = UUID.randomUUID().toString(),
                message_type = "LogRecord",
                received_date = System.currentTimeMillis(),
                raw_message = logRecord.toString(),
                fromRadio = fromRadio { this.logRecord = logRecord },
            ),
        )
    }

    private fun handleRebooted(rebooted: Boolean) {
        insertMeshLog(
            MeshLog(
                uuid = UUID.randomUUID().toString(),
                message_type = "Rebooted",
                received_date = System.currentTimeMillis(),
                raw_message = rebooted.toString(),
                fromRadio = fromRadio { this.rebooted = rebooted },
            ),
        )
    }

    private fun handleXmodemPacket(xmodemPacket: org.meshtastic.proto.XmodemProtos.XModem) {
        insertMeshLog(
            MeshLog(
                uuid = UUID.randomUUID().toString(),
                message_type = "XmodemPacket",
                received_date = System.currentTimeMillis(),
                raw_message = xmodemPacket.toString(),
                fromRadio = fromRadio { this.xmodemPacket = xmodemPacket },
            ),
        )
    }

    private fun handleDeviceUiConfig(deviceUiConfig: org.meshtastic.proto.DeviceUIProtos.DeviceUIConfig) {
        insertMeshLog(
            MeshLog(
                uuid = UUID.randomUUID().toString(),
                message_type = "DeviceUIConfig",
                received_date = System.currentTimeMillis(),
                raw_message = deviceUiConfig.toString(),
                fromRadio = fromRadio { this.deviceuiConfig = deviceUiConfig },
            ),
        )
    }

    private fun handleFileInfo(fileInfo: MeshProtos.FileInfo) {
        insertMeshLog(
            MeshLog(
                uuid = UUID.randomUUID().toString(),
                message_type = "FileInfo",
                received_date = System.currentTimeMillis(),
                raw_message = fileInfo.toString(),
                fromRadio = fromRadio { this.fileInfo = fileInfo },
            ),
        )
    }

    private fun handleReceivedMeshPacket(packet: MeshPacket, myNodeNum: Int?) {
        val rxTime =
            if (packet.rxTime == 0) (System.currentTimeMillis().milliseconds.inWholeSeconds).toInt() else packet.rxTime
        val preparedPacket = packet.toBuilder().setRxTime(rxTime).build()

        if (nodeManager.isNodeDbReady.value) {
            processReceivedMeshPacket(preparedPacket, myNodeNum)
        } else {
            synchronized(earlyReceivedPackets) {
                val queueSize = earlyReceivedPackets.size
                if (queueSize >= MAX_EARLY_PACKET_BUFFER) {
                    val dropped = earlyReceivedPackets.removeFirst()
                    historyLog(Log.WARN) {
                        val portLabel =
                            if (dropped.hasDecoded()) {
                                Portnums.PortNum.forNumber(dropped.decoded.portnumValue)?.name
                                    ?: dropped.decoded.portnumValue.toString()
                            } else {
                                "unknown"
                            }
                        "dropEarlyPacket bufferFull size=$queueSize id=${dropped.id} port=$portLabel"
                    }
                }
                earlyReceivedPackets.addLast(preparedPacket)
                val portLabel =
                    if (preparedPacket.hasDecoded()) {
                        Portnums.PortNum.forNumber(preparedPacket.decoded.portnumValue)?.name
                            ?: preparedPacket.decoded.portnumValue.toString()
                    } else {
                        "unknown"
                    }
                historyLog { "queueEarlyPacket size=${earlyReceivedPackets.size} id=${preparedPacket.id} port=$portLabel" }
            }
        }
    }

    private fun flushEarlyReceivedPackets(reason: String) {
        val packets = synchronized(earlyReceivedPackets) {
            if (earlyReceivedPackets.isEmpty()) return
            val list = earlyReceivedPackets.toList()
            earlyReceivedPackets.clear()
            list
        }
        historyLog { "replayEarlyPackets reason=$reason count=${packets.size}" }
        val myNodeNum = nodeManager.myNodeNum
        packets.forEach { processReceivedMeshPacket(it, myNodeNum) }
    }

    private fun processReceivedMeshPacket(packet: MeshPacket, myNodeNum: Int?) {
        if (!packet.hasDecoded()) return
        val log =
            MeshLog(
                uuid = UUID.randomUUID().toString(),
                message_type = "Packet",
                received_date = System.currentTimeMillis(),
                raw_message = packet.toString(),
                fromNum = packet.from,
                portNum = packet.decoded.portnumValue,
                fromRadio = fromRadio { this.packet = packet },
            )
        val logJob = insertMeshLog(log)
        logInsertJobByPacketId[packet.id] = logJob
        logUuidByPacketId[packet.id] = log.uuid

        scope.handledLaunch { serviceRepository.emitMeshPacket(packet) }

        myNodeNum?.let { myNum ->
            val isOtherNode = myNum != packet.from
            nodeManager.updateNodeInfo(myNum, withBroadcast = isOtherNode) {
                it.lastHeard = (System.currentTimeMillis().milliseconds.inWholeSeconds).toInt()
            }
            nodeManager.updateNodeInfo(packet.from, withBroadcast = false, channel = packet.channel) {
                it.lastHeard = packet.rxTime
                it.snr = packet.rxSnr
                it.rssi = packet.rxRssi
                it.hopsAway =
                    if (packet.decoded.portnumValue == Portnums.PortNum.RANGE_TEST_APP_VALUE) {
                        0
                    } else if (packet.hopStart == 0 || packet.hopLimit > packet.hopStart) {
                        -1
                    } else {
                        packet.hopStart - packet.hopLimit
                    }
            }

            try {
                if (packet.decoded.portnumValue == org.meshtastic.proto.Portnums.PortNum.TRACEROUTE_APP_VALUE) {
                    router.tracerouteHandler.handleTraceroute(packet, log.uuid, logJob)
                } else {
                    router.dataHandler.handleReceivedData(packet, myNum)
                }
            } finally {
                logUuidByPacketId.remove(packet.id)
                logInsertJobByPacketId.remove(packet.id)
            }
        }
    }

    private fun insertMeshLog(log: MeshLog): Job = scope.handledLaunch { meshLogRepository.get().insert(log) }

    private inline fun historyLog(
        priority: Int = Log.INFO,
        throwable: Throwable? = null,
        crossinline message: () -> String,
    ) {
        if (!BuildConfig.DEBUG) return
        val timber = Timber.tag("HistoryReplay")
        val msg = message()
        if (throwable != null) {
            timber.log(priority, throwable, msg)
        } else {
            timber.log(priority, msg)
        }
    }

    private fun ByteArray.toHexString(): String =
        this.joinToString(",") { byte -> String.format(Locale.US, "0x%02x", byte) }
}
