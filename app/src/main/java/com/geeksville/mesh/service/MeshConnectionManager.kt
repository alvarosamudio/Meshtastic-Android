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

import android.app.Notification
import com.geeksville.mesh.concurrent.handledLaunch
import com.geeksville.mesh.repository.radio.RadioInterfaceService
import com.meshtastic.core.strings.getString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.meshtastic.core.analytics.DataPair
import org.meshtastic.core.analytics.platform.PlatformAnalytics
import org.meshtastic.core.data.repository.NodeRepository
import org.meshtastic.core.data.repository.RadioConfigRepository
import org.meshtastic.core.prefs.ui.UiPrefs
import org.meshtastic.core.service.ConnectionState
import org.meshtastic.core.service.MeshServiceNotifications
import org.meshtastic.core.strings.Res
import org.meshtastic.core.strings.connected_count
import org.meshtastic.core.strings.connecting
import org.meshtastic.core.strings.device_sleeping
import org.meshtastic.core.strings.disconnected
import org.meshtastic.proto.ConfigProtos
import org.meshtastic.proto.MeshProtos.ToRadio
import org.meshtastic.proto.TelemetryProtos
import timber.log.Timber
import javax.inject.Inject
import javax.inject.Singleton

@Suppress("LongParameterList")
@Singleton
class MeshConnectionManager
@Inject
constructor(
    private val radioInterfaceService: RadioInterfaceService,
    private val connectionStateHolder: MeshServiceConnectionStateHolder,
    private val serviceBroadcasts: MeshServiceBroadcasts,
    private val serviceNotifications: MeshServiceNotifications,
    private val uiPrefs: UiPrefs,
    private val packetHandler: PacketHandler,
    private val nodeRepository: NodeRepository,
    private val locationManager: MeshLocationManager,
    private val mqttManager: MeshMqttManager,
    private val historyManager: MeshHistoryManager,
    private val radioConfigRepository: RadioConfigRepository,
    private val commandSender: MeshCommandSender,
    private val nodeManager: MeshNodeManager,
    private val analytics: PlatformAnalytics,
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var sleepTimeout: Job? = null
    private var locationRequestsJob: Job? = null
    private var connectTimeMsec = 0L

    private val configOnlyNonce = 69420
    private val nodeInfoNonce = 69421

    fun init() {
        radioInterfaceService.connectionState.onEach(::onRadioConnectionState).launchIn(scope)

        nodeRepository.myNodeInfo
            .onEach { myNodeEntity ->
                locationRequestsJob?.cancel()
                if (myNodeEntity != null) {
                    locationRequestsJob =
                        uiPrefs
                            .shouldProvideNodeLocation(myNodeEntity.myNodeNum)
                            .onEach { shouldProvide ->
                                if (shouldProvide) {
                                    locationManager.start { pos ->
                                        commandSender.sendPosition(pos)
                                    }
                                } else {
                                    locationManager.stop()
                                }
                            }
                            .launchIn(scope)
                }
            }
            .launchIn(scope)
    }

    private fun onRadioConnectionState(newState: ConnectionState) {
        scope.handledLaunch {
            val localConfig = radioConfigRepository.localConfigFlow.first()
            val isRouter = localConfig.device.role == ConfigProtos.Config.DeviceConfig.Role.ROUTER
            val lsEnabled = localConfig.power.isPowerSaving || isRouter

            val effectiveState =
                when (newState) {
                    is ConnectionState.Connected -> ConnectionState.Connected
                    is ConnectionState.DeviceSleep -> if (lsEnabled) ConnectionState.DeviceSleep else ConnectionState.Disconnected
                    is ConnectionState.Connecting -> ConnectionState.Connecting
                    is ConnectionState.Disconnected -> ConnectionState.Disconnected
                }
            onConnectionChanged(effectiveState)
        }
    }

    private fun onConnectionChanged(c: ConnectionState) {
        if (connectionStateHolder.connectionState.value == c && c !is ConnectionState.Connected) return
        Timber.d("onConnectionChanged: ${connectionStateHolder.connectionState.value} -> $c")

        sleepTimeout?.cancel()
        sleepTimeout = null

        when (c) {
            is ConnectionState.Connecting -> connectionStateHolder.setState(ConnectionState.Connecting)
            is ConnectionState.Connected -> handleConnected()
            is ConnectionState.DeviceSleep -> handleDeviceSleep()
            is ConnectionState.Disconnected -> handleDisconnected()
        }
        updateStatusNotification()
    }

    private fun handleConnected() {
        connectionStateHolder.setState(ConnectionState.Connecting)
        serviceBroadcasts.broadcastConnection()
        Timber.d("Starting connect")
        connectTimeMsec = System.currentTimeMillis()
        startConfigOnly()
    }

    private fun handleDeviceSleep() {
        connectionStateHolder.setState(ConnectionState.DeviceSleep)
        packetHandler.stopPacketQueue()
        locationManager.stop()
        mqttManager.stop()

        if (connectTimeMsec != 0L) {
            val now = System.currentTimeMillis()
            val duration = now - connectTimeMsec
            connectTimeMsec = 0L
            analytics.track("connected_seconds", DataPair("connected_seconds", duration / 1000.0))
        }

        sleepTimeout =
            scope.handledLaunch {
                try {
                    val localConfig = radioConfigRepository.localConfigFlow.first()
                    val timeout = (localConfig.power?.lsSecs ?: 0) + 30
                    Timber.d("Waiting for sleeping device, timeout=$timeout secs")
                    delay(timeout * 1000L)
                    Timber.w("Device timeout out, setting disconnected")
                    onConnectionChanged(ConnectionState.Disconnected)
                } catch (_: CancellationException) {
                    Timber.d("device sleep timeout cancelled")
                }
            }

        serviceBroadcasts.broadcastConnection()
    }

    private fun handleDisconnected() {
        connectionStateHolder.setState(ConnectionState.Disconnected)
        packetHandler.stopPacketQueue()
        locationManager.stop()
        mqttManager.stop()

        analytics.track(
            "mesh_disconnect",
            DataPair("num_nodes", nodeManager.nodeDBbyNodeNum.size),
            DataPair("num_online", nodeManager.nodeDBbyNodeNum.values.count { it.isOnline }),
        )
        analytics.track("num_nodes", DataPair("num_nodes", nodeManager.nodeDBbyNodeNum.size))

        serviceBroadcasts.broadcastConnection()
    }

    fun startConfigOnly() {
        packetHandler.sendToRadio(ToRadio.newBuilder().apply { wantConfigId = configOnlyNonce })
    }

    fun startNodeInfoOnly() {
        packetHandler.sendToRadio(ToRadio.newBuilder().apply { wantConfigId = nodeInfoNonce })
    }

    fun onHasSettings() {
        commandSender.processQueuedPackets()

        // Start MQTT if enabled
        scope.handledLaunch {
            val moduleConfig = radioConfigRepository.moduleConfigFlow.first()
            mqttManager.start(moduleConfig.mqtt.enabled, moduleConfig.mqtt.proxyToClientEnabled)
        }

        reportConnection()

        val myNodeNum = nodeManager.myNodeNum ?: 0
        // Request history
        scope.handledLaunch {
            val moduleConfig = radioConfigRepository.moduleConfigFlow.first()
            historyManager.requestHistoryReplay("onHasSettings", myNodeNum, moduleConfig.storeForward, "Unknown")
        }

        // Set time
        commandSender.sendAdmin(myNodeNum) { setTimeOnly = (System.currentTimeMillis() / 1000).toInt() }
    }

    private fun reportConnection() {
        val myNode = nodeManager.getMyNodeInfo()
        val radioModel = DataPair("radio_model", myNode?.model ?: "unknown")
        analytics.track(
            "mesh_connect",
            DataPair("num_nodes", nodeManager.nodeDBbyNodeNum.size),
            DataPair("num_online", nodeManager.nodeDBbyNodeNum.values.count { it.isOnline }),
            radioModel,
        )
    }

    fun updateTelemetry(telemetry: TelemetryProtos.Telemetry) {
        updateStatusNotification(telemetry)
    }

    fun updateStatusNotification(telemetry: TelemetryProtos.Telemetry? = null): Notification {
        val summary =
            when (connectionStateHolder.connectionState.value) {
                is ConnectionState.Connected -> getString(Res.string.connected_count).format(nodeManager.nodeDBbyNodeNum.values.count { it.isOnline })
                is ConnectionState.Disconnected -> getString(Res.string.disconnected)
                is ConnectionState.DeviceSleep -> getString(Res.string.device_sleeping)
                is ConnectionState.Connecting -> getString(Res.string.connecting)
            }
        return serviceNotifications.updateServiceStateNotification(summary, telemetry = telemetry)
    }
}
