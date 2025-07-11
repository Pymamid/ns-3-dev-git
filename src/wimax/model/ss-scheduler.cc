/*
 * Copyright (c) 2007,2008 INRIA
 *
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * Author: Jahanzeb Farooq <jahanzeb.farooq@sophia.inria.fr>
 */

#include "ss-scheduler.h"

#include "connection-manager.h"
#include "service-flow-manager.h"
#include "service-flow-record.h"
#include "service-flow.h"
#include "ss-net-device.h"
#include "wimax-connection.h"
#include "wimax-mac-queue.h"
#include "wimax-phy.h"

#include "ns3/log.h"
#include "ns3/node.h"
#include "ns3/simulator.h"

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("SSScheduler");

NS_OBJECT_ENSURE_REGISTERED(SSScheduler);

TypeId
SSScheduler::GetTypeId()
{
    static TypeId tid = TypeId("ns3::SSScheduler").SetParent<Object>().SetGroupName("Wimax");
    return tid;
}

SSScheduler::SSScheduler(Ptr<SubscriberStationNetDevice> ss)
    : m_ss(ss),
      m_pollMe(false)
{
}

SSScheduler::~SSScheduler()
{
}

void
SSScheduler::DoDispose()
{
    m_ss = nullptr;
}

void
SSScheduler::SetPollMe(bool pollMe)
{
    m_pollMe = pollMe;
}

bool
SSScheduler::GetPollMe() const
{
    return m_pollMe;
}

Ptr<PacketBurst>
SSScheduler::Schedule(uint16_t availableSymbols,
                      WimaxPhy::ModulationType modulationType,
                      MacHeaderType::HeaderType packetType,
                      Ptr<WimaxConnection>& connection)
{
    Time timeStamp;
    Ptr<PacketBurst> burst = CreateObject<PacketBurst>();
    uint16_t nrSymbolsRequired = 0;

    if (!connection)
    {
        connection = SelectConnection();
    }
    else
    {
        NS_ASSERT_MSG(connection->HasPackets(),
                      "SS: Error while scheduling packets: The selected connection has no packets");
    }

    Ptr<Packet> packet;

    while (connection && connection->HasPackets(packetType))
    {
        NS_LOG_INFO("FRAG_DEBUG: SS Scheduler" << std::endl);

        uint32_t availableByte = m_ss->GetPhy()->GetNrBytes(availableSymbols, modulationType);

        uint32_t requiredByte = connection->GetQueue()->GetFirstPacketRequiredByte(packetType);

        NS_LOG_INFO("\t availableByte = " << availableByte << ", requiredByte = " << requiredByte);

        if (availableByte >= requiredByte)
        {
            // The SS could sent a packet without a other fragmentation
            NS_LOG_INFO("\t availableByte >= requiredByte"
                        "\n\t Send packet without other fragmentation"
                        << std::endl);

            packet = connection->Dequeue(packetType);
            burst->AddPacket(packet);

            nrSymbolsRequired = m_ss->GetPhy()->GetNrSymbols(packet->GetSize(), modulationType);
            availableSymbols -= nrSymbolsRequired;
        }
        else
        {
            if (connection->GetType() == Cid::TRANSPORT)
            {
                NS_LOG_INFO("\t availableByte < requiredByte"
                            "\n\t Check if the fragmentation is possible");

                uint32_t headerSize = connection->GetQueue()->GetFirstPacketHdrSize(packetType);
                if (!connection->GetQueue()->CheckForFragmentation(packetType))
                {
                    NS_LOG_INFO("\t Add fragmentSubhdrSize = 2");
                    headerSize += 2;
                }
                NS_LOG_INFO("\t availableByte = " << availableByte
                                                  << " headerSize = " << headerSize);

                if (availableByte > headerSize)
                {
                    NS_LOG_INFO("\t Fragmentation IS possible");
                    packet = connection->Dequeue(packetType, availableByte);
                    burst->AddPacket(packet);

                    nrSymbolsRequired =
                        m_ss->GetPhy()->GetNrSymbols(packet->GetSize(), modulationType);
                    availableSymbols -= nrSymbolsRequired;
                }
                else
                {
                    NS_LOG_INFO("\t Fragmentation IS NOT possible" << std::endl);
                    break;
                }
            }
            else
            {
                NS_LOG_INFO("\t no Transport Connection "
                            "\n\t Fragmentation IS NOT possible, "
                            << std::endl);
                break;
            }
        }
    }
    return burst;
}

Ptr<WimaxConnection>
SSScheduler::SelectConnection()
{
    Time currentTime = Simulator::Now();

    NS_LOG_INFO("SS Scheduler: Selecting connection...");
    if (m_ss->GetInitialRangingConnection()->HasPackets())
    {
        NS_LOG_INFO("Return GetInitialRangingConnection");
        return m_ss->GetInitialRangingConnection();
    }
    if (m_ss->GetBasicConnection()->HasPackets())
    {
        NS_LOG_INFO("Return GetBasicConnection");
        return m_ss->GetBasicConnection();
    }
    if (m_ss->GetPrimaryConnection()->HasPackets())
    {
        NS_LOG_INFO("Return GetPrimaryConnection");
        return m_ss->GetPrimaryConnection();
    }

    auto serviceFlows = m_ss->GetServiceFlowManager()->GetServiceFlows(ServiceFlow::SF_TYPE_UGS);
    for (auto iter = serviceFlows.begin(); iter != serviceFlows.end(); ++iter)
    {
        // making sure that this grant was actually intended for this UGS

        if ((*iter)->HasPackets() && (currentTime + m_ss->GetPhy()->GetFrameDuration() >
                                      MilliSeconds((*iter)->GetUnsolicitedGrantInterval())))
        {
            NS_LOG_INFO("Return UGS SF: CID = " << (*iter)->GetCid()
                                                << "SFID = " << (*iter)->GetSfid());
            return (*iter)->GetConnection();
        }
    }

    /* In the following cases (rtPS, nrtPS and BE flows) connection is seletected only for data
     packets, for bandwidth request packets connection will itself be passed to Schedule () and
     hence this function will never be called. */

    serviceFlows = m_ss->GetServiceFlowManager()->GetServiceFlows(ServiceFlow::SF_TYPE_RTPS);
    for (auto iter = serviceFlows.begin(); iter != serviceFlows.end(); ++iter)
    {
        if ((*iter)->HasPackets(MacHeaderType::HEADER_TYPE_GENERIC) &&
            (currentTime + m_ss->GetPhy()->GetFrameDuration() >
             MilliSeconds((*iter)->GetUnsolicitedPollingInterval())))
        {
            NS_LOG_INFO("Return RTPS SF: CID = " << (*iter)->GetCid()
                                                 << "SFID = " << (*iter)->GetSfid());
            return (*iter)->GetConnection();
        }
    }

    serviceFlows = m_ss->GetServiceFlowManager()->GetServiceFlows(ServiceFlow::SF_TYPE_NRTPS);
    for (auto iter = serviceFlows.begin(); iter != serviceFlows.end(); ++iter)
    {
        if ((*iter)->HasPackets(MacHeaderType::HEADER_TYPE_GENERIC))
        {
            NS_LOG_INFO("Return NRTPS SF: CID = " << (*iter)->GetCid()
                                                  << "SFID = " << (*iter)->GetSfid());
            return (*iter)->GetConnection();
        }
    }

    serviceFlows = m_ss->GetServiceFlowManager()->GetServiceFlows(ServiceFlow::SF_TYPE_BE);
    for (auto iter = serviceFlows.begin(); iter != serviceFlows.end(); ++iter)
    {
        if ((*iter)->HasPackets(MacHeaderType::HEADER_TYPE_GENERIC))
        {
            NS_LOG_INFO("Return BE SF: CID = " << (*iter)->GetCid()
                                               << "SFID = " << (*iter)->GetSfid());
            return (*iter)->GetConnection();
        }
    }

    if (m_ss->GetBroadcastConnection()->HasPackets())
    {
        return m_ss->GetBroadcastConnection();
    }
    NS_LOG_INFO("NO connection is selected!");
    return nullptr;
}

} // namespace ns3
