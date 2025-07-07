/*
 * Copyright (c) 2025 Carnegie Mellon University
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

/*
 * Fat-Tree Topology Simulation
 *
 * Topology: Fat-tree with 320 servers in 20 racks, 20 aggregation switches, 16 core switches
 * - 320 servers (16 servers per    // Create UDP sink on receiver
    uint16_t port = 9000 + m_uniformRandom->GetInteger(0, 999);
    PacketSinkHelper sinkHelper("ns3::UdpSocketFactory", InetSocketAddress(Ipv4Address::GetAny(), port));
    ApplicationContainer sinkApp = sinkHelper.Install(receiver);k)
 * - 20 ToR (Top-of-Rack) switches
 * - 20 aggregation switches  
 * - 16 core switches
 *
 * Link configurations:
 * - Server-ToR: 100 Gbps, 1µs delay
 * - ToR-Aggregation: 400 Gbps, 1µs delay
 * - Aggregation-Core: 400 Gbps, 1µs delay
 * - Switch buffer: 32MB
 * - PFC enabled, XOFF at 512KB
 *
 * Run with:
 *  $ ./ns3 run "scratch/fattree"
 */

#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/traffic-control-module.h"
#include "ns3/random-variable-stream.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>
#include <map>
#include <random>
#include <algorithm>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("FatTreeSim");

// Global topology parameters
const uint32_t NUM_SERVERS = 320;
const uint32_t NUM_RACKS = 20;
const uint32_t SERVERS_PER_RACK = NUM_SERVERS / NUM_RACKS; // 16 servers per rack
const uint32_t NUM_AGG_SWITCHES = 20;
const uint32_t NUM_CORE_SWITCHES = 16;

// Network parameters
const std::string SERVER_TOR_BANDWIDTH = "100Gbps";
const std::string SWITCH_BANDWIDTH = "400Gbps";
const std::string LINK_DELAY = "1us";
const uint32_t SWITCH_BUFFER_SIZE_MB = 32;
const uint32_t PFC_XOFF_KB = 512;

// Traffic generation parameters
const double BACKGROUND_LOAD = 0.5;   // 50% load for Hadoop workload
const double INCAST_LOAD = 0.2;       // 20% load for incast traffic
const uint32_t INCAST_SENDERS = 64;   // 64 senders per incast
const uint32_t INCAST_SIZE = 20480;   // 20KB per sender (20 * 1024)
const double SIMULATION_TIME = 5.0;   // 5 seconds for testing

// Hadoop workload CDF (flow size in bytes, CDF percentage)
const std::vector<std::pair<uint32_t, double>> HADOOP_CDF = {
    {0, 0.0},
    {100, 1.0},
    {200, 2.0},
    {300, 5.0},
    {350, 15.0},
    {400, 20.0},
    {500, 30.0},
    {600, 40.0},
    {700, 50.0},
    {1000, 60.0},
    {2000, 67.0},
    {7000, 70.0},
    {30000, 72.0},
    {50000, 82.0},
    {80000, 87.0},
    {120000, 90.0},
    {300000, 95.0},
    {1000000, 97.5},
    {2000000, 99.0},
    {10000000, 100.0}
};

// Fat-tree topology class
class FatTreeTopology
{
public:
    FatTreeTopology();
    void BuildTopology();
    void ConfigureRouting();
    void PrintTopologyInfo();
    
    // Getters for nodes
    NodeContainer GetServers() { return m_servers; }
    NodeContainer GetTorSwitches() { return m_torSwitches; }
    NodeContainer GetAggSwitches() { return m_aggSwitches; }
    NodeContainer GetCoreSwitches() { return m_coreSwitches; }
    
    // Get server node by rack and position
    Ptr<Node> GetServer(uint32_t rack, uint32_t position);
    Ipv4Address GetServerAddress(uint32_t rack, uint32_t position);
    
    // Traffic generation
    void GenerateTraffic();
    void StartBackgroundTraffic();
    void StartIncastTraffic();

private:
    // Node containers
    NodeContainer m_servers;
    NodeContainer m_torSwitches;
    NodeContainer m_aggSwitches;
    NodeContainer m_coreSwitches;
    
    // Network device containers
    std::vector<NetDeviceContainer> m_serverTorLinks;
    std::vector<NetDeviceContainer> m_torAggLinks;
    std::vector<NetDeviceContainer> m_aggCoreLinks;
    
    // Point-to-point helpers
    PointToPointHelper m_serverTorHelper;
    PointToPointHelper m_switchHelper;
    
    // Internet stack
    InternetStackHelper m_stack;
    
    // IP address assignment
    Ipv4AddressHelper m_addressHelper;
    std::vector<Ipv4InterfaceContainer> m_serverInterfaces;
    std::vector<Ipv4InterfaceContainer> m_torInterfaces;
    std::vector<Ipv4InterfaceContainer> m_aggInterfaces;
    std::vector<Ipv4InterfaceContainer> m_coreInterfaces;
    
    // Traffic generation
    Ptr<UniformRandomVariable> m_uniformRandom;
    Ptr<ExponentialRandomVariable> m_exponentialRandom;
    ApplicationContainer m_backgroundApps;
    ApplicationContainer m_incastApps;
    
    // Port management to avoid conflicts
    std::map<uint32_t, uint16_t> m_receiverPortMap; // receiverIdx -> next available port
    
    // Helper methods
    void CreateNodes();
    void ConfigureLinks();
    void InstallInternetStack();
    void AssignIpAddresses();
    void ConfigureQueues();
    void SetupRouting();
    
    // Fat-tree routing helper methods
    void ConfigureFatTreeRouting();
    void ConfigureServerRouting();
    void ConfigureToRRouting();
    void ConfigureAggregationRouting();
    void ConfigureCoreRouting();
    
    // Traffic generation helper methods
    uint32_t SampleHadoopFlowSize();
    std::pair<uint32_t, uint32_t> GetRandomServerPair();
    uint16_t GetUniquePort(uint32_t receiverIdx, bool isTcp = false);
    void ScheduleNextBackgroundFlow();
    void ScheduleNextIncast();
    void StartBackgroundFlow();
    void StartIncast();
};

FatTreeTopology::FatTreeTopology()
{
    NS_LOG_FUNCTION(this);
    
    // Initialize random variables
    m_uniformRandom = CreateObject<UniformRandomVariable>();
    m_exponentialRandom = CreateObject<ExponentialRandomVariable>();
}

void
FatTreeTopology::BuildTopology()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Building Fat-Tree topology...");
    
    CreateNodes();
    ConfigureLinks();
    InstallInternetStack();
    ConfigureQueues();
    AssignIpAddresses();
    
    NS_LOG_INFO("Fat-Tree topology built successfully");
}

void
FatTreeTopology::CreateNodes()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Creating nodes...");
    
    // Create server nodes
    m_servers.Create(NUM_SERVERS);
    NS_LOG_INFO("Created " << NUM_SERVERS << " server nodes");
    
    // Create ToR switch nodes
    m_torSwitches.Create(NUM_RACKS);
    NS_LOG_INFO("Created " << NUM_RACKS << " ToR switch nodes");
    
    // Create aggregation switch nodes
    m_aggSwitches.Create(NUM_AGG_SWITCHES);
    NS_LOG_INFO("Created " << NUM_AGG_SWITCHES << " aggregation switch nodes");
    
    // Create core switch nodes
    m_coreSwitches.Create(NUM_CORE_SWITCHES);
    NS_LOG_INFO("Created " << NUM_CORE_SWITCHES << " core switch nodes");
}

void
FatTreeTopology::ConfigureLinks()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Configuring links...");
    
    // Configure server-ToR links (100 Gbps)
    m_serverTorHelper.SetDeviceAttribute("DataRate", StringValue(SERVER_TOR_BANDWIDTH));
    m_serverTorHelper.SetChannelAttribute("Delay", StringValue(LINK_DELAY));
    
    // Configure switch-to-switch links (400 Gbps)
    m_switchHelper.SetDeviceAttribute("DataRate", StringValue(SWITCH_BANDWIDTH));
    m_switchHelper.SetChannelAttribute("Delay", StringValue(LINK_DELAY));
    
    // Reserve space for link containers
    m_serverTorLinks.resize(NUM_RACKS);
    m_torAggLinks.resize(NUM_RACKS);
    m_aggCoreLinks.resize(NUM_AGG_SWITCHES);
    
    // Connect servers to ToR switches
    for (uint32_t rack = 0; rack < NUM_RACKS; ++rack)
    {
        Ptr<Node> torSwitch = m_torSwitches.Get(rack);
        
        for (uint32_t server = 0; server < SERVERS_PER_RACK; ++server)
        {
            uint32_t serverIndex = rack * SERVERS_PER_RACK + server;
            Ptr<Node> serverNode = m_servers.Get(serverIndex);
            
            NetDeviceContainer link = m_serverTorHelper.Install(serverNode, torSwitch);
            m_serverTorLinks[rack].Add(link);
        }
    }
    NS_LOG_INFO("Connected servers to ToR switches");
    
    // Connect ToR switches to aggregation switches
    // Each ToR connects to all aggregation switches for full bisection bandwidth
    for (uint32_t tor = 0; tor < NUM_RACKS; ++tor)
    {
        Ptr<Node> torSwitch = m_torSwitches.Get(tor);
        
        for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
        {
            Ptr<Node> aggSwitch = m_aggSwitches.Get(agg);
            NetDeviceContainer link = m_switchHelper.Install(torSwitch, aggSwitch);
            m_torAggLinks[tor].Add(link);
        }
    }
    NS_LOG_INFO("Connected ToR switches to aggregation switches");
    
    // Connect aggregation switches to core switches
    // Each aggregation switch connects to all core switches
    for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
    {
        Ptr<Node> aggSwitch = m_aggSwitches.Get(agg);
        
        for (uint32_t core = 0; core < NUM_CORE_SWITCHES; ++core)
        {
            Ptr<Node> coreSwitch = m_coreSwitches.Get(core);
            NetDeviceContainer link = m_switchHelper.Install(aggSwitch, coreSwitch);
            m_aggCoreLinks[agg].Add(link);
        }
    }
    NS_LOG_INFO("Connected aggregation switches to core switches");
}

void
FatTreeTopology::InstallInternetStack()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Installing Internet stack...");
    
    // Install Internet stack on all nodes
    m_stack.Install(m_servers);
    m_stack.Install(m_torSwitches);
    m_stack.Install(m_aggSwitches);
    m_stack.Install(m_coreSwitches);
    
    NS_LOG_INFO("Internet stack installed on all nodes");
}

void
FatTreeTopology::ConfigureQueues()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Configuring queues and traffic control...");
    
    // Convert buffer size from MB to bytes
    uint32_t bufferSizeBytes = SWITCH_BUFFER_SIZE_MB * 1024 * 1024;
    uint32_t xoffThresholdBytes = PFC_XOFF_KB * 1024;
    
    // Calculate packet-based thresholds (assuming 1500 byte packets)
    uint32_t avgPacketSize = 1500;
    uint32_t bufferSizePackets = bufferSizeBytes / avgPacketSize;
    uint32_t xoffThresholdPackets = xoffThresholdBytes / avgPacketSize;
    
    // Configure traffic control for server-ToR links
    TrafficControlHelper serverTorTcHelper;
    serverTorTcHelper.SetRootQueueDisc(
        "ns3::RedQueueDisc",
        "MaxSize", QueueSizeValue(QueueSize(std::to_string(bufferSizePackets) + "p")),
        "MinTh", DoubleValue(xoffThresholdPackets * 0.8),
        "MaxTh", DoubleValue(xoffThresholdPackets),
        "UseEcn", BooleanValue(true),
        "UseHardDrop", BooleanValue(false)
    );
    
    // Configure traffic control for switch-to-switch links
    TrafficControlHelper switchTcHelper;
    switchTcHelper.SetRootQueueDisc(
        "ns3::RedQueueDisc",
        "MaxSize", QueueSizeValue(QueueSize(std::to_string(bufferSizePackets) + "p")),
        "MinTh", DoubleValue(xoffThresholdPackets * 0.8),
        "MaxTh", DoubleValue(xoffThresholdPackets),
        "UseEcn", BooleanValue(true),
        "UseHardDrop", BooleanValue(false)
    );
    
    // Install queues on server-ToR links
    for (uint32_t rack = 0; rack < NUM_RACKS; ++rack)
    {
        serverTorTcHelper.Install(m_serverTorLinks[rack]);
    }
    
    // Install queues on ToR-aggregation links
    for (uint32_t tor = 0; tor < NUM_RACKS; ++tor)
    {
        switchTcHelper.Install(m_torAggLinks[tor]);
    }
    
    // Install queues on aggregation-core links
    for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
    {
        switchTcHelper.Install(m_aggCoreLinks[agg]);
    }
    
    NS_LOG_INFO("Queue configuration completed");
}

void
FatTreeTopology::AssignIpAddresses()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Assigning IP addresses...");
    
    // Reset IP address generator to avoid collisions
    Ipv4AddressGenerator::Reset();
    
    // Reserve space for interface containers
    m_serverInterfaces.resize(NUM_RACKS);
    m_torInterfaces.resize(NUM_RACKS);
    m_aggInterfaces.resize(NUM_AGG_SWITCHES);
    m_coreInterfaces.resize(NUM_CORE_SWITCHES);
    
    // Assign IP addresses to server-ToR links
    // Use 10.0.x.0/24 for each rack
    for (uint32_t rack = 0; rack < NUM_RACKS; ++rack)
    {
        std::string network = "10.0." + std::to_string(rack) + ".0";
        Ipv4AddressHelper addressHelper;
        addressHelper.SetBase(network.c_str(), "255.255.255.0");
        m_serverInterfaces[rack] = addressHelper.Assign(m_serverTorLinks[rack]);
    }
    NS_LOG_INFO("Assigned IP addresses to server-ToR links");
    
    // Assign IP addresses to ToR-aggregation links
    // Use 172.16.x.0/30 for ToR-Agg links
    uint32_t torAggSubnet = 0;
    for (uint32_t tor = 0; tor < NUM_RACKS; ++tor)
    {
        for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
        {
            std::string network = "172.16." + std::to_string(torAggSubnet / 64) + "." + std::to_string((torAggSubnet % 64) * 4);
            Ipv4AddressHelper addressHelper;
            addressHelper.SetBase(network.c_str(), "255.255.255.252");
            
            NetDeviceContainer singleLink;
            singleLink.Add(m_torAggLinks[tor].Get(agg * 2));     // ToR side
            singleLink.Add(m_torAggLinks[tor].Get(agg * 2 + 1)); // Agg side
            
            Ipv4InterfaceContainer interfaces = addressHelper.Assign(singleLink);
            torAggSubnet++;
        }
    }
    NS_LOG_INFO("Assigned IP addresses to ToR-aggregation links");
    
    // Assign IP addresses to aggregation-core links
    // Use 192.168.x.0/30 for Agg-Core links
    uint32_t aggCoreSubnet = 0;
    for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
    {
        for (uint32_t core = 0; core < NUM_CORE_SWITCHES; ++core)
        {
            std::string network = "192.168." + std::to_string(aggCoreSubnet / 64) + "." + std::to_string((aggCoreSubnet % 64) * 4);
            Ipv4AddressHelper addressHelper;
            addressHelper.SetBase(network.c_str(), "255.255.255.252");
            
            NetDeviceContainer singleLink;
            singleLink.Add(m_aggCoreLinks[agg].Get(core * 2));     // Agg side
            singleLink.Add(m_aggCoreLinks[agg].Get(core * 2 + 1)); // Core side
            
            Ipv4InterfaceContainer interfaces = addressHelper.Assign(singleLink);
            aggCoreSubnet++;
        }
    }
    NS_LOG_INFO("Assigned IP addresses to aggregation-core links");
}

void
FatTreeTopology::ConfigureRouting()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Configuring fat-tree routing...");
    
    // Enable ECMP routing
    Config::SetDefault("ns3::Ipv4GlobalRouting::RandomEcmpRouting", BooleanValue(true));
    
    // First populate global routing tables to discover all paths
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();
    
    // Configure fat-tree specific routing policies
    ConfigureFatTreeRouting();
    
    NS_LOG_INFO("Fat-tree routing configured with ECMP support");
}

void
FatTreeTopology::ConfigureFatTreeRouting()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Configuring fat-tree specific routing policies...");
    
    // Configure up-down routing for servers
    ConfigureServerRouting();
    
    // Configure ToR switch routing  
    ConfigureToRRouting();
    
    // Configure aggregation switch routing
    ConfigureAggregationRouting();
    
    // Configure core switch routing
    ConfigureCoreRouting();
    
    NS_LOG_INFO("Fat-tree routing policies configured");
}

void
FatTreeTopology::ConfigureServerRouting()
{
    NS_LOG_FUNCTION(this);
    
    for (uint32_t rack = 0; rack < NUM_RACKS; ++rack)
    {
        for (uint32_t serverPos = 0; serverPos < SERVERS_PER_RACK; ++serverPos)
        {
            uint32_t serverIndex = rack * SERVERS_PER_RACK + serverPos;
            Ptr<Node> server = m_servers.Get(serverIndex);
            Ptr<Ipv4> serverIpv4 = server->GetObject<Ipv4>();
            Ptr<Ipv4StaticRouting> serverRouting = 
                Ipv4RoutingHelper::GetRouting<Ipv4StaticRouting>(
                    serverIpv4->GetRoutingProtocol());
            
            // For servers, default route goes through their ToR switch
            // The ToR switch address is the gateway for the server's subnet
            Ipv4Address torGateway = m_serverInterfaces[rack].GetAddress(serverPos * 2 + 1);
            serverRouting->SetDefaultRoute(torGateway, 1); // Interface 1 connects to ToR
        }
    }
    NS_LOG_INFO("Configured routing for " << NUM_SERVERS << " servers");
}

void
FatTreeTopology::ConfigureToRRouting()
{
    NS_LOG_FUNCTION(this);
    
    for (uint32_t tor = 0; tor < NUM_RACKS; ++tor)
    {
        Ptr<Node> torSwitch = m_torSwitches.Get(tor);
        Ptr<Ipv4> torIpv4 = torSwitch->GetObject<Ipv4>();
        Ptr<Ipv4StaticRouting> torRouting = 
            Ipv4RoutingHelper::GetRouting<Ipv4StaticRouting>(
                torIpv4->GetRoutingProtocol());
        
        // Add routes to local servers (down-routes)
        std::string localNetwork = "10." + std::to_string(tor) + ".0.0";
        Ipv4Address localNetAddr(localNetwork.c_str());
        Ipv4Mask localNetMask("255.255.0.0");
        
        // Local servers are directly connected - no next hop needed
        for (uint32_t server = 0; server < SERVERS_PER_RACK; ++server)
        {
            uint32_t serverInterfaceIndex = server + 1; // Interface 0 is loopback
            Ipv4Address serverAddr = m_serverInterfaces[tor].GetAddress(server * 2);
            torRouting->AddHostRouteTo(serverAddr, serverInterfaceIndex);
        }
        
        // Add up-routes to other racks through aggregation switches
        // Use ECMP across all aggregation switches
        for (uint32_t otherRack = 0; otherRack < NUM_RACKS; ++otherRack)
        {
            if (otherRack != tor) // Don't route to self
            {
                std::string remoteNetwork = "10." + std::to_string(otherRack) + ".0.0";
                Ipv4Address remoteNetAddr(remoteNetwork.c_str());
                
                // Add routes through all aggregation switches for ECMP
                for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
                {
                    uint32_t aggInterfaceIndex = SERVERS_PER_RACK + 1 + agg;
                    torRouting->AddNetworkRouteTo(remoteNetAddr, localNetMask, 
                                                 aggInterfaceIndex, 1); // Metric 1 for ECMP
                }
            }
        }
    }
    NS_LOG_INFO("Configured routing for " << NUM_RACKS << " ToR switches");
}

void
FatTreeTopology::ConfigureAggregationRouting()
{
    NS_LOG_FUNCTION(this);
    
    for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
    {
        Ptr<Node> aggSwitch = m_aggSwitches.Get(agg);
        Ptr<Ipv4> aggIpv4 = aggSwitch->GetObject<Ipv4>();
        Ptr<Ipv4StaticRouting> aggRouting = 
            Ipv4RoutingHelper::GetRouting<Ipv4StaticRouting>(
                aggIpv4->GetRoutingProtocol());
        
        // Add down-routes to ToR switches (and their servers)
        for (uint32_t tor = 0; tor < NUM_RACKS; ++tor)
        {
            std::string rackNetwork = "10." + std::to_string(tor) + ".0.0";
            Ipv4Address rackNetAddr(rackNetwork.c_str());
            Ipv4Mask rackNetMask("255.255.0.0");
            
            uint32_t torInterfaceIndex = tor + 1; // ToR interfaces start after loopback
            aggRouting->AddNetworkRouteTo(rackNetAddr, rackNetMask, torInterfaceIndex, 1);
        }
        
        // Add up-routes through core switches for inter-rack traffic
        // Use ECMP across all core switches
        for (uint32_t rack1 = 0; rack1 < NUM_RACKS; ++rack1)
        {
            for (uint32_t rack2 = 0; rack2 < NUM_RACKS; ++rack2)
            {
                if (rack1 != rack2)
                {
                    std::string destNetwork = "10." + std::to_string(rack2) + ".0.0";
                    Ipv4Address destNetAddr(destNetwork.c_str());
                    Ipv4Mask destNetMask("255.255.0.0");
                    
                    // Route through all core switches for ECMP
                    for (uint32_t core = 0; core < NUM_CORE_SWITCHES; ++core)
                    {
                        uint32_t coreInterfaceIndex = NUM_RACKS + 1 + core;
                        aggRouting->AddNetworkRouteTo(destNetAddr, destNetMask, coreInterfaceIndex, 2); // Higher metric than down-routes
                    }
                }
            }
        }
    }
    NS_LOG_INFO("Configured routing for " << NUM_AGG_SWITCHES << " aggregation switches");
}

void
FatTreeTopology::ConfigureCoreRouting()
{
    NS_LOG_FUNCTION(this);
    
    for (uint32_t core = 0; core < NUM_CORE_SWITCHES; ++core)
    {
        Ptr<Node> coreSwitch = m_coreSwitches.Get(core);
        Ptr<Ipv4> coreIpv4 = coreSwitch->GetObject<Ipv4>();
        Ptr<Ipv4StaticRouting> coreRouting = 
            Ipv4RoutingHelper::GetRouting<Ipv4StaticRouting>(
                coreIpv4->GetRoutingProtocol());
        
        // Core switches only have down-routes to aggregation switches
        // They route all rack traffic down to appropriate aggregation switches
        for (uint32_t rack = 0; rack < NUM_RACKS; ++rack)
        {
            std::string rackNetwork = "10." + std::to_string(rack) + ".0.0";
            Ipv4Address rackNetAddr(rackNetwork.c_str());
            Ipv4Mask rackNetMask("255.255.0.0");
            
            // Route to rack through all aggregation switches for ECMP
            for (uint32_t agg = 0; agg < NUM_AGG_SWITCHES; ++agg)
            {
                uint32_t aggInterfaceIndex = agg + 1; // Agg interfaces start after loopback
                coreRouting->AddNetworkRouteTo(rackNetAddr, rackNetMask, 
                                              aggInterfaceIndex, 1);
            }
        }
    }
    NS_LOG_INFO("Configured routing for " << NUM_CORE_SWITCHES << " core switches");
}

void
FatTreeTopology::GenerateTraffic()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Starting traffic generation...");
    
    // Start background traffic generation
    StartBackgroundTraffic();
    
    // Start incast traffic generation
    StartIncastTraffic();
    
    NS_LOG_INFO("Traffic generation scheduled");
}

void
FatTreeTopology::StartBackgroundTraffic()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Starting background Hadoop workload traffic...");
    
    // Schedule first background flow
    Simulator::Schedule(Seconds(0.1), &FatTreeTopology::ScheduleNextBackgroundFlow, this);
}

void
FatTreeTopology::StartIncastTraffic()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("Starting incast traffic...");
    
    // Schedule first incast much earlier
    Simulator::Schedule(Seconds(0.2), &FatTreeTopology::ScheduleNextIncast, this);
}

void
FatTreeTopology::PrintTopologyInfo()
{
    NS_LOG_FUNCTION(this);
    
    std::cout << "\n=== Fat-Tree Topology Information ===" << std::endl;
    std::cout << "Servers: " << NUM_SERVERS << " (" << SERVERS_PER_RACK << " per rack)" << std::endl;
    std::cout << "Racks (ToR switches): " << NUM_RACKS << std::endl;
    std::cout << "Aggregation switches: " << NUM_AGG_SWITCHES << std::endl;
    std::cout << "Core switches: " << NUM_CORE_SWITCHES << std::endl;
    std::cout << "\nLink Specifications:" << std::endl;
    std::cout << "Server-ToR: " << SERVER_TOR_BANDWIDTH << ", " << LINK_DELAY << " delay" << std::endl;
    std::cout << "Switch-Switch: " << SWITCH_BANDWIDTH << ", " << LINK_DELAY << " delay" << std::endl;
    std::cout << "Buffer size: " << SWITCH_BUFFER_SIZE_MB << "MB" << std::endl;
    std::cout << "PFC XOFF threshold: " << PFC_XOFF_KB << "KB" << std::endl;
    std::cout << "======================================\n" << std::endl;
}

Ptr<Node>
FatTreeTopology::GetServer(uint32_t rack, uint32_t position)
{
    NS_ASSERT(rack < NUM_RACKS);
    NS_ASSERT(position < SERVERS_PER_RACK);
    
    uint32_t serverIndex = rack * SERVERS_PER_RACK + position;
    return m_servers.Get(serverIndex);
}

Ipv4Address
FatTreeTopology::GetServerAddress(uint32_t rack, uint32_t position)
{
    NS_ASSERT(rack < NUM_RACKS);
    NS_ASSERT(position < SERVERS_PER_RACK);
    
    NS_LOG_DEBUG("Getting address for rack " << rack << ", position " << position);
    NS_LOG_DEBUG("m_serverInterfaces[" << rack << "] has " << m_serverInterfaces[rack].GetN() << " interfaces");
    
    uint32_t interfaceIndex = position * 2; // Each server has one interface with ToR
    
    if (interfaceIndex >= m_serverInterfaces[rack].GetN())
    {
        NS_LOG_ERROR("Interface index " << interfaceIndex << " out of bounds for rack " << rack);
        return Ipv4Address("0.0.0.0");
    }
    
    Ipv4Address addr = m_serverInterfaces[rack].GetAddress(interfaceIndex);
    NS_LOG_DEBUG("Retrieved address: " << addr);
    
    return addr;
}

uint32_t
FatTreeTopology::SampleHadoopFlowSize()
{
    // Sample flow size from Hadoop CDF
    double randomValue = m_uniformRandom->GetValue(0.0, 100.0);
    
    for (size_t i = 0; i < HADOOP_CDF.size(); ++i)
    {
        if (randomValue <= HADOOP_CDF[i].second)
        {
            if (i == 0)
            {
                return HADOOP_CDF[i].first;
            }
            else
            {
                // Linear interpolation between CDF points
                double ratio = (randomValue - HADOOP_CDF[i-1].second) / 
                              (HADOOP_CDF[i].second - HADOOP_CDF[i-1].second);
                return HADOOP_CDF[i-1].first + 
                       (uint32_t)(ratio * (HADOOP_CDF[i].first - HADOOP_CDF[i-1].first));
            }
        }
    }
    
    // Default to maximum size if not found
    return HADOOP_CDF.back().first;
}

std::pair<uint32_t, uint32_t>
FatTreeTopology::GetRandomServerPair()
{
    uint32_t sender = m_uniformRandom->GetInteger(0, NUM_SERVERS - 1);
    uint32_t receiver;
    
    // Ensure sender and receiver are different
    do {
        receiver = m_uniformRandom->GetInteger(0, NUM_SERVERS - 1);
    } while (receiver == sender);
    
    return std::make_pair(sender, receiver);
}

uint16_t
FatTreeTopology::GetUniquePort(uint32_t receiverIdx, bool isTcp)
{
    uint16_t basePort = isTcp ? 8000 : 9000;
    
    // Check if we have a port assigned for this receiver
    if (m_receiverPortMap.find(receiverIdx) == m_receiverPortMap.end())
    {
        // First time for this receiver, start with base port + receiverIdx
        m_receiverPortMap[receiverIdx] = basePort + receiverIdx;
    }
    else
    {
        // Increment port number for this receiver
        m_receiverPortMap[receiverIdx]++;
    }
    
    return m_receiverPortMap[receiverIdx];
}

void
FatTreeTopology::ScheduleNextBackgroundFlow()
{
    NS_LOG_FUNCTION(this);
    
    // Calculate inter-arrival time for background traffic
    // Lambda = load * link_capacity / average_flow_size
    double avgFlowSize = 0.0;
    
    for (size_t i = 1; i < HADOOP_CDF.size(); ++i)
    {
        double prob = HADOOP_CDF[i].second - HADOOP_CDF[i-1].second;
        avgFlowSize += HADOOP_CDF[i].first * prob / 100.0;
    }
    
    // Calculate arrival rate (flows per second)
    double linkCapacityBps = 100e9; // 100 Gbps server links
    double lambda = (BACKGROUND_LOAD * linkCapacityBps * NUM_SERVERS) / (avgFlowSize * 8.0);
    
    // Schedule next flow
    double interArrival = m_exponentialRandom->GetValue(1.0 / lambda, 0.0);
    
    if (Simulator::Now().GetSeconds() + interArrival < SIMULATION_TIME)
    {
        Simulator::Schedule(Seconds(interArrival), &FatTreeTopology::StartBackgroundFlow, this);
    }
}

void
FatTreeTopology::StartBackgroundFlow()
{
    NS_LOG_FUNCTION(this);
    
    // Get random sender-receiver pair
    auto serverPair = GetRandomServerPair();
    uint32_t senderIdx = serverPair.first;
    uint32_t receiverIdx = serverPair.second;
    
    // Get server nodes and addresses
    Ptr<Node> sender = m_servers.Get(senderIdx);
    Ptr<Node> receiver = m_servers.Get(receiverIdx);
    
    uint32_t senderRack = senderIdx / SERVERS_PER_RACK;
    uint32_t senderPos = senderIdx % SERVERS_PER_RACK;
    uint32_t receiverRack = receiverIdx / SERVERS_PER_RACK;
    uint32_t receiverPos = receiverIdx % SERVERS_PER_RACK;
    
    Ipv4Address receiverAddr = GetServerAddress(receiverRack, receiverPos);
    
    // Sample flow size
    uint32_t flowSize = SampleHadoopFlowSize();
    
    // Validate receiver address
    if (receiverAddr == Ipv4Address("0.0.0.0"))
    {
        NS_LOG_ERROR("Invalid receiver address 0.0.0.0 for server " << receiverIdx);
        return;
    }
    
    // Create UDP sink on receiver with unique port per receiver
    uint16_t port = GetUniquePort(receiverIdx, false); // false = UDP
    
    NS_LOG_INFO("UDP Background: " << senderIdx << " -> " << receiverIdx << " (" << flowSize << " bytes)");
    
    PacketSinkHelper sinkHelper("ns3::UdpSocketFactory", InetSocketAddress(Ipv4Address::GetAny(), port));
    ApplicationContainer sinkApp = sinkHelper.Install(receiver);
    
    sinkApp.Start(Simulator::Now());
    sinkApp.Stop(Seconds(SIMULATION_TIME));
    
    // Create UDP source on sender
    OnOffHelper sourceHelper("ns3::UdpSocketFactory", InetSocketAddress(receiverAddr, port));
    sourceHelper.SetAttribute("PacketSize", UintegerValue(1024));
    sourceHelper.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1.0]"));
    sourceHelper.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=0.0]"));
    
    // Calculate data rate to send flowSize bytes over a reasonable duration
    double flowDuration = 0.1; // 100ms flow duration (more realistic)
    double dataRateBps = (flowSize * 8.0) / flowDuration; // bits per second
    
    // Cap data rate to avoid overflow and unrealistic rates (max 10 Gbps)
    double maxDataRate = 10e9; // 10 Gbps
    if (dataRateBps > maxDataRate) {
        dataRateBps = maxDataRate;
        flowDuration = (flowSize * 8.0) / dataRateBps;
        NS_LOG_INFO("Capped data rate to " << maxDataRate/1e9 << " Gbps, adjusted duration to " << flowDuration << "s");
    }
    
    sourceHelper.SetAttribute("DataRate", StringValue(std::to_string(dataRateBps) + "bps"));
    
    ApplicationContainer sourceApp = sourceHelper.Install(sender);
    sourceApp.Start(Simulator::Now());
    sourceApp.Stop(Simulator::Now() + Seconds(flowDuration));
    
    m_backgroundApps.Add(sinkApp);
    m_backgroundApps.Add(sourceApp);
    
    // Schedule next flow
    ScheduleNextBackgroundFlow();
}

void
FatTreeTopology::ScheduleNextIncast()
{
    NS_LOG_FUNCTION(this);
    
    // Calculate inter-arrival time for incast traffic
    // Lambda = load * link_capacity / (incast_size * num_senders)
    double linkCapacityBps = 100e9; // 100 Gbps server links
    double incastBytes = INCAST_SIZE * INCAST_SENDERS;
    double lambda = (INCAST_LOAD * linkCapacityBps * NUM_SERVERS) / (incastBytes * 8.0);
    
    // Schedule next incast
    double interArrival = m_exponentialRandom->GetValue(1.0 / lambda, 0.0);
    
    if (Simulator::Now().GetSeconds() + interArrival < SIMULATION_TIME)
    {
        Simulator::Schedule(Seconds(interArrival), &FatTreeTopology::StartIncast, this);
    }
}

void
FatTreeTopology::StartIncast()
{
    NS_LOG_FUNCTION(this);
    
    // Select random receiver
    uint32_t receiverIdx = m_uniformRandom->GetInteger(0, NUM_SERVERS - 1);
    Ptr<Node> receiver = m_servers.Get(receiverIdx);
    
    uint32_t receiverRack = receiverIdx / SERVERS_PER_RACK;
    uint32_t receiverPos = receiverIdx % SERVERS_PER_RACK;
    Ipv4Address receiverAddr = GetServerAddress(receiverRack, receiverPos);
    
    // Validate receiver address
    if (receiverAddr == Ipv4Address("0.0.0.0"))
    {
        NS_LOG_ERROR("Invalid receiver address 0.0.0.0 for incast receiver " << receiverIdx);
        return;
    }
    
    // Create TCP sink on receiver with unique port
    uint16_t port = GetUniquePort(receiverIdx, true); // true = TCP
    PacketSinkHelper sinkHelper("ns3::TcpSocketFactory", InetSocketAddress(Ipv4Address::GetAny(), port));
    ApplicationContainer sinkApp = sinkHelper.Install(receiver);
    sinkApp.Start(Simulator::Now());
    sinkApp.Stop(Seconds(SIMULATION_TIME));
    m_incastApps.Add(sinkApp);
    
    // Select random senders (excluding receiver)
    std::vector<uint32_t> availableSenders;
    for (uint32_t i = 0; i < NUM_SERVERS; ++i)
    {
        if (i != receiverIdx)
        {
            availableSenders.push_back(i);
        }
    }
    
    // Randomly shuffle and select INCAST_SENDERS
    std::random_shuffle(availableSenders.begin(), availableSenders.end());
    uint32_t numSenders = std::min(INCAST_SENDERS, (uint32_t)availableSenders.size());
    
    NS_LOG_INFO("TCP Incast: " << numSenders << " senders -> receiver " << receiverIdx << 
                " (" << INCAST_SIZE << " bytes each) at " << Simulator::Now().GetSeconds() << "s");
    
    // Create TCP sources on selected senders
    for (uint32_t i = 0; i < numSenders; ++i)
    {
        uint32_t senderIdx = availableSenders[i];
        Ptr<Node> sender = m_servers.Get(senderIdx);
        
        // Create bulk send application
        BulkSendHelper sourceHelper("ns3::TcpSocketFactory", InetSocketAddress(receiverAddr, port));
        sourceHelper.SetAttribute("MaxBytes", UintegerValue(INCAST_SIZE));
        sourceHelper.SetAttribute("SendSize", UintegerValue(1024));
        
        ApplicationContainer sourceApp = sourceHelper.Install(sender);
        sourceApp.Start(Simulator::Now());
        sourceApp.Stop(Seconds(SIMULATION_TIME));
        m_incastApps.Add(sourceApp);
    }
    
    // Schedule next incast
    ScheduleNextIncast();
}

int
main(int argc, char *argv[])
{
    // Configure logging - reduced verbosity for performance
    LogLevel logLevel = (LogLevel)(LOG_PREFIX_TIME | LOG_LEVEL_WARN);
    LogComponentEnable("FatTreeSim", logLevel);
    
    // Enable info level only for important messages
    LogComponentEnable("FatTreeSim", LOG_LEVEL_INFO);
    
    // Parse command line arguments
    CommandLine cmd;
    cmd.Parse(argc, argv);
    
    NS_LOG_INFO("Starting Fat-Tree topology simulation...");
    
    // Create and build the fat-tree topology
    FatTreeTopology fatTree;
    fatTree.BuildTopology();
    fatTree.PrintTopologyInfo();
    fatTree.ConfigureRouting();
    
    NS_LOG_INFO("Fat-Tree topology setup completed");
    
    // Generate traffic (background Hadoop workload + incast)
    fatTree.GenerateTraffic();
    
    NS_LOG_INFO("Running simulation for " << SIMULATION_TIME << " seconds...");
    
    // Run simulation
    Simulator::Stop(Seconds(SIMULATION_TIME));
    Simulator::Run();
    
    NS_LOG_INFO("Simulation completed successfully");
    
    Simulator::Destroy();
    
    return 0;
}
