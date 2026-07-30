# PROJECT_CONTEXT: Speakeasy

## 1. System Overview & Mission
**Speakeasy** is a federated, off-grid-capable bulletin board system (BBS) and micro-community hub built on the Reticulum Network Stack and LXMF messaging protocol. 

The goal of the project is to provide robust, resilient, and decentralized channel-based discussions, bulletin posts, and operator tools. It is primarily designed to operate over consistent IP links, but I intend to look at low-bandwidth/high-latency links in the future.

---

## 2. Technical Stack & Dependencies
* **Core Transport / Networking:** Reticulum Network Stack (`Reticulum`), LXMF (`LXMF`)
* **Application Architecture:** Federated Hub / Host Nodes <-> Client Nodes
* **Primary Target Deployment:** Single-Board Computers (Raspberry Pi 4 / Pi 5 class hardware) up to standard server Linux environments.
* **Network Interfaces:** IP

---

## 3. High-Level Node Architecture
* **Host / Hub Nodes:** Maintain state for carried channels, bulletin board posts, client sessions, and inter-hub federation peering.
* **Client Nodes:** Connect to Hosts, discover supported channels, submit/read bulletin posts, and manage local peer scores/pathing.
* **Federation Layer:** Handles automated inter-hub discovery, channel propagation prioritization, and administrative sync messages.

---

## 4. Active Engineering Backlog & Objectives

### A. Routing, Federation & Channel Discovery
* [ ] **Dynamic Channel Discovery:** Mechanism during federation discovery so Hub A prioritizes federating with Hub B if both carry matching channels.
* [ ] **Cross-Hub Channel Polling:** Client-initiated or Hub-initiated query to poll federated peers to check if a specific requested channel is already carried elsewhere.
* [ ] **Federation Graph Visibility:** Interface tools for clients and operators to visualize active federation topologies and relationships between Host nodes.
* [ ] **Cold-Start Host Recovery:** Client logic to actively probe prior-known Hosts (ordered by host score) on startup rather than passively waiting for Announce packets.
* [ ] **Host Advertisement Metadata:** Inject host age/uptime metrics into client menu views.

### B. Access Control, Moderation & Identity
* [ ] **Peer-Level User Blacklisting:** Local client-side blacklist controls to mute/block specific identities independently.
* [ ] **Operator Blacklist Recommendation System:** Out-of-band LXMF broadcast mechanism allowing an operator to send signed blacklisting recommendations (with rationale) to peer operator addresses.
* [ ] **Channel Management Matrix:** CLI/TUI/Config interface for operators to list, add, remove, and modify allowed/carried channels dynamically.

### C. Client & User Experience (UX/UI)
* [ ] **Bulletin Lifecycle & Archival:** Support user-initiated deletion of posts and automated archiving to sub-pages after a configurable threshold (default: 7 days).
* [ ] **Interactive Comments:** Repurpose the primary message input box beneath bulletin posts into a threaded comment submission control.
* [ ] **Reticulum Diagnostics:** Surface underlying Reticulum pathing failures, interface drops, and transport errors into the client system log UI.

### D. System Stability, State & Metrics
* [ ] **Session Tracking Bug:** Fix connection state leak where dropped/reconnected clients increment active user counts without purging stale interfaces.
* [ ] **Hardware Load & Capacity Profiling:** Benchmark maximum stable concurrent connections per Host running on Raspberry Pi 4/5 hardware over typical internet backhaul (50–100 Mbps).
