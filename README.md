# Distributed Key-Value Store

A high-performance, fault-tolerant distributed key-value storage system built from scratch in Python. This system allows you to store and retrieve data across multiple clients while handling network failures gracefully.

## 🎯 What Problem Does This Solve?

In today's world, applications need to share data across multiple servers and handle thousands of users simultaneously. Traditional databases can become bottlenecks when multiple programs try to access the same data at once. This project solves several critical problems:

**Data Consistency Across Multiple Clients**: When multiple programs try to update the same data simultaneously, conflicts can occur. This system ensures that all clients see the same consistent view of the data and prevents corruption.

**Network Reliability Issues**: Real networks drop messages, experience delays, and sometimes fail completely. This system automatically handles these failures by retrying operations and ensuring no data is lost or duplicated.

**Coordination Between Distributed Systems**: When you have multiple servers or programs working together, they often need to coordinate who does what. The distributed locking feature allows programs to safely share resources and avoid conflicts.

**Scalability for Growing Applications**: As applications grow, they need storage systems that can handle more clients without breaking down. This system maintains performance and correctness even under heavy load.

## 🌍 Impact and Use Cases

This project demonstrates solutions that power many real-world systems:

**Cloud Storage Services**: Like Dropbox or Google Drive, where millions of users need to store and sync files reliably across devices.

**E-commerce Platforms**: Online stores need to track inventory accurately even when thousands of customers are buying products simultaneously.

**Gaming Systems**: Multiplayer games require consistent game state across all players, even when network connections are unstable.

**Financial Systems**: Banks and payment processors need absolute data consistency to prevent double-spending or lost transactions.

**IoT and Sensor Networks**: Smart home devices and industrial sensors need to coordinate and share data reliably, even with unreliable wireless connections.

**Microservices Architecture**: Modern applications built as multiple small services need robust ways to share configuration and coordinate work.

## 🏗️ System Architecture

![System Architecture](./distributed-kv/distributed_key_value_store_design.png)

## 🔄 How It Works

This system works like a simple database that multiple programs can use at the same time. Here's how it operates:

**When you store data**: You send a key and value (like "username" and "john_doe") to the server. The server stores it with a version number to track changes. If the network drops your request, the client automatically tries again until it succeeds.

**When you read data**: You ask for a key, and the server sends back the value and its version number. This helps you know if the data changed since you last looked at it.

**When multiple clients work together**: Each client can store and read data independently. The server makes sure all operations happen in the right order, so everyone sees the same data. If two clients try to update the same key at the same time, only one succeeds - the other gets told to try again with the newer version.

**When the network has problems**: If messages get lost (which happens in real networks), the client keeps trying with smart delays. The server remembers recent requests so it won't accidentally do the same operation twice.

**When you need locks**: You can create distributed locks to coordinate work between different clients. For example, if only one client should process a file at a time, they can all try to acquire a lock, and only the winner gets to work while others wait.

## 🚀 Quick Start

```bash
# No external dependencies required - uses Python standard library only
python3 --version  # Requires Python 3.7+

# Run verification tests
cd distributed-kv
python3 verify.py

# Run interactive demos
python3 run.py demo

# Install pytest for full test suite (optional)
pip install pytest
pytest tests/ -v
```
