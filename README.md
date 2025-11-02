# 🗂️ Key-Value Store with Primary/Backup Replication

This project implements a simple **fault-tolerant key-value service** using a **primary/backup replication** model. It demonstrates core concepts of **replication, consistency, and reliability** in distributed systems.

---

## 🚀 Overview

The system maintains a distributed map of key-value pairs and supports three main operations:

- **Get(key)** → Returns the latest value for the key (or an empty string if not found).  
- **Put(key, value)** → Sets or updates the value of a key.  
- **PutHash(key, value)** → Updates the key’s value to `hash(oldValue, value)` and returns the previous value (used for testing consistency).

Clients send requests to the **primary server**, which forwards operations to the **backup server** to ensure data synchronization and fault tolerance.

---

## ⚙️ Features

- Fault-tolerant design with **primary/backup replication**
- **Reliable client request handling** using retries until successful responses
- **Data consistency** between servers through synchronized updates
- **PutHash operation** for testing and validation
- Clear modular design for educational purposes

---

## 🧩 Technologies Used

- **Go (Golang)** for server logic and concurrency  
- **RPC (Remote Procedure Call)** for communication between primary and backup servers  
- **Hashing functions** for data integrity checks  

---

## 🧠 Key Concepts

- Replication and synchronization  
- Fault tolerance and reliability  
- Consistency in distributed systems  
- Client retries and idempotent operations  

---

## 🧪 How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/YamenMohamed/Replicated-key-value-service.git
   cd <repo-name>
``

2. Build and run the servers:

   ```bash
   go run primary_server.go
   go run backup_server.go
   ```

3. Run client tests:

   ```bash
   go test
   ```

---

## 📚 Learning Outcome

This project serves as an introduction to distributed systems and fault-tolerant service design. It highlights practical challenges like replication, synchronization, and ensuring at-most-once semantics in real-world systems.

---

## 🧑‍💻 Author

**Yamen Mohamed**
Fresh Computer Science Graduate | Passionate about Systems, Backend, and Distributed Computing
[GitHub](https://github.com/YamenMohamed)

