# Inverted Index with MapReduce 🗂️

This is a simple Python implementation of the Inverted Index with MapReduce.

## Project Structure 📂
```plaintext
mapreduce/
├── master/
│   ├── Dockerfile
│   ├── src/
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   ├── coordinator.py
│   │   ├── map_functions.py
│   │   ├── job_state.py
│   │   ├── master.py   
│   │   └── task_monitor.py
│   └── requirements.txt
├── mapper/
│   ├── Dockerfile
│   ├── src/
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   ├── mapper.py
│   │   └── available_functions.py
│   │   ├── processor.py
│   ├── data/
│   │   ├── intermediate/
│   └── requirements.txt
└── reducer/
│    ├── data/
│    │   ├── final/
│    ├── Dockerfile
│    ├── src/
│    │   ├── __init__.py
│    │   ├── constants.py
│    │   ├── reducer.py
│    │   └── processor.py
│    └── requirements.txt
```

## Setup Options

### Option 1: Docker Setup 🐳

Clone the repository:

```bash
git clone https://github.com/lewiskyron/mapreduce-inverted-index.git
cd mapreduce-inverted-index
```

#### Create a Python virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```

#### Build and run using Docker Compose:
```bash
docker-compose down && docker-compose up --build
```

### Option 2: Local Setup with Zip File 📦
#### 1. Extract the ZIP file

#### 2. Create a Python virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```

#### 3.Build and run using Docker Compose:
```bash
docker-compose down && docker-compose up --build
```

#### 4. To run the scrape job run 
```bash
❯ curl -X POST http://localhost:5001/scrape
```

3. Runing on PwStern Computers
1. ssh into the machine 
2. cd into `mapreduce-swarm`
3. ls and check the docker-compose.yml file exists 

if not use vim to create one inside here 
here is the docker-compose.yml file

```plaintext
version: '3.8'
services:
  master-service:
    image: kyronnyoro/mapreduce-master:scaled
    ports:
      - "5001:5001"
    networks:
      - kyron-overlay-net
    environment:
      - MAPPER_URLS=http://mapper-service-1:5002,http://mapper-service-2:5002,http://mapper-service-3:5002
      - REDUCER_URLS=http://reducer-service:5003
    deploy:
      placement:
        max_replicas_per_node: 1
      restart_policy:
        condition: on-failure

  mapper-service-1:
    image: kyronnyoro/mapreduce-mapper:scaled
    expose:
      - "5002"
    networks:
      - kyron-overlay-net
    environment:
      - MASTER_URL=http://master-service:5001
      - MAPPER_URL=http://mapper-service-1:5002
      - MAPPER_ID=1
    deploy:
      placement:
        max_replicas_per_node: 1
      restart_policy:
        condition: on-failure
    depends_on:
      - master-service

  mapper-service-2:
    image: kyronnyoro/mapreduce-mapper:scaled
    expose:
      - "5002"
    networks:
      - kyron-overlay-net
    environment:
      - MASTER_URL=http://master-service:5001
      - MAPPER_URL=http://mapper-service-2:5002
      - MAPPER_ID=2
    deploy:
      placement:
        max_replicas_per_node: 1
      restart_policy:
        condition: on-failure
    depends_on:
      - master-service

  mapper-service-3:
    image: kyronnyoro/mapreduce-mapper:scaled
    expose:
      - "5002"
    networks:
      - kyron-overlay-net
    environment:
      - MASTER_URL=http://master-service:5001
      - MAPPER_URL=http://mapper-service-3:5002
      - MAPPER_ID=3
    deploy:
      placement:
        max_replicas_per_node: 1
      restart_policy:
        condition: on-failure
    depends_on:
      - master-service

  reducer-service:
    image: kyronnyoro/mapreduce-reducer:scaled
    expose:
      - "5003"
    networks:
      - kyron-overlay-net
    environment:
      - MASTER_URL=http://master-service:5001
      - REDUCER_URL=http://reducer-service:5003
      - REDUCER_ID=1
    deploy:
      placement:
        max_replicas_per_node: 1
      restart_policy:
        condition: on-failure
    depends_on:
      - master-service

networks:
  kyron-overlay-net:
    external: true
```

### Run 
docker stack rm mapreduce_stack
docker service ls

### Logs 
- docker service logs -f mapreduce_stack_master-service
- docker service logs -f mapreduce_stack_mapper-service-1
- docker service logs -f mapreduce_stack_mapper-service-2
- docker service logs -f mapreduce_stack_reducer-service

if master on cs1 run 
```bash
curl -X POST -v http://192.168.86.67:5001/scrape to scrap the data
```

if master on cs2 run 
```bash
curl -X POST -v http://192.168.86.48:5001/scrape to scrap the data
```

### Loom Demo
https://www.loom.com/share/892d034e8b1448c8bc39832efbd1a5c1?sid=e7236f9c-1205-4f46-a65c-db2b701e3e9b

## System Requirements 🖥️

- Python 3.7+
- Docker and Docker Compose (for Docker setup)

## MapReduce Implementation Analysis for Inverted Index Creation
### What is an Inverted Index?
An inverted index is a core data structure used in information retrieval systems and search engines to facilitate efficient full-text searches. Unlike a forward index, which maps documents to their contents, an inverted index maps each unique term to the list of documents that contain that term. This structure allows search engines to quickly identify and retrieve documents containing specific terms, making queries much faster and more scalable for large datasets.

For instance, imagine we have the following documents:
- **Doc1**: `"the cat sat"`
- **Doc2**: `"the dog ran"`
- **Doc3**: `"cat and dog played"`

From these documents, we can create an inverted index, which shows us where each term appears. The resulting inverted index would look like this:

| **Term**   | **Documents**    |
|------------|------------------|
| `the`      | [Doc1, Doc2]     |
| `cat`      | [Doc1, Doc3]     |
| `dog`      | [Doc2, Doc3]     |
| `sat`      | [Doc1]           |
| `ran`      | [Doc2]           |
| `and`      | [Doc3]           |
| `played`   | [Doc3]           |


In this example, the term “cat” is found in both Doc1 and Doc3, while the term “the” appears in Doc1 and Doc2. This structure makes it easy for the system to quickly identify which documents contain specific words, enabling efficient full-text searches.

### So how does this fit into the MapReduce framework?
Our system creates an **inverted index** using the **MapReduce framework**, breaking the process into two key phases: 
- the **Map Phase** 
- the **Reduce Phase**.

At this point, it is important to note that for our particular implementation, our `master` fetches a list of URLs from the `Rock musical group stubs` containing URLs of American rock band articles. These URLs are retrieved using the Wikipedia API, and we distribute these URIs to mappers based on the number of mappers, ensuring each mapper gets a chunk to process.


## Map Phase

In the **Map Phase**, each mapper handles a portion of the Wikipedia article URLs. The mapping process involves the following steps:

---

### 1. 🕷️ **Scraping**

- Uses **BeautifulSoup4** to scrape Wikipedia article content.  
- Targets the main content `<div>` with ID `mw-content-text`.  
- Removes unwanted elements like **tables**, **scripts**, and **style** tags.  
- Extracts clean text content from the HTML.

---

### 2. 📝 **Text Processing**

- Converts all text to **lowercase** for consistency.  
- Uses **regular expressions** to remove non-alphabetic characters.  
- Employs **NLTK (Natural Language Toolkit)** for advanced text processing:  

  - **Removes** common English stop words (e.g., "the", "is", "at").  
  - Uses **NLTK's tokenization** to break text into individual terms.  
  - **Filters out** irrelevant terms to reduce noise in the index.  

---

### 3. 📤 **Emits** `(term, document_id)` Pairs

For each term found in the document, the mapper emits `(term, document_id)` pairs.

These `(term, document_id)` pairs are temporarily stored in **intermediate files**, where they are grouped by term(**shuffled**).

Once the intermediate files are stored, the mapper notifies the master of the file's location.


## **Reduce Phase**

Reducers collect intermediate results from mappers. For each unique term:

1. **Combines** all document references.  
2. **Removes duplicates**.  
3. **Creates** final term-to-document mappings.  
4. Produces the final **inverted index** as **JSON output**.

---

This implementation allows for **parallel processing** of large document collections, with:

- Multiple **mappers** working simultaneously on different document subsets.  
- **Reducers** combining their results efficiently.



Now that we have a good understanding of the how the MapReduce works on the inverted index, let's dive into the som technical details of the implementation.

## ⚙️ **1. System Scale and Configuration**

Our system leverages **Docker Compose** to provide a flexible and scalable architecture, making it easy to manage and expand the number of mappers and reducers. The current setup consists of:

- **1 Master Node**: Coordinates the overall MapReduce job.  
- **3 Mapper Nodes**: Handle the initial processing and emit intermediate key-value pairs.  
- **2 Reducer Nodes**: Collect and combine the intermediate results to produce the final output.  

---

### 📈 **Scaling the System**

One of the great things about this setup is how easy it is to scale. If you need more mappers or reducers to handle larger workloads, you can adjust the configuration by making a few changes to the `docker-compose.yml` file. The easiest way to do this is by adding or removing service definitions in the `docker-compose.yml` file.

---

To add new mappers or reducers you’ll need to:

1. **Give it a unique container name** to avoid conflicts.  
2. **Assign a unique port mapping** so that each service communicates correctly.  
3. **Set a unique service ID** in the environment variables for identification.  
4. **Add the new service’s URL** to the master’s `MAPPER_URLS` or `REDUCER_URLS` environment variables so the master knows where to send tasks.  


This way, you can easily expand or shrink the number of mappers and reducers depending on your processing needs.

---

## **2. Architectural Design and Workflow**


### **System Initialization and Registration**

When the system starts, all containers — the **master node**, **mappers**, and **reducers** — are initialized in parallel using **Docker Compose**.

Once the **master node** starts:

1. The **MapReduceCoordinator** is initialized to oversee task distribution and system workflow.  
2. **Registries** are set up to track active mappers and reducers.  
3. The **TaskMonitor** is initialized to monitor worker health and track task progress.

---

### 📝 **Worker Registration Process**

1. **Startup and Registration**:  
   - Each worker (mapper or reducer) attempts to **register with the master**.  
   - Registration includes **retry logic** of up to **5 attempts** with **3-second delays** to handle potential network issues and situations where the master has not fully instantiated (since all workers and the master are instantiated simultaneously).

2. **Worker Details**:  
   - Workers provide their **URLs** to the master.  
   - The master confirms successful registration, making the worker available for tasks.

3. **Monitoring**:  
   - The master monitors registered workers through a **heartbeat mechanism**, periodically checking their status.  
   - If a worker stops responding, the master:  
     - **Reassigns tasks** to other workers.  
     - Marks the worker as **failed** if necessary.

## **Task Coordination and Execution**

###  **Task Distribution**

The **master node** handles task distribution through the **MapReduceCoordinator(coordinator.py)**, ensuring tasks are assigned efficiently across available workers. Here’s how the distribution process works:

1. ** Load Balancing**:  
   - The master divides the list of document URLs among the mappers.  
   - The workload is distributed **evenly** to prevent any single mapper from becoming a bottleneck.

2. ** Unique Task Assignments**:  
   - Each mapper receives a **unique task ID** along with a specific range of document IDs to process.  
   - This helps the master track which tasks have been assigned and completed.

3. ** Task Tracking**:  
   - The **TaskMonito(task_monitor.py)** component monitors each assigned task.  
   - It records progress and ensures tasks are **completed successfully** or **reassigned** if needed.

This approach ensures that the workload is balanced, tasks are efficiently managed, and the system can recover from potential failures.


## **Inter-Component Communication**

The master and workers (mappers and reducers) stay in sync through regular communication to ensure smooth task execution. Key aspects of this communication include:

- **Heartbeat Checks:** Every 5 seconds, the master sends a heartbeat signal to each worker to check their health and ensure they are still operational.
- **Status Reporting:** Workers regularly report their current status and task progress back to the master. This includes details such as task completion percentage and any potential issues encountered.
- **Real-Time System State:** The master maintains an up-to-date view of the entire system, knowing which workers are active, which tasks are in progress, and which tasks are complete.

However, processing tasks and maintaining this heartbeat mechanism can be challenging, especially in distributed systems. To address this our system uses 
a *multi-threaded* approach to ensure that workers can process tasks in parallel and communicate with the master in real-time. This multi-threading approch come with the following benefits:

- **Improved Efficiency:** The master node uses multiple threads to handle different responsibilities concurrently. One set of threads focuses on distributing tasks to mappers and reducers, while another set manages periodic heartbeat checks to monitor worker health. Additionally, the workers (both mappers and reducers) are also multi-threaded, allowing them to process multiple tasks simultaneously. This concurrent execution ensures that both task coordination and data processing happen efficiently.

- **Responsiveness:** By handling task assignments and health monitoring in parallel, the system avoids bottlenecks and remains responsive, even when managing a large number of tasks and workers.

- **Scalability:** Multi-threading allows the system to scale seamlessly with increased workloads. As the number of mappers and reducers grows, the master node can efficiently coordinate all operations without delays.



## 3. **Current Limitations and Bottlenecks**
While the system is effective for many use cases, there are some limitations and potential bottlenecks that could impact performance and scalability. Here’s a breakdown of these challenges along with suggestions for improvement.

### **Function Registry Constraints**</br>
- **Current Limitation:**<br>
The current implementation relies on hardcoded mapping functions, specifically designed for processing Wikipedia URLs. Though we have some classes for defining available functions(only limited to the names of the functions that are available), this rigid design limits the system’s flexibility and makes it challenging to adapt to different data sources or processing requirements.</br> 
- **Suggested Improvement:**<br>
Introduce a plugin architecture that allows for dynamic function loading. This would enable developers to easily add or modify mapping functions without changing the core system code. By supporting a variety of data sources and tasks, the system would become much more versatile and adaptable to different workflows.



### **Network Bandwidth**  

- **Current Limitation:**<br>  
  The current design requires transferring the entire set of intermediate results from mappers to reducers. With large datasets, this can consume significant network bandwidth and slow down overall processing.

- **Suggested Improvement:**<br>  
  Incorporate data locality awareness to reduce the need for data transfer. Assign reducers to nodes that are physically closer to the data they need to process. Additionally, support partial data transfers by combining and compressing intermediate results before sending them over the network. This would help minimize network load and improve efficiency.

---

### **Master Node Centralization**  

- **Current Limitation:**<br>  
  The system relies on a single master node to coordinate all tasks. This can become a bottleneck when the number of tasks and workers increases, limiting overall scalability and making the system vulnerable to failures at the master node.

- **Suggested Improvement:**<br>  
  Adopt a hierarchical master node design or a distributed coordination system. By introducing secondary or backup masters, the workload can be distributed more evenly. This approach enhances scalability and ensures the system remains resilient, even if the primary master node fails.


### **Static Worker Scaling**
- **Current Limitation:**<br>
    The system does not currently support dynamic scaling of workers (mappers and reducers) at runtime. Adding or removing workers requires modifying the docker-compose.yml configuration file and restarting the system, which is inefficient for handling fluctuating workloads.

- **Suggested Improvement:**<br>
    Implement a dynamic scaling mechanism that allows workers to be added or removed while the system is running. This could be achieved by integrating with orchestration tools like Kubernetes or enhancing Docker Compose to support dynamic updates. This would allow the system to respond to workload changes in real-time, optimizing resource usage.


## Fault Tolerance
### Fault Tolerance
When a node fails, the system handles the failure seamlessly through a combination of dynamic deregistration and Docker Swarm’s self-healing capabilities.

### Automatic Deregistration:
If a node becomes unresponsive, the master node deregisters it from the list of active workers. This prevents new tasks from being assigned to the failed node and ensures the system doesn’t rely on an unavailable worker.

### Docker Swarm Recovery:
Docker Swarm automatically detects when a worker node or service fails and restarts the corresponding container on an available host. Once the service is restarted, the worker re-registers with the master node, seamlessly returning to active duty.

This combination of deregistration on failure and automatic recovery by Docker Swarm ensures that the system handles node failures gracefully and recovers quickly, maintaining reliable task execution with minimal manual intervention.
