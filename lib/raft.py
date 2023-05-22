import asyncio
from threading     import Thread, Timer
from xmlrpc.client import ServerProxy
from typing        import Any, List
from enum          import Enum
from .struct.address       import Address
from .app                  import MessageQueue
from math         import ceil
import socket
import time
import json
import random


class RaftNode:
    HEARTBEAT_INTERVAL      = 1
    ELECTION_TIMEOUT_MIN    = 45
    ELECTION_TIMEOUT_MAX    = 90
    HEARTBEAT_TIMEOUT_MIN   = 30
    HEARTBEAT_TIMEOUT_MAX   = 60
    RPC_TIMEOUT             = 10

    LOG_MSG_IDX             = 0
    LOG_TERM_IDX            = 1

    SENT_ADDR_IDX           = 0
    SENT_LEN_IDX            = 1

    ACK_ADDR_IDX            = 0
    ACK_LEN_IDX             = 1

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3
        

    def __init__(self, application : Any, addr: Address, contact_addr: Address = None):
        # Address Info
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        self.address:             Address           = addr
        self.type:                RaftNode.NodeType = RaftNode.NodeType.FOLLOWER
        
        # Cluster
        self.cluster_addr_list:   List[Address]     = []
        self.cluster_leader_addr: Address           = None
        
        # Heartbeat
        self.heartbeat_thread:          Thread          = None
        self.heartbeat_timer:           int             = 0
        self.current_hearbeat_timeout:  int             = 0

        # Leader election
        self.election_term:       int                   = 0
        self.election_thread:     Thread                = None
        self.election_timer:      int                   = 0
        self.election_timeout:    int                   = 0
        self.voted_for:           Address               = None
        self.votes:               int                   = 0

        # Log
        self.log:                 List[str, int]        = []
        self.replicate_thread:    Thread                = None

        # Message Queue
        self.app:                 MessageQueue          = application

        self.sent_length:         List[Address, int]    = []
        self.acked_length:        List[Address, int]    = []
        self.commit_length:       int                   = 0

        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.sent_length.append([self.address, 0])
            self.acked_length.append([self.address, 0])
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)

    # Internal Raft Node methods
    # ---------------------------------------------------------------------------------------------
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node...")
        self.cluster_leader_addr = self.address
        self.type                = RaftNode.NodeType.LEADER
        request = {
            "cluster_leader_addr": self.address,
            "cluster_addr_list": self.cluster_addr_list,
            "log": self.log,
            "election_term": self.election_term,
        }

        # Inform to all node this is new leader
        for addr in self.cluster_addr_list:
            if addr != self.address:
                try:
                    Thread(target=self.__send_request, args=[request, "announce_as_leader", addr]).start()
                except:
                    self.__print_log(f"Failed to send announce_as_leader request to {addr}")

        # Start heartbeat
        self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()
    
    def __initialize_as_candidate(self):
        self.__print_log("Initialize as candidate node...")
        self.cluster_leader_addr    = None
        self.type                   = RaftNode.NodeType.CANDIDATE
        self.election_term          += 1
        self.voted_for              = self.address
        self.votes                  = 1

        # Start election leader
        if self.election_thread is None:
            self.election_thread = Thread(target=asyncio.run,args=[self.__leader_election()])
            self.election_thread.start()

    def __initialize_as_follower(self):
        self.__print_log("Initialize as follower node...")
        self.type                       = RaftNode.NodeType.FOLLOWER
        self.heartbeat_timer            = 0
        self.current_hearbeat_timeout   = self.__heartbeat_timeout()
        self.voted_for                  = None
        self.votes                      = 0

        # Listen heartbeat
        if self.cluster_leader_addr is not None:
            self.heartbeat_thread = Thread(target=asyncio.run,args=[self.__follower_heartbeat()])
            self.heartbeat_thread.start()

    async def __leader_election(self):
        # Start leader election
        self.election_timer = 0
        self.current_election_timeout = self.__election_timeout()
        timeout_election = False

        self.__initialize_as_candidate()
        self.__print_log("Start leader election...")
        self.__voting

        while self.cluster_leader_addr is None or timeout_election:
            if self.election_timer == 0:
                timeout_election = False
            
            self.election_timer += 1

            # Timeout election timer
            if self.election_timer >= self.current_election_timeout:
                # Check if new leader is found
                if self.cluster_leader_addr is None:
                    self.__print_log("Election timeout, new leader not found. Start new election...")
                    timeout_election = True
                    self.__cancel_election_timer()
                    self.__initialize_as_candidate()
                    self.__print_log("Start leader election...")
                    self.__voting()
                else:
                    self.election_thread = None
                    return

            await asyncio.sleep(1)
    
    def __voting(self):
        # Voting
        request = {
            "candidate_addr": self.address,
            "election_term": self.election_term,
            "log_length": len(self.log),
            "log_term": self.log[-1][1] if len(self.log) > 0 else 0,
        }
        # Send vote request to all node
        for addr in self.cluster_addr_list:
            if addr != self.address:
                self.__print_log(f"Send vote request to {addr}")
                Thread(target=self.__send_request, args=[request, "vote_request", addr]).start()
    
    def __election_timeout(self):
        # Generate random election timeout
        return random.uniform(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX)
    
    def __cancel_election_timer(self):
        # Cancel election timer
        if self.election_thread is None:
            self.election_thread = Thread(target=asyncio.run,args=[self.__leader_election()])

        self.election_timer = 0
        self.current_election_timeout = self.__election_timeout()

    async def __leader_heartbeat(self):
        # Send periodic heartbeat
        while self.type == RaftNode.NodeType.LEADER:
            self.__print_log("[Leader] Sending heartbeat...")
            self.__print_log(f'[Leader] log: {self.log}')
            self.__print_log(f'[Leader] queue: {self.app.queue}')
            for addr in self.cluster_addr_list:
                addr = Address(addr["ip"], addr["port"])
                if addr != self.address:
                    self.__print_log(f"[Leader] Heartbeat to {addr}")
                    # Send heartbeat
                    Thread(target=self.__send_request, args=[self.address, "heartbeat", addr]).start()
                    # Replicate log
                    self.replicate_log(self.address, addr)

            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
    
    def replicate_log(self, leader_id: Address, follower_id: Address):
        # Replicate log to follower
        idx = [i for i in range(len(self.sent_length)) if self.sent_length[i][int(RaftNode.SENT_ADDR_IDX)] == follower_id][0]
        prefix_len = self.sent_length[idx][RaftNode.SENT_LEN_IDX]
        suffix = self.log[prefix_len:]

        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len - 1][1]
            
        request = {
            "leader_id": leader_id,
            "current_term": self.election_term,
            "prefix_len": prefix_len,
            "prefix_term": prefix_term,
            "commit_length": len(self.log),
            "suffix": suffix,
        }

        # Send log request to follower
        Thread(target=self.__send_request, args=[request, "log_request", follower_id]).start()

    async def __follower_heartbeat(self):
        # Listen for heartbeat
        self.heartbeat_timer = 0
        self.current_hearbeat_timeout = self.__heartbeat_timeout()
        while self.type == RaftNode.NodeType.FOLLOWER:
            self.heartbeat_timer += RaftNode.HEARTBEAT_INTERVAL
            # Timeout heartbeat, leader is down
            if self.heartbeat_timer >= self.current_hearbeat_timeout:
                self.__print_log("Heartbeat not received")
                # Become candidate
                self.__initialize_as_candidate()
                return
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
    
    def __heartbeat_timeout(self):
        # Generate random heartbeat timeout
        return random.uniform(RaftNode.HEARTBEAT_TIMEOUT_MIN, RaftNode.HEARTBEAT_TIMEOUT_MAX)

    def __try_to_apply_membership(self, contact_addr: Address):
        # Try to apply membership
        redirected_addr = contact_addr
        # Find leader
        response = self.__send_request(self.address, "apply_membership", redirected_addr)

        # Retry if failed to contact leader
        while response is None:
            self.__print_log("Failed to contact leader node")
            response = self.__send_request(self.address, "apply_membership", redirected_addr)
            time.sleep(1)

        # Retry if failed to apply membership
        while response["status"] != "success":
            redirected_addr = Address(response["address"]["ip"], response["address"]["port"])
            response        = self.__send_request(self.address, "apply_membership", redirected_addr)

        self.log                 = response["log"]
        self.cluster_addr_list   = list(map(lambda addr: Address(addr["ip"], addr["port"]), response["cluster_addr_list"]))
        self.cluster_leader_addr = redirected_addr

        # Send announce new member to all node
        for (i, addr) in enumerate(self.cluster_addr_list):
            self.sent_length.append([addr, response["sent_length"][i][RaftNode.SENT_LEN_IDX]])
            self.acked_length.append([addr, response["acked_length"][i][RaftNode.ACK_LEN_IDX]])
            if addr != self.address and addr != self.cluster_leader_addr:
                Thread(target=self.__send_request, args=[self.address, "announce_new_member", addr]).start()

        # become follower
        self.__initialize_as_follower()

    def __send_request(self, request: Any, rpc_name: str, addr: Address) -> "json":
        # send rpc request to addr
        try:
            node         = ServerProxy(f"http://{addr.ip}:{addr.port}")
            json_request = json.dumps(request)
            rpc_function = getattr(node, rpc_name)
            response     = json.loads(rpc_function(json_request))
            return response
        except Exception as e:
            self.__print_log(f"Error: {e}")
            self.__print_log(f"Failed to send request to {addr}")  
    # ---------------------------------------------------------------------------------------------

    # Inter-node RPCs
    # ---------------------------------------------------------------------------------------------
    def apply_membership(self, json_request: str) -> "json":
        # Applying membership request
        request = json.loads(json_request)
        # Accept request if node is leader
        if self.type == RaftNode.NodeType.LEADER:
            addr = Address(request["ip"], request["port"])
            self.__print_log("[Leader] Received request for membership")
            self.__print_log(f"[Leader] Adding node {addr} to cluster")
            if addr not in self.cluster_addr_list:
                self.cluster_addr_list.append(addr)
                self.sent_length.append([addr, 0])
                self.acked_length.append([addr, 0])
            
            response = {
                "status": "success",
                "log": self.log,
                "cluster_addr_list": self.cluster_addr_list,
                "sent_length": self.sent_length,
                "acked_length": self.acked_length
            }
        # Redirect request to leader if node is follower
        else:
            self.__print_log("[Follower] Received request for membership")
            self.__print_log(f"[Follower] Redirecting request to leader {self.cluster_leader_addr}")
            response = {
                "status": "redirected",
                "address": {
                    "ip":   self.cluster_leader_addr.ip,
                    "port": self.cluster_leader_addr.port,
                }
            }
        
        return json.dumps(response)
    
    def announce_new_member(self, json_request: str) -> "json":
        # New member announcement
        request = json.loads(json_request)
        addr = Address(request["ip"], request["port"])

        self.__print_log(f"Received announcement from {addr} as new member")

        # Add new member to cluster if not exist
        if addr not in self.cluster_addr_list:
            self.__print_log(f"Adding node {addr} to cluster")
            self.cluster_addr_list.append(addr)
            self.sent_length.append([addr, 0])
            self.acked_length.append([addr, 0])

        response = {
            "status": "success",
        }
        return json.dumps(response)

    def announce_as_leader(self, json_request: str) -> "json":
        # Leader announcement
        request = json.loads(json_request)
        leader_addr = Address(request["cluster_leader_addr"]["ip"], request["cluster_leader_addr"]["port"])
        self.__print_log(f"Received leader announcement from {leader_addr}")

        # Update node state
        self.cluster_leader_addr    = leader_addr
        self.cluster_addr_list      = list(map(lambda addr: Address(addr["ip"], addr["port"]), request["cluster_addr_list"]))
        self.log                    = request["log"]
        
        # Node become follower
        self.__initialize_as_follower()
        response = {
            "status": "success",
        }
        return json.dumps(response)

    def heartbeat(self, json_request: str) -> "json":
        # Heartbeat
        json_request = json.loads(json_request)
        response = {
            "heartbeat_response": "success",
            "address":            self.address,
        }
        
        # Follower node receive heartbeat from leader
        if self.type != RaftNode.NodeType.LEADER:
            self.__print_log("[Follower] Received heartbeat")
            self.__print_log(f'[Follower] log: {self.log}')
            self.__print_log(f'[Follower] queue: {self.app.queue}')
            self.heartbeat_timer = 0

        return json.dumps(response)

    def vote_request(self, json_request: str) -> "json":
        # Vote request
        request  = json.loads(json_request)

        req_addr = Address(request["candidate_addr"]["ip"], request["candidate_addr"]["port"])
        req_log_length = request["log_length"]
        req_log_term   = request["log_term"]

        response = {
            "address":       self.address,
            "election_term": self.election_term,
            "vote_response": "not granted",
        }

        # Update election term if request has higher election term
        if request["election_term"] > self.election_term:
            self.election_term  = request["election_term"]
            response["election_term"] = self.election_term
            self.__print_log("Election term updated")
            self.__initialize_as_follower()
            self.__cancel_election_timer()

        last_term = self.log[-1][1] if len(self.log) > 0 else 0

        log_ok = (req_log_term > last_term) or (req_log_term == last_term and req_log_length >= len(self.log))
        
        # Accept vote request if node has not voted for any candidate and log is up to date
        if request["election_term"] == self.election_term and log_ok and self.voted_for is None:
            self.voted_for = req_addr
            self.__print_log(f"Voted for {self.voted_for}")
            response["vote_response"] = "granted"
        else:
            self.__print_log(f"Refuse voted for {req_addr}")

        # Send vote response to candidate
        Thread(target=self.__send_request, args=[response, "vote_response", req_addr]).start()

        return json.dumps({"status": "ok"})
    
    def vote_response(self, json_request: str) -> "json":
        # Vote response 
        request         = json.loads(json_request)
        term            = request["election_term"]
        vote_response   = request["vote_response"]
        granted         = vote_response == "granted"

        # Update vote count if received granted vote response 
        if self.type ==  RaftNode.NodeType.CANDIDATE and term == self.election_term and granted:
            self.votes += 1

            # Become leader if received granted vote response from majority of nodes
            if self.votes >= len(self.cluster_addr_list) // 2 + 1:
                self.__initialize_as_leader()
                self.__print_log("Elected as leader")
                self.__cancel_election_timer()
                self.sent_length = []
                self.acked_length = []

                # Replicate log to other nodes
                for addr in self.cluster_addr_list:
                    self.sent_length.append([addr, len(self.log)])
                    self.acked_length.append([addr, 0])
                    if addr != self.address:
                        Thread(target=self.replicate_log, args=[self.address ,addr]).start()   
        
        # Update current election term if received vote response has higher election term
        elif term > self.election_term:
            self.election_term = term
            self.__print_log("Election term is not valid")
            self.__initialize_as_follower()
            self.__cancel_election_timer()

        return json.dumps({"status": "ok"})

    def log_request(self, json_request: str) -> "json":
        # Log request
        request = json.loads(json_request)
        leader_id = request["leader_id"]
        term = request["current_term"]
        prefix_len = request["prefix_len"]
        prefix_term = request["prefix_term"]
        leader_commit = request["commit_length"]
        suffix = request["suffix"]

        # Cancel election timer if current term is higher than node's election term
        if term > self.election_term:
            self.election_term = term
            self.vote_for = None
            self.__cancel_election_timer()
        
        # Node become follower if current term is equal node's election term
        if term == self.election_term:
            self.type = RaftNode.NodeType.FOLLOWER
            self.cluster_leader_addr = Address(leader_id["ip"], leader_id["port"])

        log_ok = (len(self.log) >= prefix_len) and (prefix_len == 0 or self.log[prefix_len - 1][1] == prefix_term)
        
        # Append log if log is up to date
        if term == self.election_term and log_ok:
            self.append_entries(prefix_len, leader_commit, suffix)
            ack = prefix_len + len(suffix)
            success = True
        else:
            ack = 0
            success = False
            
        response = {
            "node_id": self.address,
            "current_term": self.election_term,
            "ack": ack,
            "success": success,
        }

        # Send log response to leader
        Thread(target=self.__send_request, args=[response, "log_response", Address(leader_id['ip'], leader_id['port'])]).start()

        return json.dumps({'status': 'ok'})
    
    def append_entries(self, prefix_len, leader_commit, suffix):
        # Append Entries
        # Set log to match leader's log
        if len(suffix) > 0 and len(self.log) > prefix_len:
            index = min(len(self.log), prefix_len + len(suffix) - 1)
            if self.log[index][RaftNode.LOG_TERM_IDX] != suffix[index - prefix_len][RaftNode.LOG_TERM_IDX]:
                self.log = self.log[:prefix_len - 1]
            
        # Append new entries to log
        if prefix_len + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefix_len, len(suffix)):
                self.log.append(suffix[i])

        # Commit log if leader's commit length is higher than node's commit length
        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                command = self.log[i][RaftNode.LOG_MSG_IDX]

                request = {
                    'command': command[:7] if 'dequeue' in command else command[:5],
                    'params': command[9:-2] if 'dequeue' in command else command[7:-2]
                }

                # Execute command
                self.commit(json.dumps(request))

            # Update commit length
            self.commit_length = leader_commit

    def log_response(self, json_request: str) -> "json":
        # Log response
        request = json.loads(json_request)
        follower = request["node_id"]
        follower = Address(follower['ip'], follower['port'])
        term = request["current_term"]
        ack = request["ack"]
        success = request["success"]
        
        # Leader Node
        if term == self.election_term and self.type == RaftNode.NodeType.LEADER:
            followerIdx = [i for i in range(len(self.acked_length)) if self.acked_length[i][RaftNode.ACK_ADDR_IDX] == follower][0]

            # Update acked length if received success response
            if success and ack >= self.acked_length[followerIdx][RaftNode.ACK_LEN_IDX]:
                self.sent_length[followerIdx][RaftNode.SENT_LEN_IDX] = ack
                self.acked_length[followerIdx][RaftNode.ACK_LEN_IDX] = ack
                # print('before commit log entries')
                self.commit_log_entries()

            # Replicate log if received fail response
            elif self.sent_length[followerIdx][RaftNode.SENT_LEN_IDX] > 0:
                self.sent_length[followerIdx][RaftNode.SENT_LEN_IDX] -= 1
                # print('before replicate log')
                self.replicate_log(self.address, follower)

        # Cancel election timer if current term is higher than node's election term
        elif term > self.election_term:
            self.election_term = term
            self.__initialize_as_follower()
            self.__cancel_election_timer

        return json.dumps({'status': 'ok'})

    def commit(self, json_request: str) -> "json":
        # Commit log
        request = json.loads(json_request)

        command = request["command"]
        params = request["params"]

        status = "success"

        # Commit to application
        if command == 'queue':
            self.app.push(params)
            result = "Success enqueue element"
        elif command == 'dequeue':
            popped_elmt = self.app.pop()
            result = f'Success dequeue element {popped_elmt}'
        else:
            status = "error"
            result = "Invalid command"

        response = {
            "status": status,
            "result": result,
        }

        return json.dumps(response)
    
    def broadcast_msg(self, json_request: str) -> "json":
        # Broadcasting message
        request = json.loads(json_request)
        request_addr    = Address(request["request_addr"]["ip"], request["request_addr"]["port"])
        node_id         = request["node_id"]
        msg             = request["command"]

        node_addr = Address(node_id["ip"], node_id["port"])

        response = {
            "status": "ok",
        }

        # Leader Node
        if self.type == RaftNode.NodeType.LEADER:
            self.__print_log(f"[Leader] Received request broadcast msg from {request_addr}")
            self.__print_log(f"[Leader] Broadcasting request to all nodes")

            # Append the record (msg, term) to the log
            self.log.append((msg, self.election_term))
            idx = [i for i in range(len(self.sent_length)) if self.sent_length[i][RaftNode.SENT_ADDR_IDX] == node_addr][0]
            self.acked_length[idx][RaftNode.SENT_LEN_IDX] = len(self.log)

            # Replicate log to all followers
            for follower in self.cluster_addr_list:
                if follower != self.address:
                    self.replicate_log(self.address, follower)
            
            if 'request_log' in msg:
                log = []
                for entry in self.log:
                    log.append(entry[0])
                response["log"] = log

        # Forward the request to the leader via a fifo link if the node is not the leader
        else:
            self.__print_log(f"forwarding request to leader {self.cluster_leader_addr}")
            response = self.forward_request(json_request)
        
        return json.dumps(response)
            
    def forward_request(self, json_request: str):
        # Forwarding request
        request = json.loads(json_request)
        node_id = request["node_id"]
        request_addr = request["request_addr"]
        msg     = request["command"]

        # Forward the request to the leader if node leader is exist
        if self.cluster_leader_addr is not None:
            message = {
                'type': 'ForwardRequest',
                'leader_id': self.cluster_leader_addr,
                'command': msg,
                'node_id': node_id,
                'request_addr': request_addr
            }
            
            # Send message to leader
            response = self.send_message(message, self.cluster_leader_addr)
            return response
        
        return json.dumps({"status": "failed", "message": "No leader found"})
            
    def send_message(self, message: Any, recipient: Address):
        # Send message to recipient
        # Broadcast message to recipient
        response = self.__send_request(message, "broadcast_msg", recipient)

        return response
    
    def __acks_(self, length):
        # Return number of acks for a given length
        return len([i for i in range(len(self.acked_length)) if self.acked_length[i][RaftNode.ACK_LEN_IDX] >= length])
    
    def commit_log_entries(self):
        # Commit log entries
        min_acks = ceil((len(self.cluster_addr_list) + 1) / 2)

        # Find the largest index in the log such that a majority of nodes have acked it
        ready = [i for i in range(len(self.log)) if self.__acks_(i) >= min_acks]
        
        # Commit all log entries up through the one that has been replicated on a majority of nodes
        if ready != [] and max(ready) + 1 > self.commit_length and self.log[max(ready)][1] == self.election_term:
            for i in range(self.commit_length, max(ready) + 1):
                command = self.log[i][RaftNode.LOG_MSG_IDX]
  
                request = {
                    'command': command[:7] if 'dequeue' in command else command[:5],
                    'params': command[9:-2] if 'dequeue' in command else command[7:-2]
                }
            
                # Commit to application
                self.commit(json.dumps(request))
                
            self.commit_length = max(ready) + 1
    # ---------------------------------------------------------------------------------------------             
        
    # Client RPCs
    # ---------------------------------------------------------------------------------------------
    def execute(self, json_request: str) -> "json":
        # Rpc for client to execute command
        response = self.broadcast_msg(json_request)
        response = json.loads(response)
        
        return json.dumps(response)
    # ---------------------------------------------------------------------------------------------