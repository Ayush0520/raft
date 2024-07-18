import os
import zmq
import json
import math
import time
import random
import threading

class Server:
   
  def __init__(self, currentTerm = 0, votedFor = None, commitLength = 0, serverId = None, serverIp = None, privateServerIp = None, context = None):
    self.currentTerm = currentTerm
    self.votedFor = votedFor
    self.commitLength = commitLength
    self.serverId = serverId
    self.serverIp = serverIp
    self.privateServerIp = privateServerIp
    self.currentRole = "follower"
    self.currentLeader = None
    self.votesReceived = []
    self.sentLength = {}
    self.ackedLength = {}
    self.electionTimeout = random.randint(5, 10)
    self.clusterNodes = {}
    self.reset = time.time()
    self.heartbeatTimeout = 1
    self.leaseTimeout = 6
    self.leaseTimer = time.time()
    self.leaseAvailable = time.time()
    self.context = context

    # Creating thread for Election.
    electionThread = threading.Thread(target=self.checkTimeOut)
    electionThread.start()

    # Creating Thread for Vote Requests
    receiveVoteThread = threading.Thread(target=self.receiveVoteRequest) #9002
    receiveVoteThread.start()

    # Creating Thread for Vote Responses
    voteResponseThread = threading.Thread(target=self.receiveVoteResponse) #9003
    voteResponseThread.start()

    # Creating Thread for Heartbeat
    receiveHeartbeatThread = threading.Thread(target=self.receiveHeartbeat) #9004
    receiveHeartbeatThread.start()

    # Creating Thread for Acknowledgement
    ackThread = threading.Thread(target=self.receiveAck) #9005
    ackThread.start()

    # Creating Thread for Client Communication
    clientThread = threading.Thread(target=self.receiveClient) #9006
    clientThread.start()


  def getLatestValueFromLogs(self, key):
    logs_file = "logs_node/logs.txt"
    latest_value = ""
    try:
        # Open the log file in read mode
        with open(logs_file, "r") as logFile:
            # Read and store all log entries
            logEntries = [json.loads(line.strip()) for line in logFile]

        # Iterate through log entries in reverse order
        for entry in reversed(logEntries):
            if key in entry["msg"]:
                latest_value = entry["msg"][key]
                break
    except FileNotFoundError:
        print("Logs file not found.")
    except json.JSONDecodeError:
        print("Error decoding JSON from logs file.")

    return latest_value
  
  # Write dump.txt
  def writeDump(self, string):
    file_path = "logs_node/dump.txt"
    with open(file_path, 'a') as file:
        file.write(string + '\n')



  # Receive Client Request
  def receiveClient(self):
    clientSocket = self.context.socket(zmq.ROUTER)
    clientSocket.bind(f"tcp://{self.privateServerIp}:9006")

    forwardSocket = self.context.socket(zmq.PUSH)
    print("Received Client Request!")

    while True:
      try:
        identity, message = clientSocket.recv_multipart()
        message = json.loads(message)
        print(message)
        messageType = message["type"]
        

        if messageType == "get" and (self.currentRole == "leader" or self.leaseTimer >= time.time()):
          key = message["key"]
          latestValue = self.getLatestValueFromLogs(key)
          clientSocket.send_multipart([identity, json.dumps({"value": latestValue}).encode("utf-8")])
        elif messageType == "set" and self.currentRole == "leader":
          dumpText = f"Node {self.serverId} (leader) received an {message} request."
          self.writeDump(dumpText)
          if time.time() < self.leaseAvailable:
            clientSocket.send_multipart([identity, json.dumps({"status": "Failure: Previous Leader Lease hasn't expired!"}).encode("utf-8")])
          else:
            message = {"msg": message["msg"], "term": self.currentTerm}
            self.appendToLog([message])
            self.ackedLength[self.serverId] = self.getLogLength()
            for serverId in self.clusterNodes:
              if serverId == self.serverId:
                continue
              self.replicateLog(self.serverId, serverId)
            clientSocket.send_multipart([identity, json.dumps({"status": "Success"}).encode("utf-8")])
        else:
          currentLeader = self.currentLeader
          try:
            print(f"Forwarding to new Leader: {self.currentLeader}")
            forwardSocket.connect(f"tcp://{self.clusterNodes[currentLeader]}:{9006}")
            forwardSocket.send_json(message)
            clientSocket.send_multipart([identity, json.dumps({"status": "Forwarded to new Leader"}).encode("utf-8")])
            # latestValue = self.getLatestValueFromLogs(key)
            # clientSocket.send_multipart([identity, json.dumps({"value": latestValue}).encode("utf-8")])
          except(zmq.ZMQError) as e:
            print(f"Error connecting to {self.clusterNodes[currentLeader]}: {e}")
      except(zmq.ZMQError) as e:
        print(f"Error receiving messages: {e}")

  def checkTimeOut(self):
    while True:
      self.updateMetaData()
      # if self.currentRole == "leader" and time.time() >= self.leaseAvailable: 
      #   self.leaseAcquired = True

      if self.currentRole == "leader" and time.time() - self.reset >= self.heartbeatTimeout:
        self.sendHeartbeat()
      elif self.currentRole != "leader" and time.time() - self.reset >= self.electionTimeout:
        print("Starting Election")
        dumpText = f"Node {self.serverId} election timer timed out, Starting election."
        self.writeDump(dumpText)
        self.startElection()

  # Send Heartbeat
  def sendHeartbeat(self):
    dumpText = f"Leader {self.serverId} sending heartbeat"
    self.writeDump(dumpText)
    print("Sending Hearbeats")

    clusterNodes = self.clusterNodes

    for serverId in clusterNodes:
      if serverId == self.serverId:
        continue
      self.replicateLog(self.serverId, serverId)
    self.resetTimer()

  # Acks Length
  def getAcksLength(self, length):
    len = 0
    clusterNodes = self.clusterNodes
    for serverId in clusterNodes:
      if serverId == self.serverId:
        continue
      elif self.ackedLength[serverId] >= length:
        len += 1
    return len

  # Commit Entries
  def commitEntry(self):
    minAcks = math.ceil(len(self.clusterNodes)/2)
    ready = [len for len in range(1, self.getLogLength()+1) if self.getAcksLength(len) >= minAcks]

    if (len(ready) != 0) and (max(ready) > self.commitLength) and (self.getTerm(max(ready)-1) == self.currentTerm):
      # for i in range(self.commitLength, max(ready)):
      #   # Deliver log[i].msg to the application
      #   pass
      self.commitLength = max(ready)

  # Receive Acknowledgement
  def receiveAck(self):
    recieveAckSocket = self.context.socket(zmq.PULL)
    recieveAckSocket.bind(f"tcp://{self.privateServerIp}:9005")

    while True:
      try:
        message = recieveAckSocket.recv_json()
        print("Received Acknowledgement")
        followerId = message["nodeId"]
        term = message["currentTerm"]
        ack = message["ack"]
        success = message["status"]

        if (term == self.currentTerm) and (self.currentRole == "leader"):
          if (success == True) and (ack >= self.ackedLength[followerId]):
            self.sentLength[followerId] = ack
            self.ackedLength[followerId] = ack
            self.commitEntry()
            dumpText = f"Node {self.serverId} (leader) committed the entry to the state machine."
            self.writeDump(dumpText)
          elif self.sentLength[followerId] > 0:
            self.sentLength[followerId] = self.sentLength[followerId]-1
            self.replicateLog(self.serverId, followerId)
        elif term > self.currentTerm:
          if self.currentRole == "leader":
            dumpText = f"{self.serverId} Stepping down"
            self.writeDump(dumpText)
          self.currentTerm = term
          self.currentRole = "follower"
          self.votedFor = None
          self.resetTimer()
      except(zmq.ZMQError) as e:
        print(f"Error receiving messages: {e}")


  # Receive Hearbeat
  def receiveHeartbeat(self):
    receiveHeartbeatSocket = self.context.socket(zmq.PULL)
    receiveHeartbeatSocket.bind(f"tcp://{self.privateServerIp}:9004")

    sendAckSocket = self.context.socket(zmq.PUSH)

    while True:
      try:
        message = receiveHeartbeatSocket.recv_json()
        print("Received Heartbeat")
        leaderId = message["leaderId"]
        term = message["currentTerm"]
        prefixLen = message["prefixLen"]
        prefixTerm = message["prefixTerm"]
        leaderCommit = message["commitLength"]
        suffix = message["suffix"]

        if term > self.currentTerm:
          self.currentTerm = term
          self.votedFor = None
          self.resetTimer()
          

        if term == self.currentTerm:
          self.currentRole = "follower"
          self.currentLeader = leaderId
          self.resetTimer()
          self.leaseAvailable = time.time() + self.leaseTimeout

        logOk = (self.getLogLength() >= prefixLen) and (prefixLen == 0 or (self.getTerm(prefixLen-1) == prefixTerm))

        if (term == self.currentTerm) and logOk:
          dumpText = f"Node {self.serverId} (follower) committed the entry {suffix} to the state machine."
          self.writeDump(dumpText)
          self.appendEntries(prefixLen, leaderCommit, suffix)
          ack = prefixLen + len(suffix)
          response = {"type": "Log Response", "nodeId": self.serverId, "currentTerm": self.currentTerm, "ack": ack, "status":True}
          dumpText = f"Node {self.serverId} accepted AppendEntries RPC from {self.currentLeader}."
          self.writeDump(dumpText)
        else:
          response = {"type": "Log Response", "nodeId": self.serverId, "currentTerm": self.currentTerm, "ack": 0, "status":False}
          dumpText = f"Node {self.serverId} rejected AppendEntries RPC from {self.currentLeader}."
          self.writeDump(dumpText)

        try:
          sendAckSocket.connect(f"tcp://{self.clusterNodes[leaderId]}:{9005}")
          sendAckSocket.send_json(response)
        except(zmq.ZMQError) as e:
          print(f"Error connecting to {self.clusterNodes[leaderId]}: {e}")

      except(zmq.ZMQError) as e:
        print(f"Error receiving messages: {e}")

  # Truncates Log
  def truncateLog(self, prefixLen):
    # Open the log file in read mode
    with open("logs_node/logs.txt", "r") as logFile:
      # Read and store all log entries
      logEntries = [json.loads(line.strip()) for line in logFile]

      # Check if truncation is necessary
      if prefixLen < len(logEntries):
        # Truncate the list to keep entries up to prefixLen-1 (inclusive)
        logEntries = logEntries[:prefixLen]

    # Open the log file again in write mode (overwrites existing content)
    with open("logs_node/logs.txt", "w") as logFile:
      # Convert entries back to JSON strings with newlines
      truncatedEntries = [json.dumps(entry) + "\n" for entry in logEntries]
      # Write the truncated entries to the file
      logFile.writelines(truncatedEntries)

  def appendToLog(self, suffix):
    # Open the log file in append mode
    with open("logs_node/logs.txt", "a") as logFile:
      # Convert entries to JSON strings with newlines
      entriesToAppend = [json.dumps(entry) + "\n" for entry in suffix]
      # Write the entries to the log file
      logFile.writelines(entriesToAppend)

 

  # Append Entries
  def appendEntries(self, prefixLen, leaderCommit, suffix):
    if (len(suffix) > 0) and (self.getLogLength() > prefixLen):
      index = min(self.getLogLength(), prefixLen + len(suffix))-1
      if self.getTerm(index) != suffix[index-prefixLen]["term"]:
        self.truncateLog(prefixLen)

    if prefixLen + len(suffix) > self.getLogLength():
      self.appendToLog(suffix[self.getLogLength()-prefixLen:])

    if leaderCommit > self.commitLength:
      # for i in range(self.commitLength, leaderCommit):
      #   # Deliver msg to application
      #   pass
      self.commitLength = leaderCommit

  # Replicate Log
  def replicateLog(self, leaderId, followerId):
    sendLogSocket = self.context.socket(zmq.PUSH)
    followerIp = self.clusterNodes[followerId]
    sendLogSocket.connect(f"tcp://{followerIp}:{9004}")

    prefixLen = self.sentLength[followerId]
    suffix = self.getSuffix(prefixLen)
    print("In Replicate Log")
    print(f"Suffix: {suffix}")
    print(type(suffix))

    prefixTerm = 0
    if prefixLen > 0:
      prefixTerm = self.getTerm(prefixLen-1)

    message = {"type": "Log Request", "leaderId": leaderId, "currentTerm": self.currentTerm, "prefixLen": prefixLen, 
               "prefixTerm": prefixTerm, "commitLength": self.commitLength, "suffix": suffix}
    sendLogSocket.send_json(message)
    sendLogSocket.close()


  # Get suffix data
  def getSuffix(self, prefixLen):
    # Open the log file in read mode
    with open("logs_node/logs.txt", "r") as logFile:
      # Load log entries as a list of dictionaries
      logEntries = [json.loads(line.strip()) for line in logFile]
      # Check if there are any entries to include in the suffix
      if len(logEntries) > prefixLen:
        # Slice the list to extract entries from prefixLen to the end (exclusive)
        suffix = logEntries[prefixLen:]
      else:
        # If prefixLen is equal to or greater than the number of entries, return an empty list
        suffix = []
    return suffix
  
  # Get Log-length
  def getLogLength(self):
    # Open the log file in read mode
    with open("logs_node/logs.txt", "r") as logFile:
      # Load log entries as a list of dictionaries
      logEntries = [json.loads(line.strip()) for line in logFile]
      return len(logEntries)



  # To Start Election.
  def startElection(self):
    self.currentRole = "candidate"
    self.currentTerm += 1
    self.votedFor = self.serverId
    self.votesReceived.append(self.serverId)
    lastTerm = 0
    if self.getLogLength() > 0:
      lastTerm = self.getTerm(-1)

    self.sendVoteRequest(lastTerm)
    self.resetTimer()

  # Send Vote requests to Cluster Servers.
  def sendVoteRequest(self, lastTerm):
    print("Sending Vote Request")
    sendVotesSocket = self.context.socket(zmq.PUSH)
    clusterNodes = self.clusterNodes
    failedConnectionIp = []
    failedConnectionIp.append(clusterNodes[self.serverId])

    for serverId in clusterNodes:
      if serverId == self.serverId:
        continue
      serverIp = clusterNodes[serverId]
      try:
        sendVotesSocket.connect(f"tcp://{serverIp}:{9002}")
      except zmq.error.ZMQError as e:
        print(f"Error connecting to {serverIp}: {e}")
        failedConnectionIp.append(serverIp)

    for serverId in clusterNodes:
      serverIp = clusterNodes[serverId]
      if serverIp not in failedConnectionIp:
        message = {"type": "Voting Request", "nodeId": self.serverId, "currentTerm": self.currentTerm, 
                   "logLength": self.getLogLength(), "lastTerm": lastTerm}
        sendVotesSocket.send_json(message)
        print(f"Vote request sent to serverId: {serverId} serverIp: {serverIp}")
      else:
        dumpText = f"Error occurred while sending RPC to Node {self.serverId}."
        self.writeDump(dumpText)
    sendVotesSocket.close()

  # Receive Vote Requests.
  def receiveVoteRequest(self):
    
    receiveVoteSocket = self.context.socket(zmq.PULL)
    receiveVoteSocket.bind(f"tcp://{self.privateServerIp}:9002")

    sendResponseSocket = self.context.socket(zmq.PUSH)

    while True:
      try:
        message = receiveVoteSocket.recv_json()
        

        candidateServerId = message["nodeId"]
        candidateCurrentTerm = message["currentTerm"]
        candidateLogLength = message["logLength"]
        candidateLastTerm = message["lastTerm"]

        print(f"Received Vote Request: {candidateServerId}")

        serverIp = self.clusterNodes[candidateServerId]

        if candidateCurrentTerm > self.currentTerm:
          if self.currentRole == "leader":
            dumpText = f"{self.serverId} Stepping down"
            self.writeDump(dumpText)
          self.currentTerm = candidateCurrentTerm
          self.currentRole = "follower"
          self.votedFor = None
          

        lastTerm = 0
        if self.getLogLength() > 0:
          lastTerm = self.getTerm(-1)

        logOk = (candidateLastTerm > lastTerm) or ((candidateLastTerm == lastTerm) and (candidateLogLength >= self.getLogLength()))
        
        if (candidateCurrentTerm == self.currentTerm) and logOk and self.votedFor in {candidateServerId, None}:
          self.votedFor = candidateServerId
          response = {"type":"Voting Response", "nodeId": self.serverId, "currentTerm": self.currentTerm, "status": True}
          dumpText = f"Vote granted for Node {candidateServerId} in term {candidateCurrentTerm}."
          self.writeDump(dumpText)
        else:
          response = {"type":"Voting Response", "nodeId": self.serverId, "currentTerm": self.currentTerm, "status": False}
          dumpText = f"Vote denied for Node {candidateServerId} in term {candidateCurrentTerm}."
          self.writeDump(dumpText)
        try:
          sendResponseSocket.connect(f"tcp://{serverIp}:{9003}")
          sendResponseSocket.send_json(response)
        except zmq.error.ZMQError as e:
          print(f"Error connecting to {serverIp}: {e}")

      except(zmq.ZMQError) as e:
        print(f"Error receiving messages: {e}")

  # Receive Vote Responses
  def receiveVoteResponse(self):
    
    receiveResponseSocket = self.context.socket(zmq.PULL)
    receiveResponseSocket.bind(f"tcp://{self.privateServerIp}:9003")

    while True:
      try:
        message = receiveResponseSocket.recv_json()
        

        term = message["currentTerm"]
        status = message["status"]
        voterId = message["nodeId"]

        print(f"Received Vote Response: {voterId}, Status: {status}")

        if self.currentRole == "candidate" and term == self.currentTerm and status:
          self.votesReceived.append(voterId)
          if len(self.votesReceived) >= math.ceil(len(self.clusterNodes)/2):
            print("Leader in Making!")
            dumpText = f"Node {self.serverId} became the leader for term {self.currentTerm}."
            self.writeDump(dumpText)
            self.currentRole = "leader"
            self.currentLeader = self.serverId
            self.resetTimer()
            if time.time() >= self.leaseAvailable:
              self.leaseTimer = time.time() + self.leaseTimeout
              dumpText = f"Leader {self.serverId} renewing lease"
              self.writeDump(dumpText)
            else: 
              dumpText = "New Leader waiting for Old Leader Lease to timeout"
              self.writeDump(dumpText)
            for serverId in self.clusterNodes:
              if serverId == self.serverId:
                continue
              self.sentLength[serverId] = self.getLogLength()
              self.ackedLength[serverId] = 0
              self.replicateLog(self.serverId, serverId)
        elif term > self.currentTerm:
          self.currentTerm = term
          self.currentRole = "follower"
          self.votedFor = None
          self.resetTimer()
          dumpText = f"Leader {self.serverId} lease renewal failed. Stepping Down"
          self.writeDump(dumpText)

      except(zmq.ZMQError) as e:
        print(f"Error receiving messages: {e}")

  def resetTimer(self):
    self.reset = time.time()

  def getTerm(self, index):
    # Define the path to the log file
    logFilePath = os.path.join("logs_node", "logs.txt")

    # Check if the file exists and has a length greater than 0
    if os.path.exists(logFilePath) and os.path.getsize(logFilePath) > 0:
      with open(logFilePath, "r") as logFile:
          # Load the log entries as a list of dictionaries
          logEntries = [json.loads(line.strip()) for line in logFile]

          # Get the term property of the last log entry
          lastTerm = logEntries[index]["term"]
          return lastTerm
    return 0
  
	# Update Meta Data(Server Persistent State)
  def updateMetaData(self):
    logsFolderPath = "logs_node"
    metadataPath = os.path.join(logsFolderPath, "metadata.txt")

    # If the file exists, read and potentially update state
    with open(metadataPath, "r+") as metadataFile:
      try:
        # Write the updated state to the file
        metadataFile.seek(0)  # Move to the beginning of the file
        metadataFile.truncate()  # Clear existing content
        metadata = {
          "currentTerm": self.currentTerm,
          "votedFor": self.votedFor,
          "commitLength": self.commitLength,
          "serverId": self.serverId,
          "serverIp": self.serverIp,
          "privateServerIp": self.privateServerIp
        }
        json.dump(metadata, metadataFile)
      except ValueError:  # Handle potential parsing errors if reading data
        print("Error parsing existing metadata. Resetting state.")


    
#################################################################################################################################
logsFolderPath = "logs_node"
server = None

# zmq Context
context = zmq.Context()

# Master Server Ip Address and Port No.
MasterServerAddress = "34.131.171.95"
MasterServerPort = 9001

def serverStartup():
  serverInfo = []
  # 1. Request Unique ID
  id = input("Enter a unique 5-digit ID for the server (e.g., 12345): ")
  serverInfo.append(id)
  
  pubIp = input("Enter Public Server Ip Address: ")
  serverInfo.append(pubIp)

  priIp = input("Enter Private Server Ip Address: ")
  serverInfo.append(priIp)
  return serverInfo

def initiateServer(serverId, serverIp, privateServerIp):
  try:
    os.makedirs(logsFolderPath)  # Create the folder

    # Create the text files with initial content
    with open(os.path.join(logsFolderPath, "logs.txt"), "w") as logs_file:
      pass  # Create empty logs.txt
    with open(os.path.join(logsFolderPath, "dump.txt"), "w") as dump_file:
      pass  # Create empty dump.txt

    # Write initial metadata to metadata.txt
    initial_metadata = {
      "currentTerm": 0,
      "votedFor": None,
      "commitLength": 0,
      "serverId": serverId,
      "serverIp": serverIp,
      "privateServerIp": privateServerIp,
    }
    with open(os.path.join(logsFolderPath, "metadata.txt"), "w") as metadata_file:
      json.dump(initial_metadata, metadata_file)

    print("Logs folder and files created successfully.")
  except OSError as e:
    print(f"Error creating logs folder or files: {e}")
  pass

def handleClusterInformation(context, serverIp):
  # Socket for Cluster Server Information
  clusterInfoSocket = context.socket(zmq.ROUTER)
  clusterInfoSocket.bind(f"tcp://{serverIp}:9001")

  while True:
    try:
      # Receive message from the dealer socket
      identity, message = clusterInfoSocket.recv_multipart()
      message = message.decode('utf-8')
      message = json.loads(message)

      # Print the received message
      print(f"Received message: {message}")
      server.clusterNodes = message

      # for serverId in server.clusterNodes:
      #   if serverId not in server.sentLength:
      #     server.sentLength[serverId] = 0
      #   if serverId not in server.ackedLength:
      #     server.ackedLength[serverId] = 0


    except (zmq.ZMQError) as e:
      print(f"Error receiving cluster information: {e}")

def sendRegistrationRequest(context, serverId, serverIp):
  print("Sending Registration Request")
  socket = context.socket(zmq.DEALER)
  socket.connect(f"tcp://{MasterServerAddress}:{MasterServerPort}")


  message = {"type": "REGISTER_SERVER", "serverId": serverId, "serverIp": serverIp}
  socket.send_json(message)

  response = socket.recv_json()
  print(f"Success: {response['success']}, Message: {response['message']}")

  socket.close()

def serverRecovery():  
  metadata_path = os.path.join(logsFolderPath, "metadata.txt")

  # Open the file in read mode
  with open(metadata_path, "r") as metadata_file:
    try:
      # Read the data as JSON
      metadata = json.load(metadata_file)

      # Update server variables with retrieved values
      currentTerm = metadata['currentTerm']
      votedFor = metadata['votedFor']
      commitLength = metadata['commitLength']
      serverId = metadata['serverId']
      serverIp = metadata['serverIp']
      privateServerIp = metadata['privateServerIp']
      print("Server recovered successfully. State loaded from metadata.txt.")

      return Server(currentTerm, votedFor, commitLength, serverId, serverIp, privateServerIp, context)

    except (json.JSONDecodeError, KeyError) as e:
      print(f"Error parsing metadata.txt: {e}. Resetting state.")



if __name__ == "__main__":
  if os.path.exists(logsFolderPath):
    print("Server recovery initiated.")
    server = serverRecovery()
  else:
    print("Server initializing for the first time.")
    serverId, serverIp, privateServerIp = serverStartup()
    initiateServer(serverId, serverIp, privateServerIp)
    server = Server(0, None, 0, serverId, serverIp, privateServerIp, context)

  sendRegistrationRequest(context, server.serverId, server.serverIp)

  # Creating thread for handling cluster information.
  clusterInformationThread = threading.Thread(target = handleClusterInformation, args=(context, server.privateServerIp))
  clusterInformationThread.start()
  
  while True:
    pass
