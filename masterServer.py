import zmq
import os
import json
import threading

class MasterServer:
  def __init__(self, newServer = False):
      self.nodes = {} # Dictionary to store server IDs and their IP addresses

      if newServer:
          self.initiateServer()
      
  # Initiate Server
  def initiateServer(self):
    logsFolderMasterPath = "logs_node_master"
    try:
      os.makedirs(logsFolderMasterPath)  # Create the folder

      # Write initial metadata to metadata.txt
      initial_metadata = {
        "nodes" : self.nodes
      }
      with open(os.path.join(logsFolderMasterPath, "metadata.txt"), "w") as metadata_file:
        json.dump(initial_metadata, metadata_file)

      print("Logs files created successfully.")
    except OSError as e:
      print(f"Error creating log files: {e}")

  # Update Meta Data(Server Persistent State)
  def updateMetaData(self):
    logsFolderMasterPath = "logs_node_master"
    metadataPath = os.path.join(logsFolderMasterPath, "metadata.txt")

    # If the file exists, read and potentially update state
    with open(metadataPath, "r+") as metadataFile:
      try:
        # Write the updated state to the file
        metadataFile.seek(0)  # Move to the beginning of the file
        metadataFile.truncate()  # Clear existing content
        metadata = {
          "nodes": self.nodes
        }
        json.dump(metadata, metadataFile)
      except ValueError:  # Handle potential parsing errors if reading data
        print("Error parsing existing metadata. Resetting state.")

  def addNode(self, server_id, ip_address):
      self.nodes[server_id] = ip_address
      self.updateMetaData()

  def getNodes(self):
      return self.nodes
  
  # Server recovery: Reading persistent state data
  def serverRecovery(self, path):
      metadataPath = os.path.join(path, "metadata.txt")

      # Open the file in read mode
      with open(metadataPath, "r") as metadataFile:
          try:
              # Read the data as JSON (replace with your format)
              metadata = json.load(metadataFile)

              # Update server variables with retrieved values
              self.nodes = metadata["nodes"]
              print("Server recovered successfully. State loaded from metadata.txt.")

          except (json.JSONDecodeError, KeyError) as e:
              print(f"Error parsing metadata.txt: {e}. Resetting state.")

logsFolderMasterPath = "logs_node_master"
masterServer = None

def distributeClusterInfo(context, clusterNodes):
  cluster_server_distribution_socket = context.socket(zmq.DEALER)
  failedConnectionIp = []
  
  # Connect to all servers
  for serverId in clusterNodes:
      serverIp = clusterNodes[serverId]
      try:
        cluster_server_distribution_socket.connect(f"tcp://{serverIp}:{9001}")
      except zmq.error.ZMQError as e:
          print(f"Error connecting to {serverIp}: {e}")
          failedConnectionIp.append(serverIp)

  for serverId in clusterNodes:
    serverIp = clusterNodes[serverId]
    if serverIp not in failedConnectionIp:
      cluster_server_distribution_socket.send_json(clusterNodes)
      print(f"Cluster Information sent to serverId: {serverId} serverIp: {serverIp}")



  

if __name__ == "__main__":

  if os.path.exists(logsFolderMasterPath):
    print("Master Server Recovery Initiated!")
    masterServer = MasterServer()
    masterServer.serverRecovery(logsFolderMasterPath)
  else:
    print("Server initializing for the first time.")
    masterServer = MasterServer(True)



  # Create ZeroMQ context and socket
  context = zmq.Context()

  # Socket for Cluster Server registration requests
  server_registration_socket = context.socket(zmq.ROUTER)
  server_registration_socket.bind("tcp://10.190.0.2:9001")



  while True:
    # Receives identity and server registration message
    identity, message = server_registration_socket.recv_multipart()
    serverData = json.loads(message)

    serverId = serverData["serverId"]
    serverIp = serverData["serverIp"]


    print(f"Cluster Server registration request From {serverId}")
    clusterNodes = masterServer.getNodes()

    if serverId in clusterNodes:
      print("Registration Status: Already Registered!")
      response = {"success": False, "message": "Cluster Server already registered"}
    else:
      masterServer.addNode(serverId, serverIp)
      print("Registration Status: Registeration Sucessful!")
      response = {"success": True, "message": "Cluster Server registered successfully"}

    server_registration_socket.send_multipart([identity, json.dumps(response).encode("utf-8")])
    distributeClusterInfo(context, masterServer.getNodes())


      