'''

@author: santhu
'''
import socket,time
from threading import Thread
import logging, json
import threading, Queue

semaphore = threading.Semaphore()

logging.basicConfig(level = logging.INFO)
# logging.basicConfig(filename='lamport.log', filemode='w', level=logging.INFO)

with open("config.json", "r") as configFile:
    config = json.load(configFile)

BUFFER_SIZE = config["buffer_size"]
DELIMITER = config["message_delimiter"]
MSG_TERMINATOR = config["message_terminator"]
DELAY = config["delay"]
REQ = "REQUEST"
REP = "REPLY"
REL = "RELEASE"


thread_pool = []  
lock = threading.Lock()

class ClientThread(Thread):
    dict = {}
    
    def __init__(self, socket, ip, port):
        Thread.__init__(self)
        self.socket = socket
        self.process_id = 0
        self.ip = ip
        self.port = port
        self.send_msg_queue = Queue.Queue()
        
    def run(self):
        logging.info("Connected to " + self.ip + ":" + str(self.port))
        
        data = ""
        while True:
            data = data + self.socket.recv(BUFFER_SIZE)
            if data:
                while MSG_TERMINATOR in data:
                    pos = data.find(MSG_TERMINATOR)
                    msg = data[:pos]
                    data = data[pos+1:]
                    print msg + " received from process " + str(self.process_id)
                    if "PID" in msg:
                        self.process_id = msg.split(":")[1]
                    if REQ in msg or REL in msg:
                        self.broadcast_message(msg)
                    if REP in msg:
                        with lock:
                            time.sleep(DELAY)
                            self.forward_reply_message(msg)
            
    # broadcast REQUEST/RELEASE message to all processes
    def broadcast_message(self, message):
        for thread in thread_pool:
            if int(self.process_id) != int(thread.process_id):
                print str(thread.process_id) + " was sent broadcast msg"
                semaphore.acquire()
                thread.socket.send(message + MSG_TERMINATOR)   
                semaphore.release()
    
    def forward_reply_message(self, message):
        pid = int(message.split(DELIMITER)[1])
        print "Send  msg: " + message + "to " + str(pid)
        for thread in thread_pool:
            print thread.process_id + " " + str(pid)
            if int(thread.process_id) == pid:
                semaphore.acquire()
                time.sleep(DELAY)
                thread.socket.send(message + MSG_TERMINATOR)
                time.sleep(DELAY)
                semaphore.release()
                         

#########################################################################
            
if __name__ == "__main__": 
 
    host_IP = config["IP"]
    host_port = config["port"]
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host_IP, host_port))
      
    while True:
        server_socket.listen(5)
        logging.info("Server started. Waiting for connections.")
        (connection_socket, (ip, port)) = server_socket.accept()
        thread = ClientThread(connection_socket, ip, port)
        thread.start()
        thread_pool.append(thread)
