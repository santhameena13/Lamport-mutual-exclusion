'''
Created on 15-Oct-2017

@author: santhu
'''
import socket, select, sys, time 
import logging, json
import Queue

logging.basicConfig(level = logging.INFO)
with open("config.json", "r") as configFile:
    config = json.load(configFile)

BUFFER_SIZE = config["buffer_size"] 
DELIMITER = config["message_delimiter"]
MSG_TERMINATOR = config["message_terminator"]
POST_FILE = config["post_file"]
TOTAL_PROCESSES = config["total_processes"]
MSG_DELAY = config["delay"]
REQ = "REQUEST"
REP = "REPLY"
REL = "RELEASE"


class Process:
    def __init__(self, process_id, socket):
        self.process_id = process_id
        self.socket = socket
        self.logical_clock = 0
        self.reply_count = 0
        self.like_count_queue = Queue.Queue()
        self.local_num_of_likes = 0
        self.local_queue = Queue.PriorityQueue()
        self.message_queue = Queue.Queue()
        self.communicate_with_host()
        
        
    def communicate_with_host(self):
        inputs = [self.socket, sys.stdin]
        outputs = [self.socket]
        self.message_queue.put("PID:"+self.process_id+MSG_TERMINATOR)
        data = ""
        while True:
            read_sockets, write_sockets, error_sockets = select.select(inputs, outputs, [])
            # read data from console and socket
            for socket in read_sockets:
                if socket == sys.stdin:  
                    line = socket.readline()
                    if line:
                        if "READ" in line:
                            self.read_event_handler()
                        elif "LIKE" in line:
                            try:
                                line.split(":")[1]
                                self.like_count_queue.put(int(line.split(":")[1]))
                            except:
                                self.like_count_queue.put(1)
                            self.like_event_handler()
                        elif "EXIT" in line:
                            sys.exit()
                        else:
                            print "Enter READ, LIKE:<like_count> or EXIT"
                else:
                    data = data + self.socket.recv(BUFFER_SIZE)
                    if data:
                        while MSG_TERMINATOR in data:
                            pos = data.find(MSG_TERMINATOR)
                            msg = data[:pos]
                            data = data[pos+1:]
                            print "Message received: " + msg
                            if REQ in msg:
                                self.request_message_handler(msg)
                            if REP in msg:
                                self.reply_message_handler(msg)
                            if REL in msg:
                                self.release_message_handler(msg)

                        
            for socket in write_sockets:
                try:
                    next_msg = self.message_queue.get_nowait()
                    print  "Message sent: " + next_msg
                    time.sleep(MSG_DELAY)
                    socket.send(next_msg)
                except Queue.Empty:
                    pass
                    
            for socket in error_sockets:
                print "error in sockets"
                pass
                    
    
    def like_post(self):
        with open(POST_FILE, "r+") as post_file:
            data = json.load(post_file)
            num_of_likes = int(data["num_of_likes"]) + self.like_count_queue.get()
            data["num_of_likes"] = num_of_likes
            post_file.seek(0)
            json.dump(data, post_file)
            post_file.close()

            self.local_num_of_likes = int(num_of_likes)
            self.logical_clock += 1 # for release send
            rel_msg = DELIMITER.join((REL, str(self.process_id), str(self.logical_clock), str(self.local_num_of_likes))) + MSG_TERMINATOR

            self.message_queue.put(rel_msg)

            logging.info("Like value updated by process " + str(self.process_id))
            logging.info("Logical clock value: " + str(self.logical_clock))
            logging.info("Number of likes: " + str(self.local_num_of_likes))
            logging.info("Priority queue: " + str(self.local_queue.queue))
            logging.info("=======================================  \n")
            # #reset values
            self.reply_count = 0


    def read_event_handler(self):
        with open(POST_FILE, "r") as post_file:
            data = json.load(post_file)
            post_file.close()
            self.local_num_of_likes = data["num_of_likes"]
            self.logical_clock += 1

            logging.info("Post read by process " + str(self.process_id))
            logging.info("Logical clock value: " + str(self.logical_clock))
            logging.info("Post: " + data["post"])
            logging.info("Number of likes: " + str(data["num_of_likes"]))
            logging.info("=======================================  \n")         


    def like_event_handler(self):
        self.logical_clock += 1 # clock updated due to the broadcast message
        self.local_queue.put((int(self.logical_clock), int(self.process_id)))
        message = DELIMITER.join((REQ, str(self.process_id), str(self.logical_clock))) + MSG_TERMINATOR
        self.message_queue.put(message)

        logging.info("REQUEST message ready for broadcast")
        logging.info("Priority queue: " + str(self.local_queue.queue))


    def request_message_handler(self, message):
        message_type, req_process, req_clock = message.split(DELIMITER)
        if int(req_process) != int(self.process_id):
            self.logical_clock = max(self.logical_clock, int(req_clock)) + 1
            self.local_queue.put((int(req_clock), int(req_process)))
            message = DELIMITER.join((REP, str(req_process), str(self.process_id), str(self.logical_clock))) + MSG_TERMINATOR
            self.logical_clock += 1 #for send reply 
            self.message_queue.put(message)

            logging.info("REQUEST message received from process " + req_process) 
            logging.info("Priority queue: " + str(self.local_queue.queue)) 


    def reply_message_handler(self, message):
        message_type, req_process, rep_process, rep_logical_clock = message.split(DELIMITER)
        if int(req_process) == int(self.process_id):
            self.logical_clock = max(self.logical_clock, int(rep_logical_clock)) + 1
            self.reply_count += 1
            logging.info("REPLY message received from process " + rep_process)
            #with every reply message received check if you are eligible to like
            if self.reply_count == TOTAL_PROCESSES-1:
                clock, process = self.local_queue.get()
                if int(self.process_id) == process:
                    self.like_post()
                else:
                    self.local_queue.put((int(clock), int(process)))


    def release_message_handler(self, message):
        message_type, req_process, req_logical_clock, updated_like_value = message.split(DELIMITER)
        if int(req_process) != int(self.process_id):
            self.logical_clock = max(self.logical_clock, int(req_logical_clock)) + 1
            self.local_num_of_likes = updated_like_value
            clock, process = self.local_queue.get()
            if not int(process) ==  int(req_process):
                self.local_queue.put((int(clock), int(process)))

            logging.info("RELEASE message received from process " + req_process)
            logging.info("Priority queue: " + str(self.local_queue.queue))
            logging.info("Updated LIKE value: " + str(updated_like_value))
            logging.info("=======================================  \n")

            # with every release message received, check if you are eligible to like
            if self.reply_count == TOTAL_PROCESSES-1:
                clock, process = self.local_queue.get()
                if int(self.process_id) == process:
                    self.like_post()
                else:
                    self.local_queue.put((int(clock), int(process)))


######################################################################################


if __name__ == "__main__": 
#     if len(sys.argv) < 2:
#         sys.exit("Please provide the process ID")  
#     process_id = sys.argv[1]
    process_id = raw_input("Enter process ID: ")
    host_IP = config["IP"]
    host_port = config["port"]
    process_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    process_socket.connect((host_IP, host_port))
    print "Enter READ, LIKE:<like_count> or EXIT"
    process = Process(process_id, process_socket)