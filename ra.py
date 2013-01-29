import socket
import threading
import json
import time


BUFSIZ = 1024
TIMEOUT = 10

SUPPORTED_MSG_TYPES = ["INIT","REPLY","REQUEST","ARE_YOU_THERE","YES_I_AM_HERE","DEAD","HIGHEST_SEQ_NUM"]

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

class Message(object):
    TYPE = Enum(SUPPORTED_MSG_TYPES)

    def __init__(self,msg_type=None,msg_from=None,msg_content=None):
        if (msg_type != None):
            if (msg_type not in SUPPORTED_MSG_TYPES):
                raise Exception("Unknown message type")
        self.type = msg_type
        self.sender = msg_from
        self.content = msg_content

    def __str__(self):
        return "Message: TYPE = " + ("None" if (self.type == None) else self.type) \
                + " FROM = " + ("None" if (self.type == None) else str(self.sender)) \
                + " CONTENT = " + ("None" if (self.content == None) else str(self.content))

    def prepare(self,msg_type=None,msg_from=None,msg_content=None):
        if (msg_type == None and msg_from == None and msg_content == None):
            pass
        elif (msg_type != None and msg_from != None and msg_content != None):
            self.__init__(msg_type,msg_from ,msg_content)
        else:
            raise Exception("Bad Parse Args")

        return json.dumps({"TYPE": self.type, "FROM": self.sender, "CONTENT": self.content})
        
    def parse(self,msg):
        parsed_msg = json.loads(msg)
        if (parsed_msg["TYPE"] not in SUPPORTED_MSG_TYPES):
            raise Exception("Unknown message type")
        self.type = parsed_msg["TYPE"]
        self.sender = parsed_msg["FROM"]
        self.content = parsed_msg["CONTENT"]

class RA(object):

    def __send_message_to_node(self,node,message):
        try:
            self.__send_message((self.nodes[node]["Ip"],self.nodes[node]["Port"]),message)
        except socket.error, msg:
            print 'Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]  
            print "Deleting node: " + node   
            self.var_lock.acquire()
            del self.nodes[node]
            if self.requesting_cs:
                if (self.oustanding_reply_count > 0):
                    self.oustanding_reply_count -= 1
            self.var_lock.release()
            dead = Message(Message.TYPE.DEAD,self.info,{"Status": "REMOVE", "Node": node})
            for node in self.nodes.keys():
                self.__send_message_to_node(node,dead.prepare())


    def __send_message(self,addr,message):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr) 
        s.send(message)
        s.close()

    def __handle_reply_message(self,args):
        self.var_lock.acquire()
        if (self.oustanding_reply_count > 0):
            self.oustanding_reply_count -= 1
        self.var_lock.release()

    def __handle_request_message(self,args):
        sender = args[0]
        content = args[1]
        in_seq_num = content["SeqNum"]
        in_unique_name = sender["UniqueName"]
        self.var_lock.acquire()
        self.highest_seq_num = max(self.highest_seq_num,in_seq_num)
        defer_it = (self.requesting_cs and ((in_seq_num > self.seq_num) or ((in_seq_num == self.seq_num) and in_unique_name > self.info["UniqueName"])))
        self.var_lock.release()
        if defer_it:
            self.reply_deffered[in_unique_name] = True
        else:
            reply = Message()
            to_send = reply.prepare(Message.TYPE.REPLY,self.info,{})
            try:
                self.__send_message_to_node(in_unique_name,to_send)
            except socket.error, msg:
                print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]

    def __handle_init_message(self,args):
        sender = args[0]
        content = args[1]
        addr = args[2]
        if content["Role"] == "New":
            self.acquire()
            #check if we are first
            if (len(self.nodes) == 0):
                #assume we are ok
                self.init_done = True
            #check if new node name is Unique
            mess = Message()
            if ((self.nodes.has_key(sender["UniqueName"])) or (self.info["UniqueName"] == sender["UniqueName"])):
                init_data = {"Role": "Sponsor", "Status": "NOT_UNIQUE"}
                send_to_new = mess.prepare(Message.TYPE.INIT,self.info,init_data)
            else:
                send_to_nodes = mess.prepare(Message.TYPE.INIT,self.info, {"Role": "Node", "NewData": sender})
                for node in self.nodes:
                    self.__send_message_to_node(node,send_to_nodes)
                init_data = {"Role": "Sponsor", "Status": "OK", "NodesData": self.nodes}
                send_to_new = mess.prepare(Message.TYPE.INIT,self.info,init_data)
                self.nodes[sender["UniqueName"]] = { "Ip": sender["Ip"], "Port": sender["Port"]}
            self.__send_message((sender["Ip"],sender["Port"]),send_to_new)
            self.release()
        elif content["Role"] == "Node":
            self.nodes[content["NewData"]["UniqueName"]] = { "Ip": content["NewData"]["Ip"], "Port": content["NewData"]["Port"] }
        elif content["Role"] == "Sponsor":
            self.init_status = content["Status"]
            if (self.init_status == "OK"):
                self.nodes = content["NodesData"]
                self.nodes[sender["UniqueName"]] = { "Ip": sender["Ip"], "Port": sender["Port"]}
                self.init_done = True
            else:
                self.init_done = False
            self.init_event.set()        
            self.init_event.clear()

    def __handle_are_you_there_message(self,args):
        sender = args[0]
        content = args[1]
        mess = Message(Message.TYPE.YES_I_AM_HERE,self.info,{})
        self.__send_message_to_node(sender["UniqueName"].mess.prepare())

    def __handle_dead_message(self,args):
        print "HANDLE_DEAD " + str(args[1])
        sender = args[0]
        content = args[1]
        if (content["Status"] == "REMOVE"):
            self.var_lock.acquire()
            if self.nodes.has_key(content["Node"]):
                del self.nodes[content["Node"]]
            self.var_lock.release()
        elif (content["Status"] == "RE_INIT"):
            self.var_lock.acquire()
            self.init_done = False
            self.init_status = "RE_INIT"
            self.var_lock.release()

    def __handle_highest_seq_num_message(self,args):
        sender = args[0]
        content = args[1]
        if (content["STATUS"]=="GET"):
            mess = Message(Message.TYPE.HIGHEST_SEQ_NUM,self.info,{"STATUS":"RESPONSE", "VALUE": self.highest_seq_num})
            self.__send_message_to_node(sender["UniqueName"],mess.prepare())
        elif (content["STATUS"]=="RESPONSE"):
            self.nodes_highest_seq_num[sender["UniqueName"]] = int(content["VALUE"])
            if len(self.nodes_highest_seq_num) == len(self.nodes):
                self.init_event.set()
                self.init_event.clear()

    def __handle_yes_i_am_here(self,args):
        sender = args[0]
        content = args[1]
        i_am_here= Message(Message.TYPE.YES_I_AM_HERE,self.info,{})
        self.__send_message((sender['Ip'],sender['Port']),i_am_here.prepare())

    def __handle_unknown_node(self,args):
        sender = args[0]
        content = args[1]
        dead = Message(Message.TYPE.DEAD,self.info,{"STATUS": "RE_INIT"})
        self.__send_message((sender['Ip'],sender['Port']),dead.prepare())

    def __msg_handle_dispatcher(self,sock,addr):
        data = sock.recv(BUFSIZ)
        msg = Message()
        msg.parse(data)
        args = (msg.sender, msg.content, addr)
        if (( msg.type != Message.TYPE.INIT )and ( not self.nodes.has_key(msg.sender["UniqueName"]))):
            self.__handle_unknown_node(args)
        else:
            #match msg with proper handler
            {
                Message.TYPE.REPLY                  : self.__handle_reply_message,
                Message.TYPE.REQUEST                : self.__handle_request_message,
                Message.TYPE.INIT                   : self.__handle_init_message,
                Message.TYPE.ARE_YOU_THERE          : self.__handle_are_you_there_message,
                Message.TYPE.DEAD                   : self.__handle_dead_message,
                Message.TYPE.HIGHEST_SEQ_NUM        : self.__handle_highest_seq_num_message,
                Message.TYPE.YES_I_AM_HERE          : self.__handle_yes_i_am_here
            }[msg.type](args)

    def __msg_listener(self):   
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.var_lock.acquire()
        #if port == 0 it will be set by OS
        s.bind((self.info["Ip"], self.info["Port"]))
        addr, port = s.getsockname()
        self.info['Ip'] = ("127.0.0.1" if addr == "0.0.0.0" else addr )
        self.info['Port'] = port
        self.var_lock.release()
        self.listener_event.set()
        self.listener_event.clear()
        s.listen(2)
        while 1:
            client, addr = s.accept()
            thread = threading.Thread(target = self.__msg_handle_dispatcher, args = ((client, addr)))
            thread.start()

    def __init__(self,name,ip='',port=0):
        self.seq_num = 0
        self.highest_seq_num = 0
        self.oustanding_reply_count = 0
        self.requesting_cs = False
        self.disposing = False
        self.reply_deffered = {}
        self.awaiting_reply = {}
        self.nodes_highest_seq_num = {}
        self.nodes = {}
        self.info = {}
        self.init_status = ""
        self.init_done = False
        self.init_event = threading.Event()
        self.listener_event = threading.Event()
        self.var_lock = threading.Lock()   
        self.var_lock.acquire()
        self.info['Ip'] = ip
        self.info['Port'] = port
        self.info['UniqueName'] = name
        self.var_lock.release()
        thread = threading.Thread(target = self.__msg_listener)
        thread.start()
        #wait for listener thread initialization
        self.listener_event.wait()
        
    def init(self,addr):
        mess = Message(Message.TYPE.INIT,self.info,{"Role":"New"})
        self.__send_message(addr,mess.prepare())
        #wait until initialization ends
        self.init_event.wait()
        for node in self.nodes.keys():
            mess = Message(Message.TYPE.HIGHEST_SEQ_NUM,self.info,{"STATUS":"GET"})
            self.__send_message_to_node(node, mess.prepare())
        self.init_event.wait()
        highest_num = -1
        for num in self.nodes_highest_seq_num:
            if self.nodes_highest_seq_num[num] > highest_num: highest_num = self.nodes_highest_seq_num[num]
        self.highest_seq_num = highest_num
        return (self.init_done, self.init_status)

    def acquire(self):
        if not self.init_done: return False
        if self.disposing: return False
        print "ACQUIRE"
        self.var_lock.acquire()
        self.requesting_cs = True
        print self.highest_seq_num 
        self.seq_num = int(self.highest_seq_num)+ 1
        self.var_lock.release()
        self.oustanding_reply_count = len(self.nodes)
        mess = Message(Message.TYPE.REQUEST,self.info,{"SeqNum":self.seq_num})
        for node in self.nodes.keys():
            try:
                self.__send_message_to_node(node,mess.prepare()); 
            except socket.error, msg:
                print 'Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]      
        #threading.Timer(TIMEOUT, function)
        print "Wait for RESPONSE"
        while  self.oustanding_reply_count !=  0: 
            pass
        return True

    def release(self):
        if not self.init_done: return False
        print "RELEASE"
        self.requesting_cs = False 
        for node in self.nodes.keys():
            if self.reply_deffered.get(node):
                self.reply_deffered[node] = False
                mess = Message(Message.TYPE.REPLY,self.info,{})
                try:
                    self.__send_message_to_node(node,mess.prepare()); 
                except socket.error, msg:
                    print 'Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
        return True

    def dispose(self):
        self.disposing = True
        if self.requesting_cs: self.release()
        mess = Message(Message.TYPE.DEAD,self.info,{"Status": "REMOVE", "Node": self.info["UniqueName"]})
        for node in self.nodes:
            try:
                self.__send_message_to_node(node,mess.prepare()); 
            except socket.error, msg:
                print 'Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
        self.disposing = False
        self.init_done = False

