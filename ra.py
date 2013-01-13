import socket
import threading
import json
import time


BUFSIZ = 1024

SUPPORTED_MSG_TYPES = ["INIT","REPLY","REQUEST","ARE_YOU_THERE","DEAD","INFO","DISPOSE"]

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

class Message(object):
    TYPE = Enum(SUPPORTED_MSG_TYPES)

    def __init__(self,msg_type=None,msg_content=None):
        if (msg_type != None):
            if (msg_type not in SUPPORTED_MSG_TYPES):
                raise Exception("Unknown message type")
        
        self.type=msg_type
        self.content = msg_content


    def __str__(self):
        return "Message: TYPE = " + "None" if (self.type == None) else self.type \
                + " CONTENT = " + "None" if (self.content == None) else str(self.content)

    def prepare(self,msg_type=None,msg_content=None):
        if (msg_type != None and msg_content != None):
            self.__init__(msg_type,msg_content)
        return json.dumps({ "TYPE": self.type, "CONTENT": self.content})

    def parse(self,msg):
        parsed_msg = json.loads(msg)
        if (parsed_msg["TYPE"] not in SUPPORTED_MSG_TYPES):
            raise Exception("Unknown message type")
        self.type = parsed_msg["TYPE"]
        self.content = parsed_msg["CONTENT"]

class RA(object):

    def send_message_to_node(self,node,message):
        self.send_message((self.nodes[node]["Ip"],self.nodes[node]["Port"]),message)
     
    def send_message(self,addr,message):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr) 
        s.send(message)
        s.close()

    def __handle_reply_message(self,args):
        self.var_lock.acquire()
        if (self.oustanding_reply_count > 0):
            self.oustanding_reply_count -= 1
        self.var_lock.release()

    def __handle_info_message(self,args):
        pass

    def __handle_dispose_message(self,args):
        pass

    def __handle_request_message(self,args):
        content = args[0]
        in_seq_num = content["SeqNum"]
        in_unique_name = content["UniqueName"]
        if not self.nodes.has_key(in_unique_name):
            #unknown node
            dead = Message(Message.TYPE.DEAD,{Status: "RE_INIT"})
            self.send_message(arg[1],dead)
            return
        self.var_lock.acquire()
        self.highest_seq_num = max(self.highest_seq_num,in_seq_num)
        defer_it = (self.requesting_cs and ((in_seq_num > self.seq_num) or ((in_seq_num == self.seq_num) and in_unique_name > self.info["UniqueName"])))
        self.var_lock.release()
        if defer_it:
            self.reply_deffered[in_unique_name] = True
        else:
            reply = Message()
            to_send = reply.prepare(Message.TYPE.REPLY,{})
            self.send_message_to_node(in_unique_name,to_send)

    def __handle_init_message(self,args):
        content = args[0]
        addr = args[1]
        if content["From"] == "New":
            self.acquire()
            #check if we are first
            if (len(self.nodes) == 0):
                #assume we are ok
                self.init_done = True
            #check if new node name is Unique
            mess = Message()
            if ((self.nodes.has_key(content["UniqueName"])) or (self.info["UniqueName"] == content["UniqueName"])):
                sponsor_info =  { "UniqueName": self.info["UniqueName"] }
                init_data = {"From": "Sponsor", "SponsorData": sponsor_info, "Status": "NOT_UNIQUE"}
                send_to_new = mess.prepare("INIT",init_data)
            else:
                new_info = { "UniqueName": content["UniqueName"], "Ip": addr[0], "Port": content["Port"]}
                sponsor_info =  { "UniqueName": self.info["UniqueName"] }
                send_to_nodes = mess.prepare("INIT",{"From": "Node", "SponsorData": sponsor_info, "NewData": new_info})
                for node in self.nodes:
                    self.send_message_to_node(node,send_to_nodes)
                sponsor_info =  { "UniqueName": self.info["UniqueName"], "Port": self.info["Port"] }
                init_data = {"From": "Sponsor", "SponsorData": sponsor_info,"Status": "OK", "NodesData": self.nodes}
                send_to_new = mess.prepare("INIT",init_data)
                self.nodes[content["UniqueName"]] = { "Ip": addr[0], "Port": content["Port"]}
            self.send_message((addr[0],content["Port"]),send_to_new)
            self.release()
        elif content["From"] == "Node":
            self.nodes[content["NewData"]["UniqueName"]] = { "Ip": content["NewData"]["Ip"], "Port": content["NewData"]["Port"] }
        elif content["From"] == "Sponsor":
            self.init_status = content["Status"]
            if (self.init_status == "OK"):
                self.nodes = content["NodesData"]
                self.nodes[content["SponsorData"]["UniqueName"]] = { "Ip": addr[0], "Port": content["SponsorData"]["Port"]}
                self.init_done = True
            else:
                self.init_done = False
            self.init_sem.release()        

    def __handle_are_you_there_message(self,args):
        print args + "NOT_SUPPORTED_YET"

    def __handle_dead_message(self,args):
        content = args[0]
        if (content["Status"] == "REMOVE"):
            pass
        elif (content["Status"] == "RE_INIT"):
            self.var_lock.acquire()
            self.init_done = False
            self.init_status = "RE_INIT"
            self.var_lock.release()


    def __msg_handle_dispatcher(self,sock,addr):
        data = sock.recv(BUFSIZ)
        msg = Message()
        msg.parse(data)
        #match msg with proper handler
        args = (msg.content, addr)
        {
            Message.TYPE.REPLY              : self.__handle_reply_message,
            Message.TYPE.REQUEST            : self.__handle_request_message,
            Message.TYPE.INIT               : self.__handle_init_message,
            Message.TYPE.ARE_YOU_THERE      : self.__handle_are_you_there_message,
            Message.TYPE.DEAD               : self.__handle_dead_message,
            Message.TYPE.INFO               : self.__handle_init_message,
            Message.TYPE.DISPOSE            : self.__handle_dispose_message
        }[msg.type](args)


    def __msg_listener(self):   
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', self.info["Port"]))
        addr, port = s.getsockname()
        self.var_lock.acquire()
        self.info['Port'] = port
        self.var_lock.release()
        s.listen(2)
        while 1:
            client, addr = s.accept()
            thread = threading.Thread(target = self.__msg_handle_dispatcher, args = ((client, addr)))
            thread.start()

    def __init__(self,name,port=0):
        self.seq_num = 0
        self.highest_seq_num = 0
        self.oustanding_reply_count = 0
        self.requesting_cs = False
        self.reply_deffered = {}
        self.nodes = {}
        self.info = {}
        self.init_status = ""
        self.init_done = False
        self.init_sem = threading.Semaphore(0)
        self.var_lock = threading.Lock()   
        self.var_lock.acquire()
        self.info['Port'] = port
        self.info['UniqueName'] = name
        self.var_lock.release()
        thread = threading.Thread(target = self.__msg_listener)
        thread.start()
        
    def init(self,addr):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr) 
        mess = Message(Message.TYPE.INIT,{"From": "New", "UniqueName": self.info["UniqueName"], "Port": self.info["Port"]})
        toSend = mess.prepare()
        s.send(toSend)
        #wait until initialization ends
        self.init_sem.acquire()
        return (self.init_done, self.init_status)

    def acquire(self):
        print "Node " + self.info["UniqueName"] + " used ACQUIRE"
        if not self.init_done: return False
        self.var_lock.acquire()
        self.requesting_cs = True
        self.seq_num = self.highest_seq_num + 1
        self.var_lock.release()
        self.oustanding_reply_count = len(self.nodes)
        body = {} 
        body.update(self.info)
        body.update({"SeqNum":self.seq_num})
        mess = Message(Message.TYPE.REQUEST,body)
        for node in self.nodes:
            self.send_message_to_node(node,mess.prepare()); 
        while  self.oustanding_reply_count !=  0: 
            pass
        return True

    def release(self):
        print "Node " + self.info["UniqueName"] + " used RELEASE"
        if not self.init_done: return False
        self.requesting_cs = False 
        for node in self.nodes:
            if self.reply_deffered.get(node):
                self.reply_deffered[node] = False
                mess = Message(Message.TYPE.REPLY,{})
                self.send_message_to_node(node,mess.prepare())
        return True

    def dispose(self):
        print "TODO"