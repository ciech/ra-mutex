import ra
import argparse
import time

APP_NAME = "Ra_Test"

def initLogger():
        formatter = logging.Formatter('%(asctime)s %(levelname)s::%(message)s')
        hdlrStd = logging.StreamHandler()
        hdlrStd.setFormatter(formatter)
        logger.addHandler(hdlrStd) 
        logger.setLevel(logging.DEBUG)

def parseArgs():
        parser = argparse.ArgumentParser(prog=APP_NAME, usage='%(prog)s [options]')
        parser.add_argument('--verbose','-v', action='store_false', help='verbose mode')
        parser.add_argument('--init_host_addr','-a', type=str, default = None, help='init host addr')
        parser.add_argument('--init_host_port','-i', type=int, default = None, help='init host ip')
        parser.add_argument('--use_time','-u',type=int,required	= True, help="Using resource time")
        parser.add_argument('--wait_time','-w',type=int,required = True, help="Idle time")
        parser.add_argument('--name','-n',type=str,required =True, help="Unique Node Name")
        parser.add_argument('--port','-p',type=int,required =False, default = 0, help="node communication port")
        return parser.parse_args()
 
class RaTest(object):
	def __init__(self,args):
		self.verboce = args.verbose
		self.use_time = args.use_time
		self.wait_time = args.wait_time
		self.name = args.name
		self.addr = (args.init_host_addr, args.init_host_port)
		self.port = args.port

	def runTest(self): 
		test = ra.RA(self.name,self.port)
		if (self.addr[0] != None ) and (self.addr[1] !=None):
			test.init(self.addr)

		while(True):
			time.sleep(self.wait_time)
			test.acquire()
			print "USING RESOURCE"
			time.sleep(self.use_time)
			print "FREEING RESOURCE"		
			test.release()


if __name__ == "__main__":
	test = RaTest(parseArgs())
	test.runTest()