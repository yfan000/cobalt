import threading
import xmlrpclib

from Cobalt.Server import XMLRPCServer

class TestXMLRPCServer (object):
    
    def test_listMethods (self):
        
        server = XMLRPCServer(("localhost", 5900),
            keyfile = "/etc/cobalt.key",
            certfile = "/etc/cobalt.key",
        )
        
        def client_request ():
            server = xmlrpclib.ServerProxy("https://localhost:5900")
            methods = server.system.listMethods()
            assert set(methods) == set(["ping", "system.listMethods", "system.methodHelp", "system.methodSignature"])
        
        client_thread = threading.Thread(target=client_request)
        client_thread.start()
        server.handle_request()
        client_thread.join()
        server.server_close()
    
    def test_ping (self):
        
        server = XMLRPCServer(("localhost", 5901),
            keyfile = "/etc/cobalt.key",
            certfile = "/etc/cobalt.key",
        )
        
        def client_request ():
            server = xmlrpclib.ServerProxy("https://localhost:5901")
            sent_args = (1, 5, 8, 2)
            received_args = server.ping(*sent_args)
            assert list(received_args) == list(sent_args)
        
        client_thread = threading.Thread(target=client_request)
        client_thread.start()
        server.handle_request()
        client_thread.join()
        server.server_close()
