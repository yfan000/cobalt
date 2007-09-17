import code
import threading
import xmlrpclib

from Cobalt.Component import XMLRPCServer, Component, exposed, automatic

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


class TestComponent (object):
    
    def test_exposed (self):
        
        class TestComponent (Component):
            
            @exposed
            def method1 (self):
                return "return1"
            
            @exposed
            def method2 (self):
                return "return2"
            
            def method3 (self):
                return "return3"
        
        component = TestComponent()
        assert component.method1.exposed
        assert component.method2.exposed
        assert not getattr(component.method3, "exposed", False)
        exposed_methods = component._listMethods()
        assert set(exposed_methods) == set(["method1", "method2"])
        assert component._dispatch("method1", ()) == "return1"
        assert component._dispatch("method2", ()) == "return2"
        try:
            component._dispatch("method3", ())
        except Exception:
            pass
        else:
            assert "dispatched to unexposed method"
    
    def test_automatic (self):
        
        class TestComponent (Component):
            
            runs = dict(method1=0, method2=0, method3=0)
            
            @automatic
            def method1 (self):
                self.runs['method1'] += 1
            
            @automatic
            def method2 (self):
                self.runs['method2'] += 1
            
            def method3 (self):
                self.runs['method3'] += 1
            
        component = TestComponent()
        component.do_tasks()
        assert component.runs['method1'] == 1
        assert component.runs['method2'] == 1
        assert component.runs['method3'] == 0
        component.do_tasks()
        assert component.runs['method1'] == 2
        assert component.runs['method2'] == 2
        assert component.runs['method3'] == 0
