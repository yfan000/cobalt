import os
import sets

from Cobalt.Components.system import Brooklyn

from test_base import TestComponent

class TestBrooklyn (TestComponent):
    
    def setup (self):
        self.system = Brooklyn("test_brooklyn.xml")
    
    def test_init_configure (self):
        config_file = "test_brooklyn.xml"
        assert os.path.exists(config_file)
        system = Brooklyn()
        assert not system.partitions
        assert not system.nodes
        system = Brooklyn(config_file)
        assert system.partitions
        assert system.nodes
    
    def test_check_pid (self):
        invalid_pid = -1
        my_pid = os.getpid()
        system = Brooklyn()
        assert not system.check_pid(invalid_pid)
        assert system.check_pid(my_pid)
    
    def test_configure (self):
        config_file = "test_brooklyn.xml"
        assert os.path.exists(config_file)
        system = Brooklyn()
        assert not system.partitions
        assert not system.nodes
        system.configure(config_file)
        assert system.partitions
        assert system.nodes
    
    def test_reserve_partition (self):
        idle_partitions = self.system.partitions.q_get([{'state':"idle"}])
        partition = idle_partitions[0]
        self.system.reserve_partition(partition.name)
        assert partition.state == "busy"
        for node in partition.nodes:
            assert node.state == "busy"
        for parent in partition.parents:
            assert parent.state == "blocked"
        for child in partition.children:
            assert child.state == "blocked"
    
    def test_release_partition (self):
        idle_partitions_before = self.system.partitions.q_get([{'state':"idle"}])
        partition = idle_partitions_before[0]
        self.system.reserve_partition(partition.name)
        self.system.release_partition(partition.name)
        idle_partitions_after = self.system.partitions.q_get([{'state':"idle"}])
        assert idle_partitions_before == idle_partitions_after
