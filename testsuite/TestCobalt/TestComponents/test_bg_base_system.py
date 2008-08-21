import os
import time

from Cobalt.Components.simulator import Simulator, Partition, PartitionDict
from test_base import TestComponent

__all__ = [
    "BGBaseSystem",
]

CONFIG_FILE="surveyor.xml"

class TestBGBaseSystem (TestComponent):
    
    def setup (self):
        TestComponent.setup(self)
        self.system = Simulator(config_file=CONFIG_FILE)
        self.partitions = [p.name for p in self.system._partitions.values()]    
        #self.bgsystem = BGSystem()

    def test_can_run(self):
        partition = Partition({'scheduled':True, 'functional':True})
        assert partition._can_run()
        partition = Partition({'scheduled':True, 'functional':False})
        assert not partition._can_run()
        partition = Partition({'scheduled':False, 'functional':False})
        assert not partition._can_run()

    def test_part_can_run(self):
        target_part = Partition({'name':"mine", 'functional':False, 'state':"idle", 'size':100})
        part = Partition({'children':"mine", 'functional':False, 'state':"idle"})
        partitiondict = PartitionDict({'part1':part, 'part2':part})
        assert not partitiondict.can_run(target_part, 20)
        
        target_part = Partition({'name':"mine", 'functional':True, 'scheduled':True,
                                             'state':"idle", 'size':100 })
        part = Partition({'children':"mine", 'functional':True, 'scheduled':True, 'size':100})
        partitiondict = PartitionDict({'part1':part, 'part2':part})
        assert partitiondict.can_run(target_part, 20)

        #target partition is not idle 
        target_part = Partition({'state':"running"})
        assert not partitiondict.can_run(target_part, 20)


    def test_partition_allocator(self):
        def clear_states(best_part):
            self.system.release_partition(best_part)
            self.system._partitions[best_part].reserved = False

            for part in self.system._partitions[best_part].children:
                self.system._partitions[part].reserved = False
            for part in self.system._partitions[best_part].parents:
                self.system._partitions[part].reserved = False

    
        assert self.system.partition_allocator(64, 'medium', [])[0] == None  
              
        managed = ['ANL-R00-M0-512', 'ANL-R00-M1-512']

        flags = {}  
        flags['ANL-R00-M0-512'] =  (True, True, 'default:small:medium')
        flags['ANL-R00-M1-512'] =  (True, True, 'default:small:medium')

        self.system.__setstate__({'managed_partitions':managed, 
                                  'version':2, 'config_file':CONFIG_FILE, 'partition_flags': flags})
        
        #test without exclusions or jobs for reservations
        assert self.system.partition_allocator(512, 'not_a_queue', [])[0] == None        
        assert self.system.partition_allocator(1024, 'medium', [])[0] == None

        best_part = self.system.partition_allocator(64, 'small', [])[0]
        assert best_part in managed
        clear_states(best_part)

        best_part = self.system.partition_allocator(512, 'medium', [])[0]
        assert best_part in managed
        clear_states(best_part)

        #make some partitions 'busy'
        self.system.reserve_partition('ANL-R00-M0-512')
        best_part = self.system.partition_allocator(64, 'medium', [])[0]        
        assert best_part == 'ANL-R00-M1-512'
        clear_states(best_part)

        self.system.reserve_partition('ANL-R00-M1-512')  
        assert self.system.partition_allocator(64, 'medium', [])[0] == None

        self.system.release_partition('ANL-R00-M0-512')

        best_part = self.system.partition_allocator(64, 'medium', [])[0]
        assert best_part == 'ANL-R00-M0-512'
        clear_states(best_part)

        self.system.release_partition('ANL-R00-M1-512')     

        for part_name in self.partitions:
            flags[part_name] =  (True, True, 'default:small:medium')

        self.system.__setstate__({'managed_partitions':self.partitions, 
                                  'version':2, 'config_file':CONFIG_FILE, 'partition_flags': flags})

        #exclude all 64 node partitions
        ex = [p for p in self.partitions if self.system._partitions[p].size == 64]
        assert self.system.partition_allocator(64, 'medium', ex)[0] == None

        #exclude all but one 64 node partition
        ex.remove('ANL-R00-M1-N00-64')

        best_part = self.system.partition_allocator(1, 'medium', ex)[0]
        assert best_part == 'ANL-R00-M1-N00-64' 
        clear_states(best_part)

        best_part = self.system.partition_allocator(64, 'medium', ex)[0]
        assert best_part == 'ANL-R00-M1-N00-64'
        clear_states(best_part)

        assert self.system.partition_allocator(128, 'medium', ex)[0]== None
        assert self.system.partition_allocator(512, 'medium', ex)[0] == None

        #exclude all 64 node partitions except two of the same parent
        ex.remove('ANL-R00-M1-N02-64')
        best_part = self.system.partition_allocator(64, 'medium', ex)[0]   
        assert best_part in ['ANL-R00-M1-N00-64','ANL-R00-M1-N02-64']
        clear_states('ANL-R00-M1-N00-64')

        best_part = self.system.partition_allocator(128, 'medium', ex)[0]
        assert best_part == 'ANL-R00-M1-N00-128'
        clear_states(best_part)

        assert self.system.partition_allocator(129, 'medium', ex)[0] == None
        
        #now exclude the parent
        ex.append('ANL-R00-M1-N00-128')
        assert self.system.partition_allocator(64, 'medium', ex)[0] == None
        assert self.system.partition_allocator(128, 'medium', ex)[0] == None

        #test reservation job allocation
        res = ['ANL-R00-M0-N00-64']
        best_part = self.system.partition_allocator(64, None, None, res)[0] 
        assert best_part in res
        clear_states(best_part)

        assert self.system.partition_allocator(128, None, None, res)[0] == None

        res.append('ANL-R00-M1-512')
        best_part = self.system.partition_allocator(512, None, None, res)[0] 
        assert best_part == 'ANL-R00-M1-512'
        clear_states(best_part)

        #test jobs submitted to queue with smaller than partitions than reserved
        res = ['ANL-R00-M1-512']
        res_children = self.system._partitions['ANL-R00-M1-512'].children

        best_part = self.system.partition_allocator(64, None, None, res)[0]
        assert best_part in res_children
        clear_states(best_part)
        best_part = self.system.partition_allocator(65, None, None, res)[0] 
        assert best_part in res_children
        clear_states(best_part)
        best_part = self.system.partition_allocator(128, None, None, res)[0]
        assert best_part in res_children
        clear_states(best_part)
        best_part = self.system.partition_allocator(256, None, None, res)[0]
        assert best_part in res_children
        clear_states(best_part)

        #reservation for more than one partition and children
        res = ['ANL-R00-M0-N00-128','ANL-R00-M1-N00-128']
        possible_128_res =  self.system._partitions['ANL-R00-M0-N00-128'].children + \
                            self.system._partitions['ANL-R00-M1-N00-128'].children

        best_part = self.system.partition_allocator(64, None, None, res)[0]
        assert best_part in possible_128_res 
        clear_states(best_part)

        best_part = self.system.partition_allocator(128, None, None, res)[0]
        assert best_part in res
        clear_states(best_part)
    
        #one child for each in res busy
        self.system.reserve_partition('ANL-R00-M0-N00-64')
        self.system.reserve_partition('ANL-R00-M1-N00-64')

        best_part = self.system.partition_allocator(64, None, None, res)[0]
        assert best_part in ['ANL-R00-M0-N02-64','ANL-R00-M1-N02-64']
        clear_states(best_part)

        #'ANL-R00-M0-N00-128' children busy and one of 'ANL-R00-M1-N00-128' children busy
        self.system.reserve_partition('ANL-R00-M0-N02-64')

        best_part = self.system.partition_allocator(64, None, None, res)[0]
        assert best_part == 'ANL-R00-M1-N02-64'
        clear_states(best_part)

        assert self.system.partition_allocator(128, None, None, res)[0] == None
   
        #'ANL-R00-M0-N00-128' children busy
        self.system.release_partition('ANL-R00-M1-N00-64')

        best_part = self.system.partition_allocator(64, None, None, res)[0]
        assert best_part in self.system._partitions['ANL-R00-M1-N00-128'].children
        clear_states(best_part)

        self.system.release_partition('ANL-R00-M0-N00-64')
        self.system.release_partition('ANL-R00-M0-N02-64')

        #not an actual partition
        res = ['ANL-R00-M0-N00-oops']
        assert self.system.partition_allocator(64, None, None, res)[0] == None
        clear_states(best_part)

        #check reserved state
        best_part = self.system.partition_allocator(512, 'medium', [])[0]
        assert self.system._partitions[best_part].reserved <= time.time()
        clear_states(best_part)

        #check clearing of reserved state for job that never started
        self.system._partitions[best_part].reserved = time.time() - 6*60
        self.system.partition_allocator(100000, 'medium', [])
        assert not self.system._partitions[best_part].reserved 

        

