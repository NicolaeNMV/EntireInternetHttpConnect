import netaddr

networksf = open('ipv4_lines.txt','r')
networks=[l.strip() for l in networksf]

class internetIterator:
    def __init__(self):
        self.networks = iter(networks)
        self.network = self.getNetworkIterator( self.networks.next() )
        self.index = 0

    def __iter__(self):
        return self

    def getNetworkIterator(self,network):
        return netaddr.IPNetwork( network ).iter_hosts()

    def next(self):
        self.index+=1
        # Get the next host
        try:
          return (self.index, self.network.next())
        except StopIteration:
          pass
        # Get the next network
        try:
          self.network = self.getNetworkIterator( self.networks.next() )
        except StopIteration:
          raise StopIteration

        return (self.index, self.network.next())
  
    def index(self):
        return self.index