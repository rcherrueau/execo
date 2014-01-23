from execo import *
from execo_g5k import *
from execo_engine import *

class g5k_tcp_congestion(Engine):

    def run(self):
        params = {}

# /proc/sys/net/ipv4/tcp_congestion_control
# /proc/sys/net/ipv4/tcp_available_congestion_control
# /proc/sys/net/ipv4/tcp_allowed_congestion_control
