from .echo_algorithm import *
from .ring_election import *
from .dolev_algorithm import *
from .bracha_algorithm import *
from .rco_algorithm import *

def get_algorithm(name):
    if name == "echo":
        return EchoAlgorithm
    elif name == "ring":
        return RingElection
    elif name == "dolev":
        return DolevAlgorithm
    elif name == "bracha":
        return BrachaAlgorithm
    elif name == "rco":
        return RCOAlgorithm
    else:
        raise ValueError(f"Unknown algorithm: {name}")
