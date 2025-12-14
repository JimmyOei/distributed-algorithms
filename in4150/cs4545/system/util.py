import copy
from pathlib import Path
from typing import Optional

import click
import yaml
import networkx as nx



@click.group()
def cli():
    pass


@cli.command('compose')
@click.argument('num_nodes', type=int)
@click.argument('topology_file', type=str, default='topologies/echo.yaml')
@click.argument('algorithm', type=str, default='echo')
@click.argument('faults', type=int, default=0)
@click.option('--num_byzantine', type=int, default=0)
@click.option('--limited_neighbors', type=int, default=1)
@click.option('--byzantine_behavior', type=str, default='drop')
@click.option('--min_message_delay', type=float, default=0.01)
@click.option('--max_message_delay', type=float, default=0.1)
@click.option('--broadcasters', type=int, default=1)
@click.option('--broadcasts', type=int, default=1)
@click.option('--connectivity', type=int, default=3, help='Degree of each node in the random graph')
@click.option('--debug_mode', type=int, default=1, help='Debug mode: 0=silent, 1=deliveries only, 2=all logs')
@click.option('--debug_algorithm', type=str, default='all', help='Debug algorithm filter: "all", "dolev", "bracha", etc.')
@click.option('--opt_echo_amplification', is_flag=True, help='Enable echo amplification optimization')
@click.option('--opt_single_hop_send', is_flag=True, help='Enable single-hop send messages optimization (MBD.2)')
@click.option('--opt_reduced_messages', is_flag=True, help='Enable reduced number of messages optimization (MBD.11)')
@click.option('--template_file', type=str,  default='docker-compose.template.yml')
@click.option('--overwrite_topology',is_flag=True, help='Overwrite the topology file. Useful for topologies that can be adjusted dynamically such as rings. Do not use this option if you have a static topology file that you want the preserve!')
def compose(num_nodes, topology_file, algorithm, faults, num_byzantine, limited_neighbors, byzantine_behavior, min_message_delay, max_message_delay, broadcasters, broadcasts, connectivity, debug_mode, debug_algorithm, opt_echo_amplification, opt_single_hop_send, opt_reduced_messages, template_file, overwrite_topology):
    prepare_compose_file(num_nodes, topology_file, algorithm, faults, num_byzantine, limited_neighbors, byzantine_behavior, min_message_delay, max_message_delay, broadcasters, broadcasts, connectivity, debug_mode, debug_algorithm, opt_echo_amplification, opt_single_hop_send, opt_reduced_messages, template_file, overwrite_topology=overwrite_topology)

def prepare_compose_file(num_nodes, topology_file, algorithm, faults, num_byzantine, limited_neighbors, byzantine_behavior, min_message_delay, max_message_delay, broadcasters, broadcasts, connectivity, debug_mode, debug_algorithm, opt_echo_amplification, opt_single_hop_send, opt_reduced_messages, template_file, location='cs4545', overwrite_topology = False):
    import random

    G = nx.random_regular_graph(connectivity, num_nodes)

    with open(template_file, 'r') as f:
        content = yaml.safe_load(f)

        node = content['services']['node0']
        content['x-common-variables']['TOPOLOGY'] = topology_file

        nodes = {}
        baseport = 9090
        connections = {}

        network_name = list(content['networks'].keys())[0]
        subnet = content['networks'][network_name]['ipam']['config'][0]['subnet'].split('/')[0]
        network_base = '.'.join(subnet.split('/')[0].split('.')[:-1])

        # Randomly select broadcaster nodes
        broadcaster_nodes = random.sample(range(num_nodes), min(broadcasters, num_nodes))
        byzantine_nodes = random.sample(range(num_nodes), min(num_byzantine, num_nodes))

        for i in range(num_nodes):
            n = copy.deepcopy(node)
            n['ports'] = [f'{baseport + i}:{baseport + i}']
            n['networks'][network_name]['ipv4_address'] = f'{network_base}.{10 + i}'
            n['environment']['NUM_NODES'] = num_nodes
            n['environment']['PID'] = i
            n['environment']['TOPOLOGY'] = topology_file
            n['environment']['ALGORITHM'] = algorithm
            n['environment']['LOCATION'] = location
            n['environment']['FAULTS'] = faults
            n['environment']['MIN_MESSAGE_DELAY'] = min_message_delay
            n['environment']['MAX_MESSAGE_DELAY'] = max_message_delay
            n['environment']['DEBUG_MODE'] = debug_mode
            n['environment']['DEBUG_ALGORITHM'] = debug_algorithm
            n['environment']['OPT_ECHO_AMPLIFICATION'] = 'true' if opt_echo_amplification else 'false'
            n['environment']['OPT_SINGLE_HOP_SEND'] = 'true' if opt_single_hop_send else 'false'
            n['environment']['OPT_REDUCED_MESSAGES'] = 'true' if opt_reduced_messages else 'false'

            if i in byzantine_nodes:
                n['environment']['BYZANTINE_BEHAVIOR'] = byzantine_behavior
            else:
                n['environment']['BYZANTINE_BEHAVIOR'] = 'none'

            if i in broadcaster_nodes:
                n['environment']['NUM_BROADCASTS'] = broadcasts
                if byzantine_behavior == 'limited_broadcast':
                    n['environment']['BYZANTINE_BEHAVIOR'] = byzantine_behavior
                    n['environment']['LIMITED_NEIGHBORS'] = limited_neighbors
            else:
                n['environment']['NUM_BROADCASTS'] = 0

            nodes[f'node{i}'] = n

            # Create a ring topology
            # It will only be used when the overwrite_topology is set to True
            # connections[i] = [(i + 1) % num_nodes, (i - 1) % num_nodes]

            # # Random graph
            connections[i] = [a for (a,b) in G.edges if b==i] + [b for (a,b) in G.edges if a==i]

        content['services'] = nodes

        with open('docker-compose.yml', 'w') as f2:
            yaml.safe_dump(content, f2)
            print(f'Output written to docker-compose.yml')

        if overwrite_topology:
            with open(topology_file, 'w') as f3:
                yaml.safe_dump(connections, f3)
                print(f'Output written to {topology_file}')


@cli.command('cfg')
@click.argument('cfg_file', type=str)
def prepare_from_cfg(cfg_file: str):
    with open(cfg_file, 'r') as f:
        cfg = yaml.safe_load(f)
        # print(cfg)
        if 'template' not in cfg:
            cfg['template'] = 'docker-compose.template.yml'
        if 'location' not in cfg:
            cfg['location'] = 'cs4545'
        prepare_compose_file(cfg['num_nodes'], cfg['topology'], cfg['algorithm'], cfg['template'], cfg['location'])


@cli.command()
@click.argument('cfg_file', type=str)
@click.argument('output_dir', type=str)
@click.option('--verbose', type=bool, default=True)
@click.option('--append_file', type=str)
@click.option('--name', type=str)
def eval(cfg_file: str, output_dir: str, verbose: bool = True, append_file: Optional[str] = None,
         name: Optional[str] = None):
    if verbose:
        print('Evaluating output')
    with open(cfg_file, 'r') as f:
        cfg = yaml.safe_load(f)

    out_dir = Path(output_dir)
    out_files = {}
    for f in [x for x in out_dir.iterdir() if x.suffix == '.out']:
        with open(f, 'r') as f2:
            # Load the txt in the file
            out_files[f.stem] = [x.rstrip() for x in f2.readlines()]
    valid = 0
    invalid = 0

    node_stats = [yaml.safe_load(open(x)) for x in out_dir.iterdir() if x.suffix == '.yml']

    # Aggregate the node stats where the structure is a list of dictionaries with the same keys
    agg_stats = {}
    for key in node_stats[0].keys():
        agg_stats[key] = [x[key] for x in node_stats]
        try:
            agg_stats[key] = sum(agg_stats[key])
        except Exception:
            pass
    agg_stats['num_nodes'] = len(node_stats)
    agg_stats['algorithm'] = cfg['algorithm']

    if 'expected_output' not in cfg:
        print('No expected output found in cfg')
    else:
        for node_name in cfg['expected_output']:
            node_output = iter(out_files[node_name])
            eval_output = cfg['expected_output'][node_name]

            for expected_val in eval_output:
                try:
                    node_val = next(node_output)
                    if expected_val != node_val:
                        if verbose:
                            print(f'Output mismatch for {node_name} at {expected_val} != {node_val}')
                        invalid += 1
                    else:
                        valid += 1
                except StopIteration:
                    if verbose:
                        print('Output mismatch: Expected more output')
                    invalid += 1

    if valid + invalid == 0:
        score = 0.0
    else:
        score = (valid / float(valid + invalid)) * 100.0
    if verbose:
        print(f'Valid: {valid} Invalid: {invalid}, Score: {score:.2f}%')

        print(agg_stats)
    if append_file and name:
        print(f'Appending to {append_file} for {name}')
        csv_line = ','.join([name, str(valid), str(invalid), f'{score:.2f}'])
        with open(append_file, 'a') as f:
            f.write(csv_line)
            f.write('\n')


@cli.command()
@click.argument('topology_file', type=str)
def draw_topology(topology_file: str):
    with open(topology_file, 'r') as f:
        edges = yaml.safe_load(f)
        import networkx as nx
        import matplotlib.pyplot as plt
        G = nx.DiGraph()
        for node, connections in edges.items():
            for conn in connections:
                G.add_edge(f'{node}', f'{conn}')
        pos = nx.spring_layout(G)
        nx.draw(G, with_labels=True, pos=pos)
        plt.show()


if __name__ == '__main__':
    cli()
