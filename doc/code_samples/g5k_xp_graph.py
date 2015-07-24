#!/usr/bin/env python
from execo_g5k.topology import g5k_graph, treemap
from execo.log import logger, style
from execo_g5k.oar import get_oar_job_nodes
from execo_g5k.utils import hosts_list
from networkx.algorithms.shortest_paths.generic import shortest_path
from execo_g5k.api_utils import get_host_shortname
from random import uniform

jobs = [(1696863, 'grenoble'), (1502558, 'lille'), (74715, 'luxembourg')]

logger.info('Retrieving hosts used for jobs %s',
            ', '.join([style.host(site) + ':' + style.emph(job_id)
                       for job_id, site in jobs]))
hosts = [get_host_shortname(h) for job_id, site in jobs
         for h in get_oar_job_nodes(job_id, site)]
logger.info(hosts_list(hosts))

logger.info('Creating topological graph')
g = g5k_graph(elements=hosts)

i, j = int(uniform(1, len(hosts))), int(uniform(1, len(hosts)))
path = shortest_path(g, hosts[i], hosts[j])
logger.info('Communication between %s and %s go through '
            'the following links: \n%s',
            style.host(hosts[i]),
            style.host(hosts[j]),
            ' -> '.join(path))

logger.info('Active links between nodes %s and %s are: \n%s',
            style.host(path[0]),
            style.host(path[1]),
            {k: v for k, v in g.edge[path[0]][path[1]].iteritems()
                     if v['active']})

logger.info('Generating graphical representation')
plt = treemap(g)
plt.show()

save = raw_input('Save the figure ? (y/[N]):')
if save in ['y', 'Y', 'yes']:
    outfile = 'g5k_xp_graph.png'
    plt.savefig(outfile)
    logger.info('Figure saved in %s', outfile)
