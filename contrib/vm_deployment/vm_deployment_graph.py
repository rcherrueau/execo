#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  sans titre.py
#  
#  Copyright 2012 Laurent Pouilloux <lolo@meije>

#  

import lxml.etree as ET
import networkx as nx
import matplotlib.pyplot as plt


try:
    from networkx import graphviz_layout
except ImportError:
    raise ImportError("This script needs Graphviz and either PyGraphviz or Pydot")


tree = ET.parse('vm_deployment/input-20121126_104116.xml')
root=tree.getroot()

G=nx.Graph(name='deployment')

G.add_node('deployment',attr_dict=dict(size=1400,color='blue'))
site_nodes=[]
cluster_nodes=[]
host_nodes=[]
vm_nodes=[]
for site in root.findall('site'):
	site_nodes.append(site.get('id'))
	G.add_node(site.get('id'))
	G.add_edge('deployment',site.get('id'))
	for cluster in site.findall('cluster'):
		cluster_nodes.append(cluster.get('id'))
		G.add_nodes_from([(cluster.get('id'),dict(size=11,color='blue'))])
		G.add_edge(site.get('id'),cluster.get('id'))
		for host in cluster.findall('host'):
			host_nodes.append(host.get('id'))
			G.add_node(host.get('id'))
			G.add_edge(cluster.get('id'),host.get('id'))
			for vm in host.findall('vm'):
				vm_nodes.append(vm.get('id'))
				G.add_node(vm.get('id'))
				G.add_edge(host.get('id'),vm.get('id'))


pos=nx.graphviz_layout(G,prog='twopi',args='')


plt.figure(figsize=(15,15))
nx.draw_networkx_nodes(G,pos,nodelist=['deployment'],node_size=2000)
nx.draw_networkx_nodes(G,pos,node_size=1000,nodelist=site_nodes,node_color='goldenrod')
nx.draw_networkx_nodes(G,pos,node_size=500,nodelist=cluster_nodes,node_color='mediumaquamarine')
nx.draw_networkx_nodes(G,pos,node_size=250,nodelist=host_nodes,node_color='peru')
nx.draw_networkx_nodes(G,pos,node_size=125,nodelist=vm_nodes,node_color='tomato')
nx.draw_networkx_edges(G,pos,alpha=0.5,width=2)
nx.draw(G,pos,alpha=0.0,width=2,with_labels=True)
plt.subplots_adjust(left=0, right=1, bottom=0, top=1)
plt.axis('equal')
plt.savefig('vm_deployment/deployment-20121122_171118.png')
plt.show()



