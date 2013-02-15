#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os, re, json, pprint as PP
from execo_g5k.api_utils import APIConnexion, _get_api_password, get_g5k_sites
from execo_g5k.config import  g5k_configuration
from operator import itemgetter
from itertools import groupby
import pydot as P


def format_node(node_id, kind = None):
	''' '''
	if kind == 'virtual' or 'renater' in node_id:
		shape = 'rect'
		fillcolor = '#9CF7BC'
	elif kind == 'router' or 'gw' in node_id or 'router' in node_id:
		shape = 'doubleoctagon'
		fillcolor = '#BFDFF2'
	elif kind == 'switch' or 'switch' in node_id:
		fillcolor = '#F5C9CD'
		shape = 'hexagon'
	elif kind == 'cluster' or 'cluster' in node_id:
		fillcolor = '#F0F7BE'
		shape = 'diamond'
	else:
		fillcolor = 'gray'
		shape = 'ellipse'
	
	return P.Node(node_id, style="filled", fillcolor=fillcolor, shape = shape)

def format_edge(node_from, node_to, label = None, rate = None): 
	''' '''
	if rate   == 10000000000:
		color = '#9CF7BC'
		penwidth = 3
	elif rate == 1000000000:
		color = '#BFDFF2'
		penwidth = 2
	else:
		color = '#F5C9CD'
		penwidth = 1
	
	if type(label) == type([]):
		label = sorted(label)
		set = {}
		map(set.__setitem__, label, [])
		label =  set.keys()
		ranges = []
		for k, g in groupby(enumerate(label), lambda (i,x):i-x):
		    group = map(itemgetter(1), g)
		    ranges.append((group[0], group[-1]))
		
		label = ''
		for range in ranges:
			label+=	str(range[0])+'-'+str(range[1])+' '	
	elif not type(label) ==  type('str'):
		label= ''
		
		
		
	return P.Edge(nodes[node_from], nodes[node_to], penwidth = str(penwidth), color = color, label = label)

def legend(graph):
	''' '''
	legend = P.Cluster(graph_name='legend', ratio="1")
	
	legend_nodes = ['router', 'renater', 'cluster', 'switch', 'other']
	for l_node in legend_nodes:
		node = format_node(l_node)
		nodes[l_node] = node
		node.set_fontsize('10')
		node.set_height('0.3')
		legend.add_node(node)
		
	l_edges = [('renater', '10G', 10000000000), ('switch', '1G', 1000000000), ('cluster', 'radicals', 10000000000), ('other', 'unknown', 0)]
	for l_edge in l_edges:
		edge = format_edge('router', l_edge[0], label = l_edge[1] , rate = l_edge[2])
		edge.set_fontsize('10')
		legend.add_edge(edge)
		
	graph.add_subgraph(legend)
		
	

# Creating a dict with all the informations from the API
topology ={}
g5k_api=APIConnexion( "https://api.grid5000.fr/sid/", password = _get_api_password(g5k_configuration.get('api_username')))

sites = get_g5k_sites()+['backbone']

for site in sites:
	topology[site] = {}
	if site != 'backbone':
		(_, content)=g5k_api.get('sites/'+site+'/network_equipments')
	else:
		(_, content)=g5k_api.get('/network_equipments')
	site_equipments = json.loads(content)
	for ne in site_equipments['items']:
		topology[site][ne['uid']] = {'kind': ne['kind'], 'links': []}
		if ne.has_key('linecards'):
			for linecard in ne['linecards']:
				if linecard.has_key('ports'):
					for port in linecard['ports']:
						if port.has_key('uid'):
							if not port.has_key('rate'):
								if linecard.has_key('rate'):
									port['rate'] = linecard['rate']
								else:
									port['rate'] = 0
							if not port.has_key('kind'):
								if linecard.has_key('kind'):
									port['kind'] = linecard['kind']
								else:
									port['kind'] = 'renater'
							topology[site][ne['uid']]['links'].append( {'dest': port['uid'], 'rate': port['rate'], 'kind': port['kind']} )
								

# Creating the graphs for all sites + backbone
total_nodes = {}
total_edges = {}



for site, topo in topology.iteritems():
	nodes = {}
	edges = {}
	graph = P.Dot(graph_type='graph', fontname="Verdana", ratio = "0.5")

	for equip_from, info in topo.iteritems():

		if not nodes.has_key(equip_from):
			nodes[equip_from] = format_node(equip_from, info['kind'])
			
		eq_cluster ={}	
		for link in info['links']:
			if link['kind'] != 'node':
				if not nodes.has_key(link['dest']):
					nodes[link['dest']] = format_node(link['dest'], kind = link['kind'])
				if not (edges.has_key(equip_from+'__'+link['dest']) or edges.has_key(link['dest']+'__'+equip_from)):
				  	edges[equip_from+'__'+link['dest']] = format_edge(equip_from, link['dest'], rate = link['rate'])
			else:
				cluster, radical = link['dest'].split('-')
				if not eq_cluster.has_key(cluster):
					eq_cluster[cluster] = { 'radicals': [int(radical)], 'rate': link['rate'] }
				elif int(radical) not in eq_cluster[cluster]['radicals']:
					eq_cluster[cluster]['radicals'].append(int(radical))
		if eq_cluster:
			for cluster, params in eq_cluster.iteritems():
				 nodes[cluster] = format_node(cluster, kind = 'cluster')
				 edges[equip_from+'__'+cluster] = format_edge(equip_from, cluster, label = params['radicals'] ,rate = params['rate'])
					
	for node_id, node in nodes.iteritems():
		graph.add_node(node)
		if not total_nodes.has_key(node_id):
			total_nodes[node_id] = node
	
	for edge_id, edge in edges.iteritems():
		graph.add_edge(edge)
		if not total_edges.has_key(edge_id):
			total_edges[edge_id] = edge
	
	legend(graph)
	
	
	graph.write_png('graphs/'+site+'.png')
	
	
# Creating the total graph

total_graph = P.Dot(graph_type='graph')

for node_id, node in total_nodes.iteritems():
	node.set_fontsize('30')

	total_graph.add_node(node)

for edge_id, edge in total_edges.iteritems():
	total_graph.add_edge(edge)
	
legend(total_graph)
total_graph.write_svg('grid5000.svg')

