/**
*   Great model : https://luke.deentaylor.com/wikipedia/
*   Documentation : http://visjs.org/docs/network/
*/
var network;
var container = document.getElementById('container');
var initialized = false;

var nodes = new vis.DataSet();
var edges = new vis.DataSet();



//Make the network
function makeNetwork() {
  initData();
  network = new vis.Network(container,{nodes:nodes,edges:edges},options);
  bindNetwork();
  initialized=true;
}

// Bind the network events
function bindNetwork(){
    network.on("click",clickEvent);

    network.on("doubleClick", doubleClickEvent);
}

function clickEvent (params) {
  if (params.nodes.length) { //Did the click occur on a node?
    var nodeId = params.nodes[0]; //The id of the node clicked
    expandNodeFrom(nodeId);
  }
}

function doubleClickEvent (params) {
  if (params.nodes.length) { //Did the click occur on a node?
    var nodeId = params.nodes[0]; //The id of the node clicked
    expandNodeToR(nodeId);
  }
}

function expandNodeFrom (nodeId) { // Expand all nodes from this one
    data.edges.
    filter(edge => edge.from == nodeId).
    forEach (edge => {
        if (nodes.getIds().indexOf(edge.to) == -1) {
            data.nodes.filter(node => node.id==edge.to).forEach(node => nodes.add(node));
        }
        if (!getEdgeConnecting (edge.from,edge.to)) {
            edges.add(edge);
        }
    })
}

function expandNodeToR (nodeId) { // Expand all nodes pointing TO this one RECURSIVELY
    data.edges.
    filter(edge => edge.to == nodeId).
    forEach (edge => {
        if (nodes.getIds().indexOf(edge.from) == -1) {
            data.nodes.filter(node => node.id==edge.from).forEach(node => nodes.add(node));
        }
        if (!getEdgeConnecting (edge.from,edge.to)) {
            edges.add(edge);
        }
        expandNodeToR(edge.from);
    })
}

// Get the id of the edge connecting two nodes a and b
function getEdgeConnecting(a, b) {
  var edge = edges.get({filter:function(edge) {
    return edge.from === a && edge.to === b;
  }})[0];
  if (edge instanceof Object) {
    return edge.id;
  }
}

function initData() {
    data.nodes.filter(node => node.group=="NFDatabase").forEach(node => nodes.add(node))
}


makeNetwork()