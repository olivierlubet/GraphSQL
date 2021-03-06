/**
*   Great model : https://luke.deentaylor.com/wikipedia/
*   Documentation : http://visjs.org/docs/network/
*/
var network;
var container = document.getElementById('container');
var initialized = false;

var nodes = new vis.DataSet();
var edges = new vis.DataSet();

document.getElementById('body').onkeypress = keypressEvent

//Make the network
function makeNetwork() {
  initData();
  network = new vis.Network(container,{nodes:nodes,edges:edges},options);
  bindNetwork();
  initialized=true;
}

// Bind the network events
function bindNetwork(){
    //network.on("click",clickEvent);
    //network.on("doubleClick", doubleClickEvent);
}
/*
function clickEvent (params) {
  if (params.nodes.length) { //Did the click occur on a node?
    var nodeId = params.nodes[0]; //The id of the node clicked
    expandNodeFrom(nodeId);
  }
}

function doubleClickEvent (params) {
  if (params.nodes.length) { //Did the click occur on a node?
    var nodeId = params.nodes[0]; //The id of the node clicked
    expandNodeTo(nodeId,true);
  }
}
*/

//https://www.w3schools.com/jsref/obj_keyboardevent.asp
function keypressEvent (ke) {
    //debugger
    switch (ke.key) {
        case "/" : lookForNode(); break;
        case "*" : getSelectedNode(expandSelection); break;
        case "+" : getSelectedNode(function (nodeId) {expandNodeTo(nodeId);expandNodeFrom(nodeId);}); break;
        case "<" : getSelectedNode(function (nodeId) {expandNodeTo(nodeId,true)}); break;
        case ">" : getSelectedNode(function (nodeId) {expandNodeFrom(nodeId,true)}); break;
    }
}

function getSelectedNode(callback) {
    if (network.getSelection().nodes.length != 1) {
        alert("Vous devez sélectionner un élément");
    } else {
        var nodeId = network.getSelection().nodes[0];
        callback(nodeId)
    }
}


function expandNodeFrom (nodeId, rcur=false) { // Expand all nodes from this one
    data.edges.
    filter(edge => edge.from == nodeId).
    forEach (edge => {
        if (nodes.getIds().indexOf(edge.to) == -1) {
            data.nodes.filter(node => node.id==edge.to).forEach(node => nodes.add(node));
        }
        if (!getEdgeConnecting (edge.from,edge.to)) {
            edges.add(edge);
        }
        if (rcur) expandNodeFrom(edge.to,rcur);
    })
}


function expandNodeTo (nodeId, rcur=false) { // Expand all nodes pointing TO this one
    data.edges.
    filter(edge => edge.to == nodeId).
    forEach (edge => {
        if (nodes.getIds().indexOf(edge.from) == -1) {
            data.nodes.filter(node => node.id==edge.from).forEach(node => nodes.add(node));
        }
        if (!getEdgeConnecting (edge.from,edge.to)) {
            edges.add(edge);
        }
        if (rcur) expandNodeTo(edge.from,rcur);
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
    data.nodes.filter(node => node.group=="NFDatabase").forEach(node =>nodes.add(node))
}


function lookForNode () {
    var name = prompt("base.table.champ","tiers_siren")

// TODO: remplacer par node.fullname
    if (name)
    data.nodes.filter(node => node.label.includes(name)).forEach(node => {
        if (nodes.getIds().indexOf(node.id) == -1) {
            nodes.add(node)
            expandNodeTo(node.id,true);

        }
    });
}

function expandSelection(nodeId) {
    edges.clear();
    nodes.clear();
    initData();
    data.nodes.filter(node => node.id==nodeId).forEach(node => nodes.add(node));
    expandNodeTo(nodeId,true);
    expandNodeFrom(nodeId,true);
}



makeNetwork()