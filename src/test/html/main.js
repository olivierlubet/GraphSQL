var network;

function redrawAll() {
    var container = document.getElementById('mynetwork');


    network = new vis.Network(container, data, options);
}

redrawAll()
