var options = {
    autoResize: true,
    height: '100%',
    width: '100%',

    layout: {
        improvedLayout:false
    },

    nodes: {
        shape: 'dot',
        size: 20,
        font: {
            size: 15,
            color: '#ffffff'
        },
        borderWidth: 2
    },
    edges: {
        arrows: {
            to:     {
            enabled: true,
            scaleFactor:1,
            type:'arrow'
            },

            from:     {
            enabled: true,
            scaleFactor:0,
            type:'bar'
            }
        },
        arrowStrikethrough:false,
        width: 2
    },
    groups: {
        Database: {
            color:'#aaa',
            shape: 'icon',
            icon: {
            code : '\uf1c0',
            color:'#aaa'
            },
            size:50,
            physics:false
        },
        Table: {
            shape: 'icon',
            icon: {code:'\uf0ce'},
            mass:50
        },
        Column: {
            color:'rgb(0,255,140)'
        }
    }
}