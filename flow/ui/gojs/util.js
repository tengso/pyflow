var $ = go.GraphObject.make;
var nodeBackgroundColor = '#8C8C8C';
var nodeTextColor = '#1a068c';

function init() {
    // FIXME: avoid hard-coded address here
    var wsUri = "ws://127.0.0.1:9000/ws";
    webSocket = new WebSocket(wsUri);

    myDiagram =
        $(go.Diagram, "myDiagramDiv",
            {
                initialContentAlignment: go.Spot.Left,
                initialAutoScale: go.Diagram.UniformToFill,
                layout: $(go.LayeredDigraphLayout,
                    {
                        direction: 90,
                        layeringOption: go.LayeredDigraphLayout.LayerLongestPathSink,
                        layerSpacing: 80,
                        packOption: go.LayeredDigraphLayout.PackAll,
                        initializeOption: go.LayeredDigraphLayout.InitDepthFirstIn
                    }),
                "undoManager.isEnabled": true
            }
        );

    makeGroupTemplate(myDiagram);

    myDiagram.addDiagramListener("ChangedSelection",
        function () {
            updateHighlights();
        });


    myDiagram.linkTemplate =
        $(go.Link,
            {
                routing: go.Link.AvoidsNodes,
                corner: 5,
                relinkableFrom: true,
                relinkableTo: true
            },
            $(go.Shape, {stroke: "white", strokeWidth: 3, name: 'OBJSHAPE'}),
            $(go.Shape, {stroke: "white", fill: "white", toArrow: "Standard"})
        );

    function onOpen(evt)
    {
        console.log("CONNECTED");
        webSocket.send("load");
    }

    function onClose(evt)
    {
        console.log("DISCONNECTED");
    }

    function onError(evt)
    {
        console.log("ERROR" + evt.data);
    }

    function onMessage(evt)
    {
        var links = [];
        var nodes = [];

        var message = JSON.parse(evt.data);

        if (message.message_type == 'engine_flow') {
            message.flow.forEach(function(node) {

                nodes.push({
                    "key": node.id,
                    "type": node.id,
                    "name": node.name,
                    'isGroup': node.isGroup,
                    'group': node.group
                });

                var input_ports = [];
                var output_ports = [];

                for (var i = 0; i < node.inputs.length; i++) {
                    var input = node.inputs[i];
                    input_ports.push(
                        makePort(input, "", true, (i + 1) / (node.inputs.length + 1), 0)
                    );
                }

                for (var i = 0; i < node.outputs.length; i++) {
                    var output = node.outputs[i];
                    output_ports.push(
                        makePort(output, "", false, (i + 1) / (node.inputs.length + 1), 1)
                    );
                }

                makeTemplate(node.id, "", "lightblue", input_ports, output_ports);

                node.links.forEach(function (link) {
                    links.push({
                        "from":     link.from_node,
                        "frompid":  link.from_port,
                        "to":       link.to_node,
                        "topid":    link.to_port
                    })
                })
            });

            load(nodes, links);

            // webSocket.send("run");
        }
        else if (message.message_type == "node_started") {
            nodeId = message.node
            node = myDiagram.findNodeForKey(nodeId);
            shape = node.findObject("OBJSHAPE");

            shape.stroke = 'red';
            shape.strokeWidth = 3;
            shape.fill = 'pink'
        }
        else if (message.message_type == "node_finished") {
            nodeId = message.node
            node = myDiagram.findNodeForKey(nodeId);
            shape = node.findObject("OBJSHAPE");

            shape.stroke = null;
            shape.strokeWidth = 1;
            shape.fill = nodeBackgroundColor;
        }
        else {
            console.log(message);
        }
    }

    webSocket.onopen = onOpen;
    webSocket.onclose = onClose;
    webSocket.onmessage = onMessage;
    webSocket.onerror = onError;
}

function makePort(name, label, leftside, spot_x, spot_y) {
    var port = $(go.Shape, "RoundedRectangle",
        {
            fill: "pink",
            stroke: null,
            desiredSize: new go.Size(15, 10),
            portId: name,  // declare this object to be a "port"
            toMaxLinks: 1,  // don't allow more than one link into a port
            cursor: "pointer"  // show a different cursor to indicate potential link point
            // alignment: new go.Spot(spot_x, spot_y)
        });

    var lab = $(go.TextBlock, label,  // the name of the port
        {font: "7pt sans-serif"});

    var panel = $(go.Panel, "Vertical",
        {margin: new go.Margin(0, 2)});

    // set up the port/panel based on which side of the node it will be on
    if (leftside) {
        // port.toSpot = go.Spot.Top;
        port.toLinkable = true;
        lab.margin = new go.Margin(1, 0, 0, 1);
        panel.alignment = new go.Spot(spot_x, spot_y);
        // panel.alignment = go.Spot.TopLeft;
        panel.add(port);
        panel.add(lab);
    } else {
        // port.fromSpot = go.Spot.Bottom;
        port.fromLinkable = true;
        lab.margin = new go.Margin(1, 1, 0, 0);
        // panel.alignment = go.Spot.TopRight;
        panel.add(lab);
        panel.add(port);
    }
    return panel;
}

function makeTemplate(typename, icon, background, inports, outports) {
    var node = $(go.Node, "Spot",
        $(go.Panel, "Auto",
            {width: 200, height: 90},
            $(go.Shape, "RoundedRectangle",
                {
                    fill: nodeBackgroundColor,
                    // fill: background,
                    stroke: null, strokeWidth: 0,
                    spot1: go.Spot.TopLeft, spot2: go.Spot.BottomRight,
                    name: 'OBJSHAPE'
                }),
            $(go.Panel, "Table",
                $(go.TextBlock,
                    {
                        column: 1,
                        margin: 1,
                        editable: true,
                        maxSize: new go.Size(200, 90),
                        stroke: nodeTextColor,
                        font: "16pt sans-serif"
                    },
                    new go.Binding("text", "name").makeTwoWay())
            )
        ),
        $(go.Panel, "Horizontal",
            {
                alignment: go.Spot.Top,
                alignmentFocus: new go.Spot(0.5, 0, 0, -8)
            },
            inports),
        $(go.Panel, "Horizontal",
            {
                alignment: go.Spot.Bottom,
                alignmentFocus: new go.Spot(0.5, 1, 0, 8)
            },
            outports)
    );
    myDiagram.nodeTemplateMap.add(typename, node);
}

function makeGroupTemplate(myDiagram) {
    myDiagram.groupTemplate =
        $(go.Group, "Auto",
            { // define the group's internal layout
                layout: $(go.LayeredDigraphLayout,
                    {
                        direction: 90,
                        layeringOption: go.LayeredDigraphLayout.LayerOptimalLinkLength,
                        layerSpacing: 80,
                        packOption: go.LayeredDigraphLayout.PackAll,
                        initializeOption: go.LayeredDigraphLayout.InitDepthFirstIn
                    }),
                isSubGraphExpanded: false
                // subGraphExpandedChanged: function (group) {
                //     if (group.memberParts.count === 0) {
                //         randomGroup(group.data.key);
                //     }
                // }
            },
            $(go.Shape, "Rectangle",
                {
                    fill: null,
                    stroke: "gray",
                    strokeWidth: 2
                }),
            $(go.Panel, "Vertical",
                {
                    defaultAlignment: go.Spot.Left,
                    margin: 4
                },
                $(go.Panel, "Horizontal",
                    {
                        defaultAlignment: go.Spot.Top
                    },
                    // the SubGraphExpanderButton is a panel that functions as a button to expand or collapse the subGraph
                    $("SubGraphExpanderButton"),
                    $(go.TextBlock,
                        {font: "Bold 18px Sans-Serif", margin: 4},
                        new go.Binding("text", "name"))
                ),
                $(go.Placeholder,
                    {
                        padding: new go.Margin(0, 10)
                    })
            )
        );
}

function load(nodes, links) {
    myDiagram.model = go.Model.fromJson(
        {
            "class": "go.GraphLinksModel",
            "nodeCategoryProperty": "type",
            "linkFromPortIdProperty": "frompid",
            "linkToPortIdProperty": "topid",
            "nodeDataArray": nodes,
            "linkDataArray": links
        });
}

function linksTo(x, i) {
    if (x instanceof go.Node) {
        x.findLinksInto().each(function(link) { link.highlight = i; });
    }
}

function linksFrom(x, i) {
    if (x instanceof go.Node) {
        x.findLinksOutOf().each(function(link) { link.highlight = i; });
    }
}

function nodesTo(x, i) {
    var nodesToList = new go.List("string");
    if (x instanceof go.Link) {
        x.fromNode.highlight = i;
        nodesToList.add(x.data.from);
    } else {
        x.findNodesInto().each(function(node) {
            node.highlight = i;
            nodesToList.add(node.data.key);
        });
    }
    return nodesToList;
}


function nodesFrom(x, i) {
    var nodesFromList = new go.List("string");
    if (x instanceof go.Link) {
        x.toNode.highlight = i;
        nodesFromList.add(x.data.to);
    } else {
        x.findNodesOutOf().each(function(node) {
            node.highlight = i;
            nodesFromList.add(node.data.key);
        });
    }
    return nodesFromList;
}

function updateHighlights() {
    // Set highlight to 0 for everything before updating
    myDiagram.nodes.each(function (node) {
        node.highlight = 0;
        var shape = node.findObject("OBJSHAPE");
        shape.stroke = null;
        shape.strokeWidth = 3;
    });

    myDiagram.links.each(function (link) {
        link.highlight = 0;
        var shape = link.findObject("OBJSHAPE");
        shape.stroke = "white";
        shape.strokeWidth = 3;
    });

    // Get the selected GraphObject and run the appropriate method
    var sel = myDiagram.selection.first();
    if (sel !== null) {
//        switch (e.id) {
//          case "linksIn": linksTo(sel, 1); break;
//          case "linksOut": linksFrom(sel, 1); break;
//          case "linksAll": linksAll(sel, 1); break;
//          case "nodesIn": nodesTo(sel, 1); break;
//          case "nodesOut": nodesFrom(sel, 1); break;
//          case "nodesConnect": nodesConnect(sel, 1); break;
//          case "nodesReach": nodesReach(sel, 1); break;
//          case "group": containing(sel, 1); break;
//          case "groupsAll": containingAll(sel, 1); break;
//          case "nodesMember": childNodes(sel, 1); break;
//          case "nodesMembersAll": allMemberNodes(sel, 1); break;
//          case "linksMember": childLinks(sel, 1); break;
//          case "linksMembersAll": allMemberLinks(sel, 1); break;
//        }
//         nodesConnect(sel, 4);
//         linksAll(sel, 3);
         linksTo(sel, 1);
         nodesTo(sel, 1);

         linksFrom(sel, 2);
         nodesFrom(sel, 2);
    }

    myDiagram.nodes.each(function (node) {
        var shp = node.findObject("OBJSHAPE");
        highlight(shp, null, node.highlight);
//        var grp = node.findObject("GROUPTEXT");
    });
    // links
    myDiagram.links.each(function (link) {
        var shp = link.findObject("OBJSHAPE");
        highlight(shp, null, link.highlight);
//        var arw = link.findObject("ARWSHAPE");
    });
}

// perform the actual highlighting
function highlight(shp, obj2, hl) {
    var color;
    var width = 5;

    if (hl === 1) {
        color = "orange";
    }
    else if (hl === 2) {
        color = "red";
    }
    else if (hl === 3) {
        color = "orange";
    }
    else if (hl === 4) {
        color = "red";
    }
    else {
        return;
    }
    shp.stroke = color;
    shp.strokeWidth = width;
    if (obj2 !== null) {
        obj2.stroke = color;
        obj2.fill = color;
    }
}
