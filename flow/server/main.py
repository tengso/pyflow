import json
import datetime
import cherrypy
from importlib import import_module
from flow.core import Engine, EngineListener
from ws4py.server.cherrypyserver import WebSocketPlugin, WebSocketTool
from ws4py.websocket import WebSocket

from flow.server.util import encode_flow


class WebSocketListener(EngineListener):
    def __init__(self, socket):
        self.socket = socket

    def engine_started(self, logical_time, physical_time):
        self.socket.send(json.dumps(dict(
            message_type='engine_started',
            logical_time=str(logical_time),
            physical_time=str(physical_time)
        )))

    def engine_finished(self, logical_time, physical_time):
        self.socket.send(json.dumps(dict(
            message_type='engine_finished',
            logical_time=str(logical_time),
            physical_time=str(physical_time)
        )))

    def node_started(self, node_id, logical_time, physical_time):
        self.socket.send(json.dumps(dict(
            message_type='node_started',
            node=str(node_id),
            logical_time=str(logical_time),
            physical_time=str(physical_time)
        )))

    def node_finished(self, node_id, logical_time, physical_time):
        self.socket.send(json.dumps(dict(
            message_type='node_finished',
            node=str(node_id),
            logical_time=str(logical_time),
            physical_time=str(physical_time)
        )))


class EngineSocket(WebSocket):
    def received_message(self, message):
        command = message.data.decode()
        if command == 'load':
            config = cherrypy.request.app.config['/']
            graph_factory = config['flow.graph.factory']
            p, m = graph_factory.rsplit('.', 1)
            mod = import_module(p)
            creator = getattr(mod, m)
            start_time = datetime.datetime(2009, 1, 5)
            end_time = datetime.datetime(2016, 12, 31)

            self.engine = Engine(listener=WebSocketListener(self))
            self.start_time = start_time
            self.end_time = end_time

            creator(self.engine, start_time, end_time)
            flow = encode_flow(self.engine)
            print(flow)
            message = json.dumps(dict(
                message_type='engine_flow',
                flow=flow,
            ))
            self.send(message)
        elif command == 'run':
            self.engine.start(self.start_time, self.end_time)


class Server(object):
    @cherrypy.expose
    def ws(self):
        pass

    @cherrypy.expose
    def index(self):
        doc = """
        <!DOCTYPE html>
        <html>
            <head>
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>PyFlow Graph</title>
                <meta name="description" content="data flow graph" />
                <meta charset="UTF-8">
                <script src="../release/go.js"></script>
                <script src="util.js"></script>
            </head>
            <body onload="init()">
                <div id="myDiagramDiv"
                    style="border: solid 1px black;
                    width: 100%; background: #303030;
                    height: 1000px">
                </div>
            </body>
        </html>
        """
        return doc


if __name__ == '__main__':
    # conf = r'/Users/song/IdeaProjects/pyflow/flow/server/gojs.conf'

    conf = {
        "/": {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': "/Users/song/WebstormProjects/GoJS",
            'flow.graph.factory': "strategies.tsmom.create_backtest_flow",
            'gojs.template': "/Users/song/IdeaProjects/pyflow/flow/ui/gojs/template.html",
        },
        '/ws': {
            'tools.websocket.on': True,
            'tools.websocket.handler_cls': EngineSocket,
        },
    }

    cherrypy.config.update({'server.socket_port': 9000})
    WebSocketPlugin(cherrypy.engine).subscribe()
    cherrypy.tools.websocket = WebSocketTool()

    cherrypy.quickstart(Server(), '/', conf)
