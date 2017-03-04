import cherrypy
from importlib import import_module
from flow.core import Engine


class Server(object):

    @cherrypy.expose
    def render(self):
        print(cherrypy.request.app.config)

        config = cherrypy.request.app.config['/']

        graph_factory = config['flow.graph.factory']

        p, m = graph_factory.rsplit('.', 1)
        mod = import_module(p)
        creator = getattr(mod, m)

        engine = Engine()
        creator(engine)

        page_factory = config['flow.ui.factory']
        p, m = page_factory.rsplit('.', 1)
        mod = import_module(p)
        creator = getattr(mod, m)

        return creator(engine.sort(), config)


conf = r'/Users/song/PycharmProjects/pyflow/flow/server/gojs.conf'

cherrypy.quickstart(Server(), '/', conf)
