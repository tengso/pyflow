def make_ports(port_names, is_input):
    return '[' + \
           ','.join(['makePort("{}", "{}", {})'.format(name, "", str(is_input).lower()) for name in port_names]) \
           + ']'


def make_node_template(template_name, input_port_names, output_port_names, color):
    return 'makeTemplate("{}", "image/55.png", "{}", {}, {})'.format(
        template_name,
        color,
        make_ports(input_port_names, True),
        make_ports(output_port_names, False),
    )


def make_node(node_id, node_name):
    n = '{' + '"key": "{}", "type": "{}", "name": "{}"'.format(node_id, node_id, node_name) + '}'
    return n


def make_link(from_node_id, to_node_id, from_port, to_port):
    l = '{' + '"from": "{}", "frompid": "{}", "to": "{}", "topid": "{}"'.format(
        from_node_id, from_port, to_node_id, to_port
    ) + '}'
    return l


def make_all(node):
    inputs = [str(id(input)) for input in node.inputs]
    outputs = [str(id(output)) for output in node.outputs]
    template_code = make_node_template(id(node), inputs, outputs, 'lightblue')
    node_code = make_node(id(node), node.name)
    link_code = []
    for from_port in node.outputs:
        for to_port in from_port.get_children():
            link_code.append(make_link(id(node), id(to_port.get_owner()), id(from_port), id(to_port)))

    return template_code, node_code, link_code


def make_page(nodes, conf):
    template_code = []
    nodes_code = []
    links_code = []

    for node in nodes:
        template, node, link = make_all(node)
        template_code.append(template)
        nodes_code.append(node)
        links_code.extend(link)

    template = ';'.join(template_code)
    nodes = ','.join(nodes_code)
    links = ','.join(links_code)

    path = conf['gojs.template']

    with open(path) as f:
        lines = f.readlines()
        page = '\n'.join(lines)
        page = page.replace('$TEMPLATES$', template)
        page = page.replace('$NODES$', nodes)
        page = page.replace('$LINKS$', links)

        return page
