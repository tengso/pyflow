from flow.core import FlowBase


def encode_flow(engine):
    nodes = engine.sort()
    message = list()

    group_to_group = dict()
    node_to_group = dict()

    def get_groups(child, parent):
        if isinstance(child, FlowBase):
            node_to_group[id(child)] = id(parent)
        else:
            group_to_group[id(child), str(child)] = id(parent)

    engine.traverse(get_groups)

    print(node_to_group)

    for node in nodes:
        n = dict()
        node_id = id(node)
        n['id'] = str(node_id)
        n['name'] = node.name
        inputs = [str(id(input)) for input in node.inputs]
        n['inputs'] = inputs
        outputs = [str(id(output)) for output in node.outputs]
        n['outputs'] = outputs
        links = []
        for from_port in node.outputs:
            for to_port in from_port.get_children():
                link = dict(
                    from_node=str(id(node)),
                    to_node=str(id(to_port.get_owner())),
                    from_port=str(id(from_port)),
                    to_port=str(id(to_port)),
                )
                links.append(link)
        n['links'] = links
        if node_id in node_to_group:
            n['group'] = str(node_to_group[node_id])
        n['isGroup'] = False
        message.append(n)

    for (child, name), parent in group_to_group.items():
        message.append(
            dict(
                id=str(child),
                isGroup=True,
                group=str(parent),
                inputs=[],
                outputs=[],
                name=name,
                links=[]
            )
        )

    return message


