import argparse
from stem.task_tree import TaskTree, TaskNode
from stem.workspace import IWorkspace
import networkx as nx
import matplotlib.pyplot as plt


def print_structure(workspace: IWorkspace, args: argparse.Namespace):
    def pretty(d, indent=0):
        for key, value in d.items():
            print('\t' * indent + str(key))
            if isinstance(value, dict):
                pretty(value, indent + 1)
            else:
                print('\t' * (indent + 1) + str(value))

    pretty(workspace.structure())


def run_task(workspace: IWorkspace, args: argparse.Namespace):
    pass  # TODO()


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run task in workspace')
    parser.add_argument("-w", "--workspace", metavar="WORKSPACE", required=True,
                        help="Add path to workspace or file for module workspace")

    subparsers = parser.add_subparsers(metavar="command", required=True)

    parser_structure = subparsers.add_parser(
        "structure", help="Print workspace structure")
    parser_structure.set_defaults(func=print_structure)

    parser_run = subparsers.add_parser("run", help="Run task")
    parser_run.add_argument("sds", metavar='TASKPATH')
    parser_run.add_argument("-m", "--meta", metavar="META", required=True,
                            help="Metadata for task or path to file with metadata in JSON format")
    parser_run.set_defaults(func=run_task)

    return parser


def stem_cli_main():
    parser = create_parser()
    args = parser.parse_args()
    if "func" in args:
        args.func(args)
    else:
        parser.print_help()
    return 0


def draw_tree(tree: TaskTree):
    graph = nx.Graph()
    root_name = tree.root.task.name
    graph.add_node(root_name)
    for node in tree.root.dependencies:
        node_name = node.task.name
        graph.add_node(node_name)
        graph.add_edge(root_name, node_name)

    nx.draw_circular(graph,
                     node_color='red',
                     node_size=1000,
                     with_labels=True)

    ax = plt.gca()
    ax.margins(0.20)
    plt.axis("off")
    plt.show()


def draw_tree(tree: TaskTree):
    def go_along_tree(root_node: TaskNode):
        least = [(root_node, k) for k in root_node.dependencies]
        for node in root_node.dependencies:
            least += go_along_tree(node)
        return least

    graph = nx.DiGraph()
    root = tree.root
    dc = go_along_tree(root)
    labelled = {}

    for item in dc:
        graph.add_edge(item[0].task.name, item[1].task.name)
        graph.add_node(item[0].task.name)
        graph.add_node(item[1].task.name)
        labelled[item[0].task.name] = "W: {0} \n T: {1}".format(
            item[0].workspace.name, item[0].task.name)
        labelled[item[1].task.name] = "W: {0} \n T: {1}".format(
            item[1].workspace.name, item[1].task.name)

    options = {
        "pos": nx.planar_layout(graph),
        "labels": labelled,
    }
    nx.draw(graph, **options)
    plt.show()
