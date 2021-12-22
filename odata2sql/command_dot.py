from odata2sql.odata import Context


def work(odata: Context, args):
    """Describe "foreign key" dependencies between entity types as Graphviz graph"""
    print("digraph G {")
    print("  rankdir=LR;")
    ranks = odata.get_topology()
    relevant_entities = []
    for rank, entity_types in enumerate(ranks):
        print(f"  subgraph rank_{rank} {{")
        print("    rank=same;")
        for entity_type in entity_types:
            print(f"    {entity_type.name};")
            relevant_entities.append(entity_type)
        print("  }")

    for entity_type in odata.included_entity_types:
        for dependency in odata.get_dependencies(entity_type, recursive=False):
            print(f"  {entity_type.name} -> {dependency.name};")
    print("}")
