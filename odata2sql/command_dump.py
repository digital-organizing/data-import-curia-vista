from odata2sql.odata import Context


def work(odata: Context, args):
    for entity_type in sorted(odata.included_entity_types, key=lambda et: et.name):
        print(f' - {entity_type.name}:')
        print(f'   - Columns:')
        for p in entity_type.proprties():
            print(f'      - name={p.name} type={p.type_info.name} nullable={p.nullable} max_length={p.max_length}'
                  f' fixed_length={p.fixed_length}')
        if entity_type_targets := odata.get_dependencies(entity_type, recursive=False):
            print(f'   - Directly depends on: ' + ', '.join(e.name for e in entity_type_targets))
        if multiplicity := list(odata.get_dependency_with_multiplicity(entity_type)):
            print(f'   - Directly depends on: ' + ', '.join(
                [f'{m.principal.name} ({m.principal_multiplicity})' for m in multiplicity]))
    if args.ipython:
        import IPython
        IPython.embed(colors="neutral")
