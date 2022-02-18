from odata2sql.odata import Context


def work(context: Context, args):
    for entity_type in sorted(context.include, key=lambda et: et.name):
        print(f' - {entity_type.name}:')
        print(f'   - Columns:')
        for p in entity_type.proprties():
            print(f'      - name={p.name} type={p.type_info.name} nullable={p.nullable} max_length={p.max_length}'
                  f' fixed_length={p.fixed_length}')
        if multiplicity := list(context.get_dependencies_with_multiplicity(entity_type)):
            print(f'   - Directly depends on: ' + ', '.join(
                [f'{m.principal.name} ({m.principal_multiplicity})' for m in multiplicity]))
    if args.ipython:
        import IPython
        IPython.embed(colors="neutral")
