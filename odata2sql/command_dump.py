from odata2sql.odata import Context


def work(odata: Context, args):
    for entity_type in odata.included_entity_types:
        print(f' - {entity_type.name}:')
        print(f'   - Columns:')
        for p in entity_type.proprties():
            print(f'      - name={p.name} type={p.type_info.name} nullable={p.nullable} max_length={p.max_length}')
        if entity_type_targets := odata.get_dependencies(entity_type, recursive=False):
            print(f'   - Directly depends on: ' + ', '.join(e.name for e in entity_type_targets))
    if args.ipython:
        import IPython
        IPython.embed(colors="neutral")
