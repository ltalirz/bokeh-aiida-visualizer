"""Querying the DB
"""
from bokeh.models.widgets import RangeSlider, CheckboxButtonGroup
from config import max_points
# pylint: disable=too-many-locals
data_empty = dict(x=[0], y=[0], uuid=['1234'], color=[0], name=['no data'])

def get_data(projections, sliders_dict, quantities, plot_info):
    results, order = get_data_aiida(projections, sliders_dict, quantities, plot_info)

    # remove entries containing None
    results = [ r for r in results if None not in r ]

    nresults = len(results)
    if not results:
        plot_info.text = "No matching MOFs found."
        return data_empty
    elif nresults > max_points:
        results = results[:max_points]
        plot_info.text = "{} MOFs found.\nPlotting {}...".format(
            nresults, max_points)
    else:
        plot_info.text = "{} MOFs found.\nPlotting {}...".format(
            nresults, nresults)

    print(results[0])
    tmp = zip(*results)

    # sqla uses UUID type instead of str
    tmp[0] = list(map(str, tmp[0]))
    # x,y position
    for i in [2,3]:
        tmp[i] = list(map(float, tmp[i]))

    # replace user_id with email
    from aiida.orm import User
    user_ids = set(tmp[-1])
    emails = {}
    for user_id in user_ids:
        user = User.search_for_users(id=user_id)[0]
        emails[user_id] = user.email
    tmp[-1] = [ emails[i] for i in tmp[-1] ]

    #cif_uuids = map(str, cif_uuids)
    return { order[i]: tmp[i] for i in range(5) }


def get_data_sqla(projections, sliders_dict, quantities, plot_info):
    """Query database using SQLAlchemy.
    
    Note: For efficiency, this uses the the sqlalchemy.sql interface which does
    not go via the (more convenient) ORM.
    """
    from import_db import automap_table, engine
    from sqlalchemy.sql import select, and_

    # identifer is the filename column in sqla table
    projections = ['filename', 'name'] + projections

    Table = automap_table(engine)

    selections = []
    for label in projections:
        selections.append(getattr(Table, label))

    filters = []
    for k, v in sliders_dict.items():
        if isinstance(v, RangeSlider):
            if not v.value == quantities[k]['range']:
                f = getattr(Table, k).between(v.value[0], v.value[1])
                filters.append(f)
        elif isinstance(v, CheckboxButtonGroup):
            if not len(v.active) == len(v.labels):
                f = getattr(Table, k).in_([v.tags[i] for i in v.active])
                filters.append(f)

    s = select(selections).where(and_(*filters))

    order = [ 'identifier', 'name', 'x', 'y', 'color' ]
    return engine.connect().execute(s).fetchall()


def get_data_aiida(projections, sliders_dict, quantities, plot_info):
    """Query AiiDA database"""
    from aiida import load_dbenv, is_dbenv_loaded
    from aiida.backends import settings
    if not is_dbenv_loaded():
        load_dbenv(profile=settings.AIIDADB_PROFILE)
    from aiida.orm import Group, QueryBuilder, DataFactory, CalculationFactory, WorkCalculation

    ## identifer is the uuid attribute in aiida 

    projections = [ 'attributes.{}'.format(p) for p in projections ]
    print(projections)

    filters = {}

    def add_range_filter(bounds, label):
        # a bit of cheating until this is resolved
        # https://github.com/aiidateam/aiida_core/issues/1389
        #filters['attributes.'+label] = {'>=':bounds[0]}
        filters['attributes.' + label] = {
            'and': [{
                '>=': bounds[0]
            }, {
                '<': bounds[1]
            }]
        }

    for k, v in sliders_dict.items():
        if isinstance(v, RangeSlider):
            # Note: filtering is costly, avoid if possible
            if not v.value == quantities[k]['range']:
                pass
                #add_range_filter(v.value, k)


    #zeopp = Group.get_from_string('20190111-092507:import')
    ParameterData = DataFactory('parameter')
    CifData = DataFactory('cif')

    from reentry import manager
    manager.scan()
    ZeoppCalculation = CalculationFactory('zeopp.network')

    qb = QueryBuilder()
    qb.append(CifData, project=['uuid', 'attributes.filename'], tag='cifs')
    qb.append(ZeoppCalculation, tag='calc', output_of='cifs')
    #qb.append(ParameterData, project='*', output_of='calc')
    qb.append(ParameterData, project=[projections[0]], output_of='calc')
    qb.append(WorkCalculation, tag='wf', output_of='cifs')
    #qb.append(ParameterData, project='*', output_of='wf')
    qb.append(ParameterData, project=[projections[1], 'user_id'], output_of='wf')
    #qb.limit(100)
    # note: custom projections make the query *extremely* slow

    results = qb.all()
    # This reads from the DB!
    #p_dicts = [ row[2].get_dict().update(row[3]).get_dict() for row in results ]
    #p_dicts = [ row[2]._dbnode.attributes.update(row[3]._dbnode.attributes) for row in results ]
    order = [ 'identifier', 'name', 'x', 'y', 'color']

    return results, order
