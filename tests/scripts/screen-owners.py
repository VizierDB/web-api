from vizier.client.database import VizierDBClient

vizier = VizierDBClient(__vizierdb__)
screens = vizier.get_dataset('screens')

for row in screens.rows:
    val = row.get_value('screen_name')
    if val.endswith(' Screen'):
        val = val[:-7]
    row.set_value('owner', val)
    print 'Owner of \'' + row.get_value('screen_name') + '\' is ' + val
vizier.update_dataset('screens', screens)
