# -*- coding: utf-8 -*-
'''
--------------------------------
- Calculate Collinearity Index
--------------------------------
'''
# libraries
from census import Census
from us import states

c = Census("MY_API_KEY")
c.acs5.get(('NAME', 'B25034_010E'),
          {'for': 'state:{}'.format(states.MD.fips)})

c.acs5.state(('NAME', 'B25034_010E'), states.MD.fips, year=2010)

c.sf1.state_county_tract('NAME', states.AK.fips, '170', Census.ALL)