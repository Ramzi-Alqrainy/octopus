# -*- coding: UTF-8 -*-

import sys, os, os.path

octopus_prefix=os.path.abspath((os.path.dirname(__file__)))
octopus_data=os.path.join(octopus_prefix, 'data')
octopus_templates=os.path.join(octopus_prefix, 'theme', 'templates')
octopus_static=os.path.join(octopus_prefix, 'theme', 'static')

__all__ = ('octopus_prefix', 'octopus_data', 'octopus_templates', 'octopus_static')
