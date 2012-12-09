#! /usr/bin/env python
#
# Copyright (c) 2012 Tyurin Anton noxiouz@yandex-team.ru
#
# This file is part of Combaine.
#
# Combaine is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# Combaine is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import subprocess

from cocaine.decorators import timer


COMBAINER_PATH = '/usr/lib/yandex/combaine/combainer/startCombainer.py'

@timer
def cloudMain():
    try:
        p = subprocess.Popen(['python', COMBAINER_PATH], stdout = open('/dev/null','w'))
    except Exception, err:
        pass

#if __name__  == "__main__":
#    cloudMain()

