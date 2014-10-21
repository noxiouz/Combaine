# -*- coding: utf-8 -*-
#
# Copyright (c) 2012+ Tyurin Anton noxiouz@yandex.ru
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


from combaine.utils import pluginload


from nose import tools


def test_plugin():
    pl = pluginload.Plugins("tests/fixtures/dummy", callable)
    assert pl._is_plugin("f.py")
    assert not pl._is_plugin("f.g")
    pl.get_plugin("a")


@tools.raises(pluginload.UnavailablePluginError)
def test_plugin_filter_is_set():
    pl = pluginload.Plugins("tests/fixtures/dummy", lambda x: False)
    pl.get_plugin("a")


@tools.raises(pluginload.UnavailablePluginError)
def test_plugin_wrong_name():
    pl = pluginload.Plugins("tests/fixtures/dummy")
    pl.get_plugin("b")