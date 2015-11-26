# coding: utf-8
"""
    tcc-dac.common.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    tcc-dac common exceptions module.
    :copyright: (c) 2015 by Vito.
    :license: GNU, see LICENSE for more details.
"""


class NoDataError(Exception):
    def __init__(self, variable_name, *args, **kwargs):
        super(NoDataError, self).__init__()
        self.variable_name = variable_name
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return '{} have no data.args:{}, kwargs:{}'.format(self.variable_name, self.args, self.kwargs)

    __repr__ = __str__

