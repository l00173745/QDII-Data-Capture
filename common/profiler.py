"""
Profiler: Used for performance benchmarking
Usage: Wrap procedure calls with "profiler = Profiler()" and "profiler.stop()"
From Python document:
ncalls
    for the number of calls,
tottime
    for the total time spent in the given function (and excluding time made in calls to sub-functions)
percall
    is the quotient of tottime divided by ncalls
cumtime
    is the cumulative time spent in this and all subfunctions (from invocation till exit). This figure is accurate even for recursive functions.
percall
    is the quotient of cumtime divided by primitive calls
filename:lineno(function)
    provides the respective data of each function
"""

import cProfile, pstats


class Profiler:
    __profiler__ = None

    def __init__(self):
        """
        Start profiling for the upcoming procedure calls
        :return:
        """
        self.__profiler__ = cProfile.Profile()
        self.__profiler__.enable()

    def stop(self):
        """
        Stop profiling, and prints out profile result
        :return:
        """
        self.__profiler__.disable()
        # after your program ends
        ps = pstats.Stats(self.__profiler__)
        ps.sort_stats('cumtime')
        ps.print_stats(30)
