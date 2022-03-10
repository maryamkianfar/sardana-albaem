#!/usr/bin/env python
import time

from sardana import State, DataAccess
from sardana.pool import AcqSynch
from sardana.pool.controller import CounterTimerController, Type, Access, \
    Description, Memorize, Memorized, NotMemorized
from sardana.sardanavalue import SardanaValue
from functools import wraps

from sardana_albaem.ctrl.em2 import Em2


def debug_it(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._log.debug(
            "Entering {} with args={}, kwargs={} ...".format(
                func.__name__, args, kwargs
            )
        )
        output = func(self, *args, **kwargs)
        self._log.debug(
            "Leaving {} without error ... with output {} ...".format(
                func.__name__, output
            )
        )
        return output

    return wrapper

__all__ = ['Albaem2CoTiCtrl']

TRIGGER_INPUTS = {'DIO_1': 0, 'DIO_2': 1, 'DIO_3': 2, 'DIO_4': 3,
                  'DIFF_IO_1': 4, 'DIFF_IO_2': 5, 'DIFF_IO_3': 6,
                  'DIFF_IO_4': 7, 'DIFF_IO_5': 8, 'DIFF_IO_6': 9,
                  'DIFF_IO_7': 10, 'DIFF_IO_8': 11, 'DIFF_IO_9': 12}


class Albaem2CoTiCtrl(CounterTimerController):
    MaxDevice = 5

    ctrl_properties = {
        'AlbaEmHost': {
            Description: 'AlbaEm Host name',
            Type: str
        },
        'Port': {
            Description: 'AlbaEm Host name',
            Type: int
        },
        'ExtTriggerInput': {
            Description: 'ExtTriggerInput',
            Type: str
        },
    }

    ctrl_attributes = {
        'AcquisitionMode': {
            Type: str,
            # TODO define the modes names ?? (I_AVGCURR_A, Q_CHARGE_C)
            Description: 'Acquisition Mode: CHARGE, INTEGRATION',
            Access: DataAccess.ReadWrite,
            Memorize: Memorized
        },
    }

    axis_attributes = {
        "Range": {
            Type: str,
            Description: 'Range for the channel',
            Memorize: NotMemorized,
            Access: DataAccess.ReadWrite,
        },
        "Inversion": {
            Type: bool,
            Description: 'Channel Digital inversion',
            Memorize: NotMemorized,
            Access: DataAccess.ReadWrite,

        },
        "InstantCurrent": {
            Type: float,
            Description: 'Channel instant current',
            Memorize: NotMemorized,
            Access: DataAccess.ReadOnly
        },
        "Formula":
            {
                Type: str,
                Description: 'The formula to get the real value.\n '
                             'e.g. "(value/10)*1e-06"',
                Access: DataAccess.ReadWrite
            },
    }

    def __init__(self, inst, props, *args, **kwargs):
        """Class initialization."""
        CounterTimerController.__init__(self, inst, props, *args, **kwargs)
        msg = "__init__(%s, %s): Entering...", repr(inst), repr(props)
        self._log.debug(msg)

        self._em2 = Em2(self.AlbaEmHost, self.Port)
        self._synchronization = AcqSynch.SoftwareTrigger
        self._latency_time = 0.001  # In fact, it is just 320us
        self._skipp_start = False
        self._aborted_flg = False
        self._started_flg = False
        self._points_read_per_start = 0
        self._nb_points_per_start = 0
        self._last_index_point = 0
        self._new_data = {}
        self._nb_start = 0
        self._state = State.On
        self._status = 'On'

        self.formulas = {1: 'value', 2: 'value', 3: 'value', 4: 'value'}

    def _clean_variables(self):
        status = self._em2.acquisition_state
        if status in ['ACQUIRING', 'RUNNING']:
            self._em2.stop_acquisition()

        self._skipp_start = False
        self._last_index_point = 0
        self._new_data = {}
        self._aborted_flg = False
        self._started_flg = False
        self._nb_points = 0
        self._points_read_per_start = 0
        self._nb_points_per_start = 0

    def axis_channel(self, axis):
        """Return EM2 Channel object for the given controller axis"""
        return self._em2[axis - 2]

    @debug_it
    def StateAll(self):
        """Read state of all axis."""
        status = self._em2.acquisition_state
        self._log.debug('StateAll() HW status %s', status)
        allowed_states = ['ACQUIRING', 'RUNNING', 'ON',
                          'FAULT']
        if status == 'FAULT' or status not in allowed_states:
            self._state = State.Fault
            self._status = status
            return

        # The state depends of the number of point read per start
        read_ready = self._points_read_per_start == self._nb_points_per_start
        if read_ready or self._aborted_flg:
            self._state = State.On
            self._status = 'ON'
        else:
            self._state = State.Moving
            self._status = 'MOVING'
            if status == 'ON':
                self.ReadAll()
                self._log.warning('Data not ready and state is ON')

    @debug_it
    def StateOne(self, axis):
        """Read state of one axis."""
        return self._state, self._status

    @debug_it
    def PrepareOne(self, axis, value, repetitions, latency, nb_starts):
        # Protection for the integration time
        if value < 1e-4:
            raise ValueError('The minimum integration time is 0.1 ms')

        if self._synchronization in [AcqSynch.SoftwareStart,
                                     AcqSynch.HardwareStart]:
            raise ValueError('The Start synchronization is not allowed yet')

        self._clean_variables()
        self._nb_points_per_start = repetitions
        nb_points = repetitions * nb_starts
        self._acq_time = value
        latency_time = latency

        # Select the trigger mode according to the synchronization mode

        if self._synchronization in [AcqSynch.SoftwareGate,
                                     AcqSynch.SoftwareTrigger]:

            mode = 'SOFTWARE'
        elif self._synchronization == AcqSynch.HardwareTrigger:
            mode = 'HARDWARE'
            self._skipp_start = True
        elif self._synchronization == AcqSynch.HardwareGate:
            mode = 'GATE'
            self._skipp_start = True

        # Configure the electrometer
        self._em2.acquisition_time = self._acq_time
        self._em2.trigger_mode = mode
        self._em2.nb_points = nb_points
        # This controller is not ready to use the timestamp
        self._em2.timestamp_data = False

        # Arm the electromter
        self._em2.start_acquisition(soft_trigger=False)

    @debug_it
    def LoadOne(self, axis, integ_time, repetitions, latency_time):
        # Configure the electrometer on the PrepareOne
        pass

    @debug_it
    def PreStartOne(self, axis, value):
        # Check if the communication is stable before start
        try:
            _ = self._em2.acquisition_state
        except Exception:
            self._log.error('There is not connection to the electrometer.')
            return False
        return True

    @debug_it
    def StartAll(self):
        """
        Starting the acquisition is done only if before was called
        PreStartOne for master channel.
        """
        self._points_read_per_start = 0
        if self._skipp_start:
            return

        self._em2.software_trigger()

    @debug_it
    def ReadAll(self):
        # TODO Change the ACQU:MEAS command by CHAN:CURR
        data_ready = self._em2.nb_points_ready
        if self._last_index_point < data_ready:
            data_len = data_ready - self._last_index_point
            self._points_read_per_start += data_len
            self._new_data = self._em2.read(self._last_index_point, data_len)
            try:
                for axis in range(1, 5):
                    formula = self.formulas[axis]
                    if formula.lower() != 'value':
                        channel = 'CHAN0{0}'.format(axis)
                        values = self._new_data[channel]
                        values = [eval(formula, {'value': val}) for val
                                  in values]
                        self._new_data[channel] = values

                self._new_data['CHAN00'] = [self._acq_time] * data_len
                self._last_index_point = data_ready
            except Exception as e:
                raise Exception('ReadAll error: {0}'.format(e))

    @debug_it
    def ReadOne(self, axis):
        if len(self._new_data) == 0:
            return None
        axis -= 1
        channel = 'CHAN0{0}'.format(axis)
        values = list(self._new_data[channel])

        if self._synchronization in [AcqSynch.SoftwareTrigger,
                                     AcqSynch.SoftwareGate]:
            return SardanaValue(values[0])
        else:
            self._new_data[channel] = []
            return values

    @debug_it
    def AbortOne(self, axis):
        if not self._aborted_flg:
            self._aborted_flg = True
            self._em2.stop_acquisition()

###############################################################################
#                Axis Extra Attribute Methods
###############################################################################

    @debug_it
    def GetAxisExtraPar(self, axis, name):
        if axis == 1:
            raise ValueError('The axis 1 does not use the extra attributes')

        name = name.lower()
        channel = self.axis_channel(axis)
        if name == "range":
            return channel.range
        elif name == 'inversion':
            return channel.inversion
        elif name == 'instantcurrent':
            return channel.current
        elif name == 'formula':
            return self.formulas[axis-1]

    @debug_it
    def SetAxisExtraPar(self, axis, name, value):
        if axis == 1:
            raise ValueError('The axis 1 does not use the extra attributes')

        name = name.lower()
        channel = self.axis_channel(axis)
        if name == "range":
            channel.range = value
        elif name == 'inversion':
            channel.inversion = int(value)
        elif name == 'formula':
            self.formulas[axis-1] = value


###############################################################################
#                Controller Extra Attribute Methods
###############################################################################

    @debug_it
    def SetCtrlPar(self, parameter, value):
        param = parameter.lower()
        if param == 'exttriggerinput':
            self._em2.trigger_input = value
        elif param == 'acquisitionmode':
            self._em2.acquisition_mode = value
        else:
            CounterTimerController.SetCtrlPar(self, parameter, value)

    @debug_it
    def GetCtrlPar(self, parameter):
        param = parameter.lower()
        if param == 'exttriggerinput':
            value = self._em2.trigger_input
        elif param == 'acquisitionmode':
            value = self._em2.acquisition_mode
        else:
            value = CounterTimerController.GetCtrlPar(self, parameter)
        return value


def main():
    host = 'electproto19'
    port = 5025
    ctrl = Albaem2CoTiCtrl('test', {'AlbaEmHost': host, 'Port': port})
    ctrl.AddDevice(1)
    ctrl.AddDevice(2)
    ctrl.AddDevice(3)
    ctrl.AddDevice(4)
    ctrl.AddDevice(5)

    ctrl._synchronization = AcqSynch.SoftwareTrigger
    # ctrl._synchronization = AcqSynch.HardwareTrigger
    acqtime = 1.1
    ctrl.PrepareOne(1, acqtime, 1, 0.1, 1)
    ctrl.LoadOne(1, acqtime, 10, 1)
    ctrl.StartAll()
    t0 = time.time()
    ctrl.StateAll()
    while ctrl.StateOne(1)[0] != State.On:
        ctrl.StateAll()
        time.sleep(0.1)
    print(time.time() - t0 - acqtime)
    ctrl.ReadAll()
    print(ctrl.ReadOne(2))
    return ctrl

if __name__ == '__main__':
    main()
