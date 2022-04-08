#!/usr/bin/env python
import socket
import time
import datetime
from threading import Lock

from sardana import State, DataAccess
from sardana.pool import AcqSynch
from sardana.pool.controller import OneDController, Type, Access, \
    Description, Memorize, Memorized, NotMemorized, FGet, FSet, DefaultValue
from sardana.sardanavalue import SardanaValue
from functools import wraps, partial
import six

__all__ = ['Albaem2OneDCtrl']

MINIMUM_INTEGRATION_TIME_MS = 0.1
# for details of the maximum time calculation see em2.py:
#   Em2._correct_for_long_acquisition_scaling_bug
MAXIMUM_INTEGRATION_TIME_WITHOUT_OVERFLOW_MS = 2621.0


def debug_it(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):

        self._log.debug("Entering {} with args={}, kwargs={}".format(
            func.__name__, args, kwargs))
        output = func(self, *args, **kwargs)
        self._log.debug("Leaving without error {} with output {}".format(func.__name__, output))
        return output
    return wrapper


def handle_error(func=None, msg="Error with Albaem2OneDCtrl"):
    if func is None:
        return partial(handle_error, msg=msg)
    else:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                six.raise_from(RuntimeError(msg), e)
        return wrapper


class Albaem2OneDCtrl(OneDController):
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
            Type: str,
            DefaultValue: "TRIGGER_IN",
        },
    }

    ctrl_attributes = {
        'AcquisitionMode': {
            Type: str,
            Description: 'Acquisition Mode: CHARGE, INTEGRATION',
            Access: DataAccess.ReadWrite,
            Memorize: Memorized,
            FGet: "get_AcquisitionMode",
            FSet: "set_AcquisitionMode"
        },
        'PointsPerStep': {
            Type: int,
            Description: "Points to generate or Triggers to expect per step. \
                          Only applicable for the step scan. \
                          For multiple points per step.",
            Access: DataAccess.ReadWrite,
            Memorize: Memorized,
            FGet: "get_PointsPerStep",
            FSet: "set_PointsPerStep"
        },
    }

    axis_attributes = {
        "Range": {
            Type: str,
            Description: 'Range for the channel',
            Memorize: NotMemorized,
            Access: DataAccess.ReadWrite,
            FGet: "get_Range",
            FSet: "set_Range",
        },
        "Inversion": {
            Type: bool,
            Description: 'Channel Digital inversion',
            Memorize: NotMemorized,
            Access: DataAccess.ReadWrite,
            FGet: "get_Inversion",
            FSet: "set_Inversion",

        },
        "InstantCurrent": {
            Type: float,
            Description: 'Channel instant current',
            Memorize: NotMemorized,
            Access: DataAccess.ReadOnly,
            FGet: "get_InstantCurrent",
        },
        "FORMULA": {
            Type: str,
            Description: 'The formula to get the real value.\n '
                            'e.g. "(value/10)*1e-06"',
            Access: DataAccess.ReadWrite
        },
    }

    @handle_error(msg="__init__: Could not connect to the device!")
    def __init__(self, inst, props, *args, **kwargs):
        """Class initialization."""
        OneDController.__init__(self, inst, props, *args, **kwargs)
        self.ip_config = (self.AlbaEmHost, self.Port)
        self.albaem_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.albaem_socket.settimeout(1)
        self.albaem_socket.connect(self.ip_config)
        self.itime = 0.0
        self.master = None
        self._latency_time = 0.001  # In fact, it is just 320us
        self._repetitions = 0
        self.formulas = {1: 'value', 2: 'value', 3: 'value', 4:'value'}
        self._points_per_step = 1
        self._is_aborted = False
        self.lock = Lock()

    @debug_it
    def AddDevice(self, axis):
        """Add device to controller."""
        pass

    @debug_it
    def DeleteDevice(self, axis):
        """Delete device from the controller."""
        # self.albaem_socket.close()
        pass

    @debug_it
    def PrepareOne(self, axis, value, repetitions, latency, nb_starts):
        self._is_aborted = False


    @debug_it
    def PreStateAll(self):
        pass

    @debug_it
    def StateAll(self):
        """Read state of all axis."""
        state = self.sendCmd('ACQU:STAT?')

        if state in ['STATE_ACQUIRING', 'STATE_RUNNING']:
            self.state = State.Moving

        elif state == 'STATE_ON':
            self.state = State.On

        elif state == 'STATE_FAULT':
            self.state = State.Fault

        else:
            self.state = State.Fault
        self.status = state

    @debug_it
    def StateOne(self, axis):
        """Read state of one axis."""
        return self.state, self.status

    @debug_it
    @handle_error(msg="LoadOne: Could not configure the device!")
    def LoadOne(self, axis, value, repetitions, latency_time):
        if axis != 1:
            raise Exception('The master channel should be the axis 1')

        self.itime = value

        # Set Integration time in ms
        val = self.itime * 1000
        error_msg = ""
        if val < MINIMUM_INTEGRATION_TIME_MS:
            error_msg = (
                "Requested integration time, {} ms, too short. "
                "Minimum is {} ms.".format(val, MINIMUM_INTEGRATION_TIME_MS)
            )
        elif val > MAXIMUM_INTEGRATION_TIME_WITHOUT_OVERFLOW_MS:
            error_msg = (
                "Requested integration time, {} ms, too long. "
                "Maximum without overflow is {} ms.".format(
                    val, MAXIMUM_INTEGRATION_TIME_WITHOUT_OVERFLOW_MS
                )
            )
        if error_msg:
            self._log.error(error_msg)
            raise Exception(error_msg)
        self.sendCmd('ACQU:TIME %r' % val)

        if self._synchronization in [AcqSynch.SoftwareTrigger,
                                     AcqSynch.SoftwareGate]:
            self._repetitions = 1
            source = 'SOFTWARE'

        elif self._synchronization == AcqSynch.HardwareTrigger:
            source = 'HARDWARE'
            self._repetitions = repetitions
            if repetitions == 1:
                self._repetitions = self._points_per_step
        elif self._synchronization == AcqSynch.HardwareGate:
            source = 'GATE'
            self._repetitions = repetitions
            if repetitions == 1:
                self._repetitions = self._points_per_step
        self.sendCmd('TRIG:MODE %s' % source)
        if self._synchronization in [AcqSynch.HardwareTrigger,
                                     AcqSynch.HardwareGate]:
            self.sendCmd('TRIG:INPU %s' % self.ExtTriggerInput)
        # Set Number of Triggers
        self.sendCmd('ACQU:NTRI %r' % self._repetitions)

        # Array of arrays for ID readings from all channels
        self.new_data = [[] for index in range(0, 5)]

    @debug_it
    @handle_error(msg="PreStartOne: Could not configure the device!")
    def PreStartOne(self, axis, value):
        #Check if the communication is stable before start
        state = self.sendCmd('ACQU:STAT?')
        if state is None:
            return False

        return True

    @debug_it
    @handle_error(msg="StartAll: Could not configure the device!")
    def StartAll(self):
        """
        Starting the acquisition is done only if before was called
        PreStartOneCT for master channel.
        """
        cmd = 'ACQU:START'
        if self._synchronization in [AcqSynch.SoftwareTrigger,
                                     AcqSynch.SoftwareGate]:
            # The HW needs the software trigger
            # APPEND SWTRIG TO THE START COMMAND OR SEND ANOTHER COMMAND
            # TRIG:SWSEt
            cmd += ' SWTRIG'

        self.sendCmd(cmd)
        # THIS PROTECTION HAS TO BE REVIEWED
        # FAST INTEGRATION TIMES MAY RAISE WRONG EXCEPTIONS
        # e.g. 10ms ACQTIME -> self.state MAY BE NOT MOVING BECAUSE
        # FINISHED, NOT FAILED
        self.StateAll()
        t0 = time.time()
        while (self.state != State.Moving):
            if time.time() - t0 > 3:
                raise Exception('The HW did not start the acquisition')
            self.StateAll()
        return True

    @debug_it
    def StartOne(self, axis, value):
        pass

    @debug_it
    @handle_error(msg="ReadAll: Unable to read from the device!")
    def ReadAll(self):
        self.new_data = [[] for index in range(0, 5)]
        # Skip reading for aborted scans
        if self._is_aborted:
            return
        data_ready = int(self.sendCmd('ACQU:NDAT?'))

        # THIS CONTROLLER IS NOT YET READY FOR TIMESTAMP DATA
        self.sendCmd('TMST 0')

        msg = 'ACQU:MEAS? %r,%r' % (0, data_ready)
        raw_data = self.sendCmd(msg)

        data = eval(raw_data)
        axis = 1
        for chn_name, values in data:

            # Apply the formula for each value
            formula = self.formulas[axis]
            formula = formula.lower()
            values_formula = [eval(formula, {'value': val}) for val
                                in values]
            self.new_data[axis].extend(values_formula)
            axis +=1
        time_data = [self.itime] * len(self.new_data[1])
        self.new_data[0] = (time_data)


    @debug_it
    def ReadOne(self, axis):
        if len(self.new_data) == 0:
            return None

        if self._synchronization in [AcqSynch.SoftwareTrigger,
                                     AcqSynch.SoftwareGate]:
            return [self.new_data[axis - 1][0]]
        else:
            val = self.new_data[axis - 1]
            return [val]

    @debug_it
    @handle_error(msg="AbortOne: Could not abort device!")
    def AbortOne(self, axis):
        self.sendCmd('ACQU:STOP')
        self._is_aborted = True

    @debug_it
    @handle_error(msg="sendCmd: Could not configure device!")
    def sendCmd(self, cmd, rw=True, size=8096):
        with self.lock:
            cmd += ';\n'

            # Protection in case of reconnect the device in the network.
            # It send the command and in case of broken socket it creates a
            # new one.
            retries = 2
            for i in range(retries):
                try:
                    self.albaem_socket.sendall(cmd.encode())
                    break
                except socket.timeout:
                    self._log.debug(
                        'Socket timeout! reconnecting and commanding '
                        'again %s' % cmd)
                    self.albaem_socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM)
                    self.albaem_socket.settimeout(1)
                    self.albaem_socket.connect(self.ip_config)
            if rw:
                # WARNING...
                # socket.recv(size) IS NEVER ENOUGH TO RECEIVE DATA !!!
                # you should know by the protocol either:
                # the length of data to be received
                # or
                # wait until a special end-of-transfer control
                # In this case: while not '\r' in data:
                #                 receive more data...
                ################################################
                # AS IT IS SAID IN https://docs.python.org/3/howto/sockets.html
                # SECTION "3 Using a Socket"
                #
                # A protocol like HTTP uses a socket for only one
                # transfer. The client sends a request, the reads a
                # reply. That's it. The socket is discarded. This
                # means that a client can detect the end of the reply
                # by receiving 0 bytes.
                #
                # But if you plan to reuse your socket for further
                # transfers, you need to realize that there is no
                # "EOT" (End of Transfer) on a socket. I repeat: if a
                # socket send or recv returns after handling 0 bytes,
                # the connection has been broken. If the connection
                # has not been broken, you may wait on a recv forever,
                # because the socket will not tell you that there's
                # nothing more to read (for now). Now if you think
                # about that a bit, you'll come to realize a
                # fundamental truth of sockets: messages must either
                # be fixed length (yuck), or be delimited (shrug), or
                # indicate how long they are (much better), or end by
                # shutting down the connection. The choice is entirely
                # yours, (but some ways are righter than others).
                ################################################
                data = ""
                acquired = False
                while True:
                    # SOME TIMEOUTS OCCUR WHEN USING THE WEBPAGE
                    retries = 5
                    for i in range(retries):
                        try:
                            data += self.albaem_socket.recv(size).decode()
                            acquired = True
                            break
                        except socket.timeout:
                            self._log.debug(
                                'Socket timeout! Reading... from  %s '
                                'command' %cmd[:-2])
                            self.albaem_socket = socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM)
                            self.albaem_socket.settimeout(1)
                            self.albaem_socket.connect(self.ip_config)
                            self.albaem_socket.sendall(cmd.encode())
                            pass

                    if acquired == False:
                        msg = "Unable to communicate with AlbaEm2, try to " \
                              "restart the Device"
                        raise RuntimeError(msg)
                    try:
                        if data[-1] == '\n':
                            break
                    except Exception as e:
                        self._log.error(e)
                        return None

                # NOTE: EM MAY ANSWER WITH MULTIPLE ANSWERS IN CASE OF AN
                # EXCEPTION
                # SIMPLY GET THE LAST ONE
                if data.count(';') > 1:
                    data = data.rsplit(';')[-2:]
                return data[:-2]

###############################################################################
#                Axis Extra Attribute Methods
###############################################################################

    @debug_it
    @handle_error(msg="get_Range:")
    def get_Range(self, axis):
        if axis == 1:
            raise RuntimeError('The axis 1 does not use the extra attributes')
        axis -= 1
        cmd = 'CHAN{0:02d}:CABO:RANGE?'.format(axis)
        return self.sendCmd(cmd)

    @debug_it
    @handle_error(msg="set_Range:")
    def set_Range(self, axis, value):
        if axis == 1:
            raise RuntimeError('The axis 1 does not use the extra attributes')
        axis -= 1
        cmd = 'CHAN{0:02d}:CABO:RANGE {1}'.format(axis, value)
        self.sendCmd(cmd)

    @debug_it
    @handle_error(msg="get_Inversion:")
    def get_Inversion(self, axis):
        if axis == 1:
            raise RuntimeError('The axis 1 does not use the extra attributes')
        axis -= 1
        cmd = 'CHAN{0:02d}:CABO:INVE?'.format(axis)
        val = self.sendCmd(cmd)
        if val.lower() == 'off':
            ret = False
        elif val.lower() == 'on':
            ret = True
        return ret

    @debug_it
    @handle_error(msg="set_Inversion:")
    def set_Inversion(self, axis, value):
        if axis == 1:
            raise RuntimeError('The axis 1 does not use the extra attributes')
        axis -= 1
        cmd = 'CHAN{0:02d}:CABO:INVE {1}'.format(axis, int(value))
        self.sendCmd(cmd)

    @debug_it
    @handle_error(msg="get_InstantCurrent:")
    def get_InstantCurrent(self, axis):
        if axis == 1:
            raise RuntimeError('The axis 1 does not use the extra attributes')
        axis -= 1
        cmd = 'CHAN{0:02d}:INSCurrent?'.format(axis)
        return eval(self.sendCmd(cmd))


###############################################################################
#                Controller Extra Attribute Methods
###############################################################################

    @debug_it
    @handle_error(msg="get_AcquisitionMode:")
    def get_AcquisitionMode(self):
        value = self.sendCmd('ACQU:MODE?')
        return value

    @debug_it
    @handle_error(msg="set_AcquisitionMode:")
    def set_AcquisitionMode(self, value):
        self.sendCmd('ACQU:MODE %s' % value)

    @debug_it
    @handle_error(msg="get_PointsPerStep:")
    def get_PointsPerStep(self):
        return self._points_per_step

    @debug_it
    @handle_error(msg="set_PointsPerStep:")
    def set_PointsPerStep(self, value):
        self._points_per_step = value
