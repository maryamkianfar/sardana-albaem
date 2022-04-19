'''
ALBA EM2 client
'''

import json
import logging
import queue
import sys
import threading
import time

import zmq

PY34 = sys.version_info >= (3, 4)

if PY34:
    from sockio.sio import TCP
else:
    from sockio.py2 import TCP


class Em2Error(Exception):
    pass

SCPI_CONTROL_PORT = 5025
ZMQ_STREAMING_PORT = 22003
ZMQ_READ_TIMEOUT_MS = 50
ZMQ_RECEIVER_MAX_MESSAGES_IN_FLIGHT = 1000000

CHANNEL_MIN = 1
CHANNEL_MAX = 4

CHANNEL_TEMPLATE = """\
Channel {o.nb}:
  Range: {o.range}
  inverson: {o.inversion}"""


# TODO: Remove old style class implementation when we go to py3
class Channel(object):

    def __init__(self, em, nb):
        self.em = em
        self.nb = nb

    @property
    def range(self):
        return self.em.command('CHAN{0:02d}:CABO:RANGE?'.format(self.nb))

    @range.setter
    def range(self, value):
        self.em.command('CHAN{0:02d}:CABO:RANGE {1}'.format(self.nb, value))

    @property
    def inversion(self):
        return self.em.command('CHAN{0:02d}:CABO:INVE?'.format(self.nb)) == 'On'

    @inversion.setter
    def inversion(self, value):
        value = 'Off' if value in (0, 'off', 'OFF', 'Off') else 'On'
        self.em.command('CHAN{0:02d}:CABO:INVE {1}'.format(self.nb, value))

    @property
    def current(self):
        return float(self.em.command('CHAN{0:02d}:INSC?'.format(self.nb)))

    @property
    def voltage(self):
        return eval(self.em.command('CHAN{0:02d}:INSV?'.format(self.nb)))

    @property
    def voltage_buffer(self):
        return eval(self.em.command('CHAN{0:02d}:VOLT?'.format(self.nb)))

    @property
    def current_buffer(self):
        return eval(self.em.command('CHAN{0:02d}:CURR?'.format(self.nb)))

    def __repr__(self):
        return CHANNEL_TEMPLATE.format(o=self)


TEMPLATE = """\
{o.idn}
connection: {o.host}:{o.port}
timestamp data: {o.timestamp_data}
Acquisition:
  state: {o.acquisition_state}
  mode: {o.acquisition_mode}
  time: {o.acquisition_time}s
  nb. points: {o.nb_points}
  nb. points ready: {o.nb_points_ready}
Trigger:
  mode: {o.trigger_mode}
  input: {o.trigger_input}
  delay: {o.trigger_delay}
  polarity: {o.trigger_polarity}
  precise: {o.trigger_precision}
{channels}"""


# TODO: Remove old style class implementation when we go to py3
class AcquisitionData(object):

    def __init__(self, em2):
        self.em2 = em2

    def __getitem__(self, index):
        if isinstance(index, int):
            start, nb = index, 1
        elif isinstance(index, slice):
            start = index.start or 0
            nb = None if index.stop is None else (index.stop - start)
        return self.em2.read(start, nb)

    def __len__(self):
        return self.em2.nb_points_ready


# TODO: Remove old style class implementation when we go to py3
class Em2(object):

    def __init__(self, host, port=SCPI_CONTROL_PORT, zmq_port=ZMQ_STREAMING_PORT):
        self.host = host
        self.port = port
        self._sock = TCP(host, port)
        self._zmq_receiver = ZmqStreamReceiver(host, zmq_port)
        # TODO: Remove when sardana allows to use the configuration file
        logging.getLogger('sockio').setLevel(logging.INFO)
        self.log = logging.getLogger('em2.Em2({0}:{1})'.format(host, port))
        self.log.setLevel(logging.INFO)
        self.channels = [Channel(self, i) for i in range(CHANNEL_MIN, CHANNEL_MAX + 1)]
        self._software_version = None
        self._read_index_bug = None
        self._long_acquisition_scaling_bug = None
        self._zmq_streaming_supported = None

    @property
    def read_index_bug(self):
        if self._read_index_bug is None:
            self._read_index_bug = self.software_version <= (2, 0)
        return self._read_index_bug

    @property
    def long_acquisition_scaling_bug(self):
        if self._long_acquisition_scaling_bug is None:
            self._long_acquisition_scaling_bug = (
                    (1, 3, 5) <= self.software_version < (2, 1)
            )
        return self._long_acquisition_scaling_bug

    @property
    def zmq_streaming_supported(self):
        if self._zmq_streaming_supported is None:
            self._zmq_streaming_supported = self.software_version >= (2, 2)
        return self._zmq_streaming_supported

    @property
    def zmq_streaming_required(self):
        return self.acquisition_mode.upper() == 'FAST_BUFFER'

    def __getitem__(self, i):
        return self.channels[i]

    def open(self):
        self._sock.open()

    def commands(self, *cmds):
        cmds = [cmd.encode() + b'\n' for cmd in cmds]
        self.log.debug('-> %r', cmds)
        result = [line.strip().decode()
                  for line in self._sock.writelines_readlines(cmds)]
        self.log.debug('<- %r', result)
        return result

    def command(self, cmd):
        result = self.commands(cmd)[0]
        if result.startswith('ERROR:'):
            raise Em2Error(result.split(' ', 1)[-1])
        return result

    @property
    def idn(self):
        return self.command('*idn?')

    @property
    def software_version(self):
        if self._software_version is None:
            str_version = self.idn.split(',')[-1].strip()
            self._software_version = tuple([int(x) for x in str_version.split('.')])
        return self._software_version

    @property
    def acquisition_state(self):
        return self.command('ACQU:STAT?').split('_', 1)[1]

    @property
    def acquisition_time(self):
        return float(self.command('ACQU:TIME?')) * 1E-3

    @acquisition_time.setter
    def acquisition_time(self, t):
        self.command('ACQU:TIME {0}'.format(t*1E3))

    @property
    def nb_points(self):
        return int(self.command('ACQU:NTRIG?'))

    @nb_points.setter
    def nb_points(self, value):
        self.command('ACQU:NTRIG {0}'.format(value))

    @property
    def nb_points_ready(self):
        if self._zmq_receiver.running:
            return self._zmq_receiver.nb_points_received
        else:
            return self._get_nb_points_ready_via_scpi()

    def _get_nb_points_ready_via_scpi(self):
        return int(self.command('ACQU:NDAT?'))

    @property
    def trigger_input(self):
        return self.command('TRIG:INPU?')

    @trigger_input.setter
    def trigger_input(self, value):
        self.command('TRIG:INPU {0}'.format(value))

    @property
    def trigger_mode(self):
        return self.command('TRIG:MODE?')

    @trigger_mode.setter
    def trigger_mode(self, value):
        self.command('TRIG:MODE {0}'.format(value))

    @property
    def trigger_polarity(self):
        return self.command('TRIG:POLA?')

    @trigger_polarity.setter
    def trigger_polarity(self, value):
        self.command('TRIG:POLA {0}'.format(value))

    @property
    def trigger_precision(self):
        return self.command('TRIG:PREC?').lower() == 'true'

    @trigger_precision.setter
    def trigger_precision(self, value):
        self.command('TRIG:PREC {0}'.format('True' if value else 'False'))

    @property
    def trigger_delay(self):
        return float(self.command('TRIG:DELA?')) * 1E-3

    @trigger_delay.setter
    def trigger_delay(self, value):
        self.command('TRIG:DELA {0}'.format(value*1E3))

    def software_trigger(self):
        return self.command('TRIG:SWSE True')

    @property
    def acquisition_mode(self):
        return self.command('ACQU:MODE?')

    @acquisition_mode.setter
    def acquisition_mode(self, value):
        self.command('ACQU:MODE {0}'.format(value))

    @property
    def timestamp_data(self):
        return self.command('TMST?').lower() == 'true'

    @timestamp_data.setter
    def timestamp_data(self, value):
        self.command('TMST {0}'.format('True' if value else 'False'))

    def start_acquisition(self, soft_trigger=True):
        if self.zmq_streaming_required and self.zmq_streaming_supported:
            self._zmq_receiver.start()
        self.command('ACQU:START' + (' SWTRIG' if soft_trigger else ''))

    def stop_acquisition(self):
        self.command('ACQU:STOP True')
        if self._zmq_receiver.running:
            self._zmq_receiver.stop()

    @property
    def data(self):
        return AcquisitionData(self)

    def read(self, start_position=0, nb_points=None):
        if self._zmq_receiver.running:
            data = self._zmq_receiver.read(start_position, nb_points)
        else:
            data = self._read_via_scpi(start_position, nb_points)
        return data

    def _read_via_scpi(self, start_position=0, nb_points=None):
        if self.read_index_bug:
            start_position -= 1
        cmd = 'ACQU:MEAS? {0}'.format(start_position)
        if nb_points is not None:
            cmd += ',{0}'.format(nb_points)
        data = dict(eval(self.command(cmd)))
        if self.long_acquisition_scaling_bug:
            data = self._correct_for_long_acquisition_scaling_bug(data)
        return data

    def _correct_for_long_acquisition_scaling_bug(self, data):
        nb_samples_without_overflow = 8192
        adc_raw_sampling_rate = 200e3  # Hz
        adc_oversampling_factor = 64
        sampling_rate = adc_raw_sampling_rate / adc_oversampling_factor
        accumulator_overflow_time = nb_samples_without_overflow / sampling_rate  # sec
        nb_accumulator_overflows = int(self.acquisition_time / accumulator_overflow_time)
        if nb_accumulator_overflows > 0:
            nb_bits_lost_for_overflow = int.bit_length(nb_accumulator_overflows)
            factor = 2 ** nb_bits_lost_for_overflow
            corrected_data = {}
            for channel, values in data.items():
                corrected_data[channel] = [v * factor for v in values]
        else:
            corrected_data = data
        return corrected_data

    def __repr__(self):
        channels = '\n'.join(repr(c) for c in self.channels)
        return TEMPLATE.format(o=self, channels=channels)


class ZmqStreamReceiver(object):

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._running = False
        self._thread = None
        self._reset_worker_state()

    def _reset_worker_state(self):
        self._messages = CountableQueue()
        self._expected_frame_number = 0
        self._last_worker_error = ""

    @property
    def nb_points_received(self):
        if self._last_worker_error:
            raise RuntimeError(
                "Error in worker thread: {}".format(self._last_worker_error)
            )
        return self._messages.total_count

    def read(self, start_position, nb_points):
        nb_points = self._get_nb_messages_to_read(start_position, nb_points)
        scpi_format_data = self._prepare_scpi_format_data()
        for _ in range(nb_points):
            message = self._messages.get()
            self._abort_if_any_frames_dropped(message)
            for channel in scpi_format_data:
                scpi_format_data[channel].append(message[channel])
        return scpi_format_data

    def _get_nb_messages_to_read(self, start_position, nb_points):
        front_position = self._messages.front_position
        total_count = self._messages.total_count
        available = total_count - front_position
        if start_position != front_position:
            raise RuntimeError(
                "Reads must be from the front of the queue. {} != {}."
                    .format(start_position, front_position)
            )
        if nb_points is None:
            nb_points = available
        elif nb_points > available:
            raise RuntimeError(
                "Cannot read more items than in the queue. {} > {}."
                    .format(nb_points, available)
            )
        return nb_points

    def _abort_if_any_frames_dropped(self, message):
        frame_number = message["frame_number"]
        if frame_number != self._expected_frame_number:
            raise RuntimeError(
                "Dropped frame(s): "
                "received #{} != expected #{}. "
                "System may be overloaded, or there may be multiple ZMQ receivers "
                "running.".format(frame_number, self._expected_frame_number)
            )
        self._expected_frame_number += 1

    @staticmethod
    def _prepare_scpi_format_data():
        channel_keys = ["CHAN{:02d}".format(index)
                        for index in range(CHANNEL_MIN, CHANNEL_MAX + 1)]
        scpi_format_data = {channel: [] for channel in channel_keys}
        return scpi_format_data

    def start(self):
        if self._running:
            self.stop()
        self._thread = threading.Thread(
            target=self._zmq_worker,
            name='zmq-worker',
        )
        self._thread.daemon = True
        self._reset_worker_state()
        self._running = True
        self._thread.start()

    def stop(self):
        if self._running:
            self._running = False
            self._thread.join()

    @property
    def running(self):
        return self._running

    def _zmq_worker(self):
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.set_hwm(ZMQ_RECEIVER_MAX_MESSAGES_IN_FLIGHT)
        receiver.connect('tcp://{}:{}'.format(self._host, self._port))
        poller = zmq.Poller()
        poller.register(receiver, zmq.POLLIN)
        while self._running:
            if poller.poll(timeout=ZMQ_READ_TIMEOUT_MS):
                self._try_get_zmq_message(receiver)

    def _try_get_zmq_message(self, receiver):
        try:
            raw_message = receiver.recv()
            try:
                message = json.loads(raw_message)
                if message.get("message_type") == "data":
                    self._messages.put(message)
            except json.decoder.JSONDecodeError as exc:
                self._last_worker_error = (
                    "Error deserialising JSON message: {} => {}. "
                    "Check fast buffer code on Electrometer.".format(raw_message, exc)
                )
        except Exception as exc:
            self._last_worker_error = "General error receiving message: {}.".format(exc)


class CountableQueue(object):

    def __init__(self):
        self._queue = queue.Queue()
        self._lock = threading.Lock()
        self._front_position = 0
        self._total_count = 0

    @property
    def total_count(self):
        with self._lock:
            return self._total_count

    @property
    def front_position(self):
        with self._lock:
            return self._front_position

    def put(self, item):
        with self._lock:
            self._queue.put(item)
            self._total_count += 1

    def get(self):
        with self._lock:
            item = self._queue.get()
            self._front_position += 1
            return item


def acquire(em, acq_time=None, nb_points=None, read=True):
    start = time.time()
    try:
        return _acquire(em, acq_time, nb_points, read)
    except KeyboardInterrupt:
        em.stop_acquisition()
    finally:
        logging.info('took {0}'.format(time.time()-start))


def _acquire(em, acq_time=None, nb_points=None, read=True):
    if acq_time is not None:
        em.acquisition_time = acq_time
        em.nb_points = nb_points
    start = time.time()
    em.start_acquisition()
    time.sleep(max(acq_time-0.1, 0.001))
    while em.acquisition_state != 'ON':
        time.sleep(0.01)
    logging.info('acq took {0}'.format(time.time()-start))
    if read:
        return em.read_all()


if __name__ == '__main__':
    fmt = "%(asctime)-15s %(levelname)-5s %(name)s: %(message)s"
    logging.basicConfig(format=fmt, level=logging.INFO)
    em = Em2('electproto38')
    em.log.setLevel(logging.DEBUG)

    print(em.acquisition_mode)
