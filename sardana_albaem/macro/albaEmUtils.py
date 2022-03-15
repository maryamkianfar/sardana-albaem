import time
from sardana.macroserver.macro import Macro, Type
from taurus import Device, Attribute
from taurus.core import AttrQuality
import PyTango

#TODO Change to taurus in TEP14
STATE_MOVING = PyTango.DevState.MOVING

RANGES = ['1mA', '100uA', '10uA', '1uA', '100nA', '10nA', '1nA', '100pA']


class em_range(Macro):
    """
    Macro to change the electrometer range.
    """
    param_def = [['chns',
                  [['ch', Type.CTExpChannel, None, 'electrometer chn'],
                   ['range',  Type.String, None, 'Amplifier range'],
                   {'min': 1}],
                  None, 'List of [channels,range]']]
    enabled_output = True

    def run(self, chns):
        for ch, rg in chns:
            old_range = ch.read_attribute("Range").value
            ch.write_attribute("Range", rg)
            new_range = ch.read_attribute("Range").value
            if self.enabled_output:
                self.output('%s changed range from %s to %s' % (ch, old_range,
                                                                new_range))


class em_inversion(Macro):
    """
        Macro to change the the polarity.
    """
    param_def = [['chns',
                  [['ch', Type.CTExpChannel, None, 'electrometer chn'],
                   ['enabled', Type.Boolean, None, 'Inversion enabled'],
                   {'min': 1}],
                  None, 'List of [channels, inversion]'], ]

    def run(self, chns):
        for ch, enabled in chns:
            old_state = ch.read_attribute('Inversion').value
            ch.write_attribute('Inversion', enabled)
            new_state = ch.read_attribute('Inversion').value
            self.output('%s changed inversion from %s to %s' % (ch, old_state,
                                                                new_state))


class em_autorange(Macro):
    """
        Macro to start the autorange.
    """
    param_def = [['chns',
                  [['ch', Type.CTExpChannel, None, 'electrometer chn'],
                   ['enabled', Type.Boolean, None, 'Inversion enabled'],
                   {'min': 1}],
                  None, 'List of [channels, inversion]'] ]

    def run(self, chns):
        for ch, enabled in chns:
            old_state = ch.read_attribute('Autorange').value
            ch.write_attribute('Autorange', enabled)
            new_state = ch.read_attribute('Autorange').value
            self.output('{0} changed autorange from {1} '
                        'to {2}'.format(ch, old_state, new_state))


class em_findrange(Macro):
    """
        Macro to find the range.
    """
    param_def = [['chns',
                  [['ch', Type.CTExpChannel, None, 'electrometer chn'],
                   {'min': 1}],
                  None, 'List of [channels]'],
                 ['wait_time', Type.Float, 3, 'time to applied the '
                                               'autorange']]

    def run(self, chns, wait_time):
        chns_enabled = [[chn, True] for chn in chns]
        chns_desabled = [[chn, False] for chn in chns]
        self.em_autorange(chns_enabled)
        t1 = time.time()
        try:
            while time.time()-t1 < wait_time:
                self.checkPoint()
                time.sleep(0.01)
        finally:
            self.em_autorange(chns_desabled)


class em_findmaxrange(Macro):
    """
    Macro to find the electrometer channel range according to the motor
    position

    """
    param_def = [['motor', Type.Moveable, None, 'motor to scan'],
                 ['positions',
                  [['pos', Type.Float, None, 'position'], {'min': 1}],
                  None, 'List of positions'],
                 ['channels',
                  [['chn', Type.CTExpChannel, None, 'electrometer channel'],
                   {'min': 1}],
                  None, 'List of channels'],
                 ['wait_time', Type.Float, 3, 'time to applied the autorange'],
                 ]

    RANGES = ['1mA', '100uA', '10uA', '1uA', '100nA', '10nA', '1nA',
              '100pA', 'none']

    def run(self, motor, positions, chns, wait_time):
        chns_ranges = {}
        previous_chns_ranges = {}

        for chn in chns:
            chns_ranges[chn] = 'none'
            previous_chns_ranges[chn] = chn.range
            self.debug('{0}: {1}'.format(chn.name, 'none'))

        for energy in positions:
            self.umv(motor, energy)

            self.em_findrange(chns, wait_time)
            for chn, prev_range in chns_ranges.items():
                new_range = chn.range
                new_range_idx = self.RANGES.index(new_range)
                prev_range_idx = self.RANGES.index(prev_range)
                if new_range_idx < prev_range_idx:
                    chns_ranges[chn] = new_range

        self.info('Setting maximum range...')
        chns_cfg = []
        for chn, new_range in chns_ranges.items():
            chns_cfg.append([chn, new_range])
        em_range, _ = self.createMacro('em_range', chns_cfg)
        em_range.enabled_output = False
        self.runMacro(em_range)
        for chn, new_range in chns_ranges.items():
            prev_range = previous_chns_ranges[chn]
            self.output('{0} changed range from {1} ' 
                        'to {2}'.format(chn, prev_range, new_range))


