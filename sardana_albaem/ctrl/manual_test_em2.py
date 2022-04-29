""""
Script for manually testing the Em2 class with a real electrometer.
Note:  The click dependency will have to be pip installed to use this.
"""
import time
import click
import logging
from sardana_albaem.ctrl.em2 import Em2, ZMQ_STREAMING_PORT


log = logging.getLogger('EM_TEST')


def test_scan(em, integration, repetitions, nb_starts, trig_mode, acq_mode):
    state = em.acquisition_state
    if state != 'ON':
        log.warning('State is not ON (%s) send stop', state)
        em.stop_acquisition()
        time.sleep(0.01)
        state = em.acquisition_state
        if state != 'ON':
            log.error('Not ready for acquisition after stop command, '
                      'State %s', em.acquisition_state)
            return False

    # Configure electrometer (PrepareOne)
    em.acquisition_mode = acq_mode
    em.trigger_mode = trig_mode
    em.acquisition_time = integration
    nb_points_total = repetitions * nb_starts
    em.nb_points = nb_points_total
    em.timestamp_data = False

    time_to_acquire = nb_points_total * integration
    extra_time_to_wait = 10.0
    timeout = time_to_acquire + extra_time_to_wait
    log.info("Maximum time to wait is %s seconds" % timeout)

    # Arm the electrometer
    em.start_acquisition(soft_trigger=False)

    # Check if the communication is stable before start (PreStartOne)
    if em.acquisition_state != 'RUNNING':
        log.error('State after start is not RUNNING, State %s',
                  em.acquisition_state)
    if trig_mode == 'SOFTWARE':
        for i in range(nb_starts):
            em.software_trigger()
            nb_points_expected = repetitions * (i + 1)
            if not wait_for_acquisition_to_complete(em, nb_points_expected, timeout):
                return False
            data_ready = em.nb_points_ready
            log.debug('Data ready %d', data_ready)
            new_data = em.read(data_ready - 1, 1)
            log.info('Data read %s', new_data)
            if len(new_data) != 4:
                log.error('There is not data from all 4 channels %s', new_data)
            if len(new_data['CHAN01']) != 1 or \
                    len(new_data['CHAN02']) != 1 or \
                    len(new_data['CHAN03']) != 1 or \
                    len(new_data['CHAN04']) != 1:
                log.error('There are channels without data: Point %d, '
                          'Data read %s', data_ready, new_data)
    else:
        nb_points_expected = repetitions * nb_starts
        if not wait_for_acquisition_to_complete(em, nb_points_expected, timeout):
            return False
        data_ready = em.nb_points_ready
        log.debug('Data ready %d', data_ready)
        new_data = em.read(0, data_ready)
        log.info('Data read %s', new_data)
        if len(new_data) != 4:
            log.error('There is not data from all 4 channels %s', new_data)

    return True


def wait_for_acquisition_to_complete(em, nb_points_expected, timeout):
    t0 = time.time()
    timed_out = False
    while get_acquisition_state(em, nb_points_expected) != 'DONE' and not timed_out:
        time.sleep(0.2)
        if time.time() - t0 > timeout:
            timed_out = True
            log.error('Acquisition timeout (%f), Points read %d.  Stopping acquisition!',
                      timeout, em.nb_points_ready)
            em.stop_acquisition()
    return not timed_out


def get_acquisition_state(em, nb_points_expected):
    hardware_state = em.acquisition_state
    log.debug('HW state %s', hardware_state)

    allowed_states = {'ACQUIRING', 'RUNNING', 'ON', 'FAULT'}
    if hardware_state == 'FAULT' or hardware_state not in allowed_states:
        log.error('Invalid acquisition state %r - aborting.', hardware_state)
        return 'FAULT'

    data_ready = em.nb_points_ready == nb_points_expected
    if data_ready:
        return 'DONE'
    else:
        return 'BUSY'


@click.command()
@click.argument('host')
@click.argument('port', type=click.INT)
@click.argument('nb_scans', type=click.INT)
@click.option('--integration', default=0.01, help='Integration time in seconds')
@click.option('--nb_points', default=5000, help='Number of points per scan')
@click.option('--trig_mode', default='SOFTWARE', help='Trigger mode',
              type=click.Choice(
                  ['SOFTWARE', 'HARDWARE', 'GATE', 'AUTOTRIGGER', 'HW_AUTOTRIGGER']
              ))
@click.option('--acq_mode', default='CURRENT', help='Acquisition mode',
              type=click.Choice(['CURRENT', 'CHARGE', 'STREAMING', ]))
@click.option('--debug', default=False, flag_value=True)
@click.option('--stop', default=False, flag_value=True, help='Stop acquisition after test')
@click.option('--zmq_port', type=click.INT,
              default=ZMQ_STREAMING_PORT, help="ZMQ streaming port")
def main(host, port, nb_scans, integration, nb_points, trig_mode, acq_mode, debug, stop, zmq_port):
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(level=level)
    em = Em2(host, port, zmq_port)
    em.log.setLevel(level)

    trig_mode = trig_mode.upper()
    if trig_mode == 'SOFTWARE':
        repetitions = 1
        nb_starts = nb_points
    elif trig_mode in {'HARDWARE', 'GATE', 'AUTOTRIGGER', 'HW_AUTOTRIGGER'}:
        repetitions = nb_points
        nb_starts = 1
    else:
        raise ValueError('Trigger mode %r not allowed' % trig_mode)
    result = True
    bad_test = 0
    for i in range(nb_scans):
        if not result:
            log.info('Wait 2 seconds to recover system')
            time.sleep(2)
        log.info('Start scan %d: Integration %f, Repetitions %d, Starts %d, '
                 'Trigger Mode %s, Acquisition Mode %s',
                 i + 1, integration, repetitions, nb_starts, trig_mode, acq_mode)
        result = test_scan(em, integration, repetitions, nb_starts, trig_mode, acq_mode)
        if not result:
            bad_test += 1
    log.info('-'*80)
    log.info('Failures %d of %d', bad_test, nb_scans)
    if stop:
        log.info('Stopping acquisition, as requested')
        em.stop_acquisition()


if __name__ == '__main__':
    main()
