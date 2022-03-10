import time
import click
import logging
from sardana_albaem.ctrl.em2 import Em2


log = logging.getLogger('EM_TEST')


def test_scan(em, integration, repetitions, nb_starts, mode):
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
    em.acquisition_mode = mode
    em.acquisition_time = integration
    em.nb_points = repetitions * nb_starts
    em.timestamp_data = False
    timeout = integration * 200
    data_ready = 0

    # Arm the electrometer
    em.start_acquisition(soft_trigger=False)

    # Check if the communication is stable before start (PreStartOneCT)
    if em.acquisition_state != 'RUNNING':
        log.error('State after start is not RUNNING, State %s',
                  em.acquisition_state)
    if mode == 'SOFTWARE':
        for i in range(nb_starts):
            em.software_trigger()
            t0 = time.time()
            while em.acquisition_state == 'ACQUIRING':
                log.debug('State %s', em.acquisition_state)
                time.sleep(0.01)
                if time.time() - t0 > timeout:
                    log.error('Acquisition timeout (%f), Point number %d',
                              timeout, data_ready)
                    em.stop_acquisition()
                    return False
            data_ready = em.nb_points_ready
            log.debug('Data ready %d', data_ready)
            new_data = em.read(data_ready, 1)
            log.debug('Data read %s', new_data)
            if len(new_data) != 4:
                log.error('There are not all channels %s', new_data)
            if len(new_data['CHAN01']) != 1 or \
                    len(new_data['CHAN02']) != 1 or \
                    len(new_data['CHAN03']) != 1 or \
                    len(new_data['CHAN04']) != 1:
                log.error('There are channels without data: Point %d, '
                          'Data read %s', data_ready, new_data)

    return True


@click.command()
@click.argument('host')
@click.argument('port', type=click.INT)
@click.argument('nb_scans', type=click.INT)
@click.option('--integration', default=0.01, help='Integration time')
@click.option('--nb_points', default=5000, help='Number of point per scan')
@click.option('--mode', default='SOFTWARE', help='Trigger mode',
              type=click.Choice(['SOFTWARE', 'HARDWARE', 'GATE']))
@click.option('--debug', default=False, flag_value=True)
def main(host, port, nb_scans, integration, nb_points, mode, debug):
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(level=level)
    em = Em2(host, port)
    mode = mode.upper()
    if mode == 'SOFTWARE':
        repetitions = 1
        nb_starts = nb_points
    elif mode in ['HARDWARE', 'GATE']:
        repetitions = nb_points,
        nb_starts = 1
    else:
        raise ValueError('mode not allowed')
    result = True
    bad_test = 0
    for i in range(nb_scans):
        if not result:
            log.info('Wait 2 seconds to recover system')
            time.sleep(2)
        log.info('Start scan %d: Integration %f, Repetitions %d, Starts %d, '
                 'Mode %s', i, integration, repetitions, nb_starts, mode)
        result = test_scan(em, integration, repetitions, nb_starts, mode)
        if not result:
            bad_test += 1
    log.info('-'*80)
    log.info('Failures %d of %d', bad_test, nb_scans)


if __name__ == '__main__':
    main()
