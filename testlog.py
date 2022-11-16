from ast import Try
import logging


logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
	format='[%(asctime)s %(levelname)-8s] %(message)s',
	datefmt='%Y%m%d %H:%M:%S',
	)
logger = logging.getLogger('WANG')

def circle_area(x):
    if x <= 0:
        return ValueError
    else:
        return x**2

try:
    area = circle_area(-2)
except ValueError:
    logger.info('Error: wrong input value')