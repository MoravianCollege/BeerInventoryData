
import re

containers = {'KEG': 'keg', 'BTL': 'bottle', 'CAN': 'can', 'ALUM': 'can', 'POUCH': 'pouch', "GROWLER": 'growler',
              'BAG': 'bag', 'JAR': 'jar', 'PLASTIC': 'plastic', 'SLUSHEE': 'slushee', 'HOT CIDER': 'hot cider',
              'NR': 'bottle'}
units = {'OZ': 'oz', 'LITER': 'liter', 'LTR': 'liter', 'ML': 'ml'}


def parse_size(tanczos_size):

    std_keg_re = re.compile(r'(\d/\d).*KEG')
    std_keg_match = std_keg_re.match(tanczos_size)
    if std_keg_match:
        size = std_keg_match.group(1)
        return [1, '{} keg'.format(size), 'keg']

    metric_keg_re = re.compile(r'(\d+)\s+(LITER|LTR|OZ).*KEG')
    metric_keg_match = metric_keg_re.match(tanczos_size)
    if metric_keg_match:
        size = metric_keg_match.group(1)
        return [1, '{} liter keg'.format(size), 'keg']

    containers_re = '|'.join(containers.keys())
    units_re = '|'.join(units.keys())

    groups_re = re.compile(r'(\d+)/(\d+(.\d+)?)\s+({})\s+({})'.format(units_re, containers_re))
    groups_match = groups_re.match(tanczos_size)
    if groups_match:
        quantity = int(groups_match.group(1))
        size = groups_match.group(2)
        unit = units[groups_match.group(4)]
        container = containers[groups_match.group(5)]
        return [quantity, '{} {}'.format(size, unit), container]

    single_re = re.compile(r'(\d+(.\d+)?)\s+({})\s+({})'.format(units_re, containers_re))
    single_match = single_re.match(tanczos_size)
    if single_match:
        size = single_match.group(1)
        unit = units[single_match.group(3)]
        container = containers[single_match.group(4)]
        return [1, '{} {}'.format(size, unit), container]

    if tanczos_size.startswith('EACH'):
        return [1, 'each', 'each']
