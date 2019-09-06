
from beer.ingest.size_parser import parse_size


def test_kegs():
    assert parse_size('1/6 KEG') == [1, '1/6 keg', 'keg']
    assert parse_size('1/4 PET KEG') == [1, '1/4 keg', 'keg']
    assert parse_size('1/2 KEG') == [1, '1/2 keg', 'keg']
    assert parse_size('1/2 KEG ($20 DEP)') == [1, '1/2 keg', 'keg']
    assert parse_size('10 LITER KEG') == [1, '10 liter keg', 'keg']
    assert parse_size('50 LTR KEG') == [1, '50 liter keg', 'keg']
    assert parse_size('30 LTR PET KEG') == [1, '30 liter keg', 'keg']


def test_bottle_groups():
    assert parse_size('12/12 OZ BTL') == [12, '12 oz', 'bottle']
    assert parse_size('12/11.2 OZ BTL') == [12, '11.2 oz', 'bottle']
    assert parse_size('24/12 OZ BTL *4/6 PACK*') == [24, '12 oz', 'bottle']
    assert parse_size('6/750 ML BTL') == [6, '750 ml', 'bottle']
    assert parse_size('8/16 OZ BTL/ALUM') == [8, '16 oz', 'bottle']


def test_can_groups():
    assert parse_size('12/10 OZ CAN') == [12, '10 oz', 'can']
    assert parse_size('10/14.9 OZ CAN') == [10, '14.9 oz', 'can']
    assert parse_size('24/16 OZ ALUM') == [24, '16 oz', 'can']
    assert parse_size('12/12 OZ NR') == [12, '12 oz', 'bottle']


def test_bottle_single():
    assert parse_size('11.2 OZ BTL') == [1, '11.2 oz', 'bottle']
    assert parse_size('375 ML BTL') == [1, '375 ml', 'bottle']
    assert parse_size('3 LITER BTL') == [1, '3 liter', 'bottle']
    assert parse_size('40 OZ BTL') == [1, '40 oz', 'bottle']


def test_can_singles():
    assert parse_size('12 OZ CAN') == [1, '12 oz', 'can']
    assert parse_size('19.2 OZ CAN') == [1, '19.2 oz', 'can']
    assert parse_size('3 LITER CAN') == [1, '3 liter', 'can']
    assert parse_size('440 ML CAN') == [1, '440 ml', 'can']


def test_other_groups():
    assert parse_size('24/10 OZ POUCH') == [24, '10 oz', 'pouch']
    assert parse_size('30/100 ML POUCH') == [30, '100 ml', 'pouch']
    assert parse_size('32/64 OZ GROWLER') == [32, '64 oz', 'growler']
    assert parse_size('24/16 OZ PLASTIC') == [24, '16 oz', 'plastic']
    assert parse_size('4/16.9 OZ PLASTIC') == [4, '16.9 oz', 'plastic']


def test_other_singles():
    assert parse_size('10 LITER BAG') == [1, '10 liter', 'bag']
    assert parse_size('23.5 OZ JAR') == [1, '23.5 oz', 'jar']
    assert parse_size('32 OZ GROWLER *ONLY*') == [1, '32 oz', 'growler']
    assert parse_size('12 OZ HOT CIDER') == [1, '12 oz', 'hot cider']
    assert parse_size('16 OZ SLUSHEE') == [1, '16 oz', 'slushee']
    assert parse_size('EACH') == [1, 'each', 'each']